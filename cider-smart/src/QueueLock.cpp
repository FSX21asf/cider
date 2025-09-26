#include "QueueLock.h"
#include <endian.h>
#include "Common.h"
#include "DSM.h"
#include "GlobalAddress.h"

uint64_t possible_write_handover_num[MAX_APP_THREAD];

DSM* MCS_LOCK::dsm;
DSM* MCS_WC_LOCK::dsm;
DSM* MCS_WC_VLOCK::dsm;
thread_local MCS_LOCK_ALLOCATOR mla;
void MCS_LOCK::LOCK_INIT_DSM(DSM* x) {
    dsm = x;
}
void MCS_WC_LOCK::LOCK_INIT_DSM(DSM* x) {
    dsm = x;
}
void MCS_WC_VLOCK::LOCK_INIT_DSM(DSM* x) {
    dsm = x;
}
void MCS_LOCK::LOCK_INIT_ALLOC() {
    mla.INIT_DSM(dsm);
}
void MCS_WC_LOCK::LOCK_INIT_ALLOC() {
    mla.INIT_DSM(dsm);
}
void MCS_WC_VLOCK::LOCK_INIT_ALLOC() {
    mla.INIT_DSM(dsm);
}
GlobalAddress findLockByteAddrByLockBuffer(LOCK_BUFFER_CONTENT* lb) {
    return GlobalAddress{lb->nodeID, define::kAPPThreadLockBytesOffset +
                                         lb->appID * sizeof(uint64_t)};
}
void MCS_LOCK_ALLOCATOR::INIT_DSM(DSM* x) {
    if (dsm && dsm->is_register())
        return;
    dsm = x;
    assert(dsm && dsm->is_register());
    flFront =
        (uint64_t*)(dsm->getRDMADirOffset() + define::kQueueLockBufferOffset +
                    dsm->getMyThreadID() *
                        define::kQueueLockBufferSizePerThread);
    for (int i = 0; i < (int)(define::kQueueLockBufferCnt)-1; i++) {
        *(flFront + i) = (uint64_t)(flFront + i + 1);
    }
    flEnd = flFront + (define::kQueueLockBufferCnt - 1);
    *flEnd = 0;
}
uint64_t* MCS_LOCK_ALLOCATOR::allocLockBuffer() {
    uint64_t* ret = (uint64_t*)*flFront;
    assert(ret);
    *flFront = *ret;
    *ret = 0;
    return ret;
}
void MCS_LOCK_ALLOCATOR::deAllocLockBuffer(uint64_t* addr) {
    *flEnd = (uint64_t)addr;
    flEnd = addr;
    *addr = 0;
}

MCS_LOCK::MCS_LOCK(GlobalAddress addr) {
    assert(dsm && dsm->is_register());
    MNlock = addr;
}
MCS_LOCK::~MCS_LOCK() {}
LOCKBYTES_CONTENT* MCS_LOCK::getLockBytes() {
    return (LOCKBYTES_CONTENT*)(dsm->getRDMADirOffset() +
                                define::kAPPThreadLockBytesOffset +
                                dsm->getMyThreadID() * sizeof(uint64_t));
}
void MCS_LOCK::lock(CoroContext* ctx, int coro_id) {
    lockBuffer = (LOCK_BUFFER_CONTENT*)mla.allocLockBuffer();
    assert(lockBuffer);
    uint64_t* tmp;
    volatile LOCKBYTES_CONTENT* lockByte = getLockBytes();
MCS_LOCK_RESTART:
    lockBuffer->val = 0;
    lockBuffer->valid = 1;
    lockByte->val = 0;
    lockByte->valid = 1;

#ifdef USE_FT
    uint64_t cmpv[2] = {0}, cmpm[2] = {0}, swpm[2] = {~0ull, 0},
             swpv[2] = {(dsm->localToGlobal(lockBuffer)).to_uint64(), 0};
    uint64_t readCnt = 0;
    tmp = (dsm->get_rbuf(coro_id)).get_cas128_buffer();
    if (!dsm->cas128_mask_sync(MNlock, cmpv, swpv, tmp, cmpm, swpm, ctx)) {
        assert(0);
    }
    prev = tmp[1];

#else
    tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    if (!dsm->cas_mask_sync(MNlock, 0ull,
                            (dsm->localToGlobal(lockBuffer)).to_uint64(), tmp,
                            0ull, ~(0ull), ctx)) {
        assert(0);  // GAS must success
    }
#endif
    if (*tmp == 0) {  // no followee
        return;
    }
    possible_write_handover_num[dsm->getMyThreadID()]++;
    LOCK_BUFFER_CONTENT lc{dsm->getMyNodeID(), dsm->getMyThreadID(), 0, 1, 0};
    GlobalAddress followee = *tmp;
    tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    if (!dsm->cas_mask_sync(followee, VALID_BIT, lc.to_uint64(), tmp, VALID_BIT,
                            GLOBAL_ADDRESS_SET_MASK, ctx)) {
        // tell followee to unlock itself
        assert(0);
    }
    // wait for unlock
#ifdef USE_FT
    timespec s, e;
    clock_gettime(CLOCK_REALTIME, &s);
    GlobalAddress MNlock_high = MNlock;
    MNlock_high.offset += 8;
    while (!lockByte->info) {
        clock_gettime(CLOCK_REALTIME, &e);
        uint64_t microseconds = (e.tv_sec - s.tv_sec) * 1000000 +
                                (double)(e.tv_nsec - s.tv_nsec) / 1000;
        if (microseconds > leaseTime) {
            tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
            dsm->read_sync((char*)tmp, MNlock_high, 8, ctx);
            if (*tmp == prev) {
                readCnt++;
                s = e;
            } else if (*tmp - prev <= 4000ull) {
                readCnt = 0;
                prev = *tmp;
                s = e;
            } else {
                assert(*tmp - prev >= 800000ull);
                usleep(leaseTime);
                goto MCS_LOCK_RESTART;
            }
            if (readCnt > 3) {
                dsm->fault_report(MNlock, prev);
            }
        }
    }
#endif
    while (!lockByte->info) {
    }
    lockByte->valid = 0;
}

void MCS_LOCK::unlock(CoroContext* ctx, int coro_id) {
    uint64_t* tmp;
    if (lockBuffer->isUnSet()) {
        tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
        if (dsm->cas_sync(MNlock, (dsm->localToGlobal(lockBuffer)).to_uint64(),
                          0, tmp, ctx)) {  // no follower
            lockBuffer->val = 0;
            mla.deAllocLockBuffer((uint64_t*)lockBuffer);
#ifdef USE_FT
            GlobalAddress MNlock_high = MNlock;
            MNlock_high.offset += 8;
            tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
            dsm->faa_boundary(MNlock_high, 1, tmp, 63, false, ctx);
#endif
            return;
        }
        // rare case: wait for follower
#ifdef USE_FT
        timespec s, e;
        clock_gettime(CLOCK_REALTIME, &s);
        GlobalAddress MNlock_high = MNlock;
        MNlock_high.offset += 8;
        uint64_t readCnt = 0;
        while (lockBuffer->isUnSet()) {
            clock_gettime(CLOCK_REALTIME, &e);
            uint64_t microseconds = (e.tv_sec - s.tv_sec) * 1000000 +
                                    (double)(e.tv_nsec - s.tv_nsec) / 1000;
            if (microseconds > leaseTime) {
                tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
                dsm->read_sync((char*)tmp, MNlock_high, 8, ctx);
                if (*tmp == prev) {
                    readCnt++;
                    s = e;
                } else if (*tmp - prev <= 4000ull) {
                    readCnt = 0;
                    prev = *tmp;
                    s = e;
                } else {
                    assert(*tmp - prev >= 800000ull);
                    mla.deAllocLockBuffer((uint64_t*)lockBuffer);
                    return;
                }
                if (readCnt > 3) {
                    dsm->fault_report(MNlock, prev);
                }
            }
        }
#endif
        while (lockBuffer->isUnSet())
            ;
    }
    // unlock follower
    GlobalAddress follower = findLockByteAddrByLockBuffer(lockBuffer);
    lockBuffer->val = 0;
    LOCKBYTES_CONTENT lbc;
    lbc.info = 1;
    tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
#ifdef USE_FTERR
    if (rand() % 10000 == 1) {
        printf("%lx fault\n", MNlock.to_uint64());
        mla.deAllocLockBuffer((uint64_t*)lockBuffer);
        return;
    }
#endif
    if (!dsm->cas_mask_sync(follower, VALID_BIT, lbc.to_uint64(), tmp,
                            VALID_BIT, GLOBAL_ADDRESS_SET_MASK, ctx)) {
        assert(0);
    }
    mla.deAllocLockBuffer((uint64_t*)lockBuffer);
#ifdef USE_FT
    GlobalAddress MNlock_high = MNlock;
    MNlock_high.offset += 8;
    tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    dsm->faa_boundary(MNlock_high, 1, tmp, 63, false, ctx);
#endif
}

MCS_WC_LOCK::MCS_WC_LOCK(GlobalAddress addr) {
    assert(dsm && dsm->is_register());
    MNlock = addr;
    lockBuffer = (LOCK_BUFFER_CONTENT*)mla.allocLockBuffer();
    assert(lockBuffer);
}
MCS_WC_LOCK::~MCS_WC_LOCK() {
    mla.deAllocLockBuffer((uint64_t*)lockBuffer);
}
LOCKBYTES_CONTENT* MCS_WC_LOCK::getLockBytes() {
    return (LOCKBYTES_CONTENT*)(dsm->getRDMADirOffset() +
                                define::kAPPThreadLockBytesOffset +
                                dsm->getMyThreadID() * sizeof(uint64_t));
}

uint64_t MCS_WC_LOCK::lock(bool Barrier,
                           CoroContext* ctx,
                           int coro_id) {  // pass info in return value,get lock
                                           // return 0,skip return 1,2...
    isBarrier = Barrier;
    timespec s, e;
    GlobalAddress MNlock_high = MNlock;
    MNlock_high.offset += 8;
    uint64_t readCnt = 0;
MCS_WC_LOCK_RESTART:
    shortpath = false;
    uint64_t* tmp;
    lockBuffer->val = 0;
    lockBuffer->valid = 1;
    volatile LOCKBYTES_CONTENT* lockBytes = getLockBytes();
    lockBytes->val = 0;
    lockBytes->valid = 1;
    GlobalAddress lbfg = (dsm->localToGlobal(lockBuffer));

    MNLOCK_CONTENT mc = {lbfg.offset, dsm->getMyThreadID(), lbfg.nodeID,
                         isBarrier};
    LOCK_BUFFER_CONTENT lc{dsm->getMyNodeID(), dsm->getMyThreadID(), 0, 1, 0};
#ifdef USE_FT
    clock_gettime(CLOCK_REALTIME, &s);
    uint64_t cmpv[2] = {0, 0}, cmpm[2] = {BARRIER_BIT, 0}, swpm[2] = {~0ull, 0},
             swpv[2] = {mc.to_uint64(), 0};
    readCnt = 0;
    tmp = (dsm->get_rbuf(coro_id)).get_cas128_buffer();
    while (!dsm->cas128_mask_sync(MNlock, cmpv, swpv, tmp, cmpm, swpm, ctx)) {
        // retry TODO
        prev = tmp[1];
        usleep(200);
        clock_gettime(CLOCK_REALTIME, &e);
        uint64_t microseconds = (e.tv_sec - s.tv_sec) * 1000000 +
                                (double)(e.tv_nsec - s.tv_nsec) / 1000;
        if (microseconds > leaseTime) {
            uint64_t* tmp2 = (dsm->get_rbuf(coro_id)).get_cas_buffer();
            dsm->read_sync((char*)tmp2, MNlock_high, 8, ctx);
            if (*tmp2 == prev) {
                readCnt++;
                s = e;
            } else if (*tmp2 - prev <= 4000ull) {
                readCnt = 0;
                prev = *tmp2;
                s = e;
            } else {
                assert(*tmp2 - prev >= 800000ull);
                usleep(leaseTime);
                goto MCS_WC_LOCK_RESTART;
            }
            if (readCnt > 3) {
                dsm->fault_report(MNlock, prev);
            }
        }
    }
    prev = tmp[1];

#else
    tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    while (!dsm->cas_mask_sync(MNlock, 0ull, mc.to_uint64(), tmp, BARRIER_BIT,
                               ~(0ull), ctx)) {
        // RETRY TODO
        usleep(200);
    }
#endif

    if (*tmp == 0) {  // no followee
        shortpath = true;
        return 0;
    }

    GlobalAddress followee = GlobalAddress{((MNLOCK_CONTENT*)tmp)->nodeID,
                                           ((MNLOCK_CONTENT*)tmp)->offset};

    tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    if (!dsm->cas_mask_sync(followee, VALID_BIT, lc.to_uint64(), tmp, VALID_BIT,
                            GLOBAL_ADDRESS_SET_MASK, ctx)) {
        // tell followee to unlock itself
        assert(0);
    }
    // wait for unlock

#ifdef USE_FT
    clock_gettime(CLOCK_REALTIME, &s);
    readCnt = 0;
    while (!lockBytes->info) {
        clock_gettime(CLOCK_REALTIME, &e);
        uint64_t microseconds = (e.tv_sec - s.tv_sec) * 1000000 +
                                (double)(e.tv_nsec - s.tv_nsec) / 1000;
        if (microseconds > leaseTime) {
            tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
            dsm->read_sync((char*)tmp, MNlock_high, 8, ctx);
            if (*tmp == prev) {
                readCnt++;
                s = e;
            } else if (*tmp - prev <= 4000ull) {
                readCnt = 0;
                prev = *tmp;
                s = e;
            } else {
                assert(*tmp - prev >= 800000ull);
                usleep(leaseTime);
                goto MCS_WC_LOCK_RESTART;
            }
            if (readCnt > 3) {
                dsm->fault_report(MNlock, prev);
            }
        }
    }
#endif
    while (!lockBytes->info)
        ;
    lockBytes->valid = 0;
    switch (lockBytes->info) {
        case 1: {  // normal , can be write compact leader
            if (lockBuffer->isUnSet()) {
                return 0;
            }
            tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
            dsm->read_sync((char*)tmp, MNlock, 8, ctx);
            MNLOCK_CONTENT writer = *(MNLOCK_CONTENT*)tmp;
            GlobalAddress writerLockBytesAddr = GlobalAddress{
                writer.nodeID, define::kAPPThreadLockBytesOffset +
                                   writer.appID * (sizeof(uint64_t))};
            lockBytes->val = 0;
            lockBytes->valid = 1;

            // unlock writer
            LOCKBYTES_CONTENT lbc;
            lbc.info = 2;
            lbc.appID = dsm->getMyThreadID();
            lbc.nodeID = dsm->getMyNodeID();
            tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
            if (!dsm->cas_mask_sync(writerLockBytesAddr, VALID_BIT,
                                    lbc.to_uint64(), tmp, VALID_BIT,
                                    GLOBAL_ADDRESS_SET_MASK, ctx)) {
                assert(0);
            }
#ifdef USE_FT
            clock_gettime(CLOCK_REALTIME, &s);
            while (!lockBytes->info) {
                clock_gettime(CLOCK_REALTIME, &e);
                uint64_t microseconds = (e.tv_sec - s.tv_sec) * 1000000 +
                                        (double)(e.tv_nsec - s.tv_nsec) / 1000;
                if (microseconds > leaseTime) {
                    tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
                    dsm->read_sync((char*)tmp, MNlock_high, 8, ctx);
                    if (*tmp == prev) {
                        readCnt++;
                        s = e;
                    } else if (*tmp - prev <= 4000ull) {
                        readCnt = 0;
                        prev = *tmp;
                        s = e;
                    } else {
                        assert(*tmp - prev >= 800000ull);
                        usleep(leaseTime);
                        goto MCS_WC_LOCK_RESTART;
                    }
                    if (readCnt > 3) {
                        dsm->fault_report(MNlock, prev);
                    }
                }
            }
#endif
            while (!lockBytes->info)
                ;  // spin wait for writer finish
            lockBytes->valid = 0;
            GlobalAddress originNextAddr =
                findLockByteAddrByLockBuffer(lockBuffer);
            lbc.info = 3;
            lbc.msg = lockBytes->msg;
            tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
            if (!dsm->cas_mask_sync(originNextAddr, VALID_BIT, lbc.to_uint64(),
                                    tmp, VALID_BIT, GLOBAL_ADDRESS_SET_MASK,
                                    ctx)) {
                assert(0);
            }
            return lockBytes->msg;  // write combine successfully
        }
        case 2: {
            return 0;  // as write compact writer
        }
        case 3: {  // as  write compact follower
            if (lockBuffer->isUnSet()) {
#ifdef USE_FT
                clock_gettime(CLOCK_REALTIME, &s);
                uint64_t readCnt = 0;
                while (lockBuffer->isUnSet()) {
                    clock_gettime(CLOCK_REALTIME, &e);
                    uint64_t microseconds =
                        (e.tv_sec - s.tv_sec) * 1000000 +
                        (double)(e.tv_nsec - s.tv_nsec) / 1000;
                    if (microseconds > leaseTime) {
                        tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
                        dsm->read_sync((char*)tmp, MNlock_high, 8, ctx);
                        if (*tmp == prev) {
                            readCnt++;
                            s = e;
                        } else if (*tmp - prev <= 4000ull) {
                            readCnt = 0;
                            prev = *tmp;
                            s = e;
                        } else {
                            assert(*tmp - prev >= 800000ull);
                            return lockBytes->msg;
                        }
                        if (readCnt > 3) {
                            dsm->fault_report(MNlock, prev);
                        }
                    }
                }
#endif
                while (lockBuffer->isUnSet())
                    ;
            }
#ifdef USE_FTERR
            if (rand() % 10000 == 1) {
                printf("%lx fault\n", MNlock.to_uint64());
                return lockBytes->msg;
            }
#endif
            LOCK_BUFFER_CONTENT originNext = *lockBuffer;
            GlobalAddress originNextAddr =
                findLockByteAddrByLockBuffer(lockBuffer);
            LOCKBYTES_CONTENT lbc;
            lbc.info = 3;
            lbc.msg = lockBytes->msg;
            uint64_t* tmp2 = (dsm->get_rbuf(coro_id)).get_cas_buffer();
            if (!dsm->cas_mask_sync(originNextAddr, VALID_BIT, lbc.to_uint64(),
                                    tmp2, VALID_BIT, GLOBAL_ADDRESS_SET_MASK,
                                    ctx)) {
                assert(0);
            }

            memset(lockBuffer, 0, sizeof(*lockBuffer));
            return lockBytes->msg;
        }
        default: {
            assert(0);
        }
    }
    assert(0);
}

uint64_t MCS_WC_LOCK::unlock(CoroContext* ctx, int coro_id, uint64_t msg) {
    uint64_t* tmp;
    GlobalAddress MNlock_high = MNlock;
    MNlock_high.offset += 8;
    uint64_t readCnt = 0;
    timespec s, e;
    volatile LOCKBYTES_CONTENT* lockBytes = getLockBytes();
    if (lockBytes->info == 2) {  // writer
        lockBytes->info = 0;
        lockBytes->valid = 1;
        GlobalAddress CooLockBytesAddr = GlobalAddress{
            lockBytes->nodeID, define::kAPPThreadLockBytesOffset +
                                   lockBytes->appID * (sizeof(uint64_t))};
        LOCKBYTES_CONTENT lbc;
        lbc.msg = msg;
        lbc.info = 1;
        tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
        if (!dsm->cas_mask_sync(CooLockBytesAddr, VALID_BIT, lbc.to_uint64(),
                                tmp, VALID_BIT, GLOBAL_ADDRESS_SET_MASK, ctx)) {
            assert(0);
        }
        // wait until inform for fault tolerance
#ifdef USE_FT
        clock_gettime(CLOCK_REALTIME, &s);
        readCnt = 0;
        while (!lockBytes->info) {
            clock_gettime(CLOCK_REALTIME, &e);
            uint64_t microseconds = (e.tv_sec - s.tv_sec) * 1000000 +
                                    (double)(e.tv_nsec - s.tv_nsec) / 1000;
            if (microseconds > leaseTime) {
                tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
                dsm->read_sync((char*)tmp, MNlock_high, 8, ctx);
                if (*tmp == prev) {
                    readCnt++;
                    s = e;
                } else if (*tmp - prev <= 4000ull) {
                    readCnt = 0;
                    prev = *tmp;
                    s = e;
                } else {
                    assert(*tmp - prev >= 800000ull);
                    return 0;
                }
                if (readCnt > 3) {
                    dsm->fault_report(MNlock, prev);
                }
            }
        }
#endif

        while (!lockBytes->info)
            ;
    }
    bool sendfaa = false;
    if (lockBuffer->isUnSet()) {
        tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
        GlobalAddress lbfg = (dsm->localToGlobal(lockBuffer));
        MNLOCK_CONTENT mc = {lbfg.offset, dsm->getMyThreadID(), lbfg.nodeID,
                             isBarrier};
        if (dsm->cas_sync(MNlock, mc.to_uint64(), 0, tmp,
                          ctx)) {  // no follower
            lockBuffer->valid = 0;
#ifdef USE_FT
            GlobalAddress MNlock_high = MNlock;
            MNlock_high.offset += 8;
            tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
            dsm->faa_boundary_sync(MNlock_high, 1, tmp, 63, ctx);
#endif
            return shortpath;
        }

        // rare case: wait for follower
#ifdef USE_FT
        clock_gettime(CLOCK_REALTIME, &s);
        readCnt = 0;
        while (lockBuffer->isUnSet()) {
            clock_gettime(CLOCK_REALTIME, &e);
            uint64_t microseconds = (e.tv_sec - s.tv_sec) * 1000000 +
                                    (double)(e.tv_nsec - s.tv_nsec) / 1000;
            if (microseconds > leaseTime) {
                tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
                dsm->read_sync((char*)tmp, MNlock_high, 8, ctx);
                if (*tmp == prev) {
                    readCnt++;
                    s = e;
                } else if (*tmp - prev <= 4000ull) {
                    readCnt = 0;
                    prev = *tmp;
                    s = e;
                } else {
                    assert(*tmp - prev >= 800000ull);
                    return 0;
                }
                if (readCnt > 3) {
                    dsm->fault_report(MNlock, prev);
                }
            }
        }
#endif

        while (lockBuffer->isUnSet())
            ;
    }
#ifdef USE_FTERR
    if (rand() % 10000 == 1) {
        printf("%lx fault\n", MNlock.to_uint64());
        return 0;
    }
#endif
    // unlock follower
    GlobalAddress followerLockByte = findLockByteAddrByLockBuffer(lockBuffer);
    lockBuffer->val = 0;
    LOCKBYTES_CONTENT lbc;
    lbc.info = 1;
    lbc.msg = 0;
    tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    dsm->cas_mask(followerLockByte, VALID_BIT, lbc.to_uint64(), tmp, VALID_BIT,
                  GLOBAL_ADDRESS_SET_MASK, true, ctx);
#ifdef USE_FT
    if (!sendfaa) {
        GlobalAddress MNlock_high = MNlock;
        MNlock_high.offset += 8;
        tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
        dsm->faa_boundary(MNlock_high, 1, tmp, 63, true, ctx);
        dsm->poll_rdma_cq(1);
    }
#endif
    dsm->poll_rdma_cq(1);
    return 0;
}

// MCS_WC_VLOCK BEGIN
MCS_WC_VLOCK::MCS_WC_VLOCK(GlobalAddress addr, GlobalAddress dpaddr) {
    assert(dsm && dsm->is_register());
    MNlock = addr;
    dptr = dpaddr;
    lockBuffer = (LOCK_BUFFER_CONTENT*)mla.allocLockBuffer();
    assert(lockBuffer);
}
MCS_WC_VLOCK::~MCS_WC_VLOCK() {
    mla.deAllocLockBuffer((uint64_t*)lockBuffer);
}
LOCKBYTES_CONTENT* MCS_WC_VLOCK::getLockBytes() {
    return (LOCKBYTES_CONTENT*)(dsm->getRDMADirOffset() +
                                define::kAPPThreadLockBytesOffset +
                                dsm->getMyThreadID() * sizeof(uint64_t));
}

uint64_t MCS_WC_VLOCK::lock(bool isdelete,
                            uint64_t dptrVersion,
                            CoroContext* ctx,
                            int coro_id) {  // pass info in return value,get
                                            // lock return 0,skip return 1,2...
    version = dptrVersion;
    isDelete = isdelete;
    uint64_t newVersion = ((version + isDelete) & (0xf));
    timespec s, e;
    GlobalAddress MNlock_high = MNlock;
    MNlock_high.offset += 8;
    uint64_t readCnt = 0;
MCS_WC_VLOCK_RESTART:
    shortpath = false;
    uint64_t* tmp;
    lockBuffer->val = 0;
    lockBuffer->valid = 1;
    volatile LOCKBYTES_CONTENT* lockBytes = getLockBytes();
    lockBytes->val = 0;
    lockBytes->valid = 1;
    GlobalAddress lbfg = (dsm->localToGlobal(lockBuffer));

    MNVLOCK_CONTENT mc = {lbfg.offset, dsm->getMyThreadID(), lbfg.nodeID,
                          newVersion};
    MNVLOCK_CONTENT preVer = {0, 0, 0, version};
    LOCK_BUFFER_CONTENT lc{dsm->getMyNodeID(), dsm->getMyThreadID(), 0, 1, 0};
#ifdef USE_FT
    clock_gettime(CLOCK_REALTIME, &s);
    uint64_t cmpv[2] = {preVer.to_uint64(), 0}, cmpm[2] = {VERSION_BIT, 0},
             swpm[2] = {~0ull, 0}, swpv[2] = {mc.to_uint64(), 0};
    readCnt = 0;
    tmp = (dsm->get_rbuf(coro_id)).get_cas128_buffer();
    while (!dsm->cas128_mask_sync(MNlock, cmpv, swpv, tmp, cmpm, swpm, ctx)) {
        return 3;
    }
    prev = tmp[1];

#else
    tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    while (!dsm->cas_mask_sync(MNlock, preVer.to_uint64(), mc.to_uint64(), tmp,
                               BARRIER_BIT, ~(0ull), ctx)) {
        // RETRY TODO
        return 3;
    }
#endif

    if (*tmp == 0) {  // no followee
        shortpath = true;
        return 0;
    }
    GlobalAddress followee = GlobalAddress{((MNVLOCK_CONTENT*)tmp)->nodeID,
                                           ((MNVLOCK_CONTENT*)tmp)->offset};

    tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    if (!dsm->cas_mask_sync(followee, VALID_BIT, lc.to_uint64(), tmp, VALID_BIT,
                            GLOBAL_ADDRESS_SET_MASK, ctx)) {
        // tell followee to unlock itself
        printf("followee error,wait for FT\n");
    }
    // wait for unlock

#ifdef USE_FT
    clock_gettime(CLOCK_REALTIME, &s);
    readCnt = 0;
    while (!lockBytes->info) {
        clock_gettime(CLOCK_REALTIME, &e);
        uint64_t microseconds = (e.tv_sec - s.tv_sec) * 1000000 +
                                (double)(e.tv_nsec - s.tv_nsec) / 1000;
        if (microseconds > leaseTime) {
            tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
            dsm->read_sync((char*)tmp, MNlock_high, 8, ctx);
            if (*tmp == prev) {
                readCnt++;
                s = e;
            } else if (*tmp - prev <= 4000ull) {
                readCnt = 0;
                prev = *tmp;
                s = e;
            } else {
                assert(*tmp - prev >= 800000ull);
                usleep(leaseTime);
                goto MCS_WC_VLOCK_RESTART;
            }
            if (readCnt > 3) {
                dsm->fault_reportV(MNlock, dptr, prev);
            }
        }
    }
#endif
    while (!lockBytes->info)
        ;
    lockBytes->valid = 0;
    switch (lockBytes->info) {
        case 1: {  // normal , can be write compact leader
            if (lockBuffer->isUnSet()) {
                return 0;
            }
            tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
            dsm->read_sync((char*)tmp, MNlock, 8, ctx);
            MNVLOCK_CONTENT writer = *(MNVLOCK_CONTENT*)tmp;
            GlobalAddress writerLockBytesAddr = GlobalAddress{
                writer.nodeID, define::kAPPThreadLockBytesOffset +
                                   writer.appID * (sizeof(uint64_t))};
            lockBytes->val = 0;
            lockBytes->valid = 1;

            // unlock writer
            LOCKBYTES_CONTENT lbc;
            lbc.info = 2;
            lbc.appID = dsm->getMyThreadID();
            lbc.nodeID = dsm->getMyNodeID();
            tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
            if (!dsm->cas_mask_sync(writerLockBytesAddr, VALID_BIT,
                                    lbc.to_uint64(), tmp, VALID_BIT,
                                    GLOBAL_ADDRESS_SET_MASK, ctx)) {
                printf("unlock writer error,stop wc\n");
                return 0;
            }
#ifdef USE_FT
            clock_gettime(CLOCK_REALTIME, &s);
            while (!lockBytes->info) {
                clock_gettime(CLOCK_REALTIME, &e);
                uint64_t microseconds = (e.tv_sec - s.tv_sec) * 1000000 +
                                        (double)(e.tv_nsec - s.tv_nsec) / 1000;
                if (microseconds > leaseTime) {
                    tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
                    dsm->read_sync((char*)tmp, MNlock_high, 8, ctx);
                    if (*tmp == prev) {
                        readCnt++;
                        s = e;
                    } else if (*tmp - prev <= 4000ull) {
                        readCnt = 0;
                        prev = *tmp;
                        s = e;
                    } else {
                        assert(*tmp - prev >= 800000ull);
                        usleep(leaseTime);
                        goto MCS_WC_VLOCK_RESTART;
                    }
                    if (readCnt > 3) {
                        dsm->fault_reportV(MNlock, dptr, prev);
                    }
                }
            }
#endif
            while (!lockBytes->info)
                ;  // spin wait for writer finish
            lockBytes->valid = 0;
            GlobalAddress originNextAddr =
                findLockByteAddrByLockBuffer(lockBuffer);
            lbc.info = 3;
            lbc.msg = lockBytes->msg;
            tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
            if (!dsm->cas_mask_sync(originNextAddr, VALID_BIT, lbc.to_uint64(),
                                    tmp, VALID_BIT, GLOBAL_ADDRESS_SET_MASK,
                                    ctx)) {
                printf("follower error, handle by FT\n");
            }
            return lockBytes->msg;  // write combine successfully
        }
        case 2: {
            return 0;  // as write compact writer
        }
        case 3: {  // as  write compact follower
            if (lockBuffer->isUnSet()) {
#ifdef USE_FT
                clock_gettime(CLOCK_REALTIME, &s);
                uint64_t readCnt = 0;
                while (lockBuffer->isUnSet()) {
                    clock_gettime(CLOCK_REALTIME, &e);
                    uint64_t microseconds =
                        (e.tv_sec - s.tv_sec) * 1000000 +
                        (double)(e.tv_nsec - s.tv_nsec) / 1000;
                    if (microseconds > leaseTime) {
                        tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
                        dsm->read_sync((char*)tmp, MNlock_high, 8, ctx);
                        if (*tmp == prev) {
                            readCnt++;
                            s = e;
                        } else if (*tmp - prev <= 4000ull) {
                            readCnt = 0;
                            prev = *tmp;
                            s = e;
                        } else {
                            assert(*tmp - prev >= 800000ull);
                            return lockBytes->msg;
                        }
                        if (readCnt > 3) {
                            dsm->fault_reportV(MNlock, dptr, prev);
                        }
                    }
                }
#endif
                while (lockBuffer->isUnSet())
                    ;
            }
#ifdef USE_FTERR
            if (rand() % 10000 == 1) {
                printf("%lx fault\n", MNlock.to_uint64());
                return lockBytes->msg;
            }
#endif
            LOCK_BUFFER_CONTENT originNext = *lockBuffer;
            GlobalAddress originNextAddr =
                findLockByteAddrByLockBuffer(lockBuffer);
            LOCKBYTES_CONTENT lbc;
            lbc.info = 3;
            lbc.msg = lockBytes->msg;
            uint64_t* tmp2 = (dsm->get_rbuf(coro_id)).get_cas_buffer();
            if (!dsm->cas_mask_sync(originNextAddr, VALID_BIT, lbc.to_uint64(),
                                    tmp2, VALID_BIT, GLOBAL_ADDRESS_SET_MASK,
                                    ctx)) {
                printf("can not update next\n");
            }

            memset(lockBuffer, 0, sizeof(*lockBuffer));
            return lockBytes->msg;
        }
        default: {
            assert(0);
        }
    }
    assert(0);
}

uint64_t MCS_WC_VLOCK::unlock(CoroContext* ctx, int coro_id, uint64_t msg) {
    uint64_t* tmp;
    GlobalAddress MNlock_high = MNlock;
    MNlock_high.offset += 8;
    uint64_t readCnt = 0;
    timespec s, e;
    volatile LOCKBYTES_CONTENT* lockBytes = getLockBytes();
    if (lockBytes->info == 2) {  // writer
        lockBytes->info = 0;
        lockBytes->valid = 1;
        GlobalAddress CooLockBytesAddr = GlobalAddress{
            lockBytes->nodeID, define::kAPPThreadLockBytesOffset +
                                   lockBytes->appID * (sizeof(uint64_t))};
        LOCKBYTES_CONTENT lbc;
        lbc.msg = msg;
        lbc.info = 1;
        tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
        if (!dsm->cas_mask_sync(CooLockBytesAddr, VALID_BIT, lbc.to_uint64(),
                                tmp, VALID_BIT, GLOBAL_ADDRESS_SET_MASK, ctx)) {
            assert(0);
        }
        // wait until inform for fault tolerance
#ifdef USE_FT
        clock_gettime(CLOCK_REALTIME, &s);
        readCnt = 0;
        while (!lockBytes->info) {
            clock_gettime(CLOCK_REALTIME, &e);
            uint64_t microseconds = (e.tv_sec - s.tv_sec) * 1000000 +
                                    (double)(e.tv_nsec - s.tv_nsec) / 1000;
            if (microseconds > leaseTime) {
                tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
                dsm->read_sync((char*)tmp, MNlock_high, 8, ctx);
                if (*tmp == prev) {
                    readCnt++;
                    s = e;
                } else if (*tmp - prev <= 4000ull) {
                    readCnt = 0;
                    prev = *tmp;
                    s = e;
                } else {
                    assert(*tmp - prev >= 800000ull);
                    return 0;
                }
                if (readCnt > 3) {
                    dsm->fault_reportV(MNlock, dptr, prev);
                }
            }
        }
#endif

        while (!lockBytes->info)
            ;
    }
    bool sendfaa = false;
    if (lockBuffer->isUnSet()) {
        tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
        GlobalAddress lbfg = (dsm->localToGlobal(lockBuffer));
        uint64_t newVersion = ((version + isDelete) & (0xf));
        MNVLOCK_CONTENT mc = {lbfg.offset, dsm->getMyThreadID(), lbfg.nodeID,
                              newVersion};
        MNVLOCK_CONTENT nvmc = {0, 0, 0, newVersion};
        if (dsm->cas_sync(MNlock, mc.to_uint64(), nvmc.to_uint64(), tmp,
                          ctx)) {  // no follower
            lockBuffer->valid = 0;
#ifdef USE_FT
            GlobalAddress MNlock_high = MNlock;
            MNlock_high.offset += 8;
            tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
            dsm->faa_boundary_sync(MNlock_high, 1, tmp, 63, ctx);
#endif
            return shortpath;
        }

        // rare case: wait for follower
#ifdef USE_FT
        clock_gettime(CLOCK_REALTIME, &s);
        readCnt = 0;
        while (lockBuffer->isUnSet()) {
            clock_gettime(CLOCK_REALTIME, &e);
            uint64_t microseconds = (e.tv_sec - s.tv_sec) * 1000000 +
                                    (double)(e.tv_nsec - s.tv_nsec) / 1000;
            if (microseconds > leaseTime) {
                tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
                dsm->read_sync((char*)tmp, MNlock_high, 8, ctx);
                if (*tmp == prev) {
                    readCnt++;
                    s = e;
                } else if (*tmp - prev <= 4000ull) {
                    readCnt = 0;
                    prev = *tmp;
                    s = e;
                } else {
                    assert(*tmp - prev >= 800000ull);
                    return 0;
                }
                if (readCnt > 3) {
                    dsm->fault_reportV(MNlock, dptr, prev);
                }
            }
        }
#endif

        while (lockBuffer->isUnSet())
            ;
    }
#ifdef USE_FTERR
    if (rand() % 10000 == 1) {
        printf("%lx fault\n", MNlock.to_uint64());
        return 0;
    }
#endif
    // unlock follower
    GlobalAddress followerLockByte = findLockByteAddrByLockBuffer(lockBuffer);
    lockBuffer->val = 0;
    LOCKBYTES_CONTENT lbc;
    lbc.info = 1;
    lbc.msg = 0;
    tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
    dsm->cas_mask(followerLockByte, VALID_BIT, lbc.to_uint64(), tmp, VALID_BIT,
                  GLOBAL_ADDRESS_SET_MASK, true, ctx);
#ifdef USE_FT
    if (!sendfaa) {
        GlobalAddress MNlock_high = MNlock;
        MNlock_high.offset += 8;
        tmp = (dsm->get_rbuf(coro_id)).get_cas_buffer();
        dsm->faa_boundary(MNlock_high, 1, tmp, 63, true, ctx);
        dsm->poll_rdma_cq(1);
    }
#endif
    dsm->poll_rdma_cq(1);
    return 0;
}
