#include <city.h>
#include <algorithm>
#include <atomic>
#include <functional>
#include <iostream>
#include <map>
#include <queue>
#include <set>
#include "Common.h"
#include "DSM.h"
#include "Key.h"
#include "MiniHash.h"
#include "QueueLock.h"

// enum class CCTYPE { CAS_LOCK, MCS_LOCK, ATOMIC };
// const CCTYPE curCC = CCTYPE::CAS_LOCK;
// #define USE_MCS

//[CONFIG]
// FIXME:should define USE_MCS or USE_MCS_HO or USE_CAS or
// USE_CAS_HO or USE_ATOMIC here!!
// use cmake /DUSE_MCS ..
RaceHash::RaceHash(DSM* dsm) : dsm(dsm) {
    for (int i = 0;
         i < (int)((MAX_APP_THREAD + define::kHandOverGroupSize - 1) /
                   define::kHandOverGroupSize);
         i++)
        local_lock_table[i] = new LocalLockTable();
};
GlobalAddress FindBucketByID(uint64_t bucketId) {
    GlobalAddress ret;
    ret.nodeID = bucketId % MEMORY_NODE_NUM;
    ret.offset = define::kHashBucketOffest +
                 (bucketId / MEMORY_NODE_NUM) * sizeof(Bucket);
    return ret;
}
GlobalAddress FindSlotMNLockByID(uint64_t lockId) {
    GlobalAddress ret;
    ret.nodeID = lockId % MEMORY_NODE_NUM;
    ret.offset = define::kMNLOCKForHashOffest +
                 (lockId / MEMORY_NODE_NUM) * sizeof(uint64_t) * 2;
    return ret;
}
HOLOCK_CONTEXT* RaceHash::FindLockContentByLockId(uint64_t lockId) {
    // return (HOLOCK_CONTEXT*)((
    //     dsm->getRDMADirOffset() + define::kHOlockContentOffset +
    //     (lockId + dsm->getMyThreadID() / define::kHandOverGroupSize *
    //                   define::kBucketCount * define::kSlotPerBucket) *
    //         define::kHOLockContentSize));
    return 0;
}

// bool RaceHash::acquireLocalLock(int lockId, CoroPull* sink) {
//     auto& node =
//         localLocks[lockId][dsm->getMyThreadID() /
//         define::kHandOverGroupSize];
//     bool is_local_locked = false;
//     uint64_t lock_val = node.ticket_lock.fetch_add(1);

//     uint32_t ticket = lock_val << 32 >> 32;
//     uint32_t current = lock_val >> 32;

//     while (ticket != current) {  // lock failed
//         is_local_locked = true;
//         // if (cxt != nullptr) {
//         //     hot_wait_queue.push(coro_id);
//         //     (*cxt->yield)(*cxt->master);
//         // }
//         current = node.ticket_lock.load(std::memory_order_relaxed) >> 32;
//     }
//     // if (is_local_locked) {
//     //     hierarchy_lock[dsm->getMyThreadID()][0]++;
//     // }
//     node.hand_time++;
//     return node.hand_over;
// }
// void RaceHash::releaseLocalLock(int lockId) {
//     auto& node =
//         localLocks[lockId][dsm->getMyThreadID() /
//         define::kHandOverGroupSize];
//     node.ticket_lock.fetch_add((1ull << 32));
// }
// bool RaceHash::can_hand_over(int lockId) {
//     auto& node =
//         localLocks[lockId][dsm->getMyThreadID() /
//         define::kHandOverGroupSize];
//     uint64_t lock_val = node.ticket_lock.load(std::memory_order_relaxed);
//     uint32_t ticket = lock_val << 32 >> 32;
//     uint32_t current = lock_val >> 32;
//     if (ticket <= current + 1) {  // no pending locks
//         node.hand_over = false;
//     } else {
//         node.hand_over = node.hand_time < define::kMaxHandOverTime;
//     }
//     if (!node.hand_over) {
//         node.hand_time = 0;
//     } else {
//         // handover_count[dsm->getMyThreadID()][0]++;
//     }
//     return node.hand_over;
// }
extern uint64_t cas_lock_fail[MAX_APP_THREAD];
uint64_t hash_try_write_op[MAX_APP_THREAD];
uint64_t hash_write_handover_num[MAX_APP_THREAD];
extern uint64_t exactly_write_handover_pack_count[MAX_APP_THREAD];
uint64_t hash_latency[MAX_APP_THREAD][MAX_CORO_NUM][LATENCY_WINDOWS];
extern uint64_t mcs_possible_write_handover_num[MAX_APP_THREAD];
volatile bool hash_need_stop = false;
volatile bool hash_need_clear[MAX_APP_THREAD];
void RaceHash::clear_debug_info() {
    memset(cas_lock_fail, 0, sizeof(uint64_t) * MAX_APP_THREAD);
    memset(hash_try_write_op, 0, sizeof(uint64_t) * MAX_APP_THREAD);
}
void RaceHash::before_operation(CoroPull* sink) {
    auto tid = dsm->getMyThreadID();
    if (hash_need_clear[tid]) {
        cas_lock_fail[tid] = 0;
        hash_try_write_op[tid] = 0;
        hash_need_clear[tid] = false;
        hash_write_handover_num[tid] = 0;
        exactly_write_handover_pack_count[tid] = 0;
        mcs_possible_write_handover_num[tid] = 0;
    }
}

void RaceHash::insert(const Key& k, Value v, CoroPull* sink) {
    assert(0);
    before_operation(sink);
    int ksz = k.size();
    int nkvsz = ksz + 8 + 2;
    GlobalAddress nkv = dsm->alloc(nkvsz);
    char* kvb = dsm->get_rbuf(sink).get_KV_buffer();
    kvb[0] = ksz;
    kvb[1] = 8;
    for (int i = 0; i < ksz; i++)
        kvb[2 + i] = k[i];
    *(uint64_t*)(kvb + 2 + ksz) = v;
    dsm->write(kvb, nkv, nkvsz, true, sink);

    uint64_t bucketId[2];
    GlobalAddress bucketAddr[2];
    char* bucket[2];
    for (int i = 0; i < 2; i++) {
        bucketId[i] = CityHash64WithSeed((char*)&k, sizeof(k), seeds[i]) %
                      define::kBucketCount;
        bucketAddr[i] = FindBucketByID(bucketId[i]);
        bucket[i] = dsm->get_rbuf(sink).get_bucket_buffer();
        dsm->read(bucket[i], bucketAddr[i], define::kBucketSize, true, sink);
    }

    dsm->poll_rdma_cq(3);

    uint64_t targetBucketId, targetSlotId, oval;
    int choice;
    bool searchOK = false;
    int cnt[2] = {0};
    for (int j = 0; j < 2; j++) {
        Bucket* b = (Bucket*)bucket[j];
        for (int i = 0; i < (int)define::kSlotPerBucket; i++) {
            Slot& s = b->slots[i];
            if (s.val != 0) {
                cnt[j]++;
                if (k[0] == s.fp) {
                    GlobalAddress kvAddr = {s.nodeId, s.offset};
                    char* kvb = dsm->get_rbuf(sink).get_KV_buffer();
#ifdef USE_DSLR
                    uint64_t lockId = bucketId[j] * define::kSlotPerBucket + i;
                    GlobalAddress slotLockAddr = FindSlotMNLockByID(lockId);
                    DSLR_LOCK lk(slotLockAddr);
                    lk.lock(lk.ModeShare);
#endif
#ifdef USE_SFL
                    // uint64_t lockId = bucketId[j] * define::kSlotPerBucket +
                    // i; GlobalAddress slotLockAddr =
                    // FindSlotMNLockByID(lockId); SHIFTLOCK lk(slotLockAddr);
                    // lk.lock(lk.ModeShare);
#endif
                    dsm->read_sync(kvb, kvAddr, s.len, sink);

#ifdef USE_DSLR
                    lk.unlock();
#endif
#ifdef USE_SFL
                    // lk.unlock();
#endif
                    int lenk = kvb[0], lenv = kvb[1];
                    assert(lenk == 8);
                    assert(lenv == 8);
                    if (!KeyEqual(str2key(std::string(kvb + 2, kvb + 2 + lenk)),
                                  k))
                        continue;
                    oval = s.val;
                    targetBucketId = bucketId[j];
                    targetSlotId = i;
                    choice = j;
                    searchOK = true;
                    break;
                }
            }
        }
        if (searchOK)
            break;
    }
    if (searchOK) {  // update
        hash_try_write_op[dsm->getMyThreadID()]++;
        uint64_t lockId =
            targetBucketId * define::kSlotPerBucket + targetSlotId;
        GlobalAddress slotLockAddr = FindSlotMNLockByID(lockId);
#ifdef USE_MCS
        MCS_LOCK lock(slotLockAddr);
        lock.lock(0);
#endif
#ifdef USE_CAS
        CAS_LOCK lock(slotLockAddr);
        lock.lock(0);
#endif
#ifdef USE_CAS_HO
        CAS_LOCK lock(slotLockAddr);
        if (!acquireLocalLock(lockId)) {
            lock.lock();
        }
#endif
#ifdef USE_CAS_BF
        CAS_BF_LOCK lock(slotLockAddr);
        lock.lock(0);
#endif
#ifdef USE_CAS_BF_HO
        CAS_BF_LOCK lock(slotLockAddr);
        if (!acquireLocalLock(lockId)) {
            lock.lock();
        }
#endif
#ifdef USE_MCS_HO
        MCS_LOCK_HO lock(slotLockAddr, FindLockContentByLockId(lockId));
        if (!acquireLocalLock(lockId)) {
            lock.lock();
        }
#endif
#ifdef USE_MCS_WC
        MCS_WC_LOCK lock(slotLockAddr);
        if (lock.lock(0, 0)) {
            return;
        }
#endif

#ifdef USE_TK_WC
        TK_WC_LOCK lock(slotLockAddr);
        if (!lock.lock()) {
            return;
        }
#endif
#ifdef USE_DSLR
        DSLR_LOCK lock(slotLockAddr);
        lock.lock(lock.ModeExclusive);
#endif
#ifdef USE_SFL
        SHIFTLOCK lock(slotLockAddr);
        lock.lock(lock.ModeExclusive);
#endif

        Slot ns;
        ns.fp = k[0];
        ns.len = nkvsz;
        ns.nodeId = nkv.nodeID;
        ns.offset = nkv.offset;

        GlobalAddress slotAddr{
            bucketAddr[choice].nodeID,
            bucketAddr[choice].offset + targetSlotId * sizeof(uint64_t)};

#if defined(USE_MCS) || defined(USE_CAS) || defined(USE_MCS_WC) ||  \
    defined(USE_CAS_BF) || defined(USE_DSLR) || defined(USE_SFL) || \
    defined(USE_TK_WC)
        uint64_t* tmp = dsm->get_rbuf(sink).get_cas_buffer();
        dsm->read_sync((char*)tmp, slotAddr, sizeof(uint64_t), sink);
        *tmp = ns.val;
        dsm->write_sync((char*)tmp, slotAddr, sizeof(uint64_t), sink);
        lock.unlock();
        return;
#endif
#if defined(USE_CAS_HO) || defined(USE_CAS_BF_HO)
        uint64_t* tmp = dsm->get_rbuf(sink).get_cas_buffer();
        dsm->read_sync((char*)tmp, slotAddr, sizeof(uint64_t), sink);
        *tmp = ns.val;
        dsm->write_sync((char*)tmp, slotAddr, sizeof(uint64_t), sink);
        if (can_hand_over(lockId)) {
            releaseLocalLock(lockId);
            return;
        }
        lock.unlock();
        releaseLocalLock(lockId);
        return;
#endif
#if defined(USE_MCS_HO)
        uint64_t* tmp = dsm->get_rbuf(sink).get_cas_buffer();
        dsm->read_sync((char*)tmp, slotAddr, sizeof(uint64_t), sink);
        *tmp = ns.val;
        dsm->write_sync((char*)tmp, slotAddr, sizeof(uint64_t), sink);
        if (can_hand_over(lockId)) {
            releaseLocalLock(lockId);
            lock.addRelCnt(sink);
            return;
        }
        lock.unlock();
        releaseLocalLock(lockId);
        return;
#endif
#ifdef USE_ATOMIC
        bool ret = false;
        uint64_t* tmp2;
        while (!ret) {
            tmp2 = dsm->get_rbuf(sink).get_cas_buffer();
            ret = dsm->cas_sync(slotAddr, oval, ns.val, tmp2);
            oval = *tmp2;
        }
        return;
#endif
        assert(0);
    }

    choice = cnt[0] <= cnt[1] ? 0 : 1;
    Bucket* b = (Bucket*)bucket[choice];
    for (int i = 0; i < (int)define::kSlotPerBucket; i++) {
        Slot& s = b->slots[i];
        if (s.val == 0) {
            Slot ns;
            ns.fp = k[0];
            ns.len = nkvsz;
            ns.nodeId = nkv.nodeID;
            ns.offset = nkv.offset;
            GlobalAddress slotAddr{
                bucketAddr[choice].nodeID,
                bucketAddr[choice].offset + i * sizeof(uint64_t)};

            bool ret;
            uint64_t* tmp2;
            tmp2 = dsm->get_rbuf(sink).get_cas_buffer();
            ret = dsm->cas_sync(slotAddr, s.val, ns.val, tmp2);
            if (!ret) {
                continue;  // compete failed
            } else
                return;
        }
    }
    assert(0);
}
bool RaceHash::search(const Key& k, Value& v, CoroPull* sink) {
    before_operation(sink);
    uint64_t bucketId[2];
    GlobalAddress bucketAddr[2];
    char* bucket[2];
    for (int i = 0; i < 2; i++) {
        bucketId[i] = CityHash64WithSeed((char*)&k, sizeof(k), seeds[i]) %
                      define::kBucketCount;
        bucketAddr[i] = FindBucketByID(bucketId[i]);
        bucket[i] = dsm->get_rbuf(sink).get_bucket_buffer();
        dsm->read(bucket[i], bucketAddr[i], define::kBucketSize, true, sink);
    }
    dsm->poll_rdma_cq(2);
    for (int j = 0; j < 2; j++) {
        Bucket* b = (Bucket*)bucket[j];
        for (int i = 0; i < (int)define::kSlotPerBucket; i++) {
            Slot& s = b->slots[i];
            if (s.val != 0) {
                if (k[0] == s.fp) {
                    GlobalAddress kvAddr = {s.nodeId, s.offset};
#ifdef USE_DSLR
                    uint64_t lockId = bucketId[j] * define::kSlotPerBucket + i;
                    GlobalAddress slotLockAddr = FindSlotMNLockByID(lockId);
                    DSLR_LOCK lk(slotLockAddr);
                    lk.lock(lk.ModeShare);
#endif
#ifdef USE_SFL
                    // uint64_t lockId = bucketId[j] * define::kSlotPerBucket +
                    // i; GlobalAddress slotLockAddr =
                    // FindSlotMNLockByID(lockId); SHIFTLOCK lk(slotLockAddr);
                    // lk.lock(lk.ModeShare);
#endif
                    char* kvb = dsm->get_rbuf(sink).get_KV_buffer();
                    dsm->read_sync(kvb, kvAddr, s.len, sink);

#ifdef USE_DSLR
                    lk.unlock();
#endif
#ifdef USE_SFL
                    // lk.unlock();
#endif
                    int lenk = kvb[0], lenv = kvb[1];
                    assert(lenk == 8);
                    // assert(lenv == 8);
                    if (!KeyEqual(str2key(std::string(kvb + 2, kvb + 2 + lenk)),
                                  k))
                        continue;
                    v = *(uint64_t*)(kvb + 2 + lenk);
                    return true;
                }
            }
        }
    }
    return false;
}  // return false if key is not found

bool RaceHash::insert(const Key& k, Value v, ECPolicy po, CoroPull* sink) {
    before_operation(sink);
    GlobalAddress nkv;
    bool wd = false;
    int ksz, nkvsz = 0;
    char* kvb;
#ifdef DELAY_WRITE
    uint64_t bucketId[2];
    GlobalAddress bucketAddr[2];
    char* bucket[2];
    for (int i = 0; i < 2; i++) {
        bucketId[i] = CityHash64WithSeed((char*)&k, sizeof(k), seeds[i]) %
                      define::kBucketCount;
        if (i == 1 && bucketId[1] == bucketId[0])
            bucketId[1] = (bucketId[1] + 1) % define::kBucketCount;
        bucketAddr[i] = FindBucketByID(bucketId[i]);
        bucket[i] = dsm->get_rbuf(sink).get_bucket_buffer();
        dsm->read(bucket[i], bucketAddr[i], define::kBucketSize, true, sink);
    }
    dsm->poll_rdma_cq(2);
#else
    wd = true;
    ksz = k.size();
    nkvsz = ksz + define::kFakeValueSize + 2;
    nkv = dsm->alloc(nkvsz);
    kvb = dsm->get_rbuf(sink).get_KV_buffer();
    kvb[0] = ksz;
    kvb[1] = define::kFakeValueSize;
    for (int i = 0; i < ksz; i++)
        kvb[2 + i] = k[i];
    *(uint64_t*)(kvb + 2 + ksz) = v;
    dsm->write(kvb, nkv, nkvsz, true, sink);

    uint64_t bucketId[2];
    GlobalAddress bucketAddr[2];
    char* bucket[2];
    for (int i = 0; i < 2; i++) {
        bucketId[i] = CityHash64WithSeed((char*)&k, sizeof(k), seeds[i]) %
                      define::kBucketCount;
        if (i == 1 && bucketId[1] == bucketId[0])
            bucketId[1] = (bucketId[1] + 1) % define::kBucketCount;
        bucketAddr[i] = FindBucketByID(bucketId[i]);
        bucket[i] = dsm->get_rbuf(sink).get_bucket_buffer();
        dsm->read(bucket[i], bucketAddr[i], define::kBucketSize, true, sink);
    }
    dsm->poll_rdma_cq(3);
#endif
    uint64_t targetBucketId, targetSlotId;
    Slot oslot;
    int choice;
    int searchOK = 0;
    int cnt[2] = {0};
    for (int j = 0; j < 2; j++) {
        Bucket* b = (Bucket*)bucket[j];
        for (int i = 0; i < (int)define::kSlotPerBucket; i++) {
            Slot& s = b->slots[i];
            if (s.val != 0) {
                cnt[j]++;
                if (k[0] == s.fp) {
                    GlobalAddress kvAddr = {s.nodeId, s.offset};
                    char* kvb = dsm->get_rbuf(sink).get_KV_buffer();
#ifdef USE_DSLR
                    uint64_t lockId = bucketId[j] * define::kSlotPerBucket + i;
                    GlobalAddress slotLockAddr = FindSlotMNLockByID(lockId);
                    DSLR_LOCK lk(slotLockAddr);
                    lk.lock(lk.ModeShare);
#endif
#ifdef USE_SFL
                    // uint64_t lockId = bucketId[j] * define::kSlotPerBucket +
                    // i; GlobalAddress slotLockAddr =
                    // FindSlotMNLockByID(lockId); SHIFTLOCK lk(slotLockAddr);
                    // lk.lock(lk.ModeShare);
#endif
                    dsm->read_sync(kvb, kvAddr, s.len, sink);

#ifdef USE_DSLR
                    lk.unlock();
#endif
#ifdef USE_SFL
                    // lk.unlock();
#endif
                    int lenk = kvb[0], lenv = kvb[1];
                    assert(lenk == 8);
                    // assert(lenv == 8);
                    if (!KeyEqual(str2key(std::string(kvb + 2, kvb + 2 + lenk)),
                                  k))
                        continue;
                    oslot = s;
                    targetBucketId = bucketId[j];
                    targetSlotId = i;
                    choice = j;
                    searchOK += 1;
                    break;
                }
            }
        }
        if (searchOK)
            break;
    }
    Slot ns;
    if (searchOK) {  // exist already
        if (po == ECPolicy::ignore) {
            return true;
        } else if (po == ECPolicy::retval) {
            return false;
        }
        // update
        hash_try_write_op[dsm->getMyThreadID()]++;
        uint64_t lockId =
            targetBucketId * define::kSlotPerBucket + targetSlotId;

        ns.fp = k[0];
        ns.len = nkvsz;
        ns.nodeId = nkv.nodeID;
        ns.offset = nkv.offset;
        ns.version = oslot.version;

        GlobalAddress slotAddr{
            bucketAddr[choice].nodeID,
            bucketAddr[choice].offset + targetSlotId * sizeof(uint64_t)};
        uint64_t updateSuccess =
            inner_update(slotAddr, lockId, ns, oslot, k, wd, v, sink);
        return updateSuccess == 1;
    }

    choice = cnt[0] <= cnt[1] ? 0 : 1;
    Bucket* b = (Bucket*)bucket[choice];
    bool insertSuccess = false;
    for (int i = 0; i < (int)define::kSlotPerBucket; i++) {
        Slot& s = b->slots[i];
        if (s.val == 0) {
            if (!wd) {
                wd = true;
                ksz = k.size();
                nkvsz = ksz + define::kFakeValueSize + 2;
                nkv = dsm->alloc(nkvsz);
                char* kvb = dsm->get_rbuf(sink).get_KV_buffer();
                kvb[0] = ksz;
                kvb[1] = define::kFakeValueSize;
                for (int i = 0; i < ksz; i++)
                    kvb[2 + i] = k[i];
                *(uint64_t*)(kvb + 2 + ksz) = v;
                dsm->write(kvb, nkv, nkvsz, true, sink);
                dsm->poll_rdma_cq(1);
            }
            ns.fp = k[0];
            ns.len = nkvsz;
            ns.nodeId = nkv.nodeID;
            ns.offset = nkv.offset;
            ns.version = oslot.version;
            GlobalAddress slotAddr{
                bucketAddr[choice].nodeID,
                bucketAddr[choice].offset + i * sizeof(uint64_t)};

            bool ret;
            uint64_t* tmp2;
            tmp2 = dsm->get_rbuf(sink).get_cas_buffer();
            ret = dsm->cas_sync(slotAddr, s.val, ns.val, tmp2);
            if (!ret) {
                continue;  // compete failed
            } else {
                insertSuccess = true;
                break;
            }
        }
    }
    if (!insertSuccess) {
        assert(0);
    }
    for (int i = 0; i < 2; i++) {
        bucket[i] = dsm->get_rbuf(sink).get_bucket_buffer();
        dsm->read(bucket[i], bucketAddr[i], define::kBucketSize, true, sink);
    }
    dsm->poll_rdma_cq(2);
    searchOK = 0;
    for (int j = 0; j < 2; j++) {
        Bucket* b = (Bucket*)bucket[j];
        for (int i = 0; i < (int)define::kSlotPerBucket; i++) {
            Slot& s = b->slots[i];
            if (s.val != 0) {
                if (k[0] == s.fp) {
                    GlobalAddress kvAddr = {s.nodeId, s.offset};
                    char* kvb = dsm->get_rbuf(sink).get_KV_buffer();
                    dsm->read_sync(kvb, kvAddr, s.len, sink);
                    int lenk = kvb[0], lenv = kvb[1];
                    assert(lenk == 8);
                    // assert(lenv == 8);
                    if (!KeyEqual(str2key(std::string(kvb + 2, kvb + 2 + lenk)),
                                  k))
                        continue;
                    searchOK += 1;
                    if (searchOK > 1) {
                        GlobalAddress slotAddr{
                            bucketAddr[j].nodeID,
                            bucketAddr[j].offset + i * sizeof(uint64_t)};
                        uint64_t lockId =
                            bucketId[j] * define::kSlotPerBucket + i;
                        Slot oslot = s;
                        inner_delete(slotAddr, lockId, oslot, k, sink);
                        searchOK--;
                    }
                }
            }
        }
    }

    return true;
}
thread_local std::map<uint64_t, int> RaceHash::retryCnt, RaceHash::useLockCnt;
uint64_t RaceHash::inner_update(GlobalAddress ptrAddr,
                                uint64_t idx,
                                Slot& newslot,
                                Slot& oldslot,
                                const Key& k,
                                bool& wd,
                                Value v,
                                CoroPull* sink) {
    GlobalAddress slotLockAddr = FindSlotMNLockByID(idx);
    uint64_t targetVersion = oldslot.version;
    // handover
    bool write_handover = false;
    std::pair<bool, bool> lock_res = std::make_pair(false, false);
#ifdef USE_LOCALWC
    assert(sink == 0);
    lock_res = local_lock_table[(dsm->getMyThreadID() - 1) /
                                define::kHandOverGroupSize]
                   ->acquire_local_write_lock(k, v, 0, sink);
    write_handover = (lock_res.first && !lock_res.second);
#endif
    if (write_handover) {
        hash_write_handover_num[dsm->getMyThreadID()]++;
// FIXME:assert update success here
#ifdef USE_LOCALWC
        local_lock_table[(dsm->getMyThreadID() - 1) /
                         define::kHandOverGroupSize]
            ->release_local_write_lock(k, lock_res);
#endif
        return 1;
    }

    bool useLock = true;
#ifdef USE_ATOMIC
    useLock = false;
#endif
#ifdef USE_ST
    useLock = false;
    if (useLockCnt[idx]) {
        useLock = true;
        useLockCnt[idx]--;
    }
#endif
#ifdef USE_MCS
    MCS_LOCK lock(slotLockAddr);
    if (useLock)
        lock.lock(sink);
#endif
#ifdef USE_CAS
    CAS_LOCK lock(slotLockAddr);
    if (useLock)
        lock.lock(sink);
#endif
#ifdef USE_CAS_HO
    CAS_LOCK lock(slotLockAddr);
    if (useLock) {
        if (!acquireLocalLock(idx)) {
            lock.lock(sink);
        }
    }
#endif
#ifdef USE_CAS_BF
    CAS_BF_LOCK lock(slotLockAddr);
    if (useLock)
        lock.lock(sink);
#endif
#ifdef USE_CAS_BF_HO
    CAS_BF_LOCK lock(slotLockAddr);
    if (useLock) {
        if (!acquireLocalLock(idx)) {
            lock.lock(sink);
        }
    }
#endif
#ifdef USE_MCS_HO
    MCS_LOCK_HO lock(slotLockAddr, FindLockContentByLockId(idx));
    if (useLock) {
        if (!acquireLocalLock(idx)) {
            lock.lock(sink);
        }
    }
#endif
#ifdef USE_MCS_WC
    MCS_WC_LOCK lock(slotLockAddr);
    if (useLock) {
        uint64_t ret = lock.lock(0, sink);
        if (ret) {
            hash_write_handover_num[dsm->getMyThreadID()]++;
            return ret;
        }
    }
#endif
#ifdef USE_MCS_WCV
    MCS_WC_VLOCK lock(slotLockAddr, ptrAddr);
    if (useLock) {
        uint64_t ret = lock.lock(0, oldslot.version, sink);
        if (ret == 3)
            return 3;
        if (ret) {
            hash_write_handover_num[dsm->getMyThreadID()]++;
            return ret;
        }
    }
#endif

#ifdef USE_DSLR
    DSLR_LOCK lock(slotLockAddr);
    if (useLock)
        lock.lock(lock.ModeExclusive, sink);
#endif
#ifdef USE_SFL
    SHIFTLOCK lock(slotLockAddr);
    if (useLock)
        lock.lock(lock.ModeExclusive, sink);
#endif

    if (!wd) {
        wd = true;
        int ksz = k.size();
        int nkvsz = ksz + define::kFakeValueSize + 2;
        GlobalAddress nkv = dsm->alloc(nkvsz);
        char* kvb = dsm->get_rbuf(sink).get_KV_buffer();
        kvb[0] = ksz;
        kvb[1] = define::kFakeValueSize;
        for (int i = 0; i < ksz; i++)
            kvb[2 + i] = k[i];
        *(uint64_t*)(kvb + 2 + ksz) = v;
        dsm->write(kvb, nkv, nkvsz, true, sink);
        dsm->poll_rdma_cq(1);

        newslot.fp = k[0];
        newslot.len = nkvsz;
        newslot.nodeId = nkv.nodeID;
        newslot.offset = nkv.offset;
        newslot.version = oldslot.version;
    }

    uint64_t ret = 1, retryc = 0;
    while (1) {
        uint64_t* tmp = dsm->get_rbuf(sink).get_cas_buffer();
        if (dsm->cas_sync(ptrAddr, oldslot.val, newslot.val, tmp, sink)) {
            break;
        }
        retryc++;
        cas_lock_fail[dsm->getMyThreadID()]++;
        oldslot = *(Slot*)tmp;
        if (oldslot.empty()) {
            ret = 2;
            break;
        }
        if (oldslot.version != targetVersion) {
            ret = 2;
            break;
        }
    }

#ifdef USE_ST
    if (!useLock) {
        if (retryCnt[idx] >= (int)define::kSTlimit &&
            retryc >= define::kSTlimit) {
            useLockCnt[idx] += define::kSTSenti;
        }
        retryCnt[idx] = retryc;
    }
#endif
#if defined(USE_MCS) || defined(USE_CAS) || defined(USE_CAS_BF) || \
    defined(USE_DSLR) || defined(USE_SFL)
    if (useLock)
        lock.unlock(sink);
#ifdef USE_LOCALWC
    local_lock_table[(dsm->getMyThreadID() - 1) / define::kHandOverGroupSize]
        ->release_local_write_lock(k, lock_res);
#endif
    return ret;
#endif
#if defined(USE_MCS_WC)
    if (useLock) {
        uint64_t shortpath = lock.unlock(sink, ret);
        if (shortpath)
            retryCnt[idx] /= 2;
        else
            retryCnt[idx] += 2;
    }
    return ret;
#endif
#if defined(USE_MCS_WCV)
    if (useLock) {
        uint64_t shortpath = lock.unlock(sink, ret);
        if (shortpath)
            retryCnt[idx] /= 2;
        else
            retryCnt[idx] += 2;
    }
    return ret;
#endif
#if defined(USE_CAS_HO) || defined(USE_CAS_BF_HO)
    if (useLock) {
        if (can_hand_over(idx)) {
            releaseLocalLock(idx);
            return ret;
        }
        lock.unlock(sink);
        releaseLocalLock(idx);
    }
    return ret;
#endif
#if defined(USE_MCS_HO)
    if (useLock) {
        if (can_hand_over(idx)) {
            releaseLocalLock(idx);
            lock.addRelCnt(sink);
            return ret;
        }
        lock.unlock(sink);
        releaseLocalLock(idx);
    }
    return ret;
#endif
#ifdef USE_ATOMIC
#ifdef USE_LOCALWC
    local_lock_table[(dsm->getMyThreadID() - 1) / define::kHandOverGroupSize]
        ->release_local_write_lock(k, lock_res);
#endif
    return ret;

#endif
    assert(0);
}

bool RaceHash::update(const Key& k, Value v, ECPolicy po, CoroPull* sink) {
    assert(0);
    return false;
}
bool RaceHash::deleteItem(const Key& k, ECPolicy po, CoroPull* sink) {
    before_operation(sink);
    GlobalAddress nkv;
    uint64_t bucketId[2];
    GlobalAddress bucketAddr[2];
    char* bucket[2];
    for (int i = 0; i < 2; i++) {
        bucketId[i] = CityHash64WithSeed((char*)&k, sizeof(k), seeds[i]) %
                      define::kBucketCount;
        if (i == 1 && bucketId[1] == bucketId[0])
            bucketId[1] = (bucketId[1] + 1) % define::kBucketCount;
        bucketAddr[i] = FindBucketByID(bucketId[i]);
        bucket[i] = dsm->get_rbuf(sink).get_bucket_buffer();
        dsm->read(bucket[i], bucketAddr[i], define::kBucketSize, true, sink);
    }
    dsm->poll_rdma_cq(2);
    uint64_t targetBucketId, targetSlotId;
    Slot oval;
    int choice;
    int searchOK = 0;
    int cnt[2] = {0};
    for (int j = 0; j < 2; j++) {
        Bucket* b = (Bucket*)bucket[j];
        for (int i = 0; i < (int)define::kSlotPerBucket; i++) {
            Slot& s = b->slots[i];
            if (s.val != 0) {
                cnt[j]++;
                if (k[0] == s.fp) {
                    GlobalAddress kvAddr = {s.nodeId, s.offset};
                    char* kvb = dsm->get_rbuf(sink).get_KV_buffer();
                    dsm->read_sync(kvb, kvAddr, s.len, sink);
                    int lenk = kvb[0], lenv = kvb[1];
                    assert(lenk == 8);
                    // assert(lenv == 8);
                    if (!KeyEqual(str2key(std::string(kvb + 2, kvb + 2 + lenk)),
                                  k))
                        continue;
                    oval = s;
                    targetBucketId = bucketId[j];
                    targetSlotId = i;
                    choice = j;
                    searchOK += 1;
                    break;
                }
            }
        }
        if (searchOK)
            break;
    }

    if (searchOK) {  // exist already
        // delete
        uint64_t lockId =
            targetBucketId * define::kSlotPerBucket + targetSlotId;

        GlobalAddress slotAddr{
            bucketAddr[choice].nodeID,
            bucketAddr[choice].offset + targetSlotId * sizeof(uint64_t)};
        uint64_t deleteSuccess = inner_delete(slotAddr, lockId, oval, k, sink);
        return deleteSuccess;
    } else {
        if (po == ECPolicy::ignore) {
            return true;
        } else if (po == ECPolicy::retval) {
            return false;
        }
        assert(0);
    }
    assert(0);
}
uint64_t RaceHash::inner_delete(GlobalAddress ptrAddr,
                                uint64_t idx,
                                Slot& oldslot,
                                const Key& k,
                                CoroPull* sink) {
    GlobalAddress slotLockAddr = FindSlotMNLockByID(idx);
    uint64_t targetVersion = (oldslot.version + 1) & (0xf),
             dver = oldslot.version;
    bool useLock = true;
#ifdef USE_ATOMIC
    useLock = false;
#endif
#ifdef USE_MCS
    MCS_LOCK lock(slotLockAddr);
    if (useLock)
        lock.lock(sink);
#endif
#ifdef USE_CAS
    CAS_LOCK lock(slotLockAddr);
    if (useLock)
        lock.lock(sink);
#endif
#ifdef USE_CAS_HO
    CAS_LOCK lock(slotLockAddr);
    if (useLock) {
        if (!acquireLocalLock(idx)) {
            lock.lock(sink);
        }
    }
#endif
#ifdef USE_CAS_BF
    CAS_BF_LOCK lock(slotLockAddr);
    if (useLock)
        lock.lock(sink);
#endif
#ifdef USE_MCS_WC
    MCS_WC_LOCK lock(slotLockAddr);
    if (useLock) {
        uint64_t ret = lock.lock(true, sink);
        assert(ret == 0);
    }
#endif
#ifdef USE_MCS_WCV
    MCS_WC_VLOCK lock(slotLockAddr, ptrAddr);
    if (useLock) {
        uint64_t ret = lock.lock(true, oldslot.version, sink);
        if (ret == 3)
            return 2;
        assert(ret == 0);
    }
#endif
#ifdef USE_DSLR
    DSLR_LOCK lock(slotLockAddr);
    if (useLock)
        lock.lock(lock.ModeExclusive, sink);
#endif
#ifdef USE_SFL
    SHIFTLOCK lock(slotLockAddr);
    if (useLock)
        lock.lock(lock.ModeExclusive, sink);
#endif
    Slot newslot;
    newslot.val = 0;
    newslot.version = (dver + 1) * (0xf);

    uint64_t ret = 1, retryc = 0;
    while (1) {
        uint64_t* tmp = dsm->get_rbuf(sink).get_cas_buffer();
        if (dsm->cas_sync(ptrAddr, oldslot.val, newslot.val, tmp, sink)) {
            break;
        }
        retryc++;
        oldslot = *(Slot*)tmp;
        char* kvb = dsm->get_rbuf(sink).get_KV_buffer();
        GlobalAddress kvAddr{oldslot.nodeId, oldslot.offset};
        dsm->read_sync(kvb, kvAddr, oldslot.len, sink);  // resevre reread
        if (oldslot.version != dver || oldslot.empty()) {
            ret = 2;
            break;
        }
    }
#if defined(USE_MCS) || defined(USE_CAS) || defined(USE_CAS_BF) || \
    defined(USE_DSLR) || defined(USE_SFL)
    if (useLock)
        lock.unlock(sink);
    return ret;
#endif
#if defined(USE_MCS_WC)
    if (useLock) {
        lock.unlock(sink, ret);
    }
    return ret;
#endif
#if defined(USE_MCS_WCV)
    if (useLock) {
        lock.unlock(sink, ret);
    }
    return ret;
#endif
#ifdef USE_ATOMIC
    return ret;
#endif
    assert(0);
}