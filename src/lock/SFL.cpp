#include <endian.h>
#include "Common.h"
#include "DSM.h"
#include "GlobalAddress.h"
#include "QueueLock.h"

/**
    volatile uint64_t RdrCnt : 10;
    volatile uint64_t Epoch : 1;
    volatile uint64_t nodeId : 5;
    volatile uint64_t offset : 48;
    volatile uint64_t RelCnt : 64; high64
*/

DSM* SHIFTLOCK::dsm;
void SHIFTLOCK::LOCK_INIT_DSM(DSM* x) {
    dsm = x;
}
SHIFTLOCK::SHIFTLOCK(GlobalAddress addr) {
    MNlock = addr;
    // hoctx = (SFLHO_CONTEXT*)(dsm->getRDMADirOffset() +
    //                          define::kHOlockContentOffset +
    //                          dsm->getMyThreadID() * sizeof(SFLHO_CONTEXT));

    addr.offset += 8;
    MNlock_high = addr;
    // char* tmp = dsm->get_rbuf(0).get_KV_buffer();
    // dsm->read_sync(tmp, MNlock, 16, 0);
    // SHIFTLOCK_CONTENT ee = *(SHIFTLOCK_CONTENT*)tmp;
    // printf("tmp %lx %lx %lx %lx %lx\n", ee.nodeId, ee.offset, ee.RdrCnt,
    //        ee.RelCnt, ee.Epoch);
}
void SHIFTLOCK::lock(bool mode, CoroPull* sink) {
    curMode = mode;
    hoctx->lockBuffer.val = 0;
    hoctx->lockBuffer.valid = 1;
    hoctx->lockBytes.op = 0;
    if (curMode == ModeShare) {
        uint64_t* bf = (uint64_t*)dsm->get_rbuf(0).get_KV_buffer();
        dsm->faa_boundary_sync(MNlock, 1, bf, 9, sink);
        SHIFTLOCK_CONTENT* entry = (SHIFTLOCK_CONTENT*)bf;
        if (entry->nodeId == 0 && entry->offset == 0) {
            return;
        }
        uint64_t curEpoch = entry->Epoch;
        while (1) {
            usleep(RemoteWait);
            uint64_t* bf = (uint64_t*)dsm->get_rbuf(0).get_KV_buffer();
            dsm->read_sync((char*)bf, MNlock, 16, sink);
            SHIFTLOCK_CONTENT* entry = (SHIFTLOCK_CONTENT*)bf;
            if (curEpoch != entry->Epoch)
                return;
        }
    }
    // ModeExclusive

    // uint64_t* bf1 = (uint64_t*)dsm->get_rbuf(0).get_KV_buffer();
    // dsm->read_sync((char*)bf1, MNlock, 16, sink);
    // SHIFTLOCK_CONTENT* entry1 = (SHIFTLOCK_CONTENT*)bf1;
    // printf("bf1:61 %lx %lx\n", MNlock.to_uint64(), entry1->offset);

    uint64_t cmpv[2], swpv[2], cmpm[2], swpm[2];
    uint64_t* bf = (uint64_t*)dsm->get_rbuf(0).get_KV_buffer();
    cmpm[0] = cmpm[1] = 0;
    swpm[0] = (~0x7ffull), swpm[1] = 0;
    SHIFTLOCK_CONTENT* val = (SHIFTLOCK_CONTENT*)swpv;
    GlobalAddress glb = dsm->localToGlobal(&(hoctx->lockBuffer));
    val->nodeId = glb.nodeID, val->offset = glb.offset;
    // printf("lock %lx set addr %lx %lx\n", MNlock.to_uint64(), glb.nodeID,
    //        glb.offset);
    // char* tmp = dsm->get_rbuf(0).get_KV_buffer();
    // dsm->read_sync(tmp, MNlock, 16, sink);
    dsm->cas128_mask_sync(MNlock, cmpv, swpv, bf, cmpm, swpm, sink);

    // bf1 = (uint64_t*)dsm->get_rbuf(0).get_KV_buffer();
    // dsm->read_sync((char*)bf1, MNlock, 16, sink);
    // entry1 = (SHIFTLOCK_CONTENT*)bf1;
    // printf("bf1:73 %lx %lx\n", MNlock.to_uint64(), entry1->offset);

    SHIFTLOCK_CONTENT entry = *(SHIFTLOCK_CONTENT*)bf;

    curRelCnt = entry.RelCnt;
    consWrt = 0;
    epoch = entry.Epoch;
    if (entry.nodeId != 0 || entry.offset != 0) {
        // wait for writer
        // printf("lock  %lx wait for %lx %lx\n", MNlock.to_uint64(),
        // entry.nodeId,
        //        entry.offset);
        GlobalAddress pred = {entry.nodeId, entry.offset};
        GlobalAddress lockByteGlobal =
            (dsm->localToGlobal(&(hoctx->lockBytes)));
        HOLOCK_BUFFER lc{lockByteGlobal.nodeID, lockByteGlobal.offset, 1, 0};
        uint64_t* tmp = (dsm->get_rbuf(sink)).get_cas_buffer();

        if (!dsm->cas_mask_sync(pred, VALID_BIT, lc.to_uint64(), tmp, VALID_BIT,
                                GLOBAL_ADDRESS_SET_MASK, sink))
            assert(0);
        volatile SFL_LOCKBYTE* lockByte = &(hoctx->lockBytes);
        while (!lockByte->op)
            ;
        if (lockByte->op == 1) {  // modechanged
            while (1) {
                usleep(RemoteWait);
                uint64_t* bf = (uint64_t*)dsm->get_rbuf(0).get_KV_buffer();
                dsm->read_sync((char*)bf, MNlock_high, 8, sink);
                // fault tolerance TODO
                if (*bf == lockByte->curRelCnt) {
                    curRelCnt = lockByte->curRelCnt;
                    epoch = 1 - entry.Epoch;
                    return;
                }
            }
        } else if (lockByte->op == 2) {  // handover
            curRelCnt = lockByte->curRelCnt, consWrt = lockByte->consWrt;
            return;
        }

    } else if (entry.RdrCnt != 0) {
        // wait for reader
        uint64_t expect = entry.RdrCnt + entry.RelCnt;
        while (1) {
            usleep(RemoteWait);
            uint64_t* bf = (uint64_t*)dsm->get_rbuf(0).get_KV_buffer();
            dsm->read_sync((char*)bf, MNlock_high, 8, sink);
            // fault tolerance TODO
            if (*bf == expect) {
                curRelCnt = expect;
                return;
            }
        }
    }
    // printf("RETURN\n");
    return;
}
void SHIFTLOCK::unlock(CoroPull* sink) {
    if (curMode == ModeShare) {
        uint64_t addv[2], addm[2];
        uint64_t* bf = (uint64_t*)dsm->get_rbuf(0).get_KV_buffer();
        addv[0] = 0x3ff, addv[1] = 1;
        addm[0] = (1ull << 9), addm[1] = (1ull << 63);
        dsm->faa128_boundray_sync(MNlock, addv, bf, addm, sink);
        return;
    }
    // ModeExclusive
    // printf("UNLOCK\n");
    HOLOCK_BUFFER* lockBuffer = &(hoctx->lockBuffer);
    if (lockBuffer->isUnSet()) {
        // printf("try reset\n");
        SHIFTLOCK_CONTENT eql, val;
        uint64_t cmpm[2], swpm[2];
        GlobalAddress glb = dsm->localToGlobal(lockBuffer);
        eql.nodeId = glb.nodeID, eql.offset = glb.offset;
        val.nodeId = val.offset = 0;
        val.RelCnt = curRelCnt + 1, val.Epoch = 1 - epoch;
        cmpm[0] = (~0x7ffull), cmpm[1] = 0;
        swpm[0] = (~0x3ffull), swpm[1] = (~0ull);
        uint64_t* cmpv = (uint64_t*)&eql;
        uint64_t* tmp = (uint64_t*)(dsm->get_rbuf(sink)).get_entry_buffer();
        // dsm->read_sync((char*)tmp, MNlock, 16, sink);
        // printf("read: %lx %lx\n", tmp[0], tmp[1]);
        if (dsm->cas128_mask_sync(MNlock, (uint64_t*)&eql, (uint64_t*)&val, tmp,
                                  cmpm, swpm, sink)) {  // no follower

            // uint64_t* bf1 = (uint64_t*)dsm->get_rbuf(0).get_KV_buffer();
            // dsm->read_sync((char*)bf1, MNlock, 16, sink);
            // SHIFTLOCK_CONTENT* entry1 = (SHIFTLOCK_CONTENT*)bf1;
            // printf("bf1:163 %lx\n", entry1->offset);

            lockBuffer->val = 0;
            return;
        }

        // printf("try fail %lx %lx\n", eql.nodeId, eql.offset);
    }
    // rare case: wait for follower
    while (lockBuffer->isUnSet())
        ;
    GlobalAddress succ = lockBuffer->toGlobalAddress();
    if (consWrt == HO_LIM) {  // ModeChanged
        uint64_t addv[2], addm[2];
        uint64_t* bf = (uint64_t*)dsm->get_rbuf(0).get_KV_buffer();
        addv[0] = (1 << 10), addv[1] = 1;
        addm[0] = (1ull << 10), addm[1] = (1ull << 63);
        dsm->faa128_boundray_sync(MNlock, addv, bf, addm, sink);
        SHIFTLOCK_CONTENT* entry = (SHIFTLOCK_CONTENT*)bf;
        uint64_t expect = entry->RelCnt + entry->RdrCnt + 1;

        uint64_t cmpv[2], swpv[2], cmpm[2], swpm[2];
        bf = (uint64_t*)dsm->get_rbuf(0).get_KV_buffer();
        cmpm[0] = cmpm[1] = 0;
        swpm[0] = swpm[1] = (~0ull);
        SFL_LOCKBYTE* slb = (SFL_LOCKBYTE*)swpv;
        slb->op = 1;
        slb->curRelCnt = expect;
        dsm->cas128_mask_sync(succ, cmpv, swpv, bf, cmpm, swpm, sink);
        return;
    }
    // Handover
    uint64_t* bf = (uint64_t*)dsm->get_rbuf(0).get_KV_buffer();
    dsm->faa_boundary_sync(MNlock_high, 1, bf, 63, sink);

    // uint64_t* bf1 = (uint64_t*)dsm->get_rbuf(0).get_KV_buffer();
    // dsm->read_sync((char*)bf1, MNlock, 16, sink);
    // SHIFTLOCK_CONTENT* entry1 = (SHIFTLOCK_CONTENT*)bf1;
    // printf("bf1:201 %lx\n", entry1->offset);

    uint64_t cmpv[2], swpv[2], cmpm[2], swpm[2];
    bf = (uint64_t*)dsm->get_rbuf(0).get_KV_buffer();
    cmpm[0] = cmpm[1] = 0;
    swpm[0] = swpm[1] = (~0ull);
    SFL_LOCKBYTE* slb = (SFL_LOCKBYTE*)swpv;
    slb->op = 2;
    slb->consWrt = consWrt + 1;
    slb->curRelCnt = curRelCnt + 1;
    dsm->cas128_mask_sync(succ, cmpv, swpv, bf, cmpm, swpm, sink);

    // bf1 = (uint64_t*)dsm->get_rbuf(0).get_KV_buffer();
    // dsm->read_sync((char*)bf1, MNlock, 16, sink);
    // entry1 = (SHIFTLOCK_CONTENT*)bf1;
    // printf("bf1:216 %lx\n", entry1->offset);
}