#include "MiniArray.h"
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
#include "DataPointer.h"
#include "Key.h"
#include "QueueLock.h"

//[CONFIG]
// FIXME:should define a lock type for MiniArray

MiniArray::MiniArray(DSM* dsm) : dsm(dsm) {
    for (int i = 0;
         i < (int)((MAX_APP_THREAD + define::kHandOverGroupSize - 1) /
                   define::kHandOverGroupSize);
         i++)
        local_lock_table[i] = new LocalLockTable();
};
GlobalAddress FindEntryByID(uint64_t id) {
    GlobalAddress ret;
    ret.nodeID = id % MEMORY_NODE_NUM;
    ret.offset =
        define::kHashBucketOffest + (id / MEMORY_NODE_NUM) * sizeof(uint64_t);
    return ret;
}
GlobalAddress FindMNLockByID(uint64_t idx) {
    GlobalAddress ret;
    ret.nodeID = idx % MEMORY_NODE_NUM;
    ret.offset = define::kMNLOCKForHashOffest +
                 (idx / MEMORY_NODE_NUM) * sizeof(uint64_t) * 2;
    return ret;
}
HOLOCK_CONTEXT* MiniArray::FindLockContentByLockId(uint64_t idx) {
    // return (HOLOCK_CONTEXT*)((
    //     dsm->getRDMADirOffset() + define::kHOlockContentOffset +
    //     (idx + dsm->getMyThreadID() / define::kHandOverGroupSize *
    //                define::kBucketCount * define::kSlotPerBucket) *
    //         define::kHOLockContentSize));
    return 0;
}

// bool MiniArray::acquireLocalLock(int idx, CoroPull* sink) {
//     auto& node =
//         localLocks[idx][dsm->getMyThreadID() / define::kHandOverGroupSize];
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
// void MiniArray::releaseLocalLock(int idx) {
//     auto& node =
//         localLocks[idx][dsm->getMyThreadID() / define::kHandOverGroupSize];
//     node.ticket_lock.fetch_add((1ull << 32));
// }
// bool MiniArray::can_hand_over(int idx) {
//     auto& node =
//         localLocks[idx][dsm->getMyThreadID() / define::kHandOverGroupSize];
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
uint64_t array_try_write_op[MAX_APP_THREAD];
uint64_t array_write_handover_num[MAX_APP_THREAD];
uint64_t array_pessimistic_num[MAX_APP_THREAD];
uint64_t array_latency[MAX_APP_THREAD][MAX_CORO_NUM][LATENCY_WINDOWS];
extern uint64_t mcs_possible_write_handover_num[MAX_APP_THREAD];
extern uint64_t exactly_write_handover_pack_count[MAX_APP_THREAD];
volatile bool array_need_stop = false;
volatile bool array_need_clear[MAX_APP_THREAD];
void MiniArray::clear_debug_info() {
    memset(cas_lock_fail, 0, sizeof(uint64_t) * MAX_APP_THREAD);
    memset(array_try_write_op, 0, sizeof(uint64_t) * MAX_APP_THREAD);
}
void MiniArray::before_operation(CoroPull* sink) {
    auto tid = dsm->getMyThreadID();
    if (array_need_clear[tid]) {
        cas_lock_fail[tid] = 0;
        array_write_handover_num[tid] = 0;
        array_pessimistic_num[tid] = 0;
        array_try_write_op[tid] = 0;
        array_need_clear[tid] = false;
        mcs_possible_write_handover_num[tid] = 0;
        exactly_write_handover_pack_count[tid] = 0;
    }
}

bool MiniArray::search(uint64_t idx, Value& v, CoroPull* sink) {
    before_operation(sink);
    GlobalAddress entryAddr = FindEntryByID(idx);
    DATA_POINTER* entry = (DATA_POINTER*)dsm->get_rbuf(sink).get_cas_buffer();
    dsm->read_sync((char*)entry, entryAddr, 8, sink);
    if (entry->val == 0) {
        return false;
    }
    GlobalAddress target = {entry->nodeID, entry->offset};
    uint64_t* val = (uint64_t*)dsm->get_rbuf(sink).get_KV_buffer();
    dsm->read_sync((char*)val, target, define::kFakeValueSize, sink);
    v = *val;
    return true;

}  // return false if key is not found
enum class Oper { INS, UPD };
bool MiniArray::insert(uint64_t idx, Value v, ECPolicy po, CoroPull* sink) {
    before_operation(sink);
    bool wd = false;
    GlobalAddress nkv;
    GlobalAddress entryAddr = FindEntryByID(idx);
#ifdef DELAY_WRITE
    DATA_POINTER* entry = (DATA_POINTER*)dsm->get_rbuf(sink).get_cas_buffer();
    dsm->read_sync((char*)entry, entryAddr, 8, sink);
#else
    wd = true;
    nkv = dsm->alloc(define::kFakeValueSize);
    char* kvb = dsm->get_rbuf(sink).get_KV_buffer();
    *(uint64_t*)(kvb) = v;
    dsm->write(kvb, nkv, define::kFakeValueSize, true, sink);
    DATA_POINTER* entry = (DATA_POINTER*)dsm->get_rbuf(sink).get_cas_buffer();
    dsm->read((char*)entry, entryAddr, 8, true, sink);
    dsm->poll_rdma_cq(2);
#endif
    DATA_POINTER pre = *entry;
    Oper oper = (pre.empty()) ? Oper::INS : Oper::UPD;
    if (wd) {
        assert(nkv.nodeID != 0 || nkv.offset != 0);
    }
    while (1) {
        int ret;
        if (oper == Oper::UPD) {
            if (po == ECPolicy::ignore) {
                return true;
            } else if (po == ECPolicy::retval) {
                return false;
            }
            ret = inner_update(idx, nkv, pre, wd, v, sink);
            if (ret == 1)
                return true;
            else {
                DATA_POINTER* rereadentry =
                    (DATA_POINTER*)dsm->get_rbuf(sink).get_cas_buffer();
                dsm->read_sync((char*)rereadentry, entryAddr, 8,
                               sink);  //  use mcs_wc to pass new version is ok
                pre = *rereadentry;
                pre.nodeID = pre.offset = 0;
                oper = Oper::INS;
            }
        } else {
            ret = inner_insert(idx, nkv, pre, wd, v, sink);
            if (ret == 1)
                return true;
            else {
                oper = Oper::UPD;
            }
        }
    }
}
bool MiniArray::update(uint64_t idx, Value v, ECPolicy po, CoroPull* sink) {
    before_operation(sink);
    bool wd = false;
    GlobalAddress nkv;
    GlobalAddress entryAddr = FindEntryByID(idx);
#ifdef DELAY_WRITE

    DATA_POINTER* entry = (DATA_POINTER*)dsm->get_rbuf(sink).get_cas_buffer();
    dsm->read_sync((char*)entry, entryAddr, 8, sink);
#else
    wd = true;
    nkv = dsm->alloc(define::kFakeValueSize);
    char* kvb = dsm->get_rbuf(sink).get_KV_buffer();
    *(uint64_t*)(kvb) = v;
    dsm->write(kvb, nkv, define::kFakeValueSize, true, sink);
    DATA_POINTER* entry = (DATA_POINTER*)dsm->get_rbuf(sink).get_cas_buffer();
    dsm->read((char*)entry, entryAddr, 8, true, sink);
    dsm->poll_rdma_cq(2);
#endif
    DATA_POINTER pre = *entry;
    Oper oper = (pre.empty()) ? Oper::INS : Oper::UPD;
    while (1) {
        int ret;
        if (oper == Oper::UPD) {
            ret = inner_update(idx, nkv, pre, wd, v, sink);
            if (ret == 1)
                return true;
            else {
                DATA_POINTER* rereadentry =
                    (DATA_POINTER*)dsm->get_rbuf(sink).get_cas_buffer();
                dsm->read_sync((char*)rereadentry, entryAddr, 8,
                               sink);  //  use mcs_wc to pass new version is ok
                pre = *rereadentry;
                pre.nodeID = pre.offset = 0;
                oper = Oper::INS;
            }
        } else {
            if (po == ECPolicy::ignore) {
                return true;
            } else if (po == ECPolicy::retval) {
                return false;
            }
            assert(pre.empty());
            ret = inner_insert(idx, nkv, pre, wd, v, sink);
            if (ret == 1)
                return true;
            else {
                oper = Oper::UPD;
            }
        }
    }
}
bool MiniArray::deleteItem(uint64_t idx, ECPolicy po, CoroPull* sink) {
    before_operation(sink);
    GlobalAddress entryAddr = FindEntryByID(idx);
    GlobalAddress slotLockAddr = FindMNLockByID(idx);
    DATA_POINTER* entry = (DATA_POINTER*)dsm->get_rbuf(sink).get_cas_buffer();
    dsm->read_sync((char*)entry, entryAddr, 8, sink);
    if (entry->empty()) {
        return false;
    }

    DATA_POINTER pre = *entry, ne;
    ne.nodeID = 0, ne.offset = 0, ne.version = (pre.version + 1) & (0xf);
    uint64_t dver = pre.version;
#ifdef USE_MCS_WCV
    MCS_WC_VLOCK lock(slotLockAddr, entryAddr);
    uint64_t ret = lock.lock(true, pre.version, sink);
    if (ret == 3) {
        return false;
    }
    assert(ret == 0);
#endif
    while (1) {
        uint64_t* tmp = dsm->get_rbuf(sink).get_cas_buffer();
        bool ret = dsm->cas_sync(entryAddr, pre.to_uint64(), ne.to_uint64(),
                                 tmp, sink);
        if (ret)
            return true;

        pre = *(DATA_POINTER*)tmp;

        if (pre.empty() || pre.version != dver) {  // concurrent delete
            return false;
        }
    }
#if defined(USE_MCS_WCV)
    lock.unlock(sink, 1);
    return true;
#endif
}
thread_local std::map<uint64_t, int> MiniArray::retryCnt, MiniArray::useLockCnt;
uint64_t MiniArray::inner_update(uint64_t idx,
                                 GlobalAddress& nkv,
                                 DATA_POINTER& pre,
                                 bool& wd,
                                 Value v,
                                 CoroPull* sink) {
    array_try_write_op[dsm->getMyThreadID()]++;
    GlobalAddress slotLockAddr = FindMNLockByID(idx);
    GlobalAddress entryAddr = FindEntryByID(idx);

    // handover
    bool write_handover = false;
    std::pair<bool, bool> lock_res = std::make_pair(false, false);
    Key k = int2key(idx);
#ifdef USE_LOCALWC
    assert(sink == 0);
    lock_res = local_lock_table[(dsm->getMyThreadID() - 1) /
                                define::kHandOverGroupSize]
                   ->acquire_local_write_lock(k, v, 0, sink);
    write_handover = (lock_res.first && !lock_res.second);
#endif
    if (write_handover) {
        array_write_handover_num[dsm->getMyThreadID()]++;
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
    if (useLock)
        array_pessimistic_num[dsm->getMyThreadID()]++;
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
            array_write_handover_num[dsm->getMyThreadID()]++;
            return ret;
        }
    }
#endif
#ifdef USE_MCS_WCV
    MCS_WC_VLOCK lock(slotLockAddr, entryAddr);
    if (useLock) {
        uint64_t ret = lock.lock(0, pre.version, sink);
        if (ret == 3) {
            assert(0);  // no delete
            return 2;
        }
        if (ret) {
            array_write_handover_num[dsm->getMyThreadID()]++;
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
        nkv = dsm->alloc(define::kFakeValueSize);
        char* kvb = dsm->get_rbuf(sink).get_KV_buffer();
        *(uint64_t*)(kvb) = v;
        dsm->write(kvb, nkv, define::kFakeValueSize, true, sink);
        dsm->poll_rdma_cq(1);
    }
    DATA_POINTER ne;
    ne.nodeID = nkv.nodeID;
    ne.offset = nkv.offset;
    ne.version = pre.version;
    assert(!ne.empty());
    uint64_t ret = 1, retryc = 0, firstRetry = 1;
    while (1) {
        uint64_t* tmp = dsm->get_rbuf(sink).get_cas_buffer();
        if (dsm->cas_sync(entryAddr, pre.to_uint64(), ne.val, tmp, sink)) {
            break;
        }
        retryc++;
        cas_lock_fail[dsm->getMyThreadID()]++;
        if (retryc >= define::kSTlimit) {
            mcs_possible_write_handover_num[dsm->getMyThreadID()] += firstRetry;
            firstRetry = 0;
        }
        pre = *(DATA_POINTER*)tmp;

        if (pre.version != ne.version || pre.empty()) {
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
uint64_t MiniArray::inner_insert(uint64_t idx,
                                 GlobalAddress& nkv,
                                 DATA_POINTER& pre,
                                 bool& wd,
                                 Value v,
                                 CoroPull* sink) {
    if (!wd) {
        wd = true;
        nkv = dsm->alloc(define::kFakeValueSize);
        char* kvb = dsm->get_rbuf(sink).get_KV_buffer();
        *(uint64_t*)(kvb) = v;
        dsm->write_sync(kvb, nkv, define::kFakeValueSize, sink);
    }
    GlobalAddress entryAddr = FindEntryByID(idx);
    DATA_POINTER ne;
    ne.nodeID = nkv.nodeID;
    ne.offset = nkv.offset;
    ne.version = pre.version;
    assert(!ne.empty());
    assert(pre.empty());
    while (1) {
        uint64_t* tmp = dsm->get_rbuf(sink).get_cas_buffer();
        if (dsm->cas_sync(entryAddr, pre.to_uint64(), ne.val, tmp, sink)) {
            break;
        }
        pre = *(DATA_POINTER*)tmp;
        ne.version = pre.version;
        if (!pre.empty())
            return 2;
    }
    return 1;
}