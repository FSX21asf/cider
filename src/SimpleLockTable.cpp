#include "SimpleLockTable.h"
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
#include "QueueLock.h"
GlobalAddress FindMNLockByID(uint64_t lockId) {
    GlobalAddress ret;
    ret.nodeID = lockId % MEMORY_NODE_NUM;
    ret.offset = define::kMNLOCKForHashOffest +  // FIXME:just use it...
                 (lockId / MEMORY_NODE_NUM) * 2 * sizeof(uint64_t);
    return ret;
}
bool SimpleLockTable::acquireLocalLock(int lockId, CoroPull* sink) {
    auto& node =
        localLocks[lockId][dsm->getMyThreadID() / define::kHandOverGroupSize];
    bool is_local_locked = false;
    uint64_t lock_val = node.ticket_lock.fetch_add(1);

    uint32_t ticket = lock_val << 32 >> 32;
    uint32_t current = lock_val >> 32;

    while (ticket != current) {  // lock failed
        is_local_locked = true;
        // if (cxt != nullptr) {
        //     hot_wait_queue.push(coro_id);
        //     (*cxt->yield)(*cxt->master);
        // }
        current = node.ticket_lock.load(std::memory_order_relaxed) >> 32;
    }
    // if (is_local_locked) {
    //     hierarchy_lock[dsm->getMyThreadID()][0]++;
    // }
    node.hand_time++;
    return node.hand_over;
}
void SimpleLockTable::releaseLocalLock(int lockId) {
    auto& node =
        localLocks[lockId][dsm->getMyThreadID() / define::kHandOverGroupSize];
    node.ticket_lock.fetch_add((1ull << 32));
}
bool SimpleLockTable::can_hand_over(int lockId) {
    auto& node =
        localLocks[lockId][dsm->getMyThreadID() / define::kHandOverGroupSize];
    uint64_t lock_val = node.ticket_lock.load(std::memory_order_relaxed);
    uint32_t ticket = lock_val << 32 >> 32;
    uint32_t current = lock_val >> 32;
    if (ticket <= current + 1) {  // no pending locks
        node.hand_over = false;
    } else {
        node.hand_over = node.hand_time < define::kMaxHandOverTime;
    }
    if (!node.hand_over) {
        node.hand_time = 0;
    } else {
        // handover_count[dsm->getMyThreadID()][0]++;
    }
    return node.hand_over;
}

extern uint64_t cas_lock_fail[MAX_APP_THREAD];
uint64_t slt_try_write_op[MAX_APP_THREAD];
uint64_t slt_latency[MAX_APP_THREAD][MAX_CORO_NUM][LATENCY_WINDOWS];
volatile bool slt_need_stop = false;
volatile bool slt_need_clear[MAX_APP_THREAD];
void SimpleLockTable::clear_debug_info() {
    memset(cas_lock_fail, 0, sizeof(uint64_t) * MAX_APP_THREAD);
    memset(slt_try_write_op, 0, sizeof(uint64_t) * MAX_APP_THREAD);
}
void SimpleLockTable::before_operation(CoroPull* sink) {
    auto tid = dsm->getMyThreadID();
    if (slt_need_clear[tid]) {
        cas_lock_fail[tid] = 0;
        slt_try_write_op[tid] = 0;
        slt_need_clear[tid] = false;
    }
}
HOLOCK_CONTEXT* SimpleLockTable::FindLockContentByLockId(uint64_t lockId) {
    // return (HOLOCK_CONTEXT*)((
    //     dsm->getRDMADirOffset() + define::kHOlockContentOffset +
    //     (lockId + dsm->getMyThreadID() / define::kHandOverGroupSize *
    //                   define::kSimpleLockTableSize) *
    //         define::kHOLockContentSize));
    return 0;
}
SimpleLockTable::SimpleLockTable(DSM* x) : dsm(x) {}
void SimpleLockTable::getLock(uint64_t x, CoroPull* sink, int sleepTime) {
    before_operation(sink);
    slt_try_write_op[dsm->getMyThreadID()]++;
    GlobalAddress addr = FindMNLockByID(x);

#ifdef USE_CAS
    CAS_LOCK lock(addr);
    lock.lock(sink);
#endif
#ifdef USE_PERFECT_CAS
    PERFECT_CAS_LOCK lock(addr);
    lock.lock(sink);
#endif
#ifdef USE_CAS_HO
    CAS_LOCK lock(addr);
    if (!acquireLocalLock(x, sink)) {
        lock.lock(sink);
    }
#endif
#ifdef USE_CAS_BF
    CAS_BF_LOCK lock(addr);
    lock.lock(sink);
#endif
#ifdef USE_CAS_BF_HO
    CAS_BF_LOCK lock(addr);
    if (!acquireLocalLock(x, sink)) {
        lock.lock(sink);
    }
#endif
#ifdef USE_MCS
    MCS_LOCK lock(addr);
    lock.lock(sink);
#endif
#ifdef USE_MCS_WC
    MCS_WC_LOCK lock(addr);
    if (lock.lock(0, sink))
        return;
#endif
#ifdef USE_MCS_HO
    MCS_LOCK_HO lock(addr, FindLockContentByLockId(x));
    if (!acquireLocalLock(x, sink)) {
        lock.lock(sink);
    }
#endif
    if (sleepTime) {
        long long cnt = 0;
        int i = 0;
        while (i < sleepTime * 1000)
            i++, cnt += i;
        if (!(cnt % 3 == 0 || cnt % 3 == 1 || cnt % 3 == 2))
            return;
    }
#ifdef USE_CAS
    lock.unlock(sink);
#endif
#ifdef USE_PERFECT_CAS
    lock.unlock(sink);
#endif
#ifdef USE_CAS_HO
    if (can_hand_over(x)) {
        releaseLocalLock(x);
        return;
    }
    lock.unlock(sink);
    releaseLocalLock(x);
#endif
#ifdef USE_CAS_BF
    lock.unlock(sink);
#endif
#ifdef USE_CAS_BF_HO
    if (can_hand_over(x)) {
        releaseLocalLock(x);
        return;
    }
    lock.unlock(sink);
    releaseLocalLock(x);
#endif
#ifdef USE_MCS
    lock.unlock(sink);
#endif
#ifdef USE_MCS_WC
    lock.unlock(sink);
#endif
#ifdef USE_MCS_HO
    if (can_hand_over(x)) {
        releaseLocalLock(x);
        return;
    }
    lock.unlock(sink);
    releaseLocalLock(x);
#endif
}
