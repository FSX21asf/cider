#if !defined(_MINIARRAY_H_)
#define _MINIARRAY_

#include "Common.h"
#include "DSM.h"
#include "QueueLock.h"

#include <city.h>
#include <algorithm>
#include <atomic>
#include <functional>
#include <iostream>
#include <map>
#include <queue>
#include <set>
#include "DataPointer.h"
#include "LocalLockTable.h"
#include "ShermanLocalLockNode.h"

class MiniArray {
   public:
    MiniArray(DSM* dsm);
    bool search(uint64_t idx,
                Value& v,
                CoroPull* sink = nullptr);  // return false if key is not found

    bool insert(uint64_t idx, Value v, ECPolicy po, CoroPull* sink = nullptr);
    bool update(uint64_t idx, Value v, ECPolicy po, CoroPull* sink = nullptr);
    bool deleteItem(uint64_t idx, ECPolicy po, CoroPull* sink = nullptr);

   public:
    LocalLockTable*
        local_lock_table[(MAX_APP_THREAD + define::kHandOverGroupSize - 1) /
                         define::kHandOverGroupSize];

   private:
    static thread_local std::map<uint64_t, int> retryCnt, useLockCnt;
    DSM* dsm;
    void before_operation(CoroPull* sink = nullptr);
    void clear_debug_info();
    // bool acquireLocalLock(int lockId, CoroPull* sink = nullptr);
    // void releaseLocalLock(int lockId);
    // bool can_hand_over(int lockId);
    // LOCAL_LOCK_NODE localLocks[define::kBucketCount * define::kSlotPerBucket]
    //                           [MAX_APP_THREAD / define::kHandOverGroupSize +
    //                           1];
    HOLOCK_CONTEXT* FindLockContentByLockId(uint64_t lockId);
    uint64_t inner_update(uint64_t idx,
                          GlobalAddress& nkv,
                          DATA_POINTER& pre,
                          bool& wd,
                          Value v,
                          CoroPull* sink);
    uint64_t inner_insert(uint64_t idx,
                          GlobalAddress& nkv,
                          DATA_POINTER& pre,
                          bool& wd,
                          Value v,
                          CoroPull* sink);
};
#endif
