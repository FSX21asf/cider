#if !defined(_MINIHASH_H_)
#define _MINIHASH_

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
#include "LocalLockTable.h"
#include "ShermanLocalLockNode.h"
class Slot {
   public:
    union {
        struct {
            uint64_t fp : 8;
            uint64_t len : 8;
            uint64_t nodeId : 4;
            uint64_t offset : 40;
            uint64_t version : 4;
        };
        uint64_t val;
    };
    bool empty() const { return nodeId == 0 && offset == 0; }
} __attribute__((packed));
class Bucket {
   public:
    Slot slots[define::kSlotPerBucket];
} __attribute__((packed));
// class MiniHash {
//    public:
//     MiniHash(DSM* dsm);
//     void insert(const Key& k, Value v, CoroPull* sink = nullptr);
//     void update(const Key& k,
//                 Value v,
//                 CoroPull* sink = nullptr);  // assert(false) if key is not
//                 found
//     bool search(const Key& k,
//                 Value& v,
//                 CoroPull* sink = nullptr);  // return false if key is not
//                 found
//    private:
//     DSM* dsm;
//     bool acquireLocalLock(int lockId, CoroPull* sink = nullptr);
//     void releaseLocalLock(int lockId);
//     bool can_hand_over(int lockId);
//     LOCAL_LOCK_NODE localLocks[define::kBucketCount];
//     HOLOCK_CONTEXT* FindLockContentByLockId(uint64_t lockId);
// };
// class MiniFGHash {
//    public:
//     MiniFGHash(DSM* dsm);
//     void insert(const Key& k, Value v, CoroPull* sink = nullptr);
//     void update(const Key& k,
//                 Value v,
//                 CoroPull* sink = nullptr);  // assert(false) if key is not
//                 found
//     bool search(const Key& k,
//                 Value& v,
//                 CoroPull* sink = nullptr);  // return false if key is not
//                 found
//    private:
//     DSM* dsm;
//     bool acquireLocalLock(int lockId, CoroPull* sink = nullptr);
//     void releaseLocalLock(int lockId);
//     bool can_hand_over(int lockId);
//     LOCAL_LOCK_NODE localLocks[define::kBucketCount *
//     define::kSlotPerBucket]; HOLOCK_CONTEXT* FindLockContentByLockId(uint64_t
//     lockId);
// };
// class MiniHashTC {
//    public:
//     MiniHashTC(DSM* dsm);
//     void insert(const Key& k, Value v, CoroPull* sink = nullptr);
//     void update(const Key& k,
//                 Value v,
//                 CoroPull* sink = nullptr);  // assert(false) if key is not
//                 found
//     bool search(const Key& k,
//                 Value& v,
//                 CoroPull* sink = nullptr);  // return false if key is not
//                 found
//    private:
//     DSM* dsm;
//     bool acquireLocalLock(int lockId, CoroPull* sink = nullptr);
//     void releaseLocalLock(int lockId);
//     bool can_hand_over(int lockId);
//     LOCAL_LOCK_NODE localLocks[define::kBucketCount];
//     HOLOCK_CONTEXT* FindLockContentByLockId(uint64_t lockId);
//     const uint64_t seeds[2] = {7826, 18517};
// };
// class MiniFGHashTC {
//    public:
//     MiniFGHashTC(DSM* dsm);
//     void insert(const Key& k, Value v, CoroPull* sink = nullptr);
//     void update(const Key& k,
//                 Value v,
//                 CoroPull* sink = nullptr);  // assert(false) if key is not
//                 found
//     bool search(const Key& k,
//                 Value& v,
//                 CoroPull* sink = nullptr);  // return false if key is not
//                 found
//    private:
//     DSM* dsm;
//     void before_operation(CoroPull* sink = nullptr);
//     void clear_debug_info();
//     bool acquireLocalLock(int lockId, CoroPull* sink = nullptr);
//     void releaseLocalLock(int lockId);
//     bool can_hand_over(int lockId);
//     LOCAL_LOCK_NODE localLocks[define::kBucketCount * define::kSlotPerBucket]
//                               [MAX_APP_THREAD / define::kHandOverGroupSize +
//                               1];
//     HOLOCK_CONTEXT* FindLockContentByLockId(uint64_t lockId);
//     const uint64_t seeds[2] = {7826, 18517};
// };
class RaceHash {
   public:
    RaceHash(DSM* dsm);
    void insert(const Key& k,
                Value v,
                CoroPull* sink = nullptr);  // NOTE: insert can also do update
                                            // things if key exists
    bool search(const Key& k,
                Value& v,
                CoroPull* sink = nullptr);  // return false if key is not found

    bool insert(const Key& k, Value v, ECPolicy po, CoroPull* sink = nullptr);
    bool update(const Key& k, Value v, ECPolicy po, CoroPull* sink = nullptr);
    bool deleteItem(const Key& k, ECPolicy po, CoroPull* sink = nullptr);

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
    const uint64_t seeds[2] = {7826, 18517};
    uint64_t inner_update(GlobalAddress ptrAddr,
                          uint64_t lockId,
                          Slot& newslot,
                          Slot& oldslot,
                          const Key& k,
                          bool& wd,
                          Value v,
                          CoroPull* sink);
    uint64_t inner_delete(GlobalAddress ptrAddr,
                          uint64_t lockId,
                          Slot& oldslot,
                          const Key& k,
                          CoroPull* sink);
};
#endif
