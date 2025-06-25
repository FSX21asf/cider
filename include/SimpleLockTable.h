#if !defined(_SIMPLELOCKTABLE_H_)
#define _SIMPLELOCKTABLE_

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
#include "ShermanLocalLockNode.h"
class SimpleLockTable {
   public:
    SimpleLockTable(DSM* dsm);
    void getLock(uint64_t x, CoroPull* sink = nullptr, int sleepTime = 0);

   private:
    void before_operation(CoroPull* sink = nullptr);
    void clear_debug_info();
    bool acquireLocalLock(int lockId, CoroPull* sink = nullptr);
    void releaseLocalLock(int lockId);
    bool can_hand_over(int lockId);
    LOCAL_LOCK_NODE localLocks[define::kSimpleLockTableSize]
                              [MAX_APP_THREAD / define::kHandOverGroupSize + 1];
    HOLOCK_CONTEXT* FindLockContentByLockId(uint64_t lockId);
    DSM* dsm;
};
#endif
