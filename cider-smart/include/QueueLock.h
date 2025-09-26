#if !defined(_QUEUELOCK_H_)
#define _QUEUELOCK_H_

#include "Common.h"
#include "DSM.h"
#include "GlobalAddress.h"
const static uint64_t BARRIER_BIT = 1ull << 63;
const static uint64_t VALID_BIT = 1ull << 63;
const static uint64_t VERSION_BIT = 0xfull << 59;
const static uint64_t GLOBAL_ADDRESS_SET_MASK = (~0ull) ^ VALID_BIT;
const uint64_t leaseTime = 10000;  // us
class MNLOCK_CONTENT {             // USE BY MCS_WC, MCS is just a GlobalAddress
   public:
    union {
        struct {
            volatile uint64_t offset : 48;
            volatile uint64_t appID : 8;
            volatile uint64_t nodeID : 7;
            volatile uint64_t block : 1;
        };
        volatile uint64_t val;
    };

    uint64_t to_uint64() { return val; }
} __attribute__((packed));
class MNVLOCK_CONTENT {  // USE BY MCS_WC_VLOCK
   public:
    union {
        struct {
            volatile uint64_t offset : 48;
            volatile uint64_t appID : 8;
            volatile uint64_t nodeID : 4;
            volatile uint64_t version : 4;
        };
        volatile uint64_t val;
    };
    uint64_t to_uint64() { return val; }
} __attribute__((packed));
class LOCKBYTES_CONTENT {  // INFO USE BY MCS, MCS_HO, and MCS_WC use all
   public:
    union {
        struct {
            volatile uint64_t nodeID : 16;
            volatile uint64_t appID : 16;
            volatile uint64_t msg : 16;
            volatile uint64_t info : 15;
            volatile uint64_t valid : 1;
        };
        volatile uint64_t val;
    };
    uint64_t to_uint64() { return val; }
} __attribute__((packed));
class LOCK_BUFFER_CONTENT {  // MCS, MCS_WC use appID to unlock
   public:
    union {
        struct {
            volatile uint64_t nodeID : 16;
            volatile uint64_t appID : 16;
            volatile uint64_t padding : 30;
            volatile uint64_t addset : 1;
            volatile uint64_t valid : 1;
        };
        volatile uint64_t val;
    };

    uint64_t to_uint64() { return val; }
    bool isUnSet() { return addset == 0; }
    bool isValid() { return valid; }

    GlobalAddress toGlobalAddress() {
        return GlobalAddress{nodeID, define::kAPPThreadLockBytesOffset +
                                         appID * sizeof(uint64_t)};
    }
} __attribute__((packed));
class MCS_LOCK_ALLOCATOR {  // use by MCS, MCS_WC
   public:
    MCS_LOCK_ALLOCATOR() = default;
    void INIT_DSM(DSM* dsm);
    uint64_t* allocLockBuffer();
    void deAllocLockBuffer(uint64_t*);

   private:
    DSM* dsm;
    uint64_t* flFront;
    uint64_t* flEnd;
};
class MCS_LOCK {
   public:
    void lock(CoroContext* ctx, int coro_id);
    void unlock(CoroContext* ctx, int coro_id);
    MCS_LOCK(GlobalAddress addr);
    ~MCS_LOCK();
    static void LOCK_INIT_DSM(DSM* dsm);
    static void LOCK_INIT_ALLOC();  // call when create thread

   private:
    LOCKBYTES_CONTENT* getLockBytes();
    GlobalAddress MNlock;
    LOCK_BUFFER_CONTENT* lockBuffer;
    static DSM* dsm;
    uint64_t prev;
};

class MCS_WC_LOCK {
   public:
    uint64_t lock(bool Barrier = false, CoroContext* ctx = 0, int coro_id = 0);
    uint64_t unlock(CoroContext* ctx,
                    int coro_id,
                    uint64_t msg = 1);  // return 1 if short path
    MCS_WC_LOCK(GlobalAddress addr);
    ~MCS_WC_LOCK();
    static void LOCK_INIT_DSM(DSM* dsm);
    static void LOCK_INIT_ALLOC();  // call when create thread

   private:
    LOCKBYTES_CONTENT* getLockBytes();
    GlobalAddress MNlock;
    LOCK_BUFFER_CONTENT* lockBuffer;
    static DSM* dsm;
    uint64_t prev;
    bool shortpath;
    bool isBarrier;
};
class MCS_WC_VLOCK {
   public:
    uint64_t lock(bool isdelete,
                  uint64_t dptrversion,
                  CoroContext* ctx = 0,
                  int coro_id = 0);
    uint64_t unlock(CoroContext* ctx,
                    int coro_id,
                    uint64_t msg = 1);  // return 1 if short path
    MCS_WC_VLOCK(GlobalAddress addr, GlobalAddress dpaddr);
    ~MCS_WC_VLOCK();
    static void LOCK_INIT_DSM(DSM* dsm);
    static void LOCK_INIT_ALLOC();  // call when create thread

   private:
    LOCKBYTES_CONTENT* getLockBytes();
    GlobalAddress MNlock, dptr;
    LOCK_BUFFER_CONTENT* lockBuffer;
    static DSM* dsm;
    uint64_t prev;
    bool shortpath;
    uint64_t version;
    bool isDelete;
};

#endif