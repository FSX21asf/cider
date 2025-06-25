#if !defined(_QUEUELOCK_H_)
#define _QUEUELOCK_H_

#include <map>
#include "Common.h"
#include "DSM.h"
#include "GlobalAddress.h"
const static uint64_t VALID_BIT = 1ull << 63;
const static uint64_t BARRIER_BIT = 1ull << 63;
const static uint64_t VERSION_BIT = 0xfull << 59;
const static uint64_t GLOBAL_ADDRESS_SET_MASK = (~0ull) ^ VALID_BIT;
const uint64_t leaseTime = 10000;  // us
class HOLOCK_BUFFER {              // USE BY MCS_HO
   public:
    union {
        struct {
            volatile uint64_t nodeID : 14;
            volatile uint64_t offset : 48;
            volatile uint64_t addset : 1;
            volatile uint64_t valid : 1;
        };
        volatile uint64_t val;
    };
    uint64_t to_uint64() { return val; }
    bool isUnSet() { return !addset; }
    bool isValid() { return valid; }
    GlobalAddress toGlobalAddress() { return GlobalAddress{nodeID, offset}; }
} __attribute__((packed));

class MNLOCK_CONTENT {  // USE BY MCS_WC, MCS is just a GlobalAddress
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
            volatile uint64_t msg2 : 16;
            volatile uint64_t msg : 8;
            volatile uint64_t info : 7;
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
class HOLOCK_CONTEXT {
   public:
    HOLOCK_BUFFER lockBuffer;
    LOCKBYTES_CONTENT lockBytes;
    uint64_t prev;
} __attribute__((packed));
class MCS_LOCK_HO {
   public:
    void lock(CoroPull* sink = nullptr);
    void unlock(CoroPull* sink = nullptr);
    MCS_LOCK_HO(GlobalAddress addr, HOLOCK_CONTEXT* mt);
    void addRelCnt(CoroPull* sink = nullptr);
    static void LOCK_INIT_DSM(DSM* dsm);

   private:
    GlobalAddress MNlock;
    HOLOCK_CONTEXT* hoctx;
    static DSM* dsm;
};

class MCS_LOCK {
   public:
    void lock(CoroPull* sink = nullptr);
    void unlock(CoroPull* sink = nullptr);
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
    uint64_t lock(bool Barrier = false, CoroPull* sink = nullptr);
    uint64_t unlock(CoroPull* sink = nullptr,
                    uint64_t msg = 1);  // return 1 means low contention
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
                  CoroPull* sink = nullptr);
    uint64_t unlock(CoroPull* sink = nullptr,
                    uint64_t msg = 1);  // return 1 means low contention
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
class CAS_LOCK {
   public:
    void lock(CoroPull* sink = nullptr);
    void unlock(CoroPull* sink = nullptr);
    CAS_LOCK(GlobalAddress addr);
    static void LOCK_INIT_DSM(DSM* dsm);

   private:
    static DSM* dsm;
    GlobalAddress MNlock;
};
class PERFECT_CAS_LOCK {
   public:
    void lock(CoroPull* sink = nullptr);
    void unlock(CoroPull* sink = nullptr);
    PERFECT_CAS_LOCK(GlobalAddress addr);
    static void LOCK_INIT_DSM(DSM* dsm);

   private:
    static DSM* dsm;
    GlobalAddress MNlock;
};
class CAS_BF_LOCK {
   public:
    void lock(CoroPull* sink = nullptr);
    void unlock(CoroPull* sink = nullptr);
    CAS_BF_LOCK(GlobalAddress addr);
    static void LOCK_INIT_DSM(DSM* dsm);

   private:
    const int T0 = 2;
    const int Tmax = define::kMaxBackOFFTime;
    static DSM* dsm;
    GlobalAddress MNlock;
};
struct DSLR_ENTRY {
    uint16_t nx, ns, mx, ms;
    bool valid() const { return mx || ms || ns || nx; }
    void clear() { nx = ns = mx = ms = 0; }
    uint64_t to_u64() const {
        return ((uint64_t)ms << 48) | ((uint64_t)mx << 32) |
               ((uint64_t)ns << 16) | nx;
    }
} __attribute__((packed));
class DSLR_LOCK {
   public:
    void lock(bool mode, CoroPull* sink = nullptr);
    bool inner_lock(CoroPull* sink);
    void unlock(CoroPull* sink = nullptr);
    DSLR_LOCK(GlobalAddress addr);
    static void LOCK_INIT_DSM(DSM* dsm);
    DSLR_ENTRY* addMs(CoroPull* sink);
    void minusMs(CoroPull* sink);
    DSLR_ENTRY* addMx(CoroPull* sink);
    void minusMx(CoroPull* sink);
    DSLR_ENTRY* addNs(CoroPull* sink);
    DSLR_ENTRY* addNx(CoroPull* sink);
    DSLR_ENTRY* read(CoroPull* sink);
    bool handleConflict(DSLR_ENTRY prev, CoroPull* sink);
    const bool ModeShare = false, ModeExclusive = true;

   private:
    const int T0 = 1862;  // us
    DSLR_ENTRY resetFrom;
    bool curMode;
    static DSM* dsm;
    GlobalAddress MNlock;
    const uint16_t COUNT_MAX = 32768;
};

class SHIFTLOCK_CONTENT {  // entry
   public:
    struct {
        volatile uint64_t RdrCnt : 10;
        volatile uint64_t Epoch : 1;
        volatile uint64_t nodeId : 5;
        volatile uint64_t offset : 48;
        volatile uint64_t RelCnt : 64;
    };

} __attribute__((packed));
class SFL_LOCKBYTE {
   public:
    struct {
        volatile uint32_t op;  // modechanged=1 or handover=2
        volatile uint32_t consWrt;
        volatile uint64_t curRelCnt;  // or expect in modechanged
    };
} __attribute__((packed));
class SFLHO_CONTEXT {
   public:
    SFL_LOCKBYTE lockBytes;
    HOLOCK_BUFFER lockBuffer;
    uint64_t padding;
} __attribute__((packed));
class SHIFTLOCK {  // acquire one lock at most
   public:
    void lock(bool mode, CoroPull* sink = nullptr);
    void unlock(CoroPull* sink = nullptr);
    SHIFTLOCK(GlobalAddress addr);
    static void LOCK_INIT_DSM(DSM* dsm);
    const bool ModeShare = false, ModeExclusive = true;

   private:
    const unsigned int T0 = 1862, RemoteWait = 1000;  // us
    const unsigned int HO_LIM = 16;
    bool curMode;
    static DSM* dsm;
    GlobalAddress MNlock, MNlock_high;
    // HOLOCK_BUFFER* lockBuffer;
    // SFL_LOCKBYTE* lockByte;
    SFLHO_CONTEXT* hoctx;
    uint64_t curRelCnt, consWrt, epoch;
};
class TK_WC_CONTENT {
   public:
    union {
        struct {
            uint64_t nx : 21;
            uint64_t mx : 21;
            uint64_t ex : 21;
            uint64_t wc_prepare : 1;
        };
        uint64_t val;
    };
    uint64_t to_u64() { return val; }
} __attribute__((packed));
class TK_WC_LOCK {
   public:
    bool lock(CoroPull* sink = nullptr);
    void unlock(CoroPull* sink = nullptr);
    TK_WC_LOCK(GlobalAddress addr);
    static void LOCK_INIT_DSM(DSM* dsm);

    TK_WC_CONTENT* addMx(CoroPull* sink);
    TK_WC_CONTENT* read(CoroPull* sink);

   private:
    GlobalAddress MNlock;
    uint64_t myTicket;
    static DSM* dsm;
    uint64_t addVal;
    uint64_t T0 = 3000;  // us
};

#endif