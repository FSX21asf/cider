#ifndef __DP_H__
#define __DP_H__

#include "Common.h"
class DATA_POINTER {  // USE BY MCS_WC_VLOCK
   public:
    union {
        struct {
            volatile uint64_t offset : 48;
            volatile uint64_t nodeID : 12;
            volatile uint64_t version : 4;
        };
        volatile uint64_t val;
    };
    bool empty() const { return nodeID == 0 && offset == 0; }
    uint64_t to_uint64() const { return val; }
} __attribute__((packed));

#endif