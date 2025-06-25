#if !defined(_SHERMAN_LOCAL_LOCK_NODE_H_)
#define _SHERMAN_LOCAL_LOCK_NODE_
#include <atomic>
#include "Common.h"
struct LOCAL_LOCK_NODE {
    std::atomic<uint64_t> ticket_lock;
    bool hand_over;
    uint8_t hand_time;
    LOCAL_LOCK_NODE() {
        ticket_lock.store(0);
        hand_over = 0;
        hand_time = 0;
    }
};
#endif