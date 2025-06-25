#include <endian.h>
#include "Common.h"
#include "DSM.h"
#include "GlobalAddress.h"
#include "QueueLock.h"
void MCS_LOCK_ALLOCATOR::INIT_DSM(DSM* x) {
    if (dsm && dsm->is_register())
        return;
    dsm = x;
    assert(dsm && dsm->is_register());
    flFront =
        (uint64_t*)(dsm->getRDMADirOffset() + define::kQueueLockBufferOffset +
                    dsm->getMyThreadID() *
                        define::kQueueLockBufferSizePerThread);
    for (int i = 0; i < (int)(define::kQueueLockBufferCnt)-1; i++) {
        *(flFront + i) = (uint64_t)(flFront + i + 1);
    }
    flEnd = flFront + (define::kQueueLockBufferCnt - 1);
    *flEnd = 0;
}
uint64_t* MCS_LOCK_ALLOCATOR::allocLockBuffer() {
    uint64_t* ret = (uint64_t*)*flFront;
    assert(ret);
    *flFront = *ret;
    *ret = 0;
    return ret;
}
void MCS_LOCK_ALLOCATOR::deAllocLockBuffer(uint64_t* addr) {
    *flEnd = (uint64_t)addr;
    flEnd = addr;
    *addr = 0;
}
