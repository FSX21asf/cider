#include <endian.h>
#include "Common.h"
#include "DSM.h"
#include "GlobalAddress.h"
#include "QueueLock.h"

DSM* DSLR_LOCK::dsm;
// uint16_t nx, ns, mx, ms;
DSLR_ENTRY* DSLR_LOCK::addMs(CoroPull* sink) {
    uint64_t* tmp = (dsm->get_rbuf(sink)).get_cas_buffer();
    dsm->faa_boundary_sync(MNlock, (1ll << 48ll), tmp, 63, sink);
    return (DSLR_ENTRY*)tmp;
}
void DSLR_LOCK::minusMs(CoroPull* sink) {
    uint64_t* tmp = (dsm->get_rbuf(sink)).get_cas_buffer();
    dsm->faa_boundary_sync(MNlock, (0xffffll << 48ll), tmp, 63, sink);
}
DSLR_ENTRY* DSLR_LOCK::addMx(CoroPull* sink) {
    uint64_t* tmp = (dsm->get_rbuf(sink)).get_cas_buffer();
    dsm->faa_boundary_sync(MNlock, (1ll << 32ll), tmp, 47, sink);
    return (DSLR_ENTRY*)tmp;
}
void DSLR_LOCK::minusMx(CoroPull* sink) {
    uint64_t* tmp = (dsm->get_rbuf(sink)).get_cas_buffer();
    dsm->faa_boundary_sync(MNlock, (0xffffll << 32ll), tmp, 47, sink);
}
DSLR_ENTRY* DSLR_LOCK::addNs(CoroPull* sink) {
    uint64_t* tmp = (dsm->get_rbuf(sink)).get_cas_buffer();
    dsm->faa_boundary_sync(MNlock, (1ll << 16ll), tmp, 31, sink);
    return (DSLR_ENTRY*)tmp;
}
DSLR_ENTRY* DSLR_LOCK::addNx(CoroPull* sink) {
    uint64_t* tmp = (dsm->get_rbuf(sink)).get_cas_buffer();
    dsm->faa_boundary_sync(MNlock, 1, tmp, 15, sink);
    return (DSLR_ENTRY*)tmp;
}
DSLR_ENTRY* DSLR_LOCK::read(CoroPull* sink) {
    uint64_t* tmp = (dsm->get_rbuf(sink)).get_cas_buffer();
    dsm->read_sync((char*)tmp, MNlock, 8, sink);
    return (DSLR_ENTRY*)tmp;
}
void DSLR_LOCK::LOCK_INIT_DSM(DSM* x) {
    dsm = x;
}
DSLR_LOCK::DSLR_LOCK(GlobalAddress addr) {
    MNlock = addr;
}
void DSLR_LOCK::lock(bool mode, CoroPull* sink) {
    curMode = mode;
    while (!inner_lock(sink))
        ;
}
bool DSLR_LOCK::inner_lock(CoroPull* sink) {
    resetFrom.clear();
    if (curMode == ModeShare) {
        DSLR_ENTRY* prev = addMs(sink);
        if (prev->nx == prev->mx)
            return true;
        else {
            bool ts = handleConflict(*prev, sink);

            return ts;
        }
    } else {
        DSLR_ENTRY* prev = addMx(sink);
        if (prev->nx == prev->mx && prev->ms == prev->ns)
            return true;
        else {
            bool ts = handleConflict(*prev, sink);

            return ts;
        }
    }
}
void DSLR_LOCK::unlock(CoroPull* sink) {
    // fault tolerance
    if (curMode == ModeShare) {
        addNs(sink);
    } else {
        addNx(sink);
    }
}
bool DSLR_LOCK::handleConflict(DSLR_ENTRY prev, CoroPull* sink) {
    while (1) {
        DSLR_ENTRY* val = read(sink);
        if (curMode == ModeShare) {
            if (prev.mx == val->nx)
                return true;
        } else {
            if (prev.mx == val->nx && prev.ms == val->ns)
                return true;
        }
        // fault tolerance
        int wait_count = ((prev.mx - val->nx) + (prev.ms - val->ns));
        usleep(wait_count * 5);
    }
    assert(0);
    return 0;
}