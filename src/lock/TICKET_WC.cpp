#include <endian.h>
#include "Common.h"
#include "DSM.h"
#include "GlobalAddress.h"
#include "QueueLock.h"

DSM* TK_WC_LOCK::dsm;
// uint64_t nx : 21;
// uint64_t mx : 21;
// uint64_t ex : 21;
// uint64_t wc_prepare : 1;

TK_WC_CONTENT* TK_WC_LOCK::addMx(CoroPull* sink) {
    uint64_t* tmp = (dsm->get_rbuf(sink)).get_cas_buffer();
    dsm->faa_boundary_sync(MNlock, (1ll << 21ll), tmp, 41, sink);
    return (TK_WC_CONTENT*)tmp;
}

TK_WC_CONTENT* TK_WC_LOCK::read(CoroPull* sink) {
    uint64_t* tmp = (dsm->get_rbuf(sink)).get_cas_buffer();
    dsm->read_sync((char*)tmp, MNlock, 8, sink);
    return (TK_WC_CONTENT*)tmp;
}
void TK_WC_LOCK::LOCK_INIT_DSM(DSM* x) {
    dsm = x;
}
TK_WC_LOCK::TK_WC_LOCK(GlobalAddress addr) {
    MNlock = addr;
}
bool TK_WC_LOCK::lock(CoroPull* sink) {
    TK_WC_CONTENT tc = *addMx(sink);
    if (tc.nx == tc.mx) {
        addVal = 1;
        return true;
    }
    myTicket = tc.mx;
    while (1) {
        tc = *read(sink);
        if (myTicket == tc.nx) {
            if (tc.mx - myTicket < 2) {
                addVal = 1;
                return true;
            } else {
                // CAllWC
                uint64_t* tmp = dsm->get_rbuf(sink).get_cas_buffer();
                TK_WC_CONTENT wcc;

                wcc.ex = tc.mx - 1ull;
                wcc.wc_prepare = 1;
                // printf("lock %lx wccex %lx\n", MNlock.to_uint64(), wcc.ex);
                dsm->cas_mask_sync(MNlock, 0, wcc.to_u64(), tmp, 0,
                                   (~0x3ffffffffffull), sink);
                // tc = *read(sink);
                // printf("%lx tc %lx %lx %lx %lx\n", MNlock.to_uint64(), tc.mx,
                //        tc.nx, tc.ex, tc.wc_prepare);
                while (1) {
                    usleep(T0);
                    tc = *read(sink);
                    if (tc.nx - myTicket < 1000ull &&
                        tc.nx != myTicket) {  // combined
                        return false;
                    }
                }
            }
        } else if (myTicket == tc.ex && tc.wc_prepare) {
            addVal = myTicket + 1ull - tc.nx;
            // printf("WC excute\n");
            return true;
        } else if (tc.nx - myTicket < 1000ull) {  // combined
            // printf("combined ok!\n");
            return false;
        }
        // fault tolerance
        usleep(T0);
    }
}

void TK_WC_LOCK::unlock(CoroPull* sink) {
    uint64_t* tmp = (dsm->get_rbuf(sink)).get_cas_buffer();
    if (addVal == 1) {
        dsm->faa_boundary_sync(MNlock, addVal, tmp, 20, sink);
    } else {
        // printf("addval:%lx\n", addVal);
        // TK_WC_CONTENT tc = *read(sink);
        // printf("%lx tc %lx %lx %lx %lx\n", MNlock.to_uint64(), tc.mx, tc.nx,
        //        tc.ex, tc.wc_prepare);
        dsm->faa_field_boundary_sync(MNlock, addVal | (1ull << 63), tmp,
                                     (1ull << 20) | (1ull << 63), sink);
        // tc = *read(sink);
        // printf("%lx tc %lx %lx %lx %lx\n", MNlock.to_uint64(), tc.mx, tc.nx,
        //        tc.ex, tc.wc_prepare);
        // reset wc_prepare
    }
}
