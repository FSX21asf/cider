#include <endian.h>
#include "Common.h"
#include "DSM.h"
#include "GlobalAddress.h"
#include "QueueLock.h"

uint64_t cas_lock_fail[MAX_APP_THREAD];

DSM* CAS_LOCK::dsm;
DSM* PERFECT_CAS_LOCK::dsm;
DSM* CAS_BF_LOCK::dsm;

void CAS_LOCK::LOCK_INIT_DSM(DSM* x) {
    dsm = x;
}
void PERFECT_CAS_LOCK::LOCK_INIT_DSM(DSM* x) {
    dsm = x;
}
void CAS_BF_LOCK::LOCK_INIT_DSM(DSM* x) {
    dsm = x;
}

// CAS_LOCK
CAS_LOCK::CAS_LOCK(GlobalAddress addr) {
    assert(dsm && dsm->is_register());
    MNlock = addr;
}
void CAS_LOCK::lock(CoroPull* sink) {
    uint64_t* tmp = (dsm->get_rbuf(sink)).get_cas_buffer();
    uint64_t val = 1;
    while (!dsm->cas_sync(MNlock, 0, val, tmp, sink)) {
        cas_lock_fail[dsm->getMyThreadID()]++;
    }
}
void CAS_LOCK::unlock(CoroPull* sink) {
    uint64_t* tmp = (dsm->get_rbuf(sink)).get_cas_buffer();
    *tmp = 0;
    dsm->write_sync((char*)tmp, MNlock, sizeof(uint64_t), sink);
}
// PERFECT_CAS_LOCK
PERFECT_CAS_LOCK::PERFECT_CAS_LOCK(GlobalAddress addr) {
    assert(dsm && dsm->is_register());
    MNlock = addr;
}
void PERFECT_CAS_LOCK::lock(CoroPull* sink) {
    uint64_t* tmp = (dsm->get_rbuf(sink)).get_cas_buffer();
    uint64_t val = 1;
    dsm->cas_sync(MNlock, 0, val, tmp, sink);
}
void PERFECT_CAS_LOCK::unlock(CoroPull* sink) {
    uint64_t* tmp = (dsm->get_rbuf(sink)).get_cas_buffer();
    *tmp = 0;
    dsm->write_sync((char*)tmp, MNlock, sizeof(uint64_t), sink);
}
// CAS_BF_LOCK
CAS_BF_LOCK::CAS_BF_LOCK(GlobalAddress addr) {
    assert(dsm && dsm->is_register());
    MNlock = addr;
}
void CAS_BF_LOCK::lock(CoroPull* sink) {
    uint64_t* tmp = (dsm->get_rbuf(sink)).get_cas_buffer();
    uint64_t val = 1;
    int cur = T0;
    while (!dsm->cas_sync(MNlock, 0, val, tmp, sink)) {
        cas_lock_fail[dsm->getMyThreadID()]++;
        usleep(std::min(cur, Tmax) + rand() % cur);
        cur = std::min(cur * 2, Tmax);
    }
}
void CAS_BF_LOCK::unlock(CoroPull* sink) {
    uint64_t* tmp = (dsm->get_rbuf(sink)).get_cas_buffer();
    *tmp = 0;
    dsm->write_sync((char*)tmp, MNlock, sizeof(uint64_t), sink);
}