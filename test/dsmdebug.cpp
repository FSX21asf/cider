#include "DSM.h"

#include <iostream>
#include "QueueLock.h"
#include "infiniband/verbs_exp.h"
#define TEST_NUM 102400  // 102400
DSM* dsm;
GlobalAddress target;
void dumpchar(unsigned char c) {
    int t[20] = {0}, p = 0;
    while (c) {
        t[++p] = c % 2;
        c /= 2;
    }
    for (int i = 1; i <= 8; i++)
        printf("%d", t[i]);
    printf(" ");
}
void readBuffer() {
    char* bf = (char*)dsm->get_rbuf(0).get_KV_buffer();
    dsm->read_sync(bf, target, 32, 0);
    for (int i = 0; i < 16; i++)
        dumpchar(bf[i]);
    printf("\n");
}
void dumpdslr(DSLR_ENTRY* e) {
    printf("nx %d  ns %d mx %d ms %d\n", e->nx, e->ns, e->mx, e->ms);
}

int main() {
    DSMConfig config;
    config.machineNR = 1;
    assert(MEMORY_NODE_NUM == 1);
    dsm = DSM::getInstance(config);
    dsm->registerThread();

    target = dsm->alloc(32);
    TK_WC_LOCK::LOCK_INIT_DSM(dsm);
    uint64_t* tmp = (dsm->get_rbuf(0)).get_cas_buffer();
    TK_WC_CONTENT wcc;
    wcc.ex = 0x1fffff;
    wcc.wc_prepare = 1;
    dsm->cas_mask_sync(target, 0, wcc.to_u64(), tmp, 0, (~0x3ffffffffffull), 0);
    dsm->read_sync((char*)tmp, target, 8, 0);
    wcc = *(TK_WC_CONTENT*)tmp;
    printf("%lx %lx\n", wcc.ex, wcc.wc_prepare);
    // uint64_t cmpv[2] = {0}, cmpm[2] = {0}, swpm[2] = {0, 0},
    //          swpv[2] = {0x12345678, 0x2132131321ull};
    // uint64_t* tmp = (uint64_t*)(dsm->get_rbuf(0)).get_entry_buffer();
    // tmp[0] = 0xffffffffffffffffull, tmp[1] = 0xffffffffffffffffull;
    // dsm->write_sync((char*)tmp, target, 16, 0);
    // readBuffer();
    // dsm->cas128_mask_sync(target, cmpv, swpv, tmp, cmpm, swpm, 0);
    // readBuffer();
    // printf("%lx", tmp[0]);
    // tmp[0] = 0x12345678, tmp[1] = 0x23456789;
    // dsm->write_sync((char*)tmp, target, 16, 0);
    // dsm->cas_mask_sync(target, 0ull, 0x12345678, tmp, 0ull, ~(0ull), 0);
    // printf("%lx", tmp[0]);

    // GlobalAddress MNlock_high = target;
    // MNlock_high.offset += 8;
    // dsm->faa_boundary(MNlock_high, 1, tmp, 63, false, 0);
    // readBuffer();

    // uint64_t* bf = (uint64_t*)dsm->get_rbuf(0).get_KV_buffer();
    // dsm->faa_boundary_sync(target, 1, bf, 63, 0);
    // dsm->read_sync((char*)bf, target, 8, 0);
    // printf("%lx %lx\n", *bf, __bswap_64(*bf));
    // readBuffer();

    // bf[0] = 0x1234567812345678ull;
    // bf[1] = 0;
    // dsm->write_sync((char*)bf, target, 16, 0);
    // readBuffer();
    // dsm->fault_report(target, 2);
    // while (1) {
    //     dsm->read_sync((char*)bf, target, 16, 0);
    //     if (bf[1] > 10000) {
    //         readBuffer();
    //         break;
    //     }
    //     usleep(1000);
    // }
    // bf[0] = 0x1234567812345678ull;
    // bf[1] = 0;
    // dsm->write_sync((char*)bf, target, 16, 0);
    // dsm->fault_report(target, 2);
    // sleep(1);
    // readBuffer();

    // dsm->fault_report(target, 3);
    // while (1) {
    //     dsm->read_sync((char*)bf, target, 16, 0);
    //     if (bf[1] > 10000) {
    //         readBuffer();
    //         break;
    //     }
    //     usleep(1000);
    // }
    // for (int i = 0; i < 4; i++) {
    //     uint64_t addv[2] = {0}, addm[2] = {0};
    //     addv[0] = 1;
    //     dsm->faa128_boundray_sync(target, addv, bf, addm, 0);
    //     printf("%lx %lx\n", bf[0], bf[1]);
    // }

    // uint64_t* xx = dsm->get_rbuf(0).get_cas_buffer();
    // *xx = 0x123456789ull;
    // dsm->cas_sync(target, 0, 0x123456789ull, xx, 0);
    // readBuffer();
    // auto t = target;
    // t.offset += 8;
    // dsm->cas_sync(t, 0, 0x987654321ull, xx, 0);
    // printf("rb: %lx\n", *xx);

    // uint64_t eql[2] = {0}, val[2] = {0};
    // eql[0] = 0x123456789ull;
    // uint64_t cmpm[2] = {0}, swpm[2] = {0};
    // cmpm[0] = (~0ull);
    // swpm[0] = swpm[1] = (~0ull);
    // uint64_t* tmp = (uint64_t*)(dsm->get_rbuf(0)).get_entry_buffer();
    // dsm->read_sync((char*)tmp, target, 16, 0);
    // printf("read: %lx %lx\n", tmp[0], tmp[1]);
    // if (dsm->cas128_mask_sync(target, eql, val, tmp, cmpm, swpm, 0))
    //     ;
    // printf("rb: %lx %lx\n", tmp[0], tmp[1]);
    // dsm->read_sync((char*)tmp, target, 16, 0);
    // printf("read: %lx %lx\n", tmp[0], tmp[1]);

    // char addv[32], mk[32];
    // char* bf = dsm->get_rbuf(0).get_KV_buffer();
    // memset(addv, 0, 32);
    // memset(mk, 0, 32);
    // mk[0] = 1;
    // mk[8] = 1;
    // addv[0] = 1;
    // addv[8] = 1;
    // for (int i = 0; i < 3; i++) {
    //     dsm->faa128_boundray_sync(target, (uint64_t*)addv, (uint64_t*)bf,
    //                               (uint64_t*)mk, 0);
    //     readBuffer();
    // }

    // ibv_exp_device_attr atr;
    // ibv_exp_query_device(dsm->getibvcontext(), &atr);
    // printf("%d\n", atr.ext_atom.max_fa_bit_boundary);  // 64
    // // DSLR_LOCK::LOCK_INIT_DSM(dsm);
    // target = dsm->alloc(32);
    // volatile char* target_local =
    //     (char*)(dsm->getRDMADirOffset() + target.offset);
    // readBuffer();
    // char* eql = dsm->get_rbuf(0).get_KV_buffer();
    // memset(eql, 0, 32);
    // char* val = dsm->get_rbuf(0).get_KV_buffer();
    // memset(val, 0xff, 8);
    // char* v1 = dsm->get_rbuf(0).get_KV_buffer();
    // v1[0] = 0xf;
    // v1[1] = 0xf;
    // uint64_t* bf = (uint64_t*)dsm->get_rbuf(0).get_KV_buffer();
    // dsm->cas128_mask_sync(target, (uint64_t*)eql, (uint64_t*)v1,
    // (uint64_t*)bf,
    //                       (uint64_t*)val, (uint64_t*)val, 0);
    // readBuffer();
    // dsm->cas128_mask_sync(target, (uint64_t*)v1, (uint64_t*)eql,
    // (uint64_t*)bf,
    //                       (uint64_t*)val, (uint64_t*)val, 0);
    // printf("bf:%lx %lx\n", bf[0], bf[1]);
    // readBuffer();

    // for (int i = 0; i < 1000000; i++) {
    //     DSLR_LOCK lk(target);
    //     lk.lock(lk.ModeExclusive, 0);
    //     lk.unlock(0);
    // }

    // DSLR_ENTRY* e = lk.read(0);
    // dumpdslr(e);

    // lk.addMs(0);
    // e = lk.read(0);
    // dumpdslr(e);
    // readBuffer();
    // lk.addMx(0);
    // e = lk.read(0);
    // dumpdslr(e);
    // readBuffer();

    // lk.addMs(0);
    // e = lk.read(0);
    // dumpdslr(e);
    // readBuffer();
    // lk.addMx(0);
    // e = lk.read(0);
    // dumpdslr(e);
    // readBuffer();

    // lk.addNs(0);
    // e = lk.read(0);
    // dumpdslr(e);
    // readBuffer();
    // lk.addNx(0);
    // e = lk.read(0);
    // dumpdslr(e);
    // readBuffer();

    // lk.minusMs(0);
    // e = lk.read(0);
    // dumpdslr(e);
    // readBuffer();

    // lk.minusMx(0);
    // e = lk.read(0);
    // dumpdslr(e);
    // readBuffer();
    // readBuffer();
    // for (int i = 0; i < 20; i++) {
    //     auto tg = target;
    //     tg.offset += 0;
    //     char* bf = dsm->get_rbuf(0).get_KV_buffer();
    //     uint64_t tmp = 1 << 30;
    //     // dsm->faa_boundary_sync(tg, tmp, (uint64_t*)bf, 31, 0);
    //     dsm->ext_faa_boundray_sync(tg, tmp, (uint64_t*)bf, 31, 3, 0);
    //     readBuffer();
    // }
    // for (int i = 0; i < 20; i++) {
    //     char* bf = dsm->get_rbuf(0).get_KV_buffer();
    //     dsm->read_sync(bf, target, 8, 0);
    //     printf("read: ");
    //     for (int j = 0; j < 8; j++) {
    //         dumpchar(*(bf + j));
    //     }
    //     printf("\n");
    //     printf("local: ");
    //     for (int j = 0; j < 8; j++) {
    //         dumpchar(*(target_local + j));
    //     }
    //     printf("\n");
    //     uint32_t* cur = (uint32_t*)target_local;
    //     uint32_t* nxt = cur + 1;

    //     printf("cur %x, nxt %x\n", *cur, *nxt);

    //     uint64_t* cf = dsm->get_rbuf(0).get_cas_buffer();
    //     dsm->faa_boundary_sync(target, 1, cf, 2, 0);
    //     printf("faa ret: %lx\n", *cf);
    // }
    // uint64_t* tmp = (dsm->get_rbuf(0)).get_cas_buffer();
    // uint64_t val = 0;
    // *(((uint32_t*)&val) + 1) = 100;
    // const uint64_t mask = 0xffffffff00000000ll;
    // dsm->cas_mask_sync(target, 0, val, tmp, 0, mask, 0);
    // uint32_t* cur = (uint32_t*)target_local;
    // uint32_t* nxt = cur + 1;

    // printf("cur %x, nxt %x\n", *cur, *nxt);
    return 0;
}