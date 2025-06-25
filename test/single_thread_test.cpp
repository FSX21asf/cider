#include <iostream>
#include "DSM.h"
#include "MiniHash.h"
#include "QueueLock.h"

#define TEST_NUM 102400  // 102400

int main() {
    DSMConfig config;
    config.machineNR = 2;
    assert(MEMORY_NODE_NUM == 1);
    DSM* dsm = DSM::getInstance(config);

    dsm->registerThread();
    MCS_WC_LOCK::LOCK_INIT_DSM(dsm);
    MCS_WC_LOCK::LOCK_INIT_ALLOC();
    MCS_WC_VLOCK::LOCK_INIT_DSM(dsm);
    MCS_WC_VLOCK::LOCK_INIT_ALLOC();
    auto tree = new RaceHash(dsm);

    Value v;

    if (dsm->getMyNodeID() != 0) {
        dsm->barrier("fin");
        return 0;
    }
    int ret;
    // test insert
    for (uint64_t i = 1; i <= TEST_NUM; ++i) {
        printf("inserting %lu...\n", i);
        ret = tree->insert(int2key(i), i * 2, ECPolicy::retval, 0);
        assert(ret);
    }
    printf("insert passed.\n");

    // test update
    for (uint64_t i = TEST_NUM; i >= 1; --i) {
        // printf("updating %lu...\n", i);
        ret = tree->insert(int2key(i), i * 3, ECPolicy::InsUdp, 0);
        assert(ret);
    }
    printf("update passed.\n");

    // test search
    for (uint64_t i = 1; i <= TEST_NUM; ++i) {
        assert(!tree->search(int2key(TEST_NUM + i), v));
    }
    for (uint64_t i = 1; i <= TEST_NUM; ++i) {
        auto res = tree->search(int2key(i), v);
        // std::cout << "search result:  " << (bool)res << " v: " << v << " ans:
        // " << i * 3 << std::endl;
        assert(res && v == i * 3);
        // assert(res && v == i * 2);
    }
    printf("search passed.\n");

    // test delete
    for (uint64_t i = TEST_NUM; i >= 1; i -= 2) {
        // printf("updating %lu...\n", i);
        ret = tree->deleteItem(int2key(i), ECPolicy::retval, 0);
        assert(ret);
    }
    // test search
    for (uint64_t i = 1; i <= TEST_NUM; i += 2) {
        assert(tree->search(int2key(i), v));
    }
    // test search
    for (uint64_t i = 2; i <= TEST_NUM; i += 2) {
        assert(!tree->search(int2key(i), v));
    }
    printf("delete passed.\n");

    // test insert
    for (uint64_t i = 1; i <= TEST_NUM; ++i) {
        printf("inserting %lu...\n", i);
        ret = tree->insert(int2key(i), i * 2, ECPolicy::InsUdp, 0);
    }
    printf("insert passed.\n");

    for (uint64_t i = 1; i <= TEST_NUM; ++i) {
        auto res = tree->search(int2key(i), v);
        // std::cout << "search result:  " << (bool)res << " v: " << v << " ans:
        // " << i * 3 << std::endl;
        assert(res && v == i * 2);
        // assert(res && v == i * 2);
    }
    printf("search passed.\n");

    printf("Hello!\n");
    dsm->barrier("fin");
    return 0;
}