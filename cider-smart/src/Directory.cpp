#include "Directory.h"
#include "Common.h"
#include "Connection.h"
#include "DataPointer.h"

// #include <gperftools/profiler.h>

GlobalAddress g_root_ptr = GlobalAddress::Null();
int g_root_level = -1;
bool enable_cache;

Directory::Directory(DirectoryConnection* dCon,
                     RemoteConnection* remoteInfo,
                     uint32_t machineNR,
                     uint16_t dirID,
                     uint16_t nodeID)
    : dCon(dCon),
      remoteInfo(remoteInfo),
      machineNR(machineNR),
      dirID(dirID),
      nodeID(nodeID),
      dirTh(nullptr) {
    {  // chunck alloctor
        GlobalAddress dsm_start;
        uint64_t per_directory_dsm_size = dCon->dsmSize / NR_DIRECTORY;
        dsm_start.nodeID = nodeID;
        dsm_start.offset = per_directory_dsm_size * dirID;
        constexpr uint64_t reserveSize =
            define::kReserveChunk * define::kChunkSize;
        uint64_t directory_dsm_size = per_directory_dsm_size;
        if (dirID == 0) {
            dsm_start.offset += reserveSize;
            directory_dsm_size -= reserveSize;
        }
        chunckAlloc = new GlobalAllocator(dsm_start, per_directory_dsm_size);
    }

    dirTh = new std::thread(&Directory::dirThread, this);
}

Directory::~Directory() {
    delete chunckAlloc;
}

void Directory::dirThread() {
    bindCore((CPU_PHYSICAL_CORE_NUM - 1 - dirID) * 2 +
             1);  // bind to the last CPU core
    Debug::notifyInfo("dir %d launch!\n", dirID);

    while (true) {
        struct ibv_wc wc;
        pollWithCQ(dCon->cq, 1, &wc);

        switch (int(wc.opcode)) {
            case IBV_WC_RECV:  // control message
            {
                auto* m = (RawMessage*)dCon->message->getMessage();

                process_message(m);

                break;
            }
            case IBV_WC_RDMA_WRITE: {
                break;
            }
            case IBV_WC_RECV_RDMA_WITH_IMM: {
                break;
            }
            default:
                assert(false);
        }
    }
}

void Directory::process_message(const RawMessage* m) {
    RawMessage* send = nullptr;
    switch (m->type) {
        case RpcType::MALLOC: {
            send = (RawMessage*)dCon->message->getSendPool();

            send->addr = chunckAlloc->alloc_chunck();
            break;
        }

        case RpcType::NEW_ROOT: {
            if (g_root_level < m->level) {
                g_root_ptr = m->addr;
                g_root_level = m->level;
                // if (g_root_level >= 3) {
                //   enable_cache = true;
                // }
            }

            break;
        }
        case RpcType::FAULT_REPORT: {
            assert(m->addr.nodeID == nodeID);
            uint64_t addr = m->addr.offset + (uint64_t)(dCon->dsmPool);
            if (ftmp.count(addr) && (ftmp[addr] >= m->relcnt &&
                                     ftmp[addr] - m->relcnt < 1000000ull)) {
                return;
            }
            ftmp[addr] = m->relcnt;
            std::atomic<uint64_t>* entry =
                reinterpret_cast<std::atomic<uint64_t>*>(addr);
            std::atomic<uint64_t>* relcnt =
                reinterpret_cast<std::atomic<uint64_t>*>(addr + 8);
            entry->store(0);
            relcnt->fetch_add(1000000);
            printf("recover %lx with relcnt %lx\n", addr, m->relcnt);
            break;
        }
        case RpcType::FAULT_REPORTV: {
            assert(m->addr.nodeID == nodeID);
            uint64_t addr = m->addr.offset + (uint64_t)(dCon->dsmPool);
            uint64_t dpaddr = m->dptraddr.offset + (uint64_t)(dCon->dsmPool);
            if (ftmp.count(addr) && (ftmp[addr] >= m->relcnt &&
                                     ftmp[addr] - m->relcnt < 1000000ull)) {
                return;
            }
            ftmp[addr] = m->relcnt;
            DATA_POINTER dp = *(reinterpret_cast<DATA_POINTER*>(dpaddr));
            dp.nodeID = 0, dp.offset = 0;
            std::atomic<uint64_t>* entry =
                reinterpret_cast<std::atomic<uint64_t>*>(addr);
            std::atomic<uint64_t>* relcnt =
                reinterpret_cast<std::atomic<uint64_t>*>(addr + 8);
            entry->store(dp.to_uint64());  // set the same version as dataptr
            relcnt->fetch_add(1000000);
            printf("recover %lx with relcnt %lx\n", addr, m->relcnt);
            break;
        }
        default:
            assert(false);
    }

    if (send) {
        dCon->sendMessage2App(send, m->node_id, m->app_id);
    }
}
