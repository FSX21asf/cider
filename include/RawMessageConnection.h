#ifndef __RAWMESSAGECONNECTION_H__
#define __RAWMESSAGECONNECTION_H__

#include "AbstractMessageConnection.h"
#include "GlobalAddress.h"

#include <thread>

enum RpcType : uint8_t {
    MALLOC,
    FREE,
    FAULT_REPORT,
    FAULT_REPORTV,
    NEW_ROOT,
    NOP,
};

struct RawMessage {
    RpcType type;

    uint16_t node_id;
    uint16_t app_id;

    GlobalAddress addr;  // for malloc
    GlobalAddress dptraddr;
    uint64_t relcnt;
    int level;
} __attribute__((packed));

class RawMessageConnection : public AbstractMessageConnection {
   public:
    RawMessageConnection(RdmaContext& ctx, ibv_cq* cq, uint32_t messageNR);

    void initSend();
    void sendRawMessage(RawMessage* m, uint32_t remoteQPN, ibv_ah* ah);
};

#endif /* __RAWMESSAGECONNECTION_H__ */
