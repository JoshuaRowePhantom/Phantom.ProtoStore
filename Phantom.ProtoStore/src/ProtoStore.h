#pragma once

#include <Phantom.ProtoStore/Phantom.ProtoStore.h>
#include "StandardTypes.h"

namespace Phantom::ProtoStore
{

class ProtoStore
    :
    public IProtoStore
{
    const shared_ptr<IExtentStore> m_extentStore;
    const shared_ptr<IMessageStore> m_messageStore;
    const shared_ptr<IRandomMessageAccessor> m_messageAccessor;
    const shared_ptr<IHeaderAccessor> m_headerAccessor;

public:
    ProtoStore(
        shared_ptr<IExtentStore> extentStore);

    task<> Open(
        const OpenProtoStoreRequest& openRequest
    );

    task<> Create(
        const CreateProtoStoreRequest& openRequest
    );

    virtual task<OperationResult> ExecuteOperation(
        const BeginTransactionRequest beginRequest,
        OperationVisitor visitor
    ) override;

    virtual task<ProtoIndex> GetIndex(
        const GetIndexRequest& getIndexRequest
    ) override;

    virtual task<ReadResult> Read(
        ReadRequest& readRequest
    ) override;
};
}