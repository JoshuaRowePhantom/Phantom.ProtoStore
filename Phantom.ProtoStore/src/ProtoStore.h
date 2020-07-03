#pragma once

#include <Phantom.ProtoStore/include/Phantom.ProtoStore.h>
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

    virtual task<BeginTransactionResult> BeginTransaction(
        const BeginTransactionRequest beginRequest
    ) override;

    virtual task<CommitTransactionResult> CommitTransaction(
        const CommitTransactionRequest& commitTransactionRequest
    ) override;

    virtual task<AbortTransactionResult> AbortTransaction(
        const AbortTransactionRequest& abortTransactionRequest
    ) override;

    virtual task<ProtoIndex> CreateIndex(
        const CreateIndexRequest& createIndexRequest
    ) override;

    virtual task<ProtoIndex> GetIndex(
        const GetIndexRequest& getIndexRequest
    ) override;

    virtual task<void> Write(
        const WriteRequest& writeRequest
    ) override;

    virtual task<ReadResult> Read(
        const ReadRequest& readRequest
    ) override;
};
}