#include "ProtoStore.h"
#include "StandardTypes.h"

namespace Phantom::ProtoStore
{

ProtoStore::ProtoStore(
    shared_ptr<IExtentStore> extentStore)
    :
    m_extentStore(move(extentStore))
{
}

task<> ProtoStore::Open(
    const OpenProtoStoreRequest& openRequest)
{
    co_return;
}

task<> ProtoStore::Create(
    const CreateProtoStoreRequest& createRequest)
{
    co_return;
}

task<BeginTransactionResult> ProtoStore::BeginTransaction(
    const BeginTransactionRequest beginRequest)
{
    return task<BeginTransactionResult>();
}

task<CommitTransactionResult> ProtoStore::CommitTransaction(
    const CommitTransactionRequest& commitTransactionRequest
)
{
    throw 0;
}

task<AbortTransactionResult> ProtoStore::AbortTransaction(
    const AbortTransactionRequest& abortTransactionRequest
)
{
    throw 0;
}

task<ProtoIndex> ProtoStore::CreateIndex(
    const CreateIndexRequest& createIndexRequest
)
{
    throw 0;
}

task<ProtoIndex> ProtoStore::GetIndex(
    const GetIndexRequest& getIndexRequest
)
{
    throw 0;
}

task<> ProtoStore::Write(
    const WriteRequest& writeRequest
)
{
    throw 0;
}

task<ReadResult> ProtoStore::Read(
    const ReadRequest& readRequest
)
{
    throw 0;
}

}