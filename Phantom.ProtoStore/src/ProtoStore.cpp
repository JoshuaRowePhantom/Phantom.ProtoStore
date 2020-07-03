#include "ProtoStore.h"
#include "StandardTypes.h"
#include "MessageStore.h"
#include "RandomMessageAccessor.h"
#include "HeaderAccessor.h"

namespace Phantom::ProtoStore
{

ProtoStore::ProtoStore(
    shared_ptr<IExtentStore> extentStore)
    :
    m_extentStore(move(extentStore)),
    m_messageStore(MakeMessageStore(m_extentStore)),
    m_messageAccessor(MakeRandomMessageAccessor(m_messageStore)),
    m_headerAccessor(MakeHeaderAccessor(m_messageAccessor))
{
}

task<> ProtoStore::Create(
    const CreateProtoStoreRequest& createRequest)
{
    Header header;
    header.set_version(1);
    header.set_epoch(1);
    header.set_logalignment(
        createRequest.LogAlignment);
    header.set_logreplayextentnumber(2);
    header.set_logreplaylocation(0);

    co_await m_headerAccessor->WriteHeader(
        header);

    co_await Open(
        createRequest);
}

task<> ProtoStore::Open(
    const OpenProtoStoreRequest& openRequest)
{
    Header header;
    co_await m_headerAccessor->ReadHeader(
        header);
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