#include "ProtoStore.h"
#include "StandardTypes.h"
#include "MessageStore.h"
#include "RandomMessageAccessor.h"
#include "HeaderAccessor.h"
#include "ProtoStoreInternal.pb.h"

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

task<OperationResult> ProtoStore::ExecuteOperation(
    const BeginTransactionRequest beginRequest,
    OperationVisitor visitor
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

task<ReadResult> ProtoStore::Read(
    ReadRequest& readRequest
)
{
    throw 0;
}

}