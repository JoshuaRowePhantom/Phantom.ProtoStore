#include "RandomMessageAccessorImpl.h"
#include "MessageStore.h"

namespace Phantom::ProtoStore
{
RandomMessageAccessor::RandomMessageAccessor(
    shared_ptr<IMessageStore> messageStore)
    :
    m_messageStore(move(messageStore))
{}

task<> RandomMessageAccessor::ReadMessage(
    ExtentLocation location,
    Message& message
)
{
    auto extent = co_await m_messageStore->OpenExtentForRandomReadAccess(
        location.extentNumber);

    co_await extent->Read(
        location.extentOffset,
        message);
}

task<> RandomMessageAccessor::WriteMessage(
    ExtentLocation location,
    const Message& message
)
{
    auto extent = co_await m_messageStore->OpenExtentForRandomWriteAccess(
        location.extentNumber);

    co_await extent->Write(
        location.extentOffset,
        message);
}


shared_ptr<IRandomMessageAccessor> MakeRandomMessageAccessor(
    shared_ptr<IMessageStore> messageStore)
{
    return make_shared<RandomMessageAccessor>(
        messageStore);
}
}
