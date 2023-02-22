#include "RandomMessageAccessorImpl.h"
#include "MessageStore.h"

namespace Phantom::ProtoStore
{
RandomMessageAccessor::RandomMessageAccessor(
    shared_ptr<IMessageStore> messageStore)
    :
    m_messageStore(move(messageStore))
{}

task<DataReference<StoredMessage>> RandomMessageAccessor::ReadMessage(
    ExtentLocation location
)
{
    auto extent = co_await m_messageStore->OpenExtentForRandomReadAccess(
        location.extentName);

    co_return co_await extent->Read(
        location.extentOffset);
}

task<> RandomMessageAccessor::ReadMessage(
    ExtentLocation location,
    Message& message
)
{
    auto extent = co_await m_messageStore->OpenExtentForRandomReadAccess(
        location.extentName);

    co_await extent->Read(
        location.extentOffset,
        message);
}

task<DataReference<StoredMessage>> RandomMessageAccessor::WriteMessage(
    ExtentLocation location,
    const StoredMessage& message,
    FlushBehavior flushBehavior
)
{
    auto extent = co_await m_messageStore->OpenExtentForRandomWriteAccess(
        location.extentName);

    co_return co_await extent->Write(
        location.extentOffset,
        message,
        flushBehavior);
}

task<> RandomMessageAccessor::WriteMessage(
    ExtentLocation location,
    const Message& message,
    FlushBehavior flushBehavior
)
{
    auto extent = co_await m_messageStore->OpenExtentForRandomWriteAccess(
        location.extentName);

    co_await extent->Write(
        location.extentOffset,
        message,
        flushBehavior);
}


shared_ptr<IRandomMessageAccessor> MakeRandomMessageAccessor(
    shared_ptr<IMessageStore> messageStore)
{
    return make_shared<RandomMessageAccessor>(
        messageStore);
}
}
