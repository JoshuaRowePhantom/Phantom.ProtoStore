#include "HeaderAccessorImpl.h"
#include "RandomMessageAccessor.h"
#include "src/ProtoStoreInternal_generated.h"
#include "ExtentName.h"

namespace Phantom::ProtoStore
{

static ExtentLocation MakeDefaultHeaderLocation(
    google::protobuf::uint64 copyNumber)
{
    ExtentLocation extentLocation;
    extentLocation.extentOffset = 0;
    extentLocation.extentName = MakeDatabaseHeaderExtentName(copyNumber);
    return extentLocation;
}

const ExtentLocation DefaultHeaderLocation1 = MakeDefaultHeaderLocation(0);
const ExtentLocation DefaultHeaderLocation2 = MakeDefaultHeaderLocation(1);

HeaderAccessor::HeaderAccessor(
    shared_ptr<IRandomMessageAccessor> messageAccessor,
    ExtentLocation headerLocation1,
    ExtentLocation headerLocation2
)
    :
    m_messageAccessor(move(messageAccessor)),
    m_headerLocation1(headerLocation1),
    m_headerLocation2(headerLocation2),
    m_currentLocation(headerLocation1),
    m_nextLocation(headerLocation2)
{
}

task<std::unique_ptr<FlatBuffers::DatabaseHeaderT>> HeaderAccessor::ReadHeader(
    ExtentLocation location,
    bool throwOnError)
{
    try
    {
        auto storedMessage = co_await m_messageAccessor->ReadMessage(
            location);

        FlatMessage<FlatBuffers::DatabaseHeader> headerMessage
        {
            std::move(storedMessage) 
        };

        co_return headerMessage->UnPack();
    }
    catch (...)
    {
        if (throwOnError)
        {
            throw;
        }

        co_return nullptr;
    }
}

task<std::unique_ptr<FlatBuffers::DatabaseHeaderT>> HeaderAccessor::ReadHeader()
{
    auto location1Header = co_await ReadHeader(
        m_headerLocation1,
        false);

    if (!location1Header)
    {
        m_currentLocation = m_headerLocation2;
        m_nextLocation = m_headerLocation1;

        co_return co_await ReadHeader(
            m_headerLocation2,
            true);
    }

    auto location2Header = co_await ReadHeader(
        m_headerLocation2,
        false);

    if (!location2Header)
    {
        m_currentLocation = m_headerLocation1;
        m_nextLocation = m_headerLocation2;

        co_return std::move(location1Header);
    }

    if (location1Header->epoch > location2Header->epoch)
    {
        m_currentLocation = m_headerLocation1;
        m_nextLocation = m_headerLocation2;

        co_return std::move(location1Header);
    }
    else
    {
        m_currentLocation = m_headerLocation2;
        m_nextLocation = m_headerLocation1;

        co_return std::move(location2Header);
    }
}

task<> HeaderAccessor::WriteHeader(
    const FlatBuffers::DatabaseHeaderT* header)
{
    flatbuffers::FlatBufferBuilder builder;
    builder.Finish(
        FlatBuffers::DatabaseHeader::Pack(
            builder,
            header));

    FlatMessage<FlatBuffers::DatabaseHeader> databaseHeader{ builder };

    auto result = co_await m_messageAccessor->WriteMessage(
        m_nextLocation,
        databaseHeader.data(),
        FlushBehavior::Flush);

    std::swap(
        m_currentLocation,
        m_nextLocation);
}

shared_ptr<IHeaderAccessor> MakeHeaderAccessor(
    shared_ptr<IRandomMessageAccessor> messageAccessor)
{
    return MakeHeaderAccessor(
        move(messageAccessor),
        MakeDefaultHeaderLocation(0),
        MakeDefaultHeaderLocation(1)
        );
}

shared_ptr<IHeaderAccessor> MakeHeaderAccessor(
    shared_ptr<IRandomMessageAccessor> messageAccessor,
    ExtentLocation headerLocation1,
    ExtentLocation headerLocation2)
{
    return make_shared<HeaderAccessor>(
        move(messageAccessor),
        headerLocation1,
        headerLocation2);
}

}
