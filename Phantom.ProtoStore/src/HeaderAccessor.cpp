#include "HeaderAccessorImpl.h"
#include "RandomMessageAccessor.h"
#include "Phantom.ProtoStore/ProtoStore.pb.h"
#include "src/ProtoStoreInternal.pb.h"
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

task<bool> HeaderAccessor::ReadHeader(
    ExtentLocation location,
    Header& header,
    bool throwOnError)
{
    try
    {
        co_await m_messageAccessor->ReadMessage(
            location,
            header);

        co_return true;
    }
    catch (...)
    {
        if (throwOnError)
        {
            throw;
        }

        co_return false;
    }
}

task<> HeaderAccessor::ReadHeader(
    Header& header)
{
    Header location1Header;

    if (!co_await ReadHeader(
        m_headerLocation1,
        location1Header,
        false))
    {
        co_await ReadHeader(
            m_headerLocation2,
            header,
            true);

        m_currentLocation = m_headerLocation2;
        m_nextLocation = m_headerLocation1;

        co_return;
    }

    Header location2Header;

    if (!co_await ReadHeader(
        m_headerLocation2,
        location2Header,
        false))
    {
        header = move(location1Header);
        m_currentLocation = m_headerLocation1;
        m_nextLocation = m_headerLocation2;

        co_return;
    }

    if (location1Header.epoch() > location2Header.epoch())
    {
        header = move(location1Header);
        m_currentLocation = m_headerLocation1;
        m_nextLocation = m_headerLocation2;
    }
    else
    {
        header = move(location2Header);
        m_currentLocation = m_headerLocation2;
        m_nextLocation = m_headerLocation1;
    }
}

task<> HeaderAccessor::WriteHeader(
    const Header& header)
{
    co_await m_messageAccessor->WriteMessage(
        m_nextLocation,
        header);

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
