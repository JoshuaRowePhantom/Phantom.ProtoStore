#include "HeaderAccessorImpl.h"

namespace Phantom::ProtoStore
{

HeaderAccessor::HeaderAccessor(
    shared_ptr<IMessageStore> messageStore,
    ExtentLocation headerLocation1,
    ExtentLocation headerLocation2
)
    : 
    m_messageStore(move(messageStore)),
    m_headerLocation1(headerLocation1),
    m_headerLocation2(headerLocation2)
{
}

task<> HeaderAccessor::ReadHeader(
    Header& header)
{
    throw 0;
}

task<> HeaderAccessor::WriteHeader(
    const Header& header)
{
    throw 0;
}

}