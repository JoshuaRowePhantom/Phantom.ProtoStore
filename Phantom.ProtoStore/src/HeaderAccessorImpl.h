#pragma once

#include "StandardTypes.h"
#include "HeaderAccessor.h"

namespace Phantom::ProtoStore
{

class HeaderAccessor
    :
    public IHeaderAccessor
{
    shared_ptr<IRandomMessageAccessor> m_messageAccessor;
    ExtentLocation m_headerLocation1;
    ExtentLocation m_headerLocation2;
    ExtentLocation m_currentLocation;
    ExtentLocation m_nextLocation;

    task<bool> ReadHeader(
        ExtentLocation location,
        Header& header,
        bool throwOnError);

public:
    HeaderAccessor(
        shared_ptr<IRandomMessageAccessor> messageAccessor,
        ExtentLocation headerLocation1,
        ExtentLocation headerLocation2);

    virtual task<> ReadHeader(
        Header& header
    ) override;

    virtual task<> WriteHeader(
        const Header& header
    ) override;
};

}