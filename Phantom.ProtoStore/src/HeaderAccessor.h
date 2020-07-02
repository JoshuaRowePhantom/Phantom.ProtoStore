#pragma once

#include "StandardTypes.h"
#include <memory>

namespace Phantom::ProtoStore
{
class Header;

class IHeaderAccessor
{
public:
    virtual task<> ReadHeader(
        Header& header
    ) = 0;

    virtual task<> WriteHeader(
        const Header& header
    ) = 0;
};

constexpr ExtentLocation DefaultHeaderLocation1 =
{
    0,
    0,
};

constexpr ExtentLocation DefaultHeaderLocation2 =
{
    1,
    0,
};

shared_ptr<IHeaderAccessor> MakeHeaderAccessor(
    shared_ptr<IRandomMessageAccessor> messageStore,
    ExtentLocation headerLocation1 = DefaultHeaderLocation1,
    ExtentLocation headerLocation2 = DefaultHeaderLocation2);

}