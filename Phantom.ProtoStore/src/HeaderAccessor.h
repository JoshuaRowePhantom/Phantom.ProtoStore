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

extern const ExtentLocation DefaultHeaderLocation1;
extern const ExtentLocation DefaultHeaderLocation2;

shared_ptr<IHeaderAccessor> MakeHeaderAccessor(
    shared_ptr<IRandomMessageAccessor> messageStore,
    ExtentLocation headerLocation1,
    ExtentLocation headerLocation2);

shared_ptr<IHeaderAccessor> MakeHeaderAccessor(
    shared_ptr<IRandomMessageAccessor> messageStore);

}