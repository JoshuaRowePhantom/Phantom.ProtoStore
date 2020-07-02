#pragma once

#include "StandardTypes.h"
#include "MessageStore.h"
#include <memory>

namespace Phantom::ProtoStore
{
class IHeaderAccessor
{
    virtual task<> ReadHeader(
        Header& header
    ) = 0;

    virtual task<> WriteHeader(
        const Header& header
    ) = 0;
};

shared_ptr<IHeaderAccessor> CreateHeaderAccessor(
    const shared_ptr<IMessageStore>& messageStore);

}