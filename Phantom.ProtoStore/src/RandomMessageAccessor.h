#pragma once

#include "StandardTypes.h"

namespace Phantom::ProtoStore
{
using std::shared_ptr;

class IMessageStore;

class IRandomMessageAccessor
{
public:
};

shared_ptr<IRandomMessageAccessor> CreateRandomMessageAccessor(
    shared_ptr<IMessageStore> messageStore);
}