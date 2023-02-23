#pragma once

#include "StandardTypes.h"

namespace Phantom::ProtoStore
{
using std::shared_ptr;

class IMessageStore;

class IRandomMessageAccessor
{
public:
    virtual task<DataReference<StoredMessage>> ReadMessage(
        ExtentLocation location
    ) = 0;

    virtual task<> ReadMessage(
        ExtentLocation location,
        Message& message
    ) = 0;

    virtual task<DataReference<StoredMessage>> WriteMessage(
        ExtentLocation location,
        const StoredMessage& storedMessage,
        FlushBehavior flushBehavior = FlushBehavior::Flush
    ) = 0;

    virtual task<> WriteMessage(
        ExtentLocation location,
        const Message& message,
        FlushBehavior flushBehavior = FlushBehavior::Flush
    ) = 0;
};

shared_ptr<IRandomMessageAccessor> MakeRandomMessageAccessor(
    shared_ptr<IMessageStore> messageStore);
}