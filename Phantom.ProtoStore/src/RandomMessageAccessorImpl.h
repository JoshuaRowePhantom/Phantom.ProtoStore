#pragma once

#include "RandomMessageAccessor.h"

namespace Phantom::ProtoStore
{

class RandomMessageAccessor
    :
    public IRandomMessageAccessor
{
    shared_ptr<IMessageStore> m_messageStore;

public:
    RandomMessageAccessor(
        shared_ptr<IMessageStore> messageStore
    );

    virtual task<DataReference<StoredMessage>> ReadMessage(
        ExtentLocation location
    ) override;

    virtual task<> ReadMessage(
        ExtentLocation location,
        Message& message
    ) override;


    virtual task<DataReference<StoredMessage>> WriteMessage(
        ExtentLocation location,
        const StoredMessage& message,
        FlushBehavior flushBehavior
    ) override;

    virtual task<> WriteMessage(
        ExtentLocation location,
        const Message& message,
        FlushBehavior flushBehavior
    ) override;
};

}
