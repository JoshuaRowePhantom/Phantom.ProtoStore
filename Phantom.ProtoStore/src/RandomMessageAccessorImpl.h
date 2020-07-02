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

    virtual task<> ReadMessage(
        ExtentLocation location,
        Message& message
    ) override;

    virtual task<> WriteMessage(
        ExtentLocation location,
        const Message& message
    ) override;
};

}
