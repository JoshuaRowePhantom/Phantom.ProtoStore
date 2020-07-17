#pragma once

#include "Partition.h"

namespace Phantom::ProtoStore
{

class Partition
    :
    public IPartition
{
    shared_ptr<KeyComparer> m_keyComparer;
    shared_ptr<IMessageFactory> m_keyFactory;
    shared_ptr<IMessageFactory> m_valueFactory;
    shared_ptr<IRandomMessageAccessor> m_messageAccessor;
    ExtentLocation m_topLevelMessageLocation;
    
public:
    Partition(
        shared_ptr<KeyComparer> keyComparer,
        shared_ptr<IMessageFactory> keyFactory,
        shared_ptr<IMessageFactory> valueFactory,
        shared_ptr<IRandomMessageAccessor> messageAccessor,
        ExtentLocation topLevelMessageLocation
    );

    virtual cppcoro::async_generator<const MemoryTableRow*> Enumerate(
        SequenceNumber readSequenceNumber,
        KeyRangeEnd low,
        KeyRangeEnd high
    ) override;

};
}