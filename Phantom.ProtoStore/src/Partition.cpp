#include "PartitionImpl.h"

namespace Phantom::ProtoStore
{

Partition::Partition(
    shared_ptr<KeyComparer> keyComparer,
    shared_ptr<IMessageFactory> keyFactory,
    shared_ptr<IMessageFactory> valueFactory,
    shared_ptr<IRandomMessageAccessor> messageAccessor,
    ExtentLocation topLevelMessageLocation
) :
    m_keyComparer(keyComparer),
    m_keyFactory(keyFactory),
    m_valueFactory(valueFactory),
    m_messageAccessor(messageAccessor),
    m_topLevelMessageLocation(topLevelMessageLocation)
{
}

cppcoro::async_generator<const MemoryTableRow*> Partition::Enumerate(
    SequenceNumber readSequenceNumber,
    KeyRangeEnd low,
    KeyRangeEnd high
)
{
    throw 0;
}

}
