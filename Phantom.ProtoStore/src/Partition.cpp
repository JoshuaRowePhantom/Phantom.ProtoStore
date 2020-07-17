#include "PartitionImpl.h"
#include "RandomMessageAccessor.h"
#include "src/ProtoStoreInternal.pb.h"

namespace Phantom::ProtoStore
{

Partition::Partition(
    shared_ptr<KeyComparer> keyComparer,
    shared_ptr<IMessageFactory> keyFactory,
    shared_ptr<IMessageFactory> valueFactory,
    shared_ptr<IRandomMessageAccessor> messageAccessor,
    ExtentLocation headerLocation,
    ExtentLocation dataLocation
) :
    m_keyComparer(keyComparer),
    m_keyFactory(keyFactory),
    m_valueFactory(valueFactory),
    m_messageAccessor(messageAccessor),
    m_headerLocation(headerLocation),
    m_dataLocation(dataLocation)
{
    m_rootTask = ReadRoot();
    m_asyncScope.spawn(m_rootTask);
}

shared_task<const PartitionTreeNode*> Partition::ReadRoot()
{
    auto root = make_shared<PartitionTreeNode>();
    m_root = root;

    co_await m_messageAccessor->ReadMessage(
        m_headerLocation,
        *root);

    co_return &*m_root;
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
