#pragma once

#include "Partition.h"
#include "AsyncScopeMixin.h"

namespace Phantom::ProtoStore
{

class Partition
    :
    public IPartition,
    public AsyncScopeMixin
{
    shared_ptr<KeyComparer> m_keyComparer;
    shared_ptr<IMessageFactory> m_keyFactory;
    shared_ptr<IMessageFactory> m_valueFactory;
    shared_ptr<IRandomMessageAccessor> m_messageAccessor;
    ExtentLocation m_headerLocation;
    ExtentLocation m_dataLocation;

    shared_ptr<const PartitionTreeNode> m_root;
    shared_task<const PartitionTreeNode*> m_rootTask;
    shared_task<const PartitionTreeNode*> ReadRoot();

public:
    Partition(
        shared_ptr<KeyComparer> keyComparer,
        shared_ptr<IMessageFactory> keyFactory,
        shared_ptr<IMessageFactory> valueFactory,
        shared_ptr<IRandomMessageAccessor> messageAccessor,
        ExtentLocation headerLocation,
        ExtentLocation dataLocation
    );

    virtual cppcoro::async_generator<const MemoryTableRow*> Enumerate(
        SequenceNumber readSequenceNumber,
        KeyRangeEnd low,
        KeyRangeEnd high
    ) override;

};
}