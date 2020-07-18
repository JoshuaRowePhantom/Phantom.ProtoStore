#pragma once

#include <stdint.h>
#include "Partition.h"
#include "AsyncScopeMixin.h"
#include "src/ProtoStoreInternal.pb.h"
#include "SkipList.h"
#include "Phantom.System/async_reader_writer_lock.h"
#include "PartitionTreeNodeCache.h"

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

    PartitionHeader m_partitionHeader;
    PartitionRoot m_partitionRoot;

    shared_task<> m_openTask;

    PartitionTreeNodeCache m_partitionTreeNodeCache;

    task<size_t> FindLowTreeEntry(
        const shared_ptr<PartitionTreeNodeCacheEntry>& partitionTreeNodeCacheEntry,
        KeyRangeEnd low
    );

    task<size_t> FindHighTreeEntry(
        const shared_ptr<PartitionTreeNodeCacheEntry>& partitionTreeNodeCacheEntry,
        KeyRangeEnd high
    );

    cppcoro::async_generator<ResultRow> Enumerate(
        ExtentLocation treeNodeLocation,
        SequenceNumber readSequenceNumber,
        KeyRangeEnd low,
        KeyRangeEnd high
    );

public:
    Partition(
        shared_ptr<KeyComparer> keyComparer,
        shared_ptr<IMessageFactory> keyFactory,
        shared_ptr<IMessageFactory> valueFactory,
        shared_ptr<IRandomMessageAccessor> messageAccessor,
        ExtentLocation headerLocation,
        ExtentLocation dataLocation
    );

    task<> Open();

    virtual cppcoro::async_generator<ResultRow> Enumerate(
        SequenceNumber readSequenceNumber,
        KeyRangeEnd low,
        KeyRangeEnd high
    ) override;

};
}