#pragma once

#include <stdint.h>
#include "Partition.h"
#include "AsyncScopeMixin.h"
#include "src/ProtoStoreInternal.pb.h"
#include "SkipList.h"
#include "Phantom.System/async_reader_writer_lock.h"
#include "PartitionTreeNodeCache.h"
#include "BloomFilter.h"

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
    PartitionBloomFilter m_partitionBloomFilter;
    PartitionRoot m_partitionRoot;

    typedef BloomFilter<std::hash<string>, char, span<const char>> BloomFilterVersion1;
    unique_ptr<BloomFilterVersion1> m_bloomFilter;

    shared_task<> m_openTask;

    PartitionTreeNodeCache m_partitionTreeNodeCache;

    task<size_t> FindTreeEntry(
        const shared_ptr<PartitionTreeNodeCacheEntry>& partitionTreeNodeCacheEntry,
        SequenceNumber readSequenceNumber,
        KeyRangeEnd low,
        std::weak_ordering equivalenceToUseForEquivalent
    );

    task<size_t> FindLowTreeEntry(
        const shared_ptr<PartitionTreeNodeCacheEntry>& partitionTreeNodeCacheEntry,
        SequenceNumber readSequenceNumber,
        KeyRangeEnd low
    );

    task<size_t> FindHighTreeEntry(
        const shared_ptr<PartitionTreeNodeCacheEntry>& partitionTreeNodeCacheEntry,
        SequenceNumber readSequenceNumber,
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

    virtual task<size_t> GetRowCount(
    ) override;

    virtual cppcoro::async_generator<ResultRow> Read(
        SequenceNumber readSequenceNumber,
        const Message* key
    ) override;

    virtual cppcoro::async_generator<ResultRow> Enumerate(
        SequenceNumber readSequenceNumber,
        KeyRangeEnd low,
        KeyRangeEnd high
    ) override;

};
}