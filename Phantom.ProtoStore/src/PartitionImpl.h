#pragma once

#include <stdint.h>
#include "Partition.h"
#include "AsyncScopeMixin.h"
#include "src/ProtoStoreInternal.pb.h"
#include "SkipList.h"
#include "Phantom.System/async_reader_writer_lock.h"
#include "PartitionTreeNodeCache.h"
#include "BloomFilter.h"
#include <compare>

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
    shared_ptr<IRandomMessageAccessor> m_dataHeaderMessageAccessor;
    shared_ptr<IRandomMessageAccessor> m_dataMessageAccessor;
    ExtentLocation m_headerLocation;
    ExtentLocation m_dataLocation;

    PartitionHeader m_partitionHeader;
    PartitionBloomFilter m_partitionBloomFilter;
    PartitionRoot m_partitionRoot;

    typedef BloomFilter<std::hash<string>, char, span<const char>> BloomFilterVersion1;
    optional<BloomFilterVersion1> m_bloomFilter;

    shared_task<> m_openTask;

    PartitionTreeNodeCache m_partitionTreeNodeCache;

    struct FindTreeEntryKey
    {
        SequenceNumber readSequenceNumber;
        const Message* key;
        std::weak_ordering matchingKeyComparisonResult = std::weak_ordering::equivalent;
        std::optional<int> lastFindResult;
    };

    cppcoro::async_generator<ResultRow> Enumerate(
        ExtentLocation treeNodeLocation,
        SequenceNumber readSequenceNumber,
        KeyRangeEnd low,
        KeyRangeEnd high,
        ReadValueDisposition readValueDisposition
    );

    int FindMatchingValueIndexByWriteSequenceNumber(
        const PartitionTreeEntryValueSet& valueSet,
        SequenceNumber readSequenceNumber);

    task<int> FindTreeEntry(
        const shared_ptr<PartitionTreeNodeCacheEntry>& partitionTreeNodeCacheEntry,
        const PartitionTreeNode* treeNode,
        const FindTreeEntryKey& key
    );

    task<int> FindLowTreeEntryIndex(
        const shared_ptr<PartitionTreeNodeCacheEntry>& partitionTreeNodeCacheEntry,
        const PartitionTreeNode* treeNode,
        SequenceNumber readSequenceNumber,
        KeyRangeEnd low
    );

    task<int> FindHighTreeEntryIndex(
        const shared_ptr<PartitionTreeNodeCacheEntry>& partitionTreeNodeCacheEntry,
        const PartitionTreeNode* treeNode,
        SequenceNumber readSequenceNumber,
        KeyRangeEnd high
    );

public:
    Partition(
        shared_ptr<KeyComparer> keyComparer,
        shared_ptr<IMessageFactory> keyFactory,
        shared_ptr<IMessageFactory> valueFactory,
        shared_ptr<IRandomMessageAccessor> dataHeaderMessageAccessor,
        shared_ptr<IRandomMessageAccessor> dataMessage,
        ExtentLocation headerLocation,
        ExtentLocation dataLocation
    );

    task<> Open();

    virtual task<size_t> GetRowCount(
    ) override;

    virtual cppcoro::async_generator<ResultRow> Read(
        SequenceNumber readSequenceNumber,
        const Message* key,
        ReadValueDisposition readValueDisposition
    ) override;

    virtual cppcoro::async_generator<ResultRow> Enumerate(
        SequenceNumber readSequenceNumber,
        KeyRangeEnd low,
        KeyRangeEnd high,
        ReadValueDisposition readValueDisposition
    ) override;

    virtual SequenceNumber GetLatestSequenceNumber(
    ) override;

    virtual task<optional<SequenceNumber>> CheckForWriteConflict(
        SequenceNumber readSequenceNumber,
        const Message* key
    ) override;
};
}