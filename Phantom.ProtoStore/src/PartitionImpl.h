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
    private AsyncScopeMixin,
    public virtual IJoinable
{
    shared_ptr<KeyComparer> m_keyComparer;
    shared_ptr<IMessageFactory> m_keyFactory;
    shared_ptr<IMessageFactory> m_valueFactory;
    shared_ptr<IRandomMessageAccessor> m_messageAccessor;
    ExtentLocation m_headerLocation;
    ExtentName m_dataExtentName;

    PartitionHeader m_partitionHeader;
    PartitionBloomFilter m_partitionBloomFilter;
    PartitionRoot m_partitionRoot;

    typedef BloomFilter<std::hash<string>, char, span<const char>> BloomFilterVersion1;
    optional<BloomFilterVersion1> m_bloomFilter;

    shared_task<> m_openTask;

    PartitionTreeNodeCache m_partitionTreeNodeCache;

    struct FindTreeEntryKey;
    struct FindTreeEntryKeyLessThanComparer;

    enum class EnumerateBehavior
    {
        PointInTimeRead,
        Checkpoint,
    };

    struct EnumerateLastReturnedKey;

    cppcoro::async_generator<ResultRow> Enumerate(
        ExtentLocation treeNodeLocation,
        SequenceNumber readSequenceNumber,
        KeyRangeEnd low,
        KeyRangeEnd high,
        ReadValueDisposition readValueDisposition,
        EnumerateBehavior enumerateBehavior,
        EnumerateLastReturnedKey& lastReturnedKey
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

    task<> CheckTreeNodeIntegrity(
        IntegrityCheckErrorList& errorList,
        const IntegrityCheckError& errorPrototype,
        ExtentLocation location,
        const Message* lowestKeyInclusive,
        SequenceNumber lowestKeyHighestSequenceNumber,
        const Message* maxKeyExclusive,
        SequenceNumber highestSequenceNumberForMaxKey);

    task<> CheckChildTreeEntryIntegrity(
        IntegrityCheckErrorList& errorList,
        const IntegrityCheckError& errorPrototype,
        const PartitionTreeNode& parent,
        size_t treeEntryIndex,
        const Message* currentKey,
        const Message* expectedCurrentKey,
        SequenceNumber expectedHighestSequenceNumber,
        const Message* maxKeyExclusive,
        SequenceNumber highestSequenceNumberForMaxKey);

    void GetKeyValues(
        const PartitionTreeEntry& treeEntry,
        unique_ptr<Message>& key,
        SequenceNumber& highestSequenceNumber,
        SequenceNumber& lowestSequenceNumber);

public:
    Partition(
        shared_ptr<KeyComparer> keyComparer,
        shared_ptr<IMessageFactory> keyFactory,
        shared_ptr<IMessageFactory> valueFactory,
        shared_ptr<IRandomMessageAccessor> messageAccessor,
        ExtentLocation headerLocation,
        ExtentName dataLocation
    );

    ~Partition();

    task<> Open();

    virtual task<size_t> GetRowCount(
    ) override;

    virtual task<ExtentOffset> GetApproximateDataSize(
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

    virtual cppcoro::async_generator<ResultRow> Checkpoint(
        optional<PartitionCheckpointStartKey> startKey
    ) override;

    virtual SequenceNumber GetLatestSequenceNumber(
    ) override;

    virtual task<optional<SequenceNumber>> CheckForWriteConflict(
        SequenceNumber readSequenceNumber,
        SequenceNumber writeSequenceNumber,
        const Message* key
    ) override;

    virtual task<IntegrityCheckErrorList> CheckIntegrity(
        const IntegrityCheckError& errorPrototype
    ) override;
};
}