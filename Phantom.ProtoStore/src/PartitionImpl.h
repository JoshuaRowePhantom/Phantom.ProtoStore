#pragma once

#include "Partition.h"
#include "AsyncScopeMixin.h"
#include "BloomFilter.h"
#include "Phantom.System/async_reader_writer_lock.h"
#include "SkipList.h"
#include "src/ProtoStoreInternal_generated.h"
#include <compare>
#include <stdint.h>

namespace Phantom::ProtoStore
{

struct BloomFilterV1Hash
{
    size_t operator()(const auto& value) const;
};
template<
    typename Container
>
using BloomFilterVersion1 = BloomFilter<SeedingPrngBloomFilterHashFunction<BloomFilterV1Hash>, char, Container>;

class Partition
    :
    public IPartition,
    private AsyncScopeMixin,
    private SerializationTypes,
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

    optional<BloomFilterVersion1<std::span<const char>>> m_bloomFilter;

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

    row_generator Enumerate(
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

    int FindTreeEntry(
        const shared_ptr<PartitionTreeNodeCacheEntry>& partitionTreeNodeCacheEntry,
        const PartitionTreeNode* treeNode,
        const FindTreeEntryKey& key
    );

    int FindLowTreeEntryIndex(
        const shared_ptr<PartitionTreeNodeCacheEntry>& partitionTreeNodeCacheEntry,
        const PartitionTreeNode* treeNode,
        SequenceNumber readSequenceNumber,
        KeyRangeEnd low
    );

    int FindHighTreeEntryIndex(
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

    virtual row_generator Read(
        SequenceNumber readSequenceNumber,
        std::span<const byte> key,
        ReadValueDisposition readValueDisposition
    ) override;

    virtual row_generator Enumerate(
        SequenceNumber readSequenceNumber,
        KeyRangeEnd low,
        KeyRangeEnd high,
        ReadValueDisposition readValueDisposition
    ) override;

    virtual row_generator Checkpoint(
        optional<PartitionCheckpointStartKey> startKey
    ) override;

    virtual SequenceNumber GetLatestSequenceNumber(
    ) override;

    virtual task<optional<SequenceNumber>> CheckForWriteConflict(
        SequenceNumber readSequenceNumber,
        SequenceNumber writeSequenceNumber,
        std::span<const byte> key
    ) override;

    virtual task<IntegrityCheckErrorList> CheckIntegrity(
        const IntegrityCheckError& errorPrototype
    ) override;
};
}