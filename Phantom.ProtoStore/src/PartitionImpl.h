#pragma once

#include "Partition.h"
#include "AsyncScopeMixin.h"
#include "BloomFilter.h"
#include "MessageStore.h"
#include "Phantom.System/async_reader_writer_lock.h"
#include "SkipList.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"
#include <compare>
#include <stdint.h>

namespace Phantom::ProtoStore
{

struct BloomFilterV1Hash
{
    size_t operator()(uint64_t value) const;
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
    shared_ptr<const Schema> m_schema;
    shared_ptr<const KeyComparer> m_keyComparer;
    shared_ptr<IRandomMessageReader> m_partitionDataReader;
    shared_ptr<IRandomMessageReader> m_partitionHeaderReader;

    FlatMessage<FlatBuffers::PartitionMessage> m_partitionHeaderMessage;
    FlatMessage<FlatBuffers::PartitionMessage> m_partitionRootMessage;
    FlatMessage<FlatBuffers::PartitionMessage> m_partitionRootTreeNodeMessage;
    FlatMessage<FlatBuffers::PartitionMessage> m_partitionBloomFilterMessage;

    optional<BloomFilterVersion1<std::span<const char>>> m_bloomFilter;

    shared_task<> m_openTask;

    struct FindTreeEntryKey;
    struct FindTreeEntryKeyLessThanComparer;

    enum class EnumerateBehavior
    {
        PointInTimeRead,
        Checkpoint,
    };

    struct EnumerateLastReturnedKey;

    row_generator Enumerate(
        const FlatMessage<PartitionMessage>& treeNode,
        SequenceNumber readSequenceNumber,
        KeyRangeEnd low,
        KeyRangeEnd high,
        ReadValueDisposition readValueDisposition,
        EnumerateBehavior enumerateBehavior,
        EnumerateLastReturnedKey& lastReturnedKey
    );

    int FindMatchingValueIndexByWriteSequenceNumber(
        const FlatBuffers::PartitionTreeEntryKey* keyEntry,
        SequenceNumber readSequenceNumber);

    int FindTreeEntry(
        const FlatMessage<PartitionMessage>& treeNode,
        const FindTreeEntryKey& key
    );

    int FindLowTreeEntryIndex(
        const FlatMessage<PartitionMessage>& treeNode,
        SequenceNumber readSequenceNumber,
        KeyRangeEnd low
    );

    int FindHighTreeEntryIndex(
        const FlatMessage<PartitionMessage>& treeNode,
        SequenceNumber readSequenceNumber,
        KeyRangeEnd high
    );

    task<> CheckTreeNodeIntegrity(
        IntegrityCheckErrorList& errorList,
        const IntegrityCheckError& errorPrototype,
        const FlatBuffers::MessageReference_V1* messageReference,
        AlignedMessageData minKeyExclusive,
        SequenceNumber lowestKeyHighestSequenceNumber,
        AlignedMessageData maxKeyInclusive,
        SequenceNumber highestSequenceNumberForMaxKey);

    task<> CheckChildTreeEntryIntegrity(
        IntegrityCheckErrorList& errorList,
        const IntegrityCheckError& errorPrototype,
        const FlatMessage<PartitionMessage>&parent,
        size_t treeEntryIndex,
        AlignedMessageData currentKey,
        AlignedMessageData expectedCurrentKey,
        SequenceNumber expectedHighestSequenceNumber,
        AlignedMessageData maxKeyExclusive,
        SequenceNumber highestSequenceNumberForMaxKey);

    void GetKeyValues(
        const FlatMessage<FlatBuffers::PartitionTreeEntryKey>& keyEntry,
        AlignedMessageData& key,
        SequenceNumber& highestSequenceNumber,
        SequenceNumber& lowestSequenceNumber);

    task<FlatMessage<PartitionMessage>> ReadData(
        const FlatBuffers::MessageReference_V1*
    );

public:
    Partition(
        shared_ptr<const Schema> schema,
        shared_ptr<const KeyComparer> keyComparer,
        shared_ptr<IRandomMessageReader> partitionData,
        shared_ptr<IRandomMessageReader> partitionHeader
    );

    ~Partition();

    task<> Open();

    virtual task<size_t> GetRowCount(
    ) override;

    virtual task<ExtentOffset> GetApproximateDataSize(
    ) override;

    virtual row_generator Read(
        SequenceNumber readSequenceNumber,
        const ProtoValue& key,
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
        const ProtoValue& key
    ) override;

    virtual task<IntegrityCheckErrorList> CheckIntegrity(
        const IntegrityCheckError& errorPrototype
    ) override;
};
}