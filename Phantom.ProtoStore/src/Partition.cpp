#include "Checksum.h"
#include "ValueComparer.h"
#include "PartitionImpl.h"
#include "Phantom.System/async_utility.h"
#include "MessageStore.h"
#include "Schema.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"
#include <algorithm>
#include <compare>

#include <boost/crc.hpp>
namespace Phantom::ProtoStore
{

BloomFilterV1Hash::BloomFilterV1Hash(
    std::shared_ptr<const ValueComparer> keyComparer
) : 
    m_keyComparer { std::move(keyComparer) }
{
}

size_t BloomFilterV1Hash::operator()(const ProtoValue& value) const
{
    return m_keyComparer->Hash(value);
}

struct Partition::EnumerateLastReturnedKey
{
    ProtoValue Key;
};

Partition::Partition(
    shared_ptr<const Schema> schema,
    shared_ptr<const ValueComparer> keyComparer,
    shared_ptr<IRandomMessageReader> partitionHeaderReader,
    shared_ptr<IRandomMessageReader> partitionDataReader
) :
    m_schema(std::move(schema)),
    m_keyComparer(std::move(keyComparer)),
    m_partitionHeaderReader(std::move(partitionHeaderReader)),
    m_partitionDataReader(std::move(partitionDataReader))
{
    assert(m_schema);
    assert(m_keyComparer);
    assert(m_partitionHeaderReader);
    assert(m_partitionDataReader);
}

Partition::~Partition()
{
    SyncDestroy();
}

task<FlatMessage<Partition::PartitionMessage>> Partition::ReadData(
    const FlatBuffers::MessageReference_V1* reference
)
{
    co_return FlatMessage<PartitionMessage>
    {
        co_await m_partitionDataReader->Read(
            reference)
    };
}

task<> Partition::Open()
{
    m_partitionHeaderMessage = FlatMessage<PartitionMessage>
    { 
        co_await m_partitionHeaderReader->Read(ExtentOffset(0)) 
    };

    m_partitionRootMessage = co_await ReadData(
        m_partitionHeaderMessage->header()->partition_root());
    
    if (m_partitionRootMessage->root()->bloom_filter())
    {
        m_partitionBloomFilterMessage = co_await ReadData(
            m_partitionRootMessage->root()->bloom_filter());

        std::span<const char> bloomFilterSpan
        {
            reinterpret_cast<const char*>(m_partitionBloomFilterMessage->bloom_filter()->filter()->data()),
            m_partitionBloomFilterMessage->bloom_filter()->filter()->size(),
        };

        m_bloomFilter.emplace(
            bloomFilterSpan,
            SeedingPrngBloomFilterHashFunction
            {
                m_partitionBloomFilterMessage->bloom_filter()->hash_function_count(),
                BloomFilterV1Hash { m_keyComparer }
            }
        );
    }

    m_partitionRootTreeNodeMessage = co_await ReadData(
        m_partitionRootMessage->root()->root_tree_node());
}

task<size_t> Partition::GetRowCount()
{
    co_return m_partitionRootMessage->root()->row_count();
}

task<ExtentOffset> Partition::GetApproximateDataSize()
{
    co_return 0;
}

row_generator Partition::Read(
    SequenceNumber readSequenceNumber,
    const ProtoValue& key,
    ReadValueDisposition readValueDisposition
)
{
    if (m_bloomFilter &&
        !m_bloomFilter->test(key))
    {
        co_return;
    }

    KeyRangeEnd keyRangeEnd =
    {
        .Key = key,
        .Inclusivity = Inclusivity::Inclusive,
    };

    auto enumeration = Enumerate(
        readSequenceNumber,
        keyRangeEnd,
        keyRangeEnd,
        readValueDisposition);

    for (auto iterator = co_await enumeration.begin();
        iterator != enumeration.end();
        co_await ++iterator)
    {
        co_yield *iterator;
    }
}

row_generator Partition::Enumerate(
    SequenceNumber readSequenceNumber,
    KeyRangeEnd low,
    KeyRangeEnd high,
    ReadValueDisposition readValueDisposition
)
{
    EnumerateLastReturnedKey unusedEnumerateLastReturnedKey;

    auto enumeration = Enumerate(
        m_partitionRootTreeNodeMessage,
        readSequenceNumber,
        std::move(low),
        std::move(high),
        readValueDisposition,
        EnumerateBehavior::PointInTimeRead,
        unusedEnumerateLastReturnedKey);

    for (auto iterator = co_await enumeration.begin();
        iterator != enumeration.end();
        co_await ++iterator)
    {
        co_yield *iterator;
    }
}

row_generator Partition::EnumeratePrefix(
    SequenceNumber readSequenceNumber,
    Prefix prefix,
    ReadValueDisposition readValueDisposition
)
{
    EnumerateLastReturnedKey unusedEnumerateLastReturnedKey;

    KeyRangeEnd low
    {
        .Key = std::move(prefix.Key),
        .Inclusivity = Inclusivity::Inclusive,
        .LastFieldId = prefix.LastFieldId,
    };

    auto enumeration = Enumerate(
        m_partitionRootTreeNodeMessage,
        readSequenceNumber,
        low,
        low,
        readValueDisposition,
        EnumerateBehavior::PointInTimeRead,
        unusedEnumerateLastReturnedKey);

    for (auto iterator = co_await enumeration.begin();
        iterator != enumeration.end();
        co_await ++iterator)
    {
        co_yield *iterator;
    }
}

row_generator Partition::Checkpoint(
    std::optional<PartitionCheckpointStartKey> startKey
)
{
    auto readSequenceNumber = SequenceNumber::Latest;

    KeyRangeEnd low =
    {
        .Key = startKey ? startKey->Key : ProtoValue::KeyMin(),
        .Inclusivity = Inclusivity::Inclusive,
    };

    KeyRangeEnd high =
    {
        .Key = ProtoValue::KeyMax(),
        .Inclusivity = Inclusivity::Inclusive,
    };

    EnumerateLastReturnedKey unusedEnumerateLastReturnedKey;

    auto enumeration = Enumerate(
        m_partitionRootTreeNodeMessage,
        readSequenceNumber,
        low,
        high,
        ReadValueDisposition::ReadValue,
        EnumerateBehavior::Checkpoint,
        unusedEnumerateLastReturnedKey);

    for (auto iterator = co_await enumeration.begin();
        iterator != enumeration.end();
        co_await ++iterator)
    {
        co_yield *iterator;
    }
}

struct Partition::FindTreeEntryKeyLessThanComparer
    : SerializationTypes
{
    const Schema& m_schema;
    const ValueComparer& m_keyComparer;
    
    FindTreeEntryKeyLessThanComparer(
        const Schema& schema,
        const ValueComparer& keyComparer
    ) : 
        m_schema(schema),
        m_keyComparer(keyComparer)
    {}

    bool lower_bound_less(
        const PartitionTreeEntryKey* keyEntry,
        const KeyRangeComparerArgument& key
        ) const
    {
        KeyRangeComparer comparer(
            m_keyComparer);

        auto keyEntryProtoValue = SchemaDescriptions::MakeProtoValueKey(
            m_schema,
            keyEntry->key(),
            keyEntry->flat_key()
        );

        return comparer.lower_bound_less(keyEntryProtoValue, key);
    }

    bool upper_bound_less(
        const KeyRangeComparerArgument& key,
        const PartitionTreeEntryKey* keyEntry
        ) const
    {
        KeyRangeComparer comparer(
            m_keyComparer);

        auto keyEntryProtoValue = SchemaDescriptions::MakeProtoValueKey(
            m_schema,
            keyEntry->key(),
            keyEntry->flat_key()
        );

        return comparer.upper_bound_less(key, keyEntryProtoValue);
    }
};

ptrdiff_t Partition::FindLowTreeEntryIndex(
    const FlatMessage<PartitionMessage>& treeNode,
    KeyRangeEnd low
)
{
    FindTreeEntryKeyLessThanComparer comparer { *m_schema, *m_keyComparer };

    KeyRangeComparerArgument key
    {
        low.Key,
        low.Inclusivity,
        low.LastFieldId
    };

    // VectorIterator doesn't accept its difference_type in its += operator.
#pragma warning (push)
#pragma warning (disable: 4244)
    auto upperBound = std::lower_bound(
        treeNode->tree_node()->keys()->begin(),
        treeNode->tree_node()->keys()->end(),
        key,
        std::bind_front(&FindTreeEntryKeyLessThanComparer::lower_bound_less, comparer)
    );
#pragma warning (pop)

    return upperBound - treeNode->tree_node()->keys()->begin();
}

ptrdiff_t Partition::FindHighTreeEntryIndex(
    ptrdiff_t lowTreeEntryIndex,
    const FlatMessage<PartitionMessage>& treeNode,
    KeyRangeEnd high
)
{
    FindTreeEntryKeyLessThanComparer comparer{ *m_schema, *m_keyComparer };

    KeyRangeComparerArgument key
    {
        high.Key,
        // We invert the sense of exclusivity so that
        // if the user requested an Inclusive search, we find one past the key,
        // and if the user requested an Exclusive search, we find the key or just after it.
        high.Inclusivity,
        high.LastFieldId,
    };

    auto upperBound = std::upper_bound(
        treeNode->tree_node()->keys()->begin() + numeric_cast(lowTreeEntryIndex),
        treeNode->tree_node()->keys()->end(),
        key,
        std::bind_front(&FindTreeEntryKeyLessThanComparer::upper_bound_less, comparer)
    );

    return upperBound - treeNode->tree_node()->keys()->begin();
}

ptrdiff_t Partition::FindMatchingValueIndexByWriteSequenceNumber(
    const FlatBuffers::PartitionTreeEntryKey* keyEntry,
    SequenceNumber readSequenceNumber)
{
    auto readSequenceNumberInt64 = ToUint64(readSequenceNumber);

    // Most of the time, we're returning the first entry.
    if (keyEntry->values()->Get(0)->write_sequence_number() <= readSequenceNumberInt64)
    {
        return 0;
    }

    struct comparer
    {
        bool operator()(
            const FlatBuffers::PartitionTreeEntryValue* partitionTreeEntryValue,
            SequenceNumber readSequenceNumber
            ) const
        {
            return partitionTreeEntryValue->write_sequence_number() > ToUint64(readSequenceNumber);
        }

        bool operator()(
            SequenceNumber readSequenceNumber,
            const FlatBuffers::PartitionTreeEntryValue* partitionTreeEntryValue
            ) const
        {
            return ToUint64(readSequenceNumber) > partitionTreeEntryValue->write_sequence_number();
        }
    };

    // VectorIterator doesn't accept its difference_type in its += operator.
#pragma warning (push)
#pragma warning (disable: 4244)
    return std::lower_bound(
        keyEntry->values()->begin() + 1,
        keyEntry->values()->end(),
        readSequenceNumber,
        comparer()
    ) - keyEntry->values()->begin();
#pragma warning (pop)
}

ProtoValue Partition::GetProtoValueKey(
    const FlatMessage<PartitionMessage>& treeNode,
    const FlatBuffers::PartitionTreeEntryKey* keyEntry
)
{
    ProtoValue key;

    if (keyEntry->key())
    {
        auto keyMessageData = GetAlignedMessageData(
            treeNode,
            keyEntry->key());
        key = SchemaDescriptions::MakeProtoValueKey(
            *m_schema,
            std::move(keyMessageData));
    }
    else
    {
        assert(keyEntry->flat_key());

        auto keyMessageData = AlignedMessageData(
            static_cast<DataReference<StoredMessage>>(treeNode),
            treeNode.data().Content);

        key = ProtoValue::FlatBuffer(
            std::move(keyMessageData),
            treeNode.get())
            .SubValue(
                ProtoValue::flat_buffer_message
                {
                    reinterpret_cast<const flatbuffers::Table*>(keyEntry->flat_key())
                }
        );
    }

    return std::move(key);
}

row_generator Partition::Enumerate(
    const FlatMessage<PartitionMessage>& treeNode,
    SequenceNumber readSequenceNumber,
    KeyRangeEnd low,
    KeyRangeEnd high,
    ReadValueDisposition readValueDisposition,
    EnumerateBehavior enumerateBehavior,
    EnumerateLastReturnedKey& lastReturnedKey
)
{
    auto lowTreeEntryIndex = FindLowTreeEntryIndex(
        treeNode,
        low);

    auto highTreeEntryIndex = FindHighTreeEntryIndex(
        lowTreeEntryIndex,
        treeNode,
        high);

    while (
        (
            treeNode->tree_node()->level() == 0 && lowTreeEntryIndex < highTreeEntryIndex
            ||
            treeNode->tree_node()->level() > 0 && lowTreeEntryIndex <= highTreeEntryIndex
        )
        &&
        lowTreeEntryIndex < treeNode->tree_node()->keys()->size())
    {
        const FlatBuffers::PartitionTreeEntryKey* keyEntry = treeNode->tree_node()->keys()->Get(
            numeric_cast(lowTreeEntryIndex));
        
        ProtoValue key = GetProtoValueKey(
            treeNode,
            keyEntry
        );

        if (keyEntry->child_tree_node())
        {
            auto childMessage = co_await ReadData(
                keyEntry->child_tree_node());

            EnumerateLastReturnedKey childEnumerateLastReturnedKey;

            auto subTreeEnumerator = Enumerate(
                childMessage,
                readSequenceNumber,
                low,
                high,
                readValueDisposition,
                enumerateBehavior,
                childEnumerateLastReturnedKey);

            for (auto iterator = co_await subTreeEnumerator.begin();
                iterator != subTreeEnumerator.end();
                co_await ++iterator)
            {
                co_yield *iterator;
            }

            // When we enumerate it, we might
            // need to skip the last key we returned when we enumerate the next child,
            // so set low to that key.
            if (enumerateBehavior == EnumerateBehavior::PointInTimeRead
                &&
                childEnumerateLastReturnedKey.Key)
            {
                low =
                {
                    childEnumerateLastReturnedKey.Key,
                    Inclusivity::Exclusive,
                };

                lastReturnedKey = move(
                    childEnumerateLastReturnedKey);
            }

            // But always resume at the next tree entry.
            lowTreeEntryIndex++;
        }
        else
        {
            // We're looking at a matching tree entry, now we need to find the right value
            // based on the write sequence number.
            bool hasValue;

            SequenceNumber writeSequenceNumber = SequenceNumber::Earliest;
            const FlatBuffers::DataValue* dataValue = nullptr;
            const FlatBuffers::ValuePlaceholder* valuePlaceholder = nullptr;
            const FlatBuffers::MessageReference_V1* bigValue = nullptr;
            TransactionIdReference transactionId = nullptr;

            if (keyEntry->values())
            {
                // The key entry has multiple values, so we need to find the right by sequence number.
                auto valueIndex = FindMatchingValueIndexByWriteSequenceNumber(
                    keyEntry,
                    readSequenceNumber);

                if (valueIndex < keyEntry->values()->size())
                {
                    hasValue = true;
                    auto treeEntryValue = keyEntry->values()->Get(numeric_cast(valueIndex));
                    writeSequenceNumber = ToSequenceNumber(treeEntryValue->write_sequence_number());
                    dataValue = treeEntryValue->value();
                    valuePlaceholder = treeEntryValue->flat_value();
                    bigValue = treeEntryValue->big_value();
                    transactionId = MakeTransactionIdReference(
                        treeNode,
                        treeEntryValue->distributed_transaction_id());
                }
                else
                {
                    hasValue = false;
                }
            }
            else
            {
                // The key entry has only a single value.
                if (keyEntry->lowest_write_sequence_number_for_key() > ToUint64(readSequenceNumber))
                {
                    // The key entry does not match.
                    hasValue = false;
                }
                else
                {
                    hasValue = true;
                    writeSequenceNumber = ToSequenceNumber(keyEntry->lowest_write_sequence_number_for_key());
                    dataValue = keyEntry->single_value();
                    valuePlaceholder = keyEntry->single_flat_value();
                    bigValue = keyEntry->single_big_value();
                    transactionId = MakeTransactionIdReference(
                        treeNode,
                        keyEntry->single_distributed_transaction_id());
                }
            }

            // The node matched, and we might have a value to return;
            // except we might have found a tree entry whose values are all newer
            // than the read sequence number.
            if (!hasValue)
            {
                ++lowTreeEntryIndex;
            }
            else
            {
                // At this point, we have a key that is in the tree entry.
                // We'd like to return a DataReference<ResultRow> pointing at the minimal
                // set of things.
                // If the value is also in the tree entry, we can return a DataReference
                // to the tree entry.
                // Otherwise we need to return a composite tree entry.
                ProtoValue protoValue;
                if (readValueDisposition == ReadValueDisposition::DontReadValue)
                {
                } else if (dataValue)
                {
                    protoValue = SchemaDescriptions::MakeProtoValueValue(
                        *m_schema,
                        GetAlignedMessageData(
                            treeNode,
                            dataValue));
                }
                else if (bigValue)
                {
                    auto valueMessage = co_await ReadData(
                        bigValue);
                    
                    protoValue = SchemaDescriptions::MakeProtoValueValue(
                        *m_schema,
                        GetAlignedMessageData(
                            valueMessage,
                            valueMessage->value()));
                }
                else if (valuePlaceholder)
                {
                    protoValue = ProtoValue::FlatBuffer(
                        AlignedMessageData(
                            static_cast<DataReference<StoredMessage>>(treeNode),
                            treeNode.data().Content),
                        treeNode.get())
                        .SubValue(
                            ProtoValue::flat_buffer_message
                            {
                                reinterpret_cast<const flatbuffers::Table*>(valuePlaceholder)
                            }
                        );
                }

                co_yield ResultRow
                {
                    .Key = key,
                    .WriteSequenceNumber = writeSequenceNumber,
                    .Value = std::move(protoValue),
                    .TransactionId = std::move(transactionId),
                };

                if (enumerateBehavior == EnumerateBehavior::PointInTimeRead)
                {
                    low =
                    {
                        key,
                        Inclusivity::Exclusive,
                    };

                    lowTreeEntryIndex = FindLowTreeEntryIndex(
                        treeNode,
                        low);

                    lastReturnedKey = EnumerateLastReturnedKey
                    {
                        key,
                    };
                }
                else
                {
                    assert(enumerateBehavior == EnumerateBehavior::Checkpoint);
                    lowTreeEntryIndex++;
                }
            } // end if leaf has matching value.
        } // end if leaf
    } // end while not at end
}

SequenceNumber Partition::GetLatestSequenceNumber()
{
    return ToSequenceNumber(
        m_partitionRootMessage->root()->latest_sequence_number());
}

task<optional<SequenceNumber>> Partition::CheckForWriteConflict(
    SequenceNumber readSequenceNumber,
    SequenceNumber writeSequenceNumber,
    const ProtoValue& key
)
{
    auto generator = Read(
        SequenceNumber::Latest,
        key,
        ReadValueDisposition::DontReadValue
    );

    for (auto iterator = co_await generator.begin();
        iterator != generator.end();
        co_await ++iterator)
    {
        if ((*iterator).WriteSequenceNumber >= writeSequenceNumber
            ||
            (*iterator).WriteSequenceNumber > readSequenceNumber)
        {
            co_return (*iterator).WriteSequenceNumber;
        }
        break;
    }

    co_return optional<SequenceNumber>();
}

task<> Partition::CheckTreeNodeIntegrity(
    IntegrityCheckErrorList& errorList,
    const IntegrityCheckError& errorPrototype,
    const FlatBuffers::MessageReference_V1* messageReference,
    ProtoValue minKeyExclusive,
    SequenceNumber minKeyExclusiveLowestSequenceNumber,
    ProtoValue maxKeyInclusive,
    SequenceNumber maxKeyInclusiveLowestSequenceNumber)
{
    auto treeNodeMessage = co_await ReadData(messageReference);
    auto errorLocationExtentOffset = treeNodeMessage.data().DataRange.Beginning;

    if (!treeNodeMessage->tree_node())
    {
        auto error = errorPrototype;
        error.Code = IntegrityCheckErrorCode::Partition_MissingTreeNode;
        error.Location.extentOffset = errorLocationExtentOffset;
        errorList.push_back(error);
        co_return;
    }
    
    if (treeNodeMessage->tree_node()->keys()->size() == 0)
    {
        co_return;
    }

    if (!maxKeyInclusive)
    {
        GetKeyValues(
            FlatMessage<PartitionTreeEntryKey>{ treeNodeMessage, *treeNodeMessage->tree_node()->keys()->rbegin() },
            maxKeyInclusive,
            maxKeyInclusiveLowestSequenceNumber,
            maxKeyInclusiveLowestSequenceNumber
        );
    }

    ProtoValue previousKey = minKeyExclusive;
    SequenceNumber previousKeyLowestSequenceNumber = minKeyExclusiveLowestSequenceNumber;

    for (uoffset_t index = 0;
        index < treeNodeMessage->tree_node()->keys()->size();
        index++)
    {
        auto treeEntryErrorPrototype = errorPrototype;
        treeEntryErrorPrototype.Location.extentOffset = errorLocationExtentOffset;
        treeEntryErrorPrototype.TreeNodeEntryIndex = index;
        treeEntryErrorPrototype.Key = SchemaDescriptions::MakeProtoValueKey(
            *m_schema,
            GetAlignedMessageData(
                treeNodeMessage,
                treeNodeMessage->tree_node()->keys()->Get(index)->key()));
        
        treeEntryErrorPrototype.PartitionMessage = make_shared<FlatMessage<flatbuffers::Table>>(
            treeNodeMessage,
            reinterpret_cast<const flatbuffers::Table*>(treeNodeMessage.get())
            );

        auto treeEntry = treeNodeMessage->tree_node()->keys()->Get(index);

        ProtoValue currentKey;
        SequenceNumber currentKeyHighestSequenceNumber;
        SequenceNumber currentKeyLowestSequenceNumber;

        GetKeyValues(
            FlatMessage<PartitionTreeEntryKey>{ treeNodeMessage, treeEntry },
            currentKey,
            currentKeyHighestSequenceNumber,
            currentKeyLowestSequenceNumber);

        co_await CheckChildTreeEntryIntegrity(
            errorList,
            treeEntryErrorPrototype,
            treeNodeMessage,
            index,
            currentKey,
            previousKey,
            previousKeyLowestSequenceNumber,
            currentKey,
            currentKeyLowestSequenceNumber);

        previousKey = currentKey;
        previousKeyLowestSequenceNumber = currentKeyLowestSequenceNumber;
    }
}

void Partition::GetKeyValues(
    const FlatMessage<FlatBuffers::PartitionTreeEntryKey>& keyEntry,
    ProtoValue& key,
    SequenceNumber& highestSequenceNumber,
    SequenceNumber& lowestSequenceNumber)
{
    key = SchemaDescriptions::MakeProtoValueKey(
        *m_schema,
        keyEntry->key(),
        keyEntry->flat_key());

    if (keyEntry->values())
    {
        
        lowestSequenceNumber = ToSequenceNumber(
            keyEntry->values()->rbegin()->write_sequence_number());
        highestSequenceNumber = ToSequenceNumber(
            keyEntry->values()->begin()->write_sequence_number());
    }
    else
    {
        lowestSequenceNumber = ToSequenceNumber(
            keyEntry->lowest_write_sequence_number_for_key());
        highestSequenceNumber = SequenceNumber::Latest;
    }
}

task<> Partition::CheckChildTreeEntryIntegrity(
    IntegrityCheckErrorList& errorList,
    const IntegrityCheckError& errorPrototype,
    const FlatMessage<PartitionMessage>& parent,
    size_t treeEntryIndex,
    ProtoValue currentKey,
    ProtoValue minKeyExclusive,
    SequenceNumber minKeyExclusiveLowestSequenceNumber,
    ProtoValue maxKeyInclusive,
    SequenceNumber maxKeyInclusiveLowestSequenceNumber)
{
    FlatMessage<FlatBuffers::PartitionTreeEntryKey> treeEntry
    {
        parent,
        parent->tree_node()->keys()->Get(numeric_cast(treeEntryIndex)),
    };

    SequenceNumber currentHighestSequenceNumber;
    SequenceNumber currentLowestSequenceNumber;
    
    if (treeEntry->child_tree_node())
    {
        currentHighestSequenceNumber = ToSequenceNumber(
            treeEntry->lowest_write_sequence_number_for_key());
        currentLowestSequenceNumber = ToSequenceNumber(
            treeEntry->lowest_write_sequence_number_for_key());
    }
    else if (treeEntry->values())
    {
        currentHighestSequenceNumber = ToSequenceNumber(
            treeEntry->values()->Get(0)->write_sequence_number());
        currentLowestSequenceNumber = ToSequenceNumber(
            (treeEntry->values()->rbegin())->write_sequence_number());
    }
    else
    {
        currentHighestSequenceNumber = ToSequenceNumber(
            treeEntry->lowest_write_sequence_number_for_key());
        currentLowestSequenceNumber = ToSequenceNumber(
            treeEntry->lowest_write_sequence_number_for_key());
    }

    if (currentHighestSequenceNumber > GetLatestSequenceNumber())
    {
        auto error = errorPrototype;
        error.Code = IntegrityCheckErrorCode::Partition_SequenceNumberOutOfMaxRange;
        errorList.push_back(error);
    }

    if (!m_bloomFilter->test(
            SchemaDescriptions::MakeProtoValueKey(
                *m_schema,
                treeEntry->key(),
                treeEntry->flat_key())))
    {
        auto error = errorPrototype;
        error.Code = IntegrityCheckErrorCode::Partition_KeyNotInBloomFilter;
        errorList.push_back(error);
    }

    KeyAndSequenceNumberComparer keyAndSequenceNumberComparer(*m_keyComparer);
    
    // If there is a min key, ensure the tree entry is above that value.
    std::string minKeyExclusiveString;
    if (minKeyExclusive)
    {
        auto keyComparisonResult = keyAndSequenceNumberComparer(
            { minKeyExclusive, minKeyExclusiveLowestSequenceNumber },
            { currentKey, currentLowestSequenceNumber });

        if (keyComparisonResult != std::weak_ordering::less)
        {
            auto error = errorPrototype;
            error.Code = IntegrityCheckErrorCode::Partition_OutOfOrderKey;
            error.Key = currentKey;
            errorList.push_back(error);
            co_return;
        }
    }

    // There is always a max key.
    // Ensure the tree entry is below that value.
    // if (maxKeyInclusive)
    {
        auto maxKeyComparisonResult = keyAndSequenceNumberComparer(
            { currentKey, currentLowestSequenceNumber },
            { maxKeyInclusive, maxKeyInclusiveLowestSequenceNumber});

        if (maxKeyComparisonResult == std::weak_ordering::greater)
        {
            auto error = errorPrototype;
            error.Code = IntegrityCheckErrorCode::Partition_KeyOutOfMaxRange;
            errorList.push_back(error);
            co_return;
        }
    }

    if (parent->tree_node()->level() > 0)
    {
        if (!treeEntry->child_tree_node())
        {
            auto error = errorPrototype;
            error.Code = IntegrityCheckErrorCode::Partition_NonLeafNodeNeedsChild;
            errorList.push_back(error);
            co_return;
        }

        co_await CheckTreeNodeIntegrity(
            errorList,
            errorPrototype,
            treeEntry->child_tree_node(),
            minKeyExclusive,
            minKeyExclusiveLowestSequenceNumber,
            currentKey,
            currentLowestSequenceNumber
        );
    }
    else
    {
        if (treeEntry->child_tree_node())
        {
            auto error = errorPrototype;
            error.Code = IntegrityCheckErrorCode::Partition_LeafNodeHasChild;
            errorList.push_back(error);
        }
        else if (treeEntry->values())
        {
            for (uoffset_t valueIndex = 1;
                valueIndex < treeEntry->values()->size();
                valueIndex++)
            {
                auto previousValueSequenceNumber = ToSequenceNumber(
                    treeEntry->values()->Get(valueIndex - 1)->write_sequence_number());
                auto currentValueSequenceNumber = ToSequenceNumber(
                    treeEntry->values()->Get(valueIndex)->write_sequence_number());

                if (previousValueSequenceNumber <= currentValueSequenceNumber)
                {
                    auto error = errorPrototype;
                    error.Code = IntegrityCheckErrorCode::Partition_OutOfOrderSequenceNumber;
                    error.TreeNodeValueIndex = valueIndex;
                    errorList.push_back(error);
                }

                if (currentValueSequenceNumber > GetLatestSequenceNumber())
                {
                    auto error = errorPrototype;
                    error.Code = IntegrityCheckErrorCode::Partition_SequenceNumberOutOfMaxRange;
                    errorList.push_back(error);
                }
            }
        }
    }
}

task<IntegrityCheckErrorList> Partition::CheckIntegrity(
    const IntegrityCheckError& errorPrototype)
{
    IntegrityCheckErrorList errorList;

    co_await CheckTreeNodeIntegrity(
        errorList,
        errorPrototype,
        m_partitionRootMessage->root()->root_tree_node(),
        nullptr,
        SequenceNumber::Latest,
        nullptr,
        SequenceNumber::Earliest
    );

    co_return errorList;
}

}
