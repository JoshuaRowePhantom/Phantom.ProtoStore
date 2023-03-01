#include "Checksum.h"
#include "KeyComparer.h"
#include "PartitionImpl.h"
#include "Phantom.System/async_utility.h"
#include "MessageStore.h"
#include "Schema.h"
#include "src/ProtoStoreInternal_generated.h"
#include <algorithm>
#include <compare>

#include <boost/crc.hpp>
namespace Phantom::ProtoStore
{

RawData GetRawData(
    const IsFlatMessage auto& reference,
    const FlatBuffers::PartitionDataValue* dataValue)
{
    return RawData
    {
        DataReference<StoredMessage>{ reference },
        get_byte_span(dataValue->data()),
    };
}

size_t BloomFilterV1Hash::operator()(const auto& value) const
{
    return checksum_v1(
        as_bytes(std::span{ value })
    );
}

struct Partition::EnumerateLastReturnedKey
{
    RawData Key;
};

Partition::Partition(
    shared_ptr<KeyComparer> keyComparer,
    shared_ptr<IRandomMessageReader> partitionHeaderReader,
    shared_ptr<IRandomMessageReader> partitionDataReader
) :
    m_keyComparer(std::move(keyComparer)),
    m_partitionHeaderReader(std::move(partitionHeaderReader)),
    m_partitionDataReader(std::move(partitionDataReader))
{
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
    
    m_partitionBloomFilterMessage = co_await ReadData(
        m_partitionRootMessage->root()->bloom_filter());

    std::span<const char> bloomFilterSpan
    {
        reinterpret_cast<const char*>(m_partitionBloomFilterMessage->bloom_filter()->filter()->data()),
        m_partitionBloomFilterMessage->bloom_filter()->filter()->size(),
    };

    m_bloomFilter.emplace(
        bloomFilterSpan,
        m_partitionBloomFilterMessage->bloom_filter()->hash_function_count()
    );

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
    std::span<const byte> key,
    ReadValueDisposition readValueDisposition
)
{
    if (!m_bloomFilter->test(
        key))
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
        low,
        high,
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
        .Key = KeyMinSpan,
        .Inclusivity = Inclusivity::Inclusive,
    };

    KeyRangeEnd high =
    {
        .Key = KeyMaxSpan,
        .Inclusivity = Inclusivity::Inclusive,
    };

    if (startKey)
    {
        low.Key = *startKey->Key;
        readSequenceNumber = startKey->WriteSequenceNumber;
    }

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
    const KeyComparer& m_keyComparer;
    
    FindTreeEntryKeyLessThanComparer(
        const KeyComparer& keyComparer
    ) : m_keyComparer(keyComparer)
    {}

    bool operator()(
        const PartitionTreeEntryKey* keyEntry,
        const KeyRangeComparerArgument& key
        )
    {
        KeyRangeLessThanComparer comparer(
            m_keyComparer);

        auto keySpan = get_byte_span(
            keyEntry->key()->data());

        KeyAndSequenceNumberComparerArgument cacheEntryKey
        {
            keySpan,
            ToSequenceNumber(keyEntry->lowest_write_sequence_number_for_key())
        };

        return comparer(cacheEntryKey, key);
    }

    bool operator()(
        const KeyRangeComparerArgument& key,
        const PartitionTreeEntryKey* keyEntry
        )
    {
        KeyRangeLessThanComparer comparer(
            m_keyComparer);

        auto keySpan = get_byte_span(
            keyEntry->key()->data());

        KeyAndSequenceNumberComparerArgument cacheEntryKey
        {
            keySpan,
            ToSequenceNumber(keyEntry->lowest_write_sequence_number_for_key()),
        };

        return comparer(key, cacheEntryKey);
    }
};

int Partition::FindLowTreeEntryIndex(
    const FlatMessage<PartitionMessage>& treeNode,
    SequenceNumber readSequenceNumber,
    KeyRangeEnd low
)
{
    KeyRangeLessThanComparer keyRangeLessThanComparer{ *m_keyComparer };
    
    KeyRangeComparerArgument key
    {
        low.Key,
        readSequenceNumber,
        low.Inclusivity,
    };

    // VectorIterator doesn't accept its difference_type in its += operator.
#pragma warning (push)
#pragma warning (disable: 4244)
    auto lowerBound = std::lower_bound(
        treeNode->tree_node()->keys()->begin(),
        treeNode->tree_node()->keys()->end(),
        key,
        FindTreeEntryKeyLessThanComparer { *m_keyComparer }
    );
#pragma warning (pop)

    return lowerBound - treeNode->tree_node()->keys()->begin();
}

int Partition::FindHighTreeEntryIndex(
    const FlatMessage<PartitionMessage>& treeNode,
    SequenceNumber readSequenceNumber,
    KeyRangeEnd high
)
{
    KeyRangeLessThanComparer keyRangeLessThanComparer{ *m_keyComparer };

    KeyRangeComparerArgument key
    {
        high.Key,
        readSequenceNumber,
        // We invert the sense of exclusivity so that
        // if the user requested an Inclusive search, we find one past the key,
        // and if the user requested an Exclusive search, we find the key or just after it.
        high.Inclusivity == Inclusivity::Inclusive ? Inclusivity::Exclusive : Inclusivity::Inclusive,
    };

    auto lowerBound = std::lower_bound(
        treeNode->tree_node()->keys()->begin(),
        treeNode->tree_node()->keys()->end(),
        key,
        FindTreeEntryKeyLessThanComparer{ *m_keyComparer }
    );

    return lowerBound - treeNode->tree_node()->keys()->begin();
}

int Partition::FindMatchingValueIndexByWriteSequenceNumber(
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
    int lowTreeEntryIndex = FindLowTreeEntryIndex(
        treeNode,
        readSequenceNumber,
        low);

    int highTreeEntryIndex = FindHighTreeEntryIndex(
        treeNode,
        readSequenceNumber,
        high);

    const Message* lastReturnedKeyMessage = nullptr;

    while (
        (
            treeNode->tree_node()->level() == 0 && lowTreeEntryIndex < highTreeEntryIndex
            ||
            treeNode->tree_node()->level() > 0 && lowTreeEntryIndex <= highTreeEntryIndex
        )
        &&
        lowTreeEntryIndex < treeNode->tree_node()->keys()->size())
    {
        const FlatBuffers::PartitionTreeEntryKey* keyEntry = treeNode->tree_node()->keys()->Get(lowTreeEntryIndex);
        
        RawData key = GetRawData(
            treeNode,
            keyEntry->key());

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
                childEnumerateLastReturnedKey.Key->data())
            {
                low =
                {
                    *childEnumerateLastReturnedKey.Key,
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
            const FlatBuffers::PartitionTreeEntryValue* treeEntryValue;
            auto valueIndex = FindMatchingValueIndexByWriteSequenceNumber(
                keyEntry,
                readSequenceNumber);

            if (valueIndex < keyEntry->values()->size())
            {
                treeEntryValue = keyEntry->values()->Get(valueIndex);
            }
            else
            {
                treeEntryValue = nullptr;
            }

            RawData value;

            // The node matched, and we might have a value to return;
            // except we might have found a tree entry whose values are all newer
            // than the read sequence number.
            if (treeEntryValue == nullptr)
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
                if (readValueDisposition == ReadValueDisposition::DontReadValue)
                {
                } else if (treeEntryValue->value())
                {
                    value = GetRawData(
                        treeNode,
                        treeEntryValue->value());
                }
                else if (treeEntryValue->big_value())
                {
                    auto valueMessage = co_await ReadData(
                        treeEntryValue->big_value());

                    value = GetRawData(
                        valueMessage,
                        valueMessage->value());
                }

                co_yield ResultRow
                {
                    .Key = key,
                    .WriteSequenceNumber = ToSequenceNumber(treeEntryValue->write_sequence_number()),
                    .Value = std::move(value),
                };

                if (enumerateBehavior == EnumerateBehavior::PointInTimeRead)
                {
                    low =
                    {
                        *key,
                        Inclusivity::Exclusive,
                    };

                    lowTreeEntryIndex = FindLowTreeEntryIndex(
                        treeNode,
                        readSequenceNumber,
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
    std::span<const byte> key
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
    RawData minKeyExclusive,
    SequenceNumber minKeyExclusiveLowestSequenceNumber,
    RawData maxKeyInclusive,
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

    if (!maxKeyInclusive->data())
    {
        GetKeyValues(
            FlatMessage<PartitionTreeEntryKey>{ treeNodeMessage, *treeNodeMessage->tree_node()->keys()->rbegin() },
            maxKeyInclusive,
            maxKeyInclusiveLowestSequenceNumber,
            maxKeyInclusiveLowestSequenceNumber
        );
    }

    RawData previousKey = minKeyExclusive;
    SequenceNumber previousKeyLowestSequenceNumber = minKeyExclusiveLowestSequenceNumber;

    for (auto index = 0;
        index < treeNodeMessage->tree_node()->keys()->size();
        index++)
    {
        auto treeEntryErrorPrototype = errorPrototype;
        treeEntryErrorPrototype.Location.extentOffset = errorLocationExtentOffset;
        treeEntryErrorPrototype.TreeNodeEntryIndex = index;
        treeEntryErrorPrototype.Key = GetRawData(
            treeNodeMessage,
            treeNodeMessage->tree_node()->keys()->Get(index)->key());

        auto treeEntry = treeNodeMessage->tree_node()->keys()->Get(index);

        RawData currentKey;
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
    RawData& key,
    SequenceNumber& highestSequenceNumber,
    SequenceNumber& lowestSequenceNumber)
{
    key = GetRawData(
        keyEntry,
        keyEntry->key());

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
    RawData currentKey,
    RawData minKeyExclusive,
    SequenceNumber minKeyExclusiveLowestSequenceNumber,
    RawData maxKeyInclusive,
    SequenceNumber maxKeyInclusiveLowestSequenceNumber)
{
    FlatMessage<FlatBuffers::PartitionTreeEntryKey> treeEntry
    {
        parent,
        parent->tree_node()->keys()->Get(treeEntryIndex),
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
        auto error = errorPrototype;
        error.Code = IntegrityCheckErrorCode::Partition_NoContentInTreeEntry;
        errorList.push_back(error);
        co_return;
    }

    if (currentHighestSequenceNumber > GetLatestSequenceNumber())
    {
        auto error = errorPrototype;
        error.Code = IntegrityCheckErrorCode::Partition_SequenceNumberOutOfMaxRange;
        errorList.push_back(error);
    }

    if (!m_bloomFilter->test(
        get_byte_span(treeEntry->key()->data())))
    {
        auto error = errorPrototype;
        error.Code = IntegrityCheckErrorCode::Partition_KeyNotInBloomFilter;
        errorList.push_back(error);
    }

    KeyAndSequenceNumberComparer keyAndSequenceNumberComparer(*m_keyComparer);
    
    // If there is a min key, ensure the tree entry is above that value.
    std::string minKeyExclusiveString;
    if (minKeyExclusive->data())
    {
        auto keyComparisonResult = keyAndSequenceNumberComparer(
            { *minKeyExclusive, minKeyExclusiveLowestSequenceNumber },
            { *currentKey, currentLowestSequenceNumber });

        if (keyComparisonResult != std::weak_ordering::greater)
        {
            auto error = errorPrototype;
            error.Code = IntegrityCheckErrorCode::Partition_OutOfOrderKey;
            errorList.push_back(error);
            co_return;
        }
    }

    // There is always a max key.
    // Ensure the tree entry is below that value.
    // if (maxKeyInclusive)
    {
        auto maxKeyComparisonResult = keyAndSequenceNumberComparer(
            { *currentKey, currentLowestSequenceNumber },
            { *maxKeyInclusive, maxKeyInclusiveLowestSequenceNumber});

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
            for (auto valueIndex = 1;
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
        else
        {
            auto error = errorPrototype;
            error.Code = IntegrityCheckErrorCode::Partition_LeafNodeNeedsValueOrValueSet;
            errorList.push_back(error);
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
