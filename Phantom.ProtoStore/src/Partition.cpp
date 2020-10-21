#include "PartitionImpl.h"
#include "RandomMessageAccessor.h"
#include "src/ProtoStoreInternal.pb.h"
#include "Schema.h"
#include "KeyComparer.h"
#include <compare>
#include <algorithm>
#include "PartitionTreeNodeCache.h"
#include "KeyComparer.h"
#include "Phantom.System/async_utility.h"

namespace Phantom::ProtoStore
{

struct Partition::EnumerateLastReturnedKey
{
    shared_ptr<PartitionTreeNodeCacheEntry> CacheEntry;
    const Message* Key = nullptr;
};

Partition::Partition(
    shared_ptr<KeyComparer> keyComparer,
    shared_ptr<IMessageFactory> keyFactory,
    shared_ptr<IMessageFactory> valueFactory,
    shared_ptr<IRandomMessageAccessor> messageAccessor,
    ExtentLocation headerLocation,
    ExtentName dataExtentName
) :
    m_keyComparer(keyComparer),
    m_keyFactory(keyFactory),
    m_valueFactory(valueFactory),
    m_messageAccessor(messageAccessor),
    m_headerLocation(headerLocation),
    m_dataExtentName(dataExtentName),
    m_partitionTreeNodeCache(
        keyFactory,
        m_messageAccessor
    )
{
}

Partition::~Partition()
{
    SyncDestroy();
}

task<> Partition::Open()
{
    PartitionMessage message;

    message.Clear();
    co_await m_messageAccessor->ReadMessage(
        m_headerLocation,
        message
    );
    assert(message.has_partitionheader());
    m_partitionHeader = move(*message.mutable_partitionheader());

    message.Clear();
    co_await m_messageAccessor->ReadMessage(
        ExtentLocation
        {
            m_dataExtentName,
            m_partitionHeader.partitionrootoffset(),
        },
        message);
    assert(message.has_partitionroot());
    m_partitionRoot = move(*message.mutable_partitionroot());

    co_await m_messageAccessor->ReadMessage(
        ExtentLocation
        {
            m_dataExtentName,
            m_partitionRoot.bloomfilteroffset(),
        },
        message);
    assert(message.has_partitionbloomfilter());
    m_partitionBloomFilter = move(*message.mutable_partitionbloomfilter());

    auto span = std::span(
        m_partitionBloomFilter.filter().cbegin(),
        m_partitionBloomFilter.filter().cend()
    );

    m_bloomFilter.emplace(
        span,
        m_partitionBloomFilter.hashfunctioncount()
        );
}

task<size_t> Partition::GetRowCount()
{
    co_return m_partitionRoot.rowcount();
}

task<ExtentOffset> Partition::GetApproximateDataSize()
{
    co_return m_partitionHeader.partitionrootoffset();
}

cppcoro::async_generator<ResultRow> Partition::Read(
    SequenceNumber readSequenceNumber,
    const Message* key,
    ReadValueDisposition readValueDisposition
)
{
    std::string serializedKey;
    key->SerializeToString(
        &serializedKey
    );

    if (!m_bloomFilter->test(
        serializedKey))
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

cppcoro::async_generator<ResultRow> Partition::Enumerate(
    SequenceNumber readSequenceNumber,
    KeyRangeEnd low,
    KeyRangeEnd high,
    ReadValueDisposition readValueDisposition
)
{
    EnumerateLastReturnedKey unusedEnumerateLastReturnedKey;

    auto enumeration = Enumerate(
        ExtentLocation 
        { 
            m_dataExtentName,
            m_partitionRoot.roottreenodeoffset()
        },
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

cppcoro::async_generator<ResultRow> Partition::Checkpoint(
    std::optional<PartitionCheckpointStartKey> startKey
)
{
    auto readSequenceNumber = SequenceNumber::Latest;

    KeyRangeEnd low =
    {
        .Key = &KeyMinMessage,
        .Inclusivity = Inclusivity::Inclusive,
    };

    KeyRangeEnd high =
    {
        .Key = &KeyMaxMessage,
        .Inclusivity = Inclusivity::Inclusive,
    };

    if (startKey)
    {
        low.Key = startKey->Key;
        readSequenceNumber = startKey->WriteSequenceNumber;
    }

    EnumerateLastReturnedKey unusedEnumerateLastReturnedKey;

    auto enumeration = Enumerate(
        ExtentLocation
        {
            m_dataExtentName,
            m_partitionRoot.roottreenodeoffset()
        },
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
{
    const KeyComparer& m_keyComparer;
    
    FindTreeEntryKeyLessThanComparer(
        const KeyComparer& keyComparer
    ) : m_keyComparer(keyComparer)
    {}

    task<bool> operator()(
        const PartitionTreeNodeCacheEntry::iterator_type::value_type& cacheEntry,
        const KeyRangeComparerArgument& key
        )
    {
        KeyRangeLessThanComparer comparer(
            m_keyComparer);

        KeyAndSequenceNumberComparerArgument cacheEntryKey
        {
            co_await *(cacheEntry.Key),
            ToSequenceNumber(
                cacheEntry.TreeEntry->has_child()
                    ? cacheEntry.TreeEntry->child().lowestwritesequencenumberforkey()
                    : (cacheEntry.TreeEntry->valueset().values().end() - 1)->writesequencenumber()
            )
        };

        co_return comparer(cacheEntryKey, key);
    }

    task<bool> operator()(
        const KeyRangeComparerArgument& key,
        const PartitionTreeNodeCacheEntry::iterator_type::value_type& cacheEntry
        )
    {
        KeyRangeLessThanComparer comparer(
            m_keyComparer);

        KeyAndSequenceNumberComparerArgument cacheEntryKey
        {
            co_await *(cacheEntry.Key),
            ToSequenceNumber(
                cacheEntry.TreeEntry->has_child()
                    ? cacheEntry.TreeEntry->child().lowestwritesequencenumberforkey()
                    : (cacheEntry.TreeEntry->valueset().values().end() - 1)->writesequencenumber()
            )
        };

        co_return comparer(key, cacheEntryKey);
    }
};

task<int> Partition::FindLowTreeEntryIndex(
    const shared_ptr<PartitionTreeNodeCacheEntry>& partitionTreeNodeCacheEntry,
    const PartitionTreeNode* treeNode,
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

    auto lowerBound = co_await async_lower_bound(
        co_await partitionTreeNodeCacheEntry->begin(),
        co_await partitionTreeNodeCacheEntry->end(),
        key,
        FindTreeEntryKeyLessThanComparer { *m_keyComparer }
    );

    co_return lowerBound - co_await partitionTreeNodeCacheEntry->begin();
}

task<int> Partition::FindHighTreeEntryIndex(
    const shared_ptr<PartitionTreeNodeCacheEntry>& partitionTreeNodeCacheEntry,
    const PartitionTreeNode* treeNode,
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

    auto lowerBound = co_await async_lower_bound(
        co_await partitionTreeNodeCacheEntry->begin(),
        co_await partitionTreeNodeCacheEntry->end(),
        key,
        FindTreeEntryKeyLessThanComparer{ *m_keyComparer }
    );

    co_return lowerBound - co_await partitionTreeNodeCacheEntry->begin();
}

int Partition::FindMatchingValueIndexByWriteSequenceNumber(
    const PartitionTreeEntryValueSet& valueSet,
    SequenceNumber readSequenceNumber)
{
    auto readSequenceNumberInt64 = ToUint64(readSequenceNumber);

    // Most of the time, we're returning the first entry.
    if (valueSet.values(0).writesequencenumber() <= readSequenceNumberInt64)
    {
        return 0;
    }

    struct comparer
    {
        bool operator()(
            const PartitionTreeEntryValue& partitionTreeEntryValue,
            SequenceNumber readSequenceNumber
            ) const
        {
            return partitionTreeEntryValue.writesequencenumber() > ToUint64(readSequenceNumber);
        }

        bool operator()(
            SequenceNumber readSequenceNumber,
            const PartitionTreeEntryValue& partitionTreeEntryValue
            ) const
        {
            return ToUint64(readSequenceNumber) > partitionTreeEntryValue.writesequencenumber();
        }
    };

    return std::lower_bound(
        valueSet.values().begin() + 1,
        valueSet.values().end(),
        readSequenceNumber,
        comparer()
    ) - valueSet.values().begin();
}

cppcoro::async_generator<ResultRow> Partition::Enumerate(
    ExtentLocation treeNodeLocation,
    SequenceNumber readSequenceNumber,
    KeyRangeEnd low,
    KeyRangeEnd high,
    ReadValueDisposition readValueDisposition,
    EnumerateBehavior enumerateBehavior,
    EnumerateLastReturnedKey& lastReturnedKey
)
{
    auto cacheEntry = co_await m_partitionTreeNodeCache.GetPartitionTreeNodeCacheEntry(
        treeNodeLocation);

    auto treeNode = co_await cacheEntry->ReadTreeNode();

//#ifndef NDEBUG
//    auto treeNodeString = treeNode->DebugString();
//#endif

    int lowTreeEntryIndex = co_await FindLowTreeEntryIndex(
        cacheEntry,
        treeNode,
        readSequenceNumber,
        low);

    int highTreeEntryIndex = co_await FindHighTreeEntryIndex(
        cacheEntry,
        treeNode,
        readSequenceNumber,
        high);

    const Message* lastReturnedKeyMessage = nullptr;

    while (
        (
            treeNode->level() == 0 && lowTreeEntryIndex < highTreeEntryIndex
            ||
            treeNode->level() > 0 && lowTreeEntryIndex <= highTreeEntryIndex
        )
        &&
        lowTreeEntryIndex < treeNode->treeentries_size())
    {
        auto& treeNodeEntry = treeNode->treeentries(lowTreeEntryIndex);
        auto key = co_await cacheEntry->GetKey(lowTreeEntryIndex);

        if (treeNodeEntry.has_child())
        {
            ExtentLocation enumerationLocation =
            {
                m_dataExtentName,
                treeNodeEntry.child().treenodeoffset(),
            };

            EnumerateLastReturnedKey childEnumerateLastReturnedKey;

            auto subTreeEnumerator = Enumerate(
                enumerationLocation,
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
            unique_ptr<Message> value;

            // We're looking at a matching tree entry, now we need to find the right value
            // based on the write sequence number.
            const PartitionTreeEntryValue* treeEntryValue;
            auto valueIndex = FindMatchingValueIndexByWriteSequenceNumber(
                treeNodeEntry.valueset(),
                readSequenceNumber);

            if (valueIndex < treeNodeEntry.valueset().values_size())
            {
                treeEntryValue = &treeNodeEntry.valueset().values(valueIndex);
            }
            else
            {
                treeEntryValue = nullptr;
            }

            // The node matched, and we might have a value to return;
            // except we might have found a tree entry whose values are all newer
            // than the read sequence number.
            if (treeEntryValue == nullptr)
            {
                ++lowTreeEntryIndex;
            }
            else
            {
                if (PartitionTreeEntryValue::kValueOffset == treeEntryValue->PartitionTreeEntryValue_case())
                {
                    PartitionMessage message;
                    co_await m_messageAccessor->ReadMessage(
                        {
                            .extentName = m_dataExtentName,
                            .extentOffset = treeEntryValue->valueoffset(),
                        },
                        message);

                    assert(message.PartitionMessageType_case() == PartitionMessage::kValue);

                    value.reset(m_valueFactory->GetPrototype()->New());
                    value->ParseFromString(
                        message.value());
                }
                else if (readValueDisposition == ReadValueDisposition::DontReadValue)
                {
                    value.reset();
                }
                else if (PartitionTreeEntryValue::kValue == treeEntryValue->PartitionTreeEntryValue_case())
                {
                    value.reset(m_valueFactory->GetPrototype()->New());
                    value->ParseFromString(
                        treeEntryValue->value());
                }
                else
                {
                    assert(PartitionTreeEntryValue::kDeleted == treeEntryValue->PartitionTreeEntryValue_case());
                    value.reset();
                }

                co_yield ResultRow
                {
                    .Key = key,
                    .WriteSequenceNumber = ToSequenceNumber(treeEntryValue->writesequencenumber()),
                    .Value = value.get(),
                };

                if (enumerateBehavior == EnumerateBehavior::PointInTimeRead)
                {
                    low =
                    {
                        key,
                        Inclusivity::Exclusive,
                    };

                    lowTreeEntryIndex = co_await FindLowTreeEntryIndex(
                        cacheEntry,
                        treeNode,
                        readSequenceNumber,
                        low);

                    lastReturnedKey = EnumerateLastReturnedKey
                    {
                        cacheEntry,
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
        m_partitionRoot.latestsequencenumber());
}

task<optional<SequenceNumber>> Partition::CheckForWriteConflict(
    SequenceNumber readSequenceNumber,
    SequenceNumber writeSequenceNumber,
    const Message* key
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
    ExtentLocation treeNodeLocation,
    const Message* minKeyExclusive,
    SequenceNumber minKeyExclusiveLowestSequenceNumber,
    const Message* maxKeyInclusive,
    SequenceNumber maxKeyInclusiveLowestSequenceNumber)
{
    PartitionMessage treeNodeMessage;
    co_await m_messageAccessor->ReadMessage(
        treeNodeLocation,
        treeNodeMessage
    );

    if (!treeNodeMessage.has_partitiontreenode())
    {
        auto error = errorPrototype;
        error.Code = IntegrityCheckErrorCode::Partition_MissingTreeNode;
        error.Location = treeNodeLocation;
        errorList.push_back(error);
        co_return;
    }
    
    if (treeNodeMessage.partitiontreenode().treeentries_size() == 0)
    {
        co_return;
    }

    unique_ptr<Message> maxKeyInclusiveHolder;
    if (!maxKeyInclusive)
    {
        GetKeyValues(
            *(treeNodeMessage.partitiontreenode().treeentries().end() - 1),
            maxKeyInclusiveHolder,
            maxKeyInclusiveLowestSequenceNumber,
            maxKeyInclusiveLowestSequenceNumber
        );
        maxKeyInclusive = maxKeyInclusiveHolder.get();
    }

    unique_ptr<Message> previousKeyHolder;
    const Message* previousKey = minKeyExclusive;
    SequenceNumber previousKeyLowestSequenceNumber = minKeyExclusiveLowestSequenceNumber;

    for (auto index = 0;
        index < treeNodeMessage.partitiontreenode().treeentries_size();
        index++)
    {
        auto treeEntryErrorPrototype = errorPrototype;
        treeEntryErrorPrototype.Location = treeNodeLocation;
        treeEntryErrorPrototype.TreeNodeEntryIndex = index;
        treeEntryErrorPrototype.Key = treeNodeMessage.partitiontreenode().treeentries(index).key();

        auto& treeEntry = treeNodeMessage.partitiontreenode().treeentries(index);

        unique_ptr<Message> currentKeyHolder;
        const Message* currentKey;
        SequenceNumber currentKeyHighestSequenceNumber;
        SequenceNumber currentKeyLowestSequenceNumber;

        GetKeyValues(
            treeNodeMessage.partitiontreenode().treeentries(index),
            currentKeyHolder,
            currentKeyHighestSequenceNumber,
            currentKeyLowestSequenceNumber);
        currentKey = currentKeyHolder.get();

        co_await CheckChildTreeEntryIntegrity(
            errorList,
            treeEntryErrorPrototype,
            treeNodeMessage.partitiontreenode(),
            index,
            currentKey,
            previousKey,
            previousKeyLowestSequenceNumber,
            currentKey,
            currentKeyLowestSequenceNumber);

        previousKeyHolder = move(currentKeyHolder);
        previousKey = currentKeyHolder.get();
        previousKeyLowestSequenceNumber = currentKeyLowestSequenceNumber;
    }
}

void Partition::GetKeyValues(
    const PartitionTreeEntry& treeEntry,
    unique_ptr<Message>& key,
    SequenceNumber& highestSequenceNumber,
    SequenceNumber& lowestSequenceNumber)
{
    key.reset(
        m_keyFactory->GetPrototype()->New());

    if (treeEntry.has_child())
    {
        highestSequenceNumber = ToSequenceNumber(
            treeEntry.child().lowestwritesequencenumberforkey());
        lowestSequenceNumber = ToSequenceNumber(
            treeEntry.child().lowestwritesequencenumberforkey());
    }
    else if (treeEntry.has_valueset()
        && treeEntry.valueset().values_size() > 0)
    {
        highestSequenceNumber = ToSequenceNumber(
            treeEntry.valueset().values().begin()->writesequencenumber());
        lowestSequenceNumber = ToSequenceNumber(
            (treeEntry.valueset().values().end() - 1)->writesequencenumber());
    }
    else
    {
        highestSequenceNumber = SequenceNumber::Latest;
        lowestSequenceNumber = SequenceNumber::Earliest;
    }
}

task<> Partition::CheckChildTreeEntryIntegrity(
    IntegrityCheckErrorList& errorList,
    const IntegrityCheckError& errorPrototype,
    const PartitionTreeNode& parent,
    size_t treeEntryIndex,
    const Message* currentKey,
    const Message* minKeyExclusive,
    SequenceNumber minKeyExclusiveLowestSequenceNumber,
    const Message* maxKeyInclusive,
    SequenceNumber maxKeyInclusiveLowestSequenceNumber)
{
    const PartitionTreeEntry& treeEntry = parent.treeentries(treeEntryIndex);

    SequenceNumber currentHighestSequenceNumber;
    SequenceNumber currentLowestSequenceNumber;
    if (treeEntry.has_child())
    {
        currentHighestSequenceNumber = ToSequenceNumber(
            treeEntry.child().lowestwritesequencenumberforkey());
        currentLowestSequenceNumber = ToSequenceNumber(
            treeEntry.child().lowestwritesequencenumberforkey());
    }
    else if (treeEntry.has_valueset()
        && treeEntry.valueset().values_size() > 0)
    {
        currentHighestSequenceNumber = ToSequenceNumber(
            treeEntry.valueset().values(0).writesequencenumber());
        currentLowestSequenceNumber = ToSequenceNumber(
            (treeEntry.valueset().values().end() - 1)->writesequencenumber());
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
        treeEntry.key()))
    {
        auto error = errorPrototype;
        error.Code = IntegrityCheckErrorCode::Partition_KeyNotInBloomFilter;
        errorList.push_back(error);
    }

    KeyAndSequenceNumberComparer keyAndSequenceNumberComparer(*m_keyComparer);

    // If there is a min key, ensure the tree entry is above that value.
    if (minKeyExclusive)
    {
        auto keyComparisonResult = keyAndSequenceNumberComparer(
            { minKeyExclusive, minKeyExclusiveLowestSequenceNumber },
            { currentKey, currentLowestSequenceNumber });

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
    // if (maxKeyExclusive)
    {
        auto maxKeyComparisonResult = keyAndSequenceNumberComparer(
            { currentKey, currentLowestSequenceNumber },
            { maxKeyInclusive, maxKeyInclusiveLowestSequenceNumber });

        if (maxKeyComparisonResult == std::weak_ordering::greater)
        {
            auto error = errorPrototype;
            error.Code = IntegrityCheckErrorCode::Partition_KeyOutOfMaxRange;
            errorList.push_back(error);
            co_return;
        }
    }

    if (parent.level() > 0)
    {
        if (!treeEntry.has_child())
        {
            auto error = errorPrototype;
            error.Code = IntegrityCheckErrorCode::Partition_NonLeafNodeNeedsChild;
            errorList.push_back(error);
            co_return;
        }

        co_await CheckTreeNodeIntegrity(
            errorList,
            errorPrototype,
            { m_dataExtentName, treeEntry.child().treenodeoffset() },
            minKeyExclusive,
            minKeyExclusiveLowestSequenceNumber,
            currentKey,
            currentLowestSequenceNumber
        );
    }
    else
    {
        if (treeEntry.has_child())
        {
            auto error = errorPrototype;
            error.Code = IntegrityCheckErrorCode::Partition_LeafNodeHasChild;
            errorList.push_back(error);
        }
        else if (treeEntry.has_valueset())
        {
            for (auto valueIndex = 1;
                valueIndex < treeEntry.valueset().values_size();
                valueIndex++)
            {
                auto previousValueSequenceNumber = ToSequenceNumber(
                    treeEntry.valueset().values(valueIndex - 1).writesequencenumber());
                auto currentValueSequenceNumber = ToSequenceNumber(
                    treeEntry.valueset().values(valueIndex).writesequencenumber());

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
        { m_dataExtentName,  m_partitionRoot.roottreenodeoffset() },
        nullptr,
        SequenceNumber::Latest,
        nullptr,
        SequenceNumber::Earliest
    );

    co_return errorList;
}

}
