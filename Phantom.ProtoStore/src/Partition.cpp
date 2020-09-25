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

Partition::Partition(
    shared_ptr<KeyComparer> keyComparer,
    shared_ptr<IMessageFactory> keyFactory,
    shared_ptr<IMessageFactory> valueFactory,
    shared_ptr<IRandomMessageAccessor> dataHeaderMessageAccessor,
    shared_ptr<IRandomMessageAccessor> dataMessageAccessor,
    ExtentLocation headerLocation,
    ExtentLocation dataLocation
) :
    m_keyComparer(keyComparer),
    m_keyFactory(keyFactory),
    m_valueFactory(valueFactory),
    m_dataHeaderMessageAccessor(dataHeaderMessageAccessor),
    m_dataMessageAccessor(dataMessageAccessor),
    m_headerLocation(headerLocation),
    m_dataLocation(dataLocation),
    m_partitionTreeNodeCache(
        keyFactory,
        m_dataMessageAccessor
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
    co_await m_dataHeaderMessageAccessor->ReadMessage(
        m_headerLocation,
        message
    );
    assert(message.has_partitionheader());
    m_partitionHeader = move(*message.mutable_partitionheader());

    message.Clear();
    co_await m_dataMessageAccessor->ReadMessage(
        ExtentLocation
        {
            m_dataLocation.extentNumber,
            m_partitionHeader.partitionrootoffset(),
        },
        message);
    assert(message.has_partitionroot());
    m_partitionRoot = move(*message.mutable_partitionroot());

    co_await m_dataMessageAccessor->ReadMessage(
        ExtentLocation
        {
            m_dataLocation.extentNumber,
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
    auto enumeration = Enumerate(
        ExtentLocation 
        { 
            m_dataLocation.extentNumber,
            m_partitionRoot.roottreenodeoffset()
        },
        readSequenceNumber,
        low,
        high,
        readValueDisposition,
        EnumerateBehavior::PointInTimeRead);

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
        .Key = nullptr,
        .Inclusivity = Inclusivity::Inclusive,
    };

    KeyRangeEnd high =
    {
        .Key = nullptr,
        .Inclusivity = Inclusivity::Inclusive,
    };

    if (startKey)
    {
        low.Key = startKey->Key;
        readSequenceNumber = startKey->WriteSequenceNumber;
    }

    auto enumeration = Enumerate(
        ExtentLocation
        {
            m_dataLocation.extentNumber,
            m_partitionRoot.roottreenodeoffset()
        },
        readSequenceNumber,
        low,
        high,
        ReadValueDisposition::ReadValue,
        EnumerateBehavior::Checkpoint);

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
                    : cacheEntry.TreeEntry->has_value()
                    ? cacheEntry.TreeEntry->value().writesequencenumber()
                    : cacheEntry.TreeEntry->valueset().values(cacheEntry.TreeEntry->valueset().values_size() - 1).writesequencenumber()
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
                    : cacheEntry.TreeEntry->has_value()
                    ? cacheEntry.TreeEntry->value().writesequencenumber()
                    : cacheEntry.TreeEntry->valueset().values(cacheEntry.TreeEntry->valueset().values_size() - 1).writesequencenumber()
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
        // and if the user requested an Exclusive search, we find the key.
        high.Inclusivity == Inclusivity::Inclusive ? Inclusivity::Exclusive : Inclusivity::Inclusive,
    };

    auto lowerBound = co_await async_lower_bound(
        co_await partitionTreeNodeCacheEntry->begin(),
        co_await partitionTreeNodeCacheEntry->end(),
        key,
        FindTreeEntryKeyLessThanComparer { *m_keyComparer }
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
    EnumerateBehavior enumerateBehavior
)
{
    auto cacheEntry = co_await m_partitionTreeNodeCache.GetPartitionTreeNodeCacheEntry(
        treeNodeLocation);

    auto treeNode = co_await cacheEntry->ReadTreeNode();

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

    while (
        lowTreeEntryIndex < highTreeEntryIndex
        &&
        lowTreeEntryIndex < treeNode->treeentries_size())
    {
        auto& treeNodeEntry = treeNode->treeentries(lowTreeEntryIndex);
        auto key = co_await cacheEntry->GetKey(lowTreeEntryIndex);
        const Message* nextKey;

        if (treeNodeEntry.has_child())
        {
            ExtentLocation enumerationLocation =
            {
                m_dataLocation.extentNumber,
                treeNodeEntry.child().treenodeoffset(),
            };

            auto subTreeEnumerator = Enumerate(
                enumerationLocation,
                readSequenceNumber,
                low,
                high,
                readValueDisposition,
                enumerateBehavior);

            for (auto iterator = co_await subTreeEnumerator.begin();
                iterator != subTreeEnumerator.end();
                co_await ++iterator)
            {
                co_yield *iterator;
                nextKey = (*iterator).Key;
            }
        }
        else
        {
            unique_ptr<Message> value;

            // We're looking at a matching tree entry, now we need to find the right value
            // based on the write sequence number.
            const PartitionTreeEntryValue* treeEntryValue;
            if (treeNodeEntry.has_value())
            {
                treeEntryValue = &treeNodeEntry.value();
                if (treeEntryValue->writesequencenumber() > ToUint64(readSequenceNumber))
                {
                    treeEntryValue = nullptr;
                }
            }
            else
            {
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
            }

            // treeEntryValue is null if the node matched, but the read sequence number was earlier than any
            // value in the tree node.
            if (treeEntryValue != nullptr)
            {
                // The node matched, and we should have a value to return.
                if (PartitionTreeEntryValue::kValueOffset == treeEntryValue->PartitionTreeEntryValue_case())
                {
                    PartitionMessage message;
                    co_await m_dataMessageAccessor->ReadMessage(
                        {
                            .extentNumber = m_dataLocation.extentNumber,
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
            }

            nextKey = key;
        }

        if (enumerateBehavior == EnumerateBehavior::PointInTimeRead)
        {
            KeyRangeEnd low =
            {
                key,
                Inclusivity::Exclusive,
            };

            lowTreeEntryIndex = co_await FindLowTreeEntryIndex(
                cacheEntry,
                treeNode,
                readSequenceNumber,
                low);
        }
        else
        {
            assert(enumerateBehavior == EnumerateBehavior::Checkpoint);
            lowTreeEntryIndex++;
        }
    }
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
    const Message* precedingKey,
    SequenceNumber precedingSequenceNumber,
    const Message* maxKey,
    SequenceNumber lowestSequenceNumberForMaxKey)
{
    PartitionMessage treeNodeMessage;
    co_await m_dataMessageAccessor->ReadMessage(
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

    const Message* precedingKeyForChild = precedingKey;
    SequenceNumber precedingSequenceNumberForChild = precedingSequenceNumber;
    unique_ptr<Message> localPrecedingKeyForChild;

    for (auto index = 0;
        index < treeNodeMessage.partitiontreenode().treeentries_size();
        index++)
    {
        auto treeEntryErrorPrototype = errorPrototype;
        treeEntryErrorPrototype.Location = treeNodeLocation;
        treeEntryErrorPrototype.TreeNodeEntryIndex = index;
        treeEntryErrorPrototype.Key = treeNodeMessage.partitiontreenode().treeentries(index).key();

        co_await CheckChildTreeEntryIntegrity(
            errorList,
            treeEntryErrorPrototype,
            treeNodeMessage.partitiontreenode(),
            index,
            precedingKeyForChild,
            precedingSequenceNumber,
            maxKey,
            lowestSequenceNumberForMaxKey);

        localPrecedingKeyForChild.reset(
            m_keyFactory->GetPrototype()->New());

        auto& treeEntry = treeNodeMessage.partitiontreenode().treeentries(index);

        localPrecedingKeyForChild->ParseFromString(
            treeEntry.key());
        precedingKeyForChild = localPrecedingKeyForChild.get();

        if (treeEntry.has_child())
        {
            precedingSequenceNumberForChild = ToSequenceNumber(
                treeEntry.child().lowestwritesequencenumberforkey());
        }
        else if (treeEntry.has_value())
        {
            precedingSequenceNumberForChild = ToSequenceNumber(
                treeEntry.value().writesequencenumber());
        }
        else if (treeEntry.has_valueset())
        {
            if (treeEntry.valueset().values_size() < 2)
            {
                auto error = treeEntryErrorPrototype;
                error.Code = IntegrityCheckErrorCode::Partition_ValueSetTooSmall;
                errorList.push_back(error);
            }

            if (treeEntry.valueset().values_size() > 0)
            {
                precedingSequenceNumberForChild = ToSequenceNumber(
                    treeEntry.valueset().values(treeEntry.valueset().values_size() - 1).writesequencenumber());
            }
        }
        else
        {
            auto error = treeEntryErrorPrototype;
            error.Code = IntegrityCheckErrorCode::Partition_NoContentInTreeEntry;
            errorList.push_back(error);
        }
    }
}

task<> Partition::CheckChildTreeEntryIntegrity(
    IntegrityCheckErrorList& errorList,
    const IntegrityCheckError& errorPrototype,
    const PartitionTreeNode& parent,
    size_t treeEntryIndex,
    const Message* precedingKey,
    SequenceNumber precedingSequenceNumber,
    const Message* maxKey,
    SequenceNumber lowestSequenceNumberForMaxKey)
{
    const PartitionTreeEntry& treeEntry = parent.treeentries(treeEntryIndex);

    unique_ptr<Message> keyMessage(
        m_keyFactory->GetPrototype()->New());
    keyMessage->ParseFromString(
        treeEntry.key());

    if (!m_bloomFilter->test(
        treeEntry.key()))
    {
        auto error = errorPrototype;
        error.Code = IntegrityCheckErrorCode::Partition_KeyNotInBloomFilter;
        errorList.push_back(error);
    }

    auto keyComparisonResult = std::weak_ordering::less;
    
    if (precedingKey)
    {
        keyComparisonResult = m_keyComparer->Compare(
            precedingKey,
            keyMessage.get());
    }

    if (keyComparisonResult == std::weak_ordering::greater)
    {
        auto error = errorPrototype;
        error.Code = IntegrityCheckErrorCode::Partition_OutOfOrderKey;
        errorList.push_back(error);
    }

    if (maxKey)
    {
        auto maxKeyComparisonResult = m_keyComparer->Compare(
            keyMessage.get(),
            maxKey);

        if (maxKeyComparisonResult == std::weak_ordering::greater)
        {
            auto error = errorPrototype;
            error.Code = IntegrityCheckErrorCode::Partition_KeyOutOfMaxRange;
            errorList.push_back(error);
        }

        if (maxKeyComparisonResult == std::weak_ordering::equivalent)
        {
            SequenceNumber lowestSequenceNumberForKey;

            if (treeEntry.has_child())
            {
                lowestSequenceNumberForKey = ToSequenceNumber(
                    treeEntry.child().lowestwritesequencenumberforkey());
            }
            else if (treeEntry.has_value())
            {
                lowestSequenceNumberForKey = ToSequenceNumber(
                    treeEntry.value().writesequencenumber());
            }
            else if (treeEntry.has_valueset())
            {
                lowestSequenceNumberForKey = ToSequenceNumber(
                    treeEntry.valueset().values(
                        treeEntry.valueset().values_size() - 1
                    ).writesequencenumber());
            }
            else
            {
                auto error = errorPrototype;
                error.Code = IntegrityCheckErrorCode::Partition_NoContentInTreeEntry;
                errorList.push_back(error);
                co_return;
            }

            if (ToUint64(lowestSequenceNumberForKey) < ToUint64(lowestSequenceNumberForMaxKey))
            {
                auto error = errorPrototype;
                error.Code = IntegrityCheckErrorCode::Partition_SequenceNumberOutOfMinRange;
                errorList.push_back(error);
            }
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

        auto childSequenceNumber = treeEntry.child().lowestwritesequencenumberforkey();

        if (keyComparisonResult == std::weak_ordering::equivalent
            && ToUint64(precedingSequenceNumber) < childSequenceNumber)
        {
            auto error = errorPrototype;
            error.Code = IntegrityCheckErrorCode::Partition_OutOfOrderSequenceNumber;
            errorList.push_back(error);
        }

        if (childSequenceNumber > ToUint64(GetLatestSequenceNumber()))
        {
            auto error = errorPrototype;
            error.Code = IntegrityCheckErrorCode::Partition_SequenceNumberOutOfMaxRange;
            errorList.push_back(error);
        }

        co_await CheckTreeNodeIntegrity(
            errorList,
            errorPrototype,
            { m_dataLocation.extentNumber, treeEntry.child().treenodeoffset() },
            precedingKey,
            precedingSequenceNumber,
            keyMessage.get(),
            ToSequenceNumber(childSequenceNumber)
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
        else if (treeEntry.has_value())
        {
            auto valueSequenceNumber = treeEntry.value().writesequencenumber();

            if (keyComparisonResult == std::weak_ordering::equivalent
                && ToUint64(precedingSequenceNumber) < valueSequenceNumber)
            {
                auto error = errorPrototype;
                error.Code = IntegrityCheckErrorCode::Partition_OutOfOrderSequenceNumber;
                errorList.push_back(error);
            }

            if (valueSequenceNumber > ToUint64(GetLatestSequenceNumber()))
            {
                auto error = errorPrototype;
                error.Code = IntegrityCheckErrorCode::Partition_SequenceNumberOutOfMaxRange;
                errorList.push_back(error);
            }
        }
        else if (treeEntry.has_valueset())
        {
            for (auto valueIndex = 0;
                valueIndex < treeEntry.valueset().values_size();
                valueIndex++)
            {
                auto valueSequenceNumber = treeEntry.valueset().values(valueIndex).writesequencenumber();

                if (keyComparisonResult == std::weak_ordering::equivalent
                    && ToUint64(precedingSequenceNumber) < valueSequenceNumber)
                {
                    auto error = errorPrototype;
                    error.Code = IntegrityCheckErrorCode::Partition_OutOfOrderSequenceNumber;
                    error.TreeNodeValueIndex = valueIndex;
                    errorList.push_back(error);
                }

                if (valueSequenceNumber > ToUint64(GetLatestSequenceNumber()))
                {
                    auto error = errorPrototype;
                    error.Code = IntegrityCheckErrorCode::Partition_SequenceNumberOutOfMaxRange;
                    errorList.push_back(error);
                }

                keyComparisonResult = std::weak_ordering::equivalent;
                precedingSequenceNumber = ToSequenceNumber(valueSequenceNumber);
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
        { m_dataLocation.extentNumber,  m_partitionRoot.roottreenodeoffset() },
        nullptr,
        SequenceNumber::Latest,
        nullptr,
        SequenceNumber::Earliest
    );

    co_return errorList;
}

}
