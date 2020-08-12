#include "PartitionImpl.h"
#include "RandomMessageAccessor.h"
#include "src/ProtoStoreInternal.pb.h"
#include "Schema.h"
#include "KeyComparer.h"
#include <compare>

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

task<int> Partition::FindTreeEntry(
    const shared_ptr<PartitionTreeNodeCacheEntry>& partitionTreeNodeCacheEntry,
    const PartitionTreeNode* treeNode,
    const FindTreeEntryKey& findEntryKey
)
{
    auto left = findEntryKey.lastFindResult.value_or(-1) + 1;
    auto right = treeNode->treeentries_size();
    auto readSequenceNumberUint64 = ToUint64(findEntryKey.readSequenceNumber);
    
    while (right > left)
    {
        auto middle = left + (right - left) / 2;

        auto middleKey = co_await partitionTreeNodeCacheEntry->GetKey(
            middle);
        auto& middleTreeNodeEntry = treeNode->treeentries(middle);

        auto keyComparison = m_keyComparer->Compare(
            findEntryKey.key,
            middleKey);

        if (keyComparison == std::weak_ordering::equivalent)
        {
            keyComparison = findEntryKey.matchingKeyComparisonResult;
        }

        if (keyComparison == std::weak_ordering::equivalent)
        {
            if (middleTreeNodeEntry.lowestwritesequencenumber() > readSequenceNumberUint64)
            {
                keyComparison = std::weak_ordering::greater;
            }
            if (middleTreeNodeEntry.highestwritesequencenumber() < readSequenceNumberUint64)
            {
                keyComparison = std::weak_ordering::less;
            }
        }

        if (keyComparison == std::weak_ordering::equivalent)
        {
            co_return middle;
        }
        else if (keyComparison == std::weak_ordering::less)
        {
            right = middle;
        }
        else
        {
            left = middle + 1;
        }
    }

    co_return left;
}

task<int> Partition::FindLowTreeEntryIndex(
    const shared_ptr<PartitionTreeNodeCacheEntry>& partitionTreeNodeCacheEntry,
    const PartitionTreeNode* treeNode,
    SequenceNumber readSequenceNumber,
    KeyRangeEnd low
)
{
    int lowTreeEntryIndex = 0;
    std::weak_ordering lowTreeEntryComparison = std::weak_ordering::less;

    if (low.Key)
    {
        FindTreeEntryKey key;
        key.key = low.Key;
        key.readSequenceNumber = readSequenceNumber;
        
        // We always ignore even non-leaf tree entries whose
        // key matches our low if we're low-exclusive.
        if (low.Inclusivity == Inclusivity::Exclusive)
        {
            key.matchingKeyComparisonResult = std::weak_ordering::greater;
        }

        lowTreeEntryIndex = co_await FindTreeEntry(
            partitionTreeNodeCacheEntry,
            treeNode,
            key);
    }

    co_return lowTreeEntryIndex;
}

task<int> Partition::FindHighTreeEntryIndex(
    const shared_ptr<PartitionTreeNodeCacheEntry>& partitionTreeNodeCacheEntry,
    const PartitionTreeNode* treeNode,
    SequenceNumber readSequenceNumber,
    KeyRangeEnd high
)
{
    int highTreeEntryIndex = treeNode->treeentries_size();
    std::weak_ordering highTreeEntryComparison = std::weak_ordering::less;

    if (high.Key)
    {
        FindTreeEntryKey key;
        key.key = high.Key;
        key.readSequenceNumber = readSequenceNumber;

        if (high.Inclusivity == Inclusivity::Exclusive
            && treeNode->level() == 0)
        {
            key.matchingKeyComparisonResult = std::weak_ordering::equivalent;
        }
        else
        {
            key.matchingKeyComparisonResult = std::weak_ordering::greater;
        }

        highTreeEntryIndex = co_await FindTreeEntry(
            partitionTreeNodeCacheEntry,
            treeNode,
            key);

        if (treeNode->level() != 0)
        {
            highTreeEntryIndex++;
        }
    }

    co_return highTreeEntryIndex;
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

    // Otherwise, do a binary search in the remaining items.
    int left = 1;
    int right = valueSet.values_size();

    while (left < right)
    {
        auto middle = left + (right - left) / 2;
        auto writeSequenceNumber = valueSet.values(middle).writesequencenumber();

        if (writeSequenceNumber > readSequenceNumberInt64)
        {
            left = middle + 1;
        }
        else if (writeSequenceNumber < readSequenceNumberInt64)
        {
            right = middle;
        }
        else
        {
            return middle;
        }
    }

    return left;
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

        if (PartitionTreeEntry::kTreeNodeOffset == treeNodeEntry.PartitionTreeEntryType_case())
        {
            ExtentLocation enumerationLocation =
            {
                m_dataLocation.extentNumber,
                treeNodeEntry.treenodeoffset(),
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
            FindTreeEntryKey findEntryKey;
            findEntryKey.key = key;
            findEntryKey.matchingKeyComparisonResult = std::weak_ordering::greater;
            findEntryKey.lastFindResult = lowTreeEntryIndex;
            findEntryKey.readSequenceNumber = readSequenceNumber;

            lowTreeEntryIndex = co_await FindTreeEntry(
                cacheEntry,
                treeNode,
                findEntryKey);
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
        if ((*iterator).WriteSequenceNumber > readSequenceNumber)
        {
            co_return (*iterator).WriteSequenceNumber;
        }
        break;
    }

    co_return optional<SequenceNumber>();
}
}
