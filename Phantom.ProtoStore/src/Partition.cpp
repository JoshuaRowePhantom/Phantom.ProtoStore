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

cppcoro::async_generator<ResultRow> Partition::Read(
    SequenceNumber readSequenceNumber,
    const Message* key
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
        keyRangeEnd);

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
    KeyRangeEnd high
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
        high);

    for (auto iterator = co_await enumeration.begin();
        iterator != enumeration.end();
        co_await ++iterator)
    {
        co_yield *iterator;
    }
}

task<std::tuple<int, std::weak_ordering>> Partition::FindTreeEntry(
    const shared_ptr<PartitionTreeNodeCacheEntry>& partitionTreeNodeCacheEntry,
    const FindTreeEntryKey& findEntryKey
)
{
    auto treeNode = co_await partitionTreeNodeCacheEntry->ReadTreeNode();
    auto left = findEntryKey.lastFindResult.value_or(0);
    auto right = treeNode->treeentries_size();
    auto readSequenceNumberUint64 = ToUint64(findEntryKey.readSequenceNumber);

    while (left < right)
    {
        auto middle = left + (right - left) / 2;

        auto middleKey = co_await partitionTreeNodeCacheEntry->GetKey(
            middle);
        auto& middleTreeNodeEntry = treeNode->treeentries(middle);

        auto keyComparison = m_keyComparer->Compare(
            findEntryKey.key,
            middleKey);

        if (keyComparison == std::weak_ordering::equivalent
            && findEntryKey.inclusivity == Inclusivity::Exclusive)
        {
            keyComparison = std::weak_ordering::greater;
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
            co_return make_tuple(middle, keyComparison);
        }
        else if (keyComparison == std::weak_ordering::less)
        {
            right = middle - 1;
        }
        else
        {
            left = middle + 1;
        }
    }

    co_return make_tuple(left, std::weak_ordering::greater);
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
    int right = valueSet.values_size() - 1;

    while (left < right)
    {
        auto middle = left + (right - left) / 2;
        auto writeSequenceNumber = valueSet.values(middle).writesequencenumber();

        if (writeSequenceNumber > readSequenceNumberInt64)
        {
            right = middle - 1;
        }
        else if (writeSequenceNumber < readSequenceNumberInt64)
        {
            left = middle + 1;
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
    KeyRangeEnd high
)
{
    auto cacheEntry = co_await m_partitionTreeNodeCache.GetPartitionTreeNodeCacheEntry(
        treeNodeLocation);

    auto treeNode = co_await cacheEntry->ReadTreeNode();

    int lowTreeEntryIndex = 0;
    std::weak_ordering lowTreeEntryComparison = std::weak_ordering::less;

    if (low.Key)
    {
        FindTreeEntryKey key;
        key.key = low.Key;
        key.inclusivity = low.Inclusivity;
        key.readSequenceNumber = readSequenceNumber;

        tie(lowTreeEntryIndex, lowTreeEntryComparison) = co_await FindTreeEntry(
            cacheEntry,
            key);
    }

    int highTreeEntryIndex = treeNode->treeentries_size();
    std::weak_ordering highTreeEntryComparison = std::weak_ordering::less;

    if (high.Key)
    {
        FindTreeEntryKey key;
        key.key = low.Key;
        key.inclusivity = Inclusivity::Inclusive,
        key.readSequenceNumber = readSequenceNumber;

        tie(highTreeEntryIndex, highTreeEntryComparison) = co_await FindTreeEntry(
            cacheEntry,
            key);

        // If the user asked for an exclusive high end,
        // and the resulting search returned an exact match,
        // move one node earlier.
        if (high.Inclusivity == Inclusivity::Exclusive
            &&
            highTreeEntryComparison == std::weak_ordering::equivalent
            &&
            treeNode->treeentries(highTreeEntryIndex).PartitionTreeEntryType_case() != PartitionTreeEntry::kTreeNodeOffset)
        {
            highTreeEntryIndex--;
            highTreeEntryComparison = std::weak_ordering::greater;
        }
    }

    while (
        lowTreeEntryIndex <= highTreeEntryIndex
        &&
        lowTreeEntryIndex < treeNode->treeentries_size())
    {
        auto& treeNodeEntry = treeNode->treeentries(lowTreeEntryIndex);

        if (PartitionTreeEntry::kTreeNodeOffset == treeNodeEntry.PartitionTreeEntryType_case())
        {
            auto subTreeEnumerator = Enumerate(
                {
                    m_dataLocation.extentNumber,
                    treeNodeEntry.treenodeoffset(),
                },
                readSequenceNumber,
                low,
                high);

            for (auto iterator = co_await subTreeEnumerator.begin();
                iterator != subTreeEnumerator.end();
                co_await ++iterator)
            {
                co_yield *iterator;
            }

            // Once we've enumerated a non-leaf node, we should just move
            // to the next non-leaf node.
            ++lowTreeEntryIndex;
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
            }
            else
            {
                auto valueIndex = FindMatchingValueIndexByWriteSequenceNumber(
                    treeNodeEntry.valueset(),
                    readSequenceNumber);

                treeEntryValue = &treeNodeEntry.valueset().values(valueIndex);
            }

            if (PartitionTreeEntryValue::kValueOffset == treeEntryValue->PartitionTreeEntryValue_case())
            {
                value.reset(m_valueFactory->GetPrototype()->New());
                co_await m_dataMessageAccessor->ReadMessage(
                    {
                        .extentNumber = m_dataLocation.extentNumber,
                        .extentOffset = treeEntryValue->valueoffset(),
                    },
                    *value);
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

            auto key = co_await cacheEntry->GetKey(lowTreeEntryIndex);
            co_yield ResultRow
            {
                .Key = key,
                .WriteSequenceNumber = ToSequenceNumber(treeEntryValue->writesequencenumber()),
                .Value = value.get(),
            };

            // This is a leaf node, so we should find the next key.
            FindTreeEntryKey findEntryKey;
            findEntryKey.key = key;
            findEntryKey.inclusivity = Inclusivity::Exclusive;
            findEntryKey.lastFindResult = lowTreeEntryIndex;
            findEntryKey.readSequenceNumber = readSequenceNumber;

            tie(lowTreeEntryIndex, lowTreeEntryComparison) = co_await FindTreeEntry(
                cacheEntry,
                findEntryKey);
        }
    }

}

}
