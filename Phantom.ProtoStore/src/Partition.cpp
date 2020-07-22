#include "PartitionImpl.h"
#include "RandomMessageAccessor.h"
#include "src/ProtoStoreInternal.pb.h"
#include "Schema.h"
#include "KeyComparer.h"

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
    co_await m_dataHeaderMessageAccessor->ReadMessage(
        m_headerLocation,
        m_partitionHeader
    );

    co_await m_dataMessageAccessor->ReadMessage(
        ExtentLocation
        {
            m_dataLocation.extentNumber,
            m_partitionHeader.partitionrootoffset(),
        },
        m_partitionRoot);

    co_await m_dataMessageAccessor->ReadMessage(
        ExtentLocation
        {
            m_dataLocation.extentNumber,
            m_partitionRoot.bloomfilteroffset(),
        },
        m_partitionBloomFilter);

    auto span = std::span(
        m_partitionBloomFilter.filter().cbegin(),
        m_partitionBloomFilter.filter().cend()
    );

    m_bloomFilter = make_unique<BloomFilterVersion1>(
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

cppcoro::task<size_t> Partition::FindTreeEntry(
    const shared_ptr<PartitionTreeNodeCacheEntry>& partitionTreeNodeCacheEntry,
    SequenceNumber readSequenceNumber,
    KeyRangeEnd keyRangeEnd,
    std::weak_ordering equivalenceToUseForEquivalent
)
{
    auto treeNode = co_await partitionTreeNodeCacheEntry->ReadTreeNode();
    auto left = 0;
    auto right = treeNode->treeentries_size();

    while (left < right)
    {
        auto middle = left + (right - left) / 2;

        auto middleKey = co_await partitionTreeNodeCacheEntry->GetKey(
            middle);
        auto& middleTreeNodeEntry = treeNode->treeentries(middle);

        auto keyComparison = m_keyComparer->Compare(
            keyRangeEnd.Key,
            middleKey);

        if (keyComparison == std::weak_ordering::equivalent
            &&
            keyRangeEnd.Inclusivity == Inclusivity::Exclusive)
        {
            keyComparison = std::weak_ordering::less;
        }

        if (keyComparison == std::weak_ordering::equivalent)
        {
            keyComparison = equivalenceToUseForEquivalent;
        }

        if (keyComparison == std::weak_ordering::equivalent)
        {
            keyComparison = ToUint64(readSequenceNumber) <=> middleTreeNodeEntry.writesequencenumber();
        }

        if (keyComparison == std::weak_ordering::equivalent)
        {
            co_return middle;
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

    co_return left;
}

cppcoro::task<size_t> Partition::FindLowTreeEntry(
    const shared_ptr<PartitionTreeNodeCacheEntry>& partitionTreeNodeCacheEntry,
    SequenceNumber readSequenceNumber,
    KeyRangeEnd low
)
{
    if (low.Key == nullptr)
    {
        co_return 0;
    }

    co_return co_await FindTreeEntry(
        partitionTreeNodeCacheEntry,
        readSequenceNumber,
        low,
        std::weak_ordering::equivalent
    );
}

cppcoro::task<size_t> Partition::FindHighTreeEntry(
    const shared_ptr<PartitionTreeNodeCacheEntry>& partitionTreeNodeCacheEntry,
    SequenceNumber readSequenceNumber,
    KeyRangeEnd high
)
{
    if (high.Key == nullptr)
    {
        co_return (co_await partitionTreeNodeCacheEntry->ReadTreeNode())->treeentries_size();
    }

    co_return co_await FindTreeEntry(
        partitionTreeNodeCacheEntry,
        readSequenceNumber,
        high,
        std::weak_ordering::less
    );
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

    size_t lowTreeEntryIndex = 0;

    auto highTreeEntryIndex = co_await FindHighTreeEntry(
        cacheEntry,
        readSequenceNumber,
        high);

    while (
        (highTreeEntryIndex >= (lowTreeEntryIndex = co_await FindLowTreeEntry(
            cacheEntry,
            readSequenceNumber,
            low)))
        && lowTreeEntryIndex < treeNode->treeentries_size())
    {
        auto& treeNodeEntry = treeNode->treeentries(lowTreeEntryIndex);
        auto key = co_await cacheEntry->GetKey(lowTreeEntryIndex);

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
        }
        else
        {
            unique_ptr<Message> value;

            if (PartitionTreeEntry::kValueOffset == treeNodeEntry.PartitionTreeEntryType_case())
            {
                value.reset(m_valueFactory->GetPrototype()->New());
                co_await m_dataMessageAccessor->ReadMessage(
                    {
                        .extentNumber = m_dataLocation.extentNumber,
                        .extentOffset = treeNodeEntry.valueoffset(),
                    },
                    *value);
            }

            if (PartitionTreeEntry::kValue == treeNodeEntry.PartitionTreeEntryType_case())
            {
                value.reset(m_valueFactory->GetPrototype()->New());
                value->ParseFromString(
                    treeNodeEntry.value());
            }

            co_yield ResultRow
            {
                .Key = key,
                .WriteSequenceNumber = ToSequenceNumber(treeNodeEntry.writesequencenumber()),
                .Value = value.get(),
            };
        }

        low.Inclusivity = Inclusivity::Exclusive;
        low.Key = key;
    }

}

}
