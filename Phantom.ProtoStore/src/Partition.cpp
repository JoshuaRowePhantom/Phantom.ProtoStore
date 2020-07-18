#include "PartitionImpl.h"
#include "RandomMessageAccessor.h"
#include "src/ProtoStoreInternal.pb.h"
#include "Schema.h"

namespace Phantom::ProtoStore
{

Partition::Partition(
    shared_ptr<KeyComparer> keyComparer,
    shared_ptr<IMessageFactory> keyFactory,
    shared_ptr<IMessageFactory> valueFactory,
    shared_ptr<IRandomMessageAccessor> messageAccessor,
    ExtentLocation headerLocation,
    ExtentLocation dataLocation
) :
    m_keyComparer(keyComparer),
    m_keyFactory(keyFactory),
    m_valueFactory(valueFactory),
    m_messageAccessor(messageAccessor),
    m_headerLocation(headerLocation),
    m_dataLocation(dataLocation),
    m_partitionTreeNodeCache(
        keyFactory,
        messageAccessor
    )
{
}

task<> Partition::Open()
{
    co_await m_messageAccessor->ReadMessage(
        m_headerLocation,
        m_partitionHeader
    );

    co_await m_messageAccessor->ReadMessage(
        ExtentLocation
        {
            m_dataLocation.extentNumber,
            m_partitionHeader.partitionrootoffset(),
        },
        m_partitionHeader);

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
        high);

    while (
        highTreeEntryIndex > 0
        &&
        (highTreeEntryIndex >= (lowTreeEntryIndex = co_await FindLowTreeEntry(
            cacheEntry,
            low))))
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
        }
        else
        {
            unique_ptr<Message> value;

            if (PartitionTreeEntry::kValueOffset == treeNodeEntry.PartitionTreeEntryType_case())
            {
                value.reset(m_valueFactory->GetPrototype()->New());
                co_await m_messageAccessor->ReadMessage(
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
                .Key = co_await cacheEntry->GetKey(lowTreeEntryIndex),
                .WriteSequenceNumber = ToSequenceNumber(treeNodeEntry.writesequencenumber()),
                .Value = value.get(),
            };
        }
    } while (true);

}

}
