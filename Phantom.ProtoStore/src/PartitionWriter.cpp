#pragma once

#include "PartitionWriterImpl.h"
#include "MessageStore.h"
#include <vector>
#include "src/ProtoStoreInternal.pb.h"
#include "BloomFilter.h"

namespace Phantom::ProtoStore
{

PartitionWriter::PartitionWriter(
    PartitionWriterParameters parameters,
    shared_ptr<ISequentialMessageWriter> dataWriter,
    shared_ptr<ISequentialMessageWriter> headerWriter
)
    :
    m_parameters(parameters),
    m_dataWriter(dataWriter),
    m_headerWriter(headerWriter)
{}


task<WriteMessageResult> PartitionWriter::Write(
    const PartitionMessage& partitionMessage,
    FlushBehavior flushBehavior
)
{
    co_return co_await m_dataWriter->Write(
        partitionMessage,
        flushBehavior
    );
}

task<WriteMessageResult> PartitionWriter::Write(
    PartitionHeader&& partitionHeader
)
{
    PartitionMessage message;
    *message.mutable_partitionheader() = move(partitionHeader);

    co_await m_headerWriter->Write(
        message,
        FlushBehavior::Flush
    );

    co_return co_await Write(
        message,
        FlushBehavior::Flush);
}

task<WriteMessageResult> PartitionWriter::Write(
    PartitionRoot&& partitionRoot
)
{
    PartitionMessage message;
    *message.mutable_partitionroot() = move(partitionRoot);
    co_return co_await Write(message);
}

task<WriteMessageResult> PartitionWriter::Write(
    PartitionTreeNode&& partitionTreeNode
)
{
    PartitionMessage message;
    *message.mutable_partitiontreenode() = move(partitionTreeNode);
    co_return co_await Write(message);
}

task<WriteMessageResult> PartitionWriter::Write(
    PartitionBloomFilter&& partitionBloomFilter
)
{
    PartitionMessage message;
    *message.mutable_partitionbloomfilter() = move(partitionBloomFilter);
    co_return co_await Write(message);
}

task<WriteMessageResult> PartitionWriter::Write(
    string&& value
)
{
    PartitionMessage message;
    message.set_value(move(value));
    co_return co_await Write(message);
}

task<> PartitionWriter::AddValueToTreeEntry(
    string&& key,
    PartitionTreeEntryValue&& partitionTreeEntryValue)
{
    std::optional<PartitionTreeEntry> partitionTreeEntryToAddHolder;
    PartitionTreeEntry* partitionTreeEntryToAdd = m_currentLeafTreeEntry ? &*m_currentLeafTreeEntry : nullptr;
    
    for (int index = 0; index < m_treeNodeStack.size(); index++)
    {
        auto treeNode = &m_treeNodeStack[index];

        // If we're at index 0, we try to insert the current PartitionTreeEntryValue.
        // If the key is for the same tree entry,
        // and if the partition tree entry value will fit in the current
        // node without the size expanding too big according to our parameters,
        // fit it in.  
        if (
            // If we're at the leaf level
            index == 0
            &&
            // And the key is the same as the previous partition tree entry
            partitionTreeEntryToAdd->key() == key
            &&
            // And the resulting TreeNode message would be in-spec
            (
                (treeNode->ByteSizeLong()
                    + partitionTreeEntryToAdd->ByteSizeLong()
                    + partitionTreeEntryValue.ByteSizeLong()
                    < m_parameters.DesiredTreeNodeSize
                    )
                ||
                (treeNode->treeentries_size() < m_parameters.MinimumKeysPerTreeNode)
                ||
                ((partitionTreeEntryToAdd->has_value() ? 1 : partitionTreeEntryToAdd->valueset().values_size())
                    <  m_parameters.MinimumValuesPerTreeNodeEntry)
            )
            )
        {
            // The we can insert the value into the current partition tree entry.

            // We've concluded we should insert the value into the current partition tree entry.
            // Because we're inserting a value, assert we're at the lowest level
            // of the tree in this time through the loop.
            assert(partitionTreeEntryToAdd == &*m_currentLeafTreeEntry);

            // The tree entry might have "value" set, in which case we need
            // to move it to a new valueset via a temporary.
            if (partitionTreeEntryToAdd->PartitionTreeEntryType_case()
                == PartitionTreeEntryValue::kValue)
            {
                auto existingValue = move(*partitionTreeEntryToAdd->mutable_value());
                *partitionTreeEntryToAdd->mutable_valueset()->add_values() = move(existingValue);
            }
            // Now we can just append to the valueset.
            assert(partitionTreeEntryToAdd->PartitionTreeEntryType_case()
                == PartitionTreeEntry::kValueSet);

            partitionTreeEntryToAdd->set_lowestwritesequencenumber(
                partitionTreeEntryValue.writesequencenumber());
            *partitionTreeEntryToAdd->mutable_valueset()->add_values() = move(partitionTreeEntryValue);
            // Since we did this at level zero, there's definitely no more work to do on this insertion.
            co_return;
        }

        // We decided that the current tree entry has to go.  
        // See if we can fit it into the current node.
        if (
            (treeNode->ByteSizeLong()
                + partitionTreeEntryToAdd->ByteSizeLong()
                < m_parameters.DesiredTreeNodeSize
                )
            ||
            (treeNode->treeentries_size() < m_parameters.MinimumKeysPerTreeNode)
            )
        {
            // The resulting tree node will be in-spec.
            *treeNode->add_treeentries() = move(*partitionTreeEntryToAdd);
            partitionTreeEntryToAdd = nullptr;
            break;
        }


        auto newPartitionTreeEntry = PartitionTreeEntry();

        newPartitionTreeEntry.set_key(
            treeNode->treeentries(treeNode->treeentries_size() - 1).key());
        newPartitionTreeEntry.set_highestwritesequencenumber(
            treeNode->treeentries(treeNode->treeentries_size() - 1).highestwritesequencenumber());
        newPartitionTreeEntry.set_lowestwritesequencenumber(
            treeNode->treeentries(treeNode->treeentries_size() - 1).lowestwritesequencenumber());

        // The current tree entry doesn't fit, so we need to flush the tree node.
        auto treeNodeWriteResult = co_await Write(
            move(*treeNode));

        newPartitionTreeEntry.set_treenodeoffset(
            treeNodeWriteResult.DataRange.Beginning);

        treeNode->Clear();
        *treeNode->add_treeentries() = move(*partitionTreeEntryToAdd);
        treeNode->set_level(index);

        partitionTreeEntryToAddHolder = move(newPartitionTreeEntry);
        partitionTreeEntryToAdd = &*partitionTreeEntryToAddHolder;
    }

    // If we reached here, it's because we couldn't fit the value into the current tree.
    // We also might have needed a new tree node.
    
    // First, add a new tree node.
    if (partitionTreeEntryToAdd)
    {
        auto treeNode = &m_treeNodeStack.emplace_back();
        treeNode->set_level(m_treeNodeStack.size() - 1);
        *treeNode->add_treeentries() = *move(partitionTreeEntryToAdd);
    }

    // Now we need a place to keep the value.
    m_currentLeafTreeEntry = PartitionTreeEntry();
    m_currentLeafTreeEntry->set_lowestwritesequencenumber(partitionTreeEntryValue.writesequencenumber());
    m_currentLeafTreeEntry->set_highestwritesequencenumber(partitionTreeEntryValue.writesequencenumber());
    *m_currentLeafTreeEntry->mutable_key() = move(key);
    *m_currentLeafTreeEntry->mutable_value() = move(partitionTreeEntryValue);
}

task<WriteMessageResult> PartitionWriter::WriteLeftoverTreeEntries()
{
    // The current leaf tree entry still needs to be added.
    // We'll violate the tree node limits for this one.
    // It's possible there is no tree node if there was only one row.
    if (m_treeNodeStack.empty())
    {
        auto treeNode = &m_treeNodeStack.emplace_back();
        treeNode->set_level(0);
    }
    *m_treeNodeStack[0].add_treeentries() = move(*m_currentLeafTreeEntry);

    // Now go up the stack, writing each node and adding a new tree node entry
    // for the written node to its parent.
    for (size_t index = 0; index < m_treeNodeStack.size() - 1; index++)
    {
        auto treeNode = &m_treeNodeStack[index];

        auto treeNodeEntry = m_treeNodeStack[index + 1].add_treeentries();
        treeNodeEntry->set_key(
            treeNode->treeentries(treeNode->treeentries_size() - 1).key());
        treeNodeEntry->set_lowestwritesequencenumber(
            treeNode->treeentries(treeNode->treeentries_size() - 1).lowestwritesequencenumber());
        treeNodeEntry->set_highestwritesequencenumber(
            treeNode->treeentries(treeNode->treeentries_size() - 1).highestwritesequencenumber());

        auto treeNodeWriteResult = co_await Write(
            move(*treeNode));

        treeNodeEntry->set_treenodeoffset(
            treeNodeWriteResult.DataRange.Beginning);
    }

    // This will be the root tree node.
    co_return co_await Write(
        move(m_treeNodeStack.back()));
}

task<> PartitionWriter::WriteRows(
    size_t rowCount,
    row_generator rows
)
{
    double desiredBloomFilterFalsePositiveRate = .001;
    auto bloomFilterBitCount = get_BloomFilter_optimal_bit_count(
        desiredBloomFilterFalsePositiveRate,
        rowCount);
    auto bloomFilterHashFunctionCount = get_BloomFilter_optimal_hash_function_count_for_optimal_bit_count(
        desiredBloomFilterFalsePositiveRate);

    PartitionBloomFilter partitionBloomFilter;
    partitionBloomFilter.set_algorithm(
        PartitionBloomFilterHashAlgorithm::Version1
    );
    partitionBloomFilter.mutable_filter()->resize(
        (bloomFilterBitCount + 7) / 8);

    partitionBloomFilter.set_hashfunctioncount(
        bloomFilterHashFunctionCount);

    BloomFilter<std::hash<string>, char, span<char>> bloomFilter(
        std::span(
            partitionBloomFilter.mutable_filter()->begin(),
            partitionBloomFilter.mutable_filter()->end()),
        bloomFilterHashFunctionCount
    );

    size_t discoveredRowCount = 0;
    auto earliestSequenceNumber = SequenceNumber::Latest;
    auto latestSequenceNumber = SequenceNumber::Earliest;

    for (auto iterator = co_await rows.begin();
        iterator != rows.end();
        co_await ++iterator)
    {
        ++discoveredRowCount;
        auto& row = *iterator;
     
        if (row.WriteSequenceNumber > latestSequenceNumber)
        {
            latestSequenceNumber = row.WriteSequenceNumber;
        }
        if (row.WriteSequenceNumber < earliestSequenceNumber)
        {
            earliestSequenceNumber = row.WriteSequenceNumber;
        }

        std::string keyString;

        row.Key->SerializeToString(
            &keyString);

        bloomFilter.add(
            keyString);

        PartitionTreeEntryValue partitionTreeEntryValue;

        partitionTreeEntryValue.set_writesequencenumber(
            ToUint64(row.WriteSequenceNumber));

        if (!row.Value)
        {
            partitionTreeEntryValue.set_deleted(true);
        }
        else if (row.Value->ByteSizeLong() > 20)
        {
            string value;
            row.Value->SerializeToString(
                &value);

            auto largeValueWrite = co_await Write(
                move(value));

            partitionTreeEntryValue.set_valueoffset(
                largeValueWrite.DataRange.Beginning);
        }
        else
        {
            row.Value->SerializeToString(
                partitionTreeEntryValue.mutable_value());
        }

        co_await AddValueToTreeEntry(
            move(keyString),
            move(partitionTreeEntryValue));
    }

    assert(rowCount == discoveredRowCount);

    auto rootTreeNodeWriteResult = co_await WriteLeftoverTreeEntries();

    auto bloomFilterWriteResult = co_await Write(
        move(partitionBloomFilter));

    PartitionRoot partitionRoot;
    partitionRoot.set_roottreenodeoffset(
        rootTreeNodeWriteResult.DataRange.Beginning);
    partitionRoot.set_rowcount(
        rowCount);
    partitionRoot.set_bloomfilteroffset(
        bloomFilterWriteResult.DataRange.Beginning
    );
    partitionRoot.set_earliestsequencenumber(
        ToUint64(earliestSequenceNumber));
    partitionRoot.set_latestsequencenumber(
        ToUint64(latestSequenceNumber));

    auto partitionRootWriteResult = co_await Write(
        move(partitionRoot));

    PartitionHeader partitionHeader;
    partitionHeader.set_partitionrootoffset(
        partitionRootWriteResult.DataRange.Beginning);

    co_await Write(
        move(partitionHeader));
}

}
