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

static google::protobuf::uint64 GetLowestWriteSequenceNumberForLastChild(
    const PartitionTreeNode& treeNode
)
{
    auto& lastChild = treeNode.treeentries(treeNode.treeentries_size() - 1);
    if (lastChild.has_child())
    {
        return lastChild.child().lowestwritesequencenumberforkey();
    }
    if (lastChild.has_value())
    {
        return lastChild.value().writesequencenumber();
    }
    return lastChild.valueset().values(
        lastChild.valueset().values_size() - 1
    ).writesequencenumber();
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
        newPartitionTreeEntry.mutable_child()->set_lowestwritesequencenumberforkey(
            GetLowestWriteSequenceNumberForLastChild(*treeNode));

        // The current tree entry doesn't fit, so we need to flush the tree node.
        auto treeNodeWriteResult = co_await Write(
            move(*treeNode));

        newPartitionTreeEntry.mutable_child()->set_treenodeoffset(
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
        treeNodeEntry->mutable_child()->set_lowestwritesequencenumberforkey(
            GetLowestWriteSequenceNumberForLastChild(*treeNode));

        auto treeNodeWriteResult = co_await Write(
            move(*treeNode));

        treeNodeEntry->mutable_child()->set_treenodeoffset(
            treeNodeWriteResult.DataRange.Beginning);
    }

    // This will be the root tree node.
    co_return co_await Write(
        move(m_treeNodeStack.back()));
}

task<WriteRowsResult> PartitionWriter::WriteRows(
    WriteRowsRequest writeRowsRequest
)
{
    auto iterator = writeRowsRequest.rows->begin();
    WriteRowsResult result =
    {
        .rowsIterated = 0,
        .rowsWritten = 0,
        .resumptionRow = co_await move(iterator),
    };

    auto approximateRowCountToWrite = writeRowsRequest.approximateRowCount;
    if (writeRowsRequest.inputSize > writeRowsRequest.targetSize)
    {
        // Compute the approximate fraction of rows we're going to write,
        // and add a few % for noise.  It's okay if we compute a number
        // greater or smaller than the number of rows we actually write; this is for
        // bloom filter sizing.
        auto approximateRowCountFractionToWrite =
            static_cast<double>(writeRowsRequest.targetSize) / static_cast<double>(writeRowsRequest.inputSize) * 1.1;

        approximateRowCountToWrite = static_cast<size_t>(
            writeRowsRequest.approximateRowCount * approximateRowCountFractionToWrite);
    }

    double desiredBloomFilterFalsePositiveRate = .001;
    auto bloomFilterBitCount = get_BloomFilter_optimal_bit_count(
        desiredBloomFilterFalsePositiveRate,
        approximateRowCountToWrite);
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

    auto earliestSequenceNumber = SequenceNumber::Latest;
    auto latestSequenceNumber = SequenceNumber::Earliest;

    for (;
        result.resumptionRow != writeRowsRequest.rows->end();
        co_await ++result.resumptionRow)
    {
        if (co_await m_dataWriter->CurrentOffset() > writeRowsRequest.targetSize)
        {
            break;
        }

        ++result.rowsIterated;
        ++result.rowsWritten;

        auto& row = *result.resumptionRow;
     
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

    auto rootTreeNodeWriteResult = co_await WriteLeftoverTreeEntries();

    auto bloomFilterWriteResult = co_await Write(
        move(partitionBloomFilter));

    PartitionRoot partitionRoot;
    partitionRoot.set_roottreenodeoffset(
        rootTreeNodeWriteResult.DataRange.Beginning);
    partitionRoot.set_rowcount(
        result.rowsWritten);
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

    result.writtenDataSize = co_await m_dataWriter->CurrentOffset();
    result.writtenExtentSize = co_await m_dataWriter->CurrentOffset();
    co_return move(result);
}

}
