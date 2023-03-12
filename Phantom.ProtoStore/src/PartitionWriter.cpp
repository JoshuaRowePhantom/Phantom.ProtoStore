#pragma once

#include "BloomFilter.h"
#include "KeyComparer.h"
#include "MessageStore.h"
#include "PartitionImpl.h"
#include "PartitionWriterImpl.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"
#include "Schema.h"
#include <vector>

namespace Phantom::ProtoStore
{

PartitionWriterBase::PartitionWriterBase(
    const shared_ptr<const Schema> schema,
    const shared_ptr<const KeyComparer> keyComparer,
    shared_ptr<ISequentialMessageWriter> dataWriter
) :
    m_schema{ std::move(schema) },
    m_keyComparer{ std::move(keyComparer) },
    m_dataWriter{ std::move(dataWriter) }
{}

task<FlatBuffers::MessageReference_V1> PartitionWriterBase::Write(
    const FlatMessage<FlatBuffers::PartitionMessage>& partitionMessage,
    FlushBehavior flushBehavior = FlushBehavior::DontFlush
)
{
    auto storedMessage = co_await m_dataWriter->Write(
        partitionMessage.data(),
        FlushBehavior::DontFlush);

    auto header = storedMessage->Header_V1();

    FlatBuffers::MessageReference_V1 messageReference =
    {
        *header,
        storedMessage->DataRange.Beginning,
    };

    co_return messageReference;
}

PartitionTreeWriter::PartitionTreeWriter(
    shared_ptr<const Schema> schema,
    shared_ptr<const KeyComparer> keyComparer,
    shared_ptr<ISequentialMessageWriter> dataWriter,
    WriteRowsRequest& writeRowsRequest,
    WriteRowsResult& writeRowsResult,
    BloomFilterVersion1<std::span<char>>& bloomFilter
) :
    PartitionWriterBase(
        std::move(schema),
        std::move(keyComparer),
        std::move(dataWriter)),
    m_writeRowsRequest(writeRowsRequest),
    m_writeRowsResult(writeRowsResult),
    m_bloomFilter(bloomFilter)
{
}

void PartitionTreeWriter::FinishKey(
    PartitionTreeEntryValueOffsetVector& treeEntryValues
)
{
    if (!current().highestKey)
    {
        assert(treeEntryValues.empty());
        return;
    }

    assert(!treeEntryValues.empty());

    auto& builder = m_treeNodeStack.front().partitionTreeNodeBuilder;

    auto keyOffset = CreateDataValue(
        builder,
        current().highestKey.as_aligned_message_if());

    auto partitionTreeEntryKeyOffset = FlatBuffers::CreatePartitionTreeEntryKeyDirect(
        builder,
        keyOffset,
        ToUint64(current().lowestSequenceNumberForKey),
        &treeEntryValues
    );

    m_treeNodeStack.front().keyOffsets.push_back(partitionTreeEntryKeyOffset);

    treeEntryValues.clear();
}

PartitionTreeWriter::StackEntry& PartitionTreeWriter::current()
{
    return m_treeNodeStack[0];
}

task<FlatBuffers::MessageReference_V1> PartitionTreeWriter::Flush(
    uint8_t level,
    bool isFinishing)
{
    StackEntry* current = &m_treeNodeStack[level];

    bool hasParent = m_treeNodeStack.size() > level + 1;
    StackEntry* parent = hasParent ? &m_treeNodeStack[level + 1] : nullptr;

    // If we're finishing up writing the tree,
    // then don't propagate a key out of the root node.
    bool propagateKeyToParent =
        !isFinishing
        ||
        hasParent;

    // We need to flush the next higher level IF
    // the higher level exists AND adding the highest key in this level to the next level
    // would cause it to exceed its size target.
    if (propagateKeyToParent)
    {
        StackEntry& next = m_treeNodeStack[level + 1];
        auto approximateSize = current->highestKey.as_aligned_message_if().Payload.size() + 100;
        if (next.partitionTreeNodeBuilder.GetSize() + approximateSize > m_writeRowsRequest.targetMessageSize)
        {
            // Note that we don't forward the value of "isFinishing",
            // because we -do- want a flushed root node to result
            // in a new top-level node.
            co_await Flush(level + 1, false);
        }
    }

    auto treeNodeOffset = FlatBuffers::CreatePartitionTreeNodeDirect(
        current->partitionTreeNodeBuilder,
        &current->keyOffsets,
        level,
        0,
        0,
        0
    );

    auto partitionMessageOffset = FlatBuffers::CreatePartitionMessage(
        current->partitionTreeNodeBuilder,
        treeNodeOffset
    );

    current->partitionTreeNodeBuilder.Finish(
        partitionMessageOffset);

    FlatMessage<FlatBuffers::PartitionMessage> partitionMessage{ current->partitionTreeNodeBuilder };

    auto messageReference = co_await Write(
        partitionMessage
    );

    if (propagateKeyToParent)
    {
        if (!parent)
        {
            parent = &m_treeNodeStack.emplace_back(StackEntry());
        }

        parent->highestKey = std::move(current->highestKey);
        parent->lowestSequenceNumberForKey = current->lowestSequenceNumberForKey;

        auto nextKeyDataOffset = CreateDataValue(
            parent->partitionTreeNodeBuilder,
            parent->highestKey.as_aligned_message_if()
        );

        auto nextPartitionTreeEntryKey = FlatBuffers::CreatePartitionTreeEntryKeyDirect(
            parent->partitionTreeNodeBuilder,
            nextKeyDataOffset,
            ToUint64(parent->lowestSequenceNumberForKey),
            nullptr,
            &messageReference
        );

        parent->keyOffsets.push_back(
            nextPartitionTreeEntryKey);
    }

    current->highestKey = {};
    current->keyOffsets.clear();
    current->partitionTreeNodeBuilder.Clear();

    co_return messageReference;
}

task<FlatBuffers::MessageReference_V1> PartitionTreeWriter::WriteRows()
{
    auto& iterator = m_writeRowsResult.resumptionRow;

    auto earliestSequenceNumber = SequenceNumber::Latest;
    auto latestSequenceNumber = SequenceNumber::Earliest;

    m_treeNodeStack.push_back(StackEntry());
    DataValueOffset currentKeyOffset;
    PartitionTreeEntryValueOffsetVector currentValues;

    auto& partitionTreeNodeBuilder = m_treeNodeStack.front().partitionTreeNodeBuilder;

    for (;
        iterator != m_writeRowsRequest.rows->end();
        co_await ++iterator)
    {
        auto approximateNeededExtentSize =
            // Bloom filter
            m_bloomFilter.to_span().size() + 100
            // Root
            + 100
            // Tree nodes higher in the stack
            + m_treeNodeStack.size() * m_writeRowsRequest.targetMessageSize;

        ResultRow& row = *iterator;

        auto approximateRowSize =
            row.Key.as_aligned_message_if().Payload.size()
            + row.Value.as_aligned_message_if().Payload.size()
            + row.TransactionId->Payload.size()
            + 100;

        if ((co_await m_dataWriter->CurrentOffset() + partitionTreeNodeBuilder.GetSize() + approximateRowSize) > m_writeRowsRequest.targetExtentSize)
        {
            // We need enough space left in the extent to write the bloom filter,
            // root, and remaining stack entries, 
            // so finish up here and stop this extent.
            FinishKey(
                currentValues
            );

            co_await Flush(0, false);
            break;
        }
        if (partitionTreeNodeBuilder.GetSize() + approximateRowSize > m_writeRowsRequest.targetMessageSize)
        {
            // We need enough space left in the message to write the key entry,
            // so finish up this message.
            FinishKey(
                currentValues
            );

            co_await Flush(0, false);
        }

        ++m_writeRowsResult.rowsIterated;
        ++m_writeRowsResult.rowsWritten;
        m_writeRowsResult.earliestSequenceNumber = ToSequenceNumber(std::min(
            ToUint64(m_writeRowsResult.earliestSequenceNumber),
            ToUint64(row.WriteSequenceNumber)
        ));
        m_writeRowsResult.latestSequenceNumber = ToSequenceNumber(std::max(
            ToUint64(m_writeRowsResult.latestSequenceNumber),
            ToUint64(row.WriteSequenceNumber)
        ));

        if (row.WriteSequenceNumber > latestSequenceNumber)
        {
            latestSequenceNumber = row.WriteSequenceNumber;
        }
        if (row.WriteSequenceNumber < earliestSequenceNumber)
        {
            earliestSequenceNumber = row.WriteSequenceNumber;
        }
        
        if (current().highestKey
            && m_keyComparer->Compare(
                current().highestKey,
                row.Key
            ) != std::weak_ordering::equivalent)
        {
            FinishKey(
                currentValues
            );
        }

        current().highestKey = std::move(row.Key);
        current().lowestSequenceNumberForKey = row.WriteSequenceNumber;
        m_bloomFilter.add(
            m_keyComparer->Hash(current().highestKey));

        auto valueDataOffset = CreateDataValue(
            partitionTreeNodeBuilder,
            row.Value.as_aligned_message_if());

        auto transactionIdOffset = CreateDataValue(
            partitionTreeNodeBuilder,
            *row.TransactionId);

        auto partitionTreeEntryValueOffset = FlatBuffers::CreatePartitionTreeEntryValue(
            partitionTreeNodeBuilder,
            ToUint64(row.WriteSequenceNumber),
            valueDataOffset,
            nullptr,
            transactionIdOffset);

        currentValues.push_back(
            partitionTreeEntryValueOffset);
    }

    FinishKey(
        currentValues);

    FlatBuffers::MessageReference_V1 root;

    // There may be leftover nodes in the tree node stack.
    // Flush them.
    // The rightmost side of the tree will be unbalanced.
    for (auto level = 0; level < m_treeNodeStack.size(); level++)
    {
        root = co_await Flush(level, true);
    }

    co_return root;
}

PartitionWriter::PartitionWriter(
    shared_ptr<const Schema> schema,
    shared_ptr<const KeyComparer> keyComparer,
    shared_ptr<ISequentialMessageWriter> dataWriter,
    shared_ptr<ISequentialMessageWriter> headerWriter
) :
    PartitionWriterBase 
    { 
        std::move(schema), 
        std::move(keyComparer),
        std::move(dataWriter),
    },
    m_headerWriter{ std::move(headerWriter) }
{
}

task<WriteRowsResult> PartitionWriter::WriteRows(
    WriteRowsRequest& writeRowsRequest
)
{
    WriteRowsResult writeRowsResult =
    {
        .rowsIterated = 0,
        .rowsWritten = 0,
        .resumptionRow = co_await writeRowsRequest.rows->begin(),
    };

    auto approximateRowCountToWrite = writeRowsRequest.approximateRowCount;
    if (writeRowsRequest.inputSize > writeRowsRequest.targetExtentSize)
    {
        // Compute the approximate fraction of rows we're going to write,
        // and add a few % for noise.  It's okay if we compute a number
        // greater or smaller than the number of rows we actually write; this is for
        // bloom filter sizing.
        auto approximateRowCountFractionToWrite =
            static_cast<double>(writeRowsRequest.targetExtentSize) / static_cast<double>(writeRowsRequest.inputSize) * 1.1;

        approximateRowCountToWrite = static_cast<size_t>(
            writeRowsRequest.approximateRowCount * approximateRowCountFractionToWrite);
    }

    double desiredBloomFilterFalsePositiveRate = .001;
    auto bloomFilterBitCount = get_BloomFilter_optimal_bit_count(
        desiredBloomFilterFalsePositiveRate,
        approximateRowCountToWrite);
    auto bloomFilterHashFunctionCount = get_BloomFilter_optimal_hash_function_count_for_optimal_bit_count(
        desiredBloomFilterFalsePositiveRate);

    auto partitionBloomFilter = std::make_unique<FlatBuffers::PartitionBloomFilterT>();
    partitionBloomFilter->filter.resize((bloomFilterBitCount + 7) / 8);
    partitionBloomFilter->algorithm = FlatBuffers::PartitionBloomFilterHashAlgorithm::Version1;
    partitionBloomFilter->hash_function_count = bloomFilterHashFunctionCount;

    auto bloomFilterSpan = std::span(
        reinterpret_cast<char*>(partitionBloomFilter->filter.data()), 
        partitionBloomFilter->filter.size());

    BloomFilterVersion1<std::span<char>> bloomFilter(
        bloomFilterSpan,
        bloomFilterHashFunctionCount
    );

    PartitionTreeWriter treeWriter(
        m_schema,
        m_keyComparer,
        m_dataWriter,
        writeRowsRequest,
        writeRowsResult,
        bloomFilter
    );

    auto rootTreeEntryReference = co_await treeWriter.WriteRows();

    FlatBuffers::PartitionMessageT bloomFilterPartitionMessage;
    bloomFilterPartitionMessage.bloom_filter = std::move(partitionBloomFilter);

    FlatMessage<FlatBuffers::PartitionMessage> bloomFilterPartitionFlatMessage{ &bloomFilterPartitionMessage };

    auto bloomFilterReference = co_await Write(
        bloomFilterPartitionFlatMessage);

    auto partitionRoot = std::make_unique<FlatBuffers::PartitionRootT>();
    partitionRoot->bloom_filter = std::make_unique<FlatBuffers::MessageReference_V1>(bloomFilterReference);
    partitionRoot->root_tree_node = std::make_unique<FlatBuffers::MessageReference_V1>(rootTreeEntryReference);
    partitionRoot->earliest_sequence_number = ToUint64(writeRowsResult.earliestSequenceNumber);
    partitionRoot->latest_sequence_number = ToUint64(writeRowsResult.latestSequenceNumber);
    partitionRoot->row_count = writeRowsResult.rowsWritten;
    
    FlatBuffers::PartitionMessageT partitionRootMessage;
    partitionRootMessage.root = std::move(partitionRoot);

    FlatMessage<FlatBuffers::PartitionMessage> partitionRootFlatMessage{ &partitionRootMessage };

    auto partitionRootReference = co_await Write(
        partitionRootFlatMessage,
        FlushBehavior::Flush);

    auto partitionHeader = std::make_unique<FlatBuffers::PartitionHeaderT>();
    partitionHeader->partition_root = std::make_unique<FlatBuffers::MessageReference_V1>(partitionRootReference);

    FlatBuffers::PartitionMessageT partitionHeaderMessage;
    partitionHeaderMessage.header = std::move(partitionHeader);

    FlatMessage<FlatBuffers::PartitionMessage> partitionHeaderFlatMessage{ &partitionHeaderMessage };

    // We write the partition header to both the
    // data writer and the header writer.
    // This makes it possible to recover the header from the data partition.

    // This also flushes all the writes that have been performed against the
    // data extent. All the previous writes were only Committed.
    co_await m_dataWriter->Write(
        partitionHeaderFlatMessage.data(),
        FlushBehavior::Flush
    );

    co_await m_headerWriter->Write(
        partitionHeaderFlatMessage.data(),
        FlushBehavior::Flush
    );

    co_return writeRowsResult;
}
}
