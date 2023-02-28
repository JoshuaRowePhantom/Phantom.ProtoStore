#pragma once

#include "PartitionImpl.h"
#include "PartitionWriterImpl.h"
#include "MessageStore.h"
#include <vector>
#include "src/ProtoStoreInternal_generated.h"
#include "BloomFilter.h"

namespace Phantom::ProtoStore
{

PartitionWriterBase::PartitionWriterBase(
    shared_ptr<ISequentialMessageWriter> dataWriter)
    : m_dataWriter{ dataWriter }
{}

task<FlatBuffers::MessageReference_V1> PartitionWriterBase::Write(
    const FlatMessage<FlatBuffers::PartitionMessage>& partitionMessage,
    FlushBehavior flushBehavior = FlushBehavior::DontFlush
)
{
    auto storedMessage = co_await m_dataWriter->Write(
        partitionMessage.data(),
        FlushBehavior::DontFlush);

    auto header = reinterpret_cast<const FlatBuffers::MessageHeader_V1*>(
        storedMessage->Header.data());

    FlatBuffers::MessageReference_V1 messageReference =
    {
        *header,
        storedMessage->DataRange.Beginning,
    };

    co_return messageReference;
}

PartitionTreeWriter::PartitionTreeWriter(
    shared_ptr<ISequentialMessageWriter> dataWriter,
    WriteRowsRequest& writeRowsRequest,
    WriteRowsResult& writeRowsResult,
    BloomFilterVersion1<std::span<char>>& bloomFilter
) :
    PartitionWriterBase(std::move(dataWriter)),
    m_writeRowsRequest(writeRowsRequest),
    m_writeRowsResult(writeRowsResult),
    m_bloomFilter(bloomFilter)
{
}

PartitionTreeWriter::PartitionDataValueOffset PartitionTreeWriter::WriteRawData(
    FlatBufferBuilder& builder,
    const RawData& rawData
)
{
    if (!rawData->data())
    {
        return {};
    }

    auto dataVectorOffset = builder.CreateVector<int8_t>(
        get_int8_t_span(*rawData).data(),
        rawData->size());

    return FlatBuffers::CreatePartitionDataValue(
        builder,
        dataVectorOffset,
        1);
}

void PartitionTreeWriter::FinishKey(
    RawData& currentKey,
    SequenceNumber lowestSequenceNumberForKey,
    PartitionTreeEntryValueOffsetVector& treeEntryValues
)
{
    if (!currentKey->data())
    {
        assert(treeEntryValues.empty());
        return;
    }

    assert(!treeEntryValues.empty());

    auto& builder = m_treeNodeStack.front().partitionTreeNodeBuilder;

    auto keyOffset = WriteRawData(
        builder,
        currentKey);

    auto partitionTreeEntryKeyOffset = FlatBuffers::CreatePartitionTreeEntryKeyDirect(
        builder,
        keyOffset,
        ToUint64(lowestSequenceNumberForKey),
        &treeEntryValues
    );

    m_treeNodeStack.front().highestKey = std::move(currentKey);
    m_treeNodeStack.front().lowestSequenceNumberForKey = lowestSequenceNumberForKey;
    m_treeNodeStack.front().keyOffsets.push_back(partitionTreeEntryKeyOffset);

    treeEntryValues.clear();
}

task<FlatBuffers::MessageReference_V1> PartitionTreeWriter::Flush(
    size_t level)
{
    StackEntry& current = m_treeNodeStack[level];

    // We need to flush the next higher level IF
    // the higher level exists AND adding the highest key in this level to the next level
    // would cause it to exceed its size target.
    if (m_treeNodeStack.size() > level + 1)
    {
        StackEntry& next = m_treeNodeStack[level + 1];
        auto approximateSize = current.highestKey->size() + 100;
        if (next.partitionTreeNodeBuilder.GetSize() + approximateSize > m_writeRowsRequest.targetMessageSize)
        {
            co_await Flush(level + 1);
        }
    }

    auto treeNodeOffset = FlatBuffers::CreatePartitionTreeNodeDirect(
        current.partitionTreeNodeBuilder,
        &current.keyOffsets,
        level,
        0,
        0,
        0
    );

    auto partitionMessageOffset = FlatBuffers::CreatePartitionMessage(
        current.partitionTreeNodeBuilder,
        treeNodeOffset
    );

    current.partitionTreeNodeBuilder.Finish(
        partitionMessageOffset);

    FlatMessage<FlatBuffers::PartitionMessage> partitionMessage{ current.partitionTreeNodeBuilder };

    auto messageReference = co_await Write(
        partitionMessage
    );

    if (m_treeNodeStack.size() == level + 1)
    {
        m_treeNodeStack.push_back(StackEntry());
    }
    StackEntry& next = m_treeNodeStack[level + 1];

    next.highestKey = std::move(current.highestKey);
    next.lowestSequenceNumberForKey = current.lowestSequenceNumberForKey;

    auto nextKeyDataOffset = WriteRawData(
        next.partitionTreeNodeBuilder,
        next.highestKey
    );

    auto nextPartitionTreeEntryKey = FlatBuffers::CreatePartitionTreeEntryKeyDirect(
        next.partitionTreeNodeBuilder,
        nextKeyDataOffset,
        ToUint64(next.lowestSequenceNumberForKey),
        nullptr,
        &messageReference
    );

    next.keyOffsets.push_back(
        nextPartitionTreeEntryKey);

    current.highestKey = {};
    current.keyOffsets.clear();
    current.partitionTreeNodeBuilder.Clear();

    co_return messageReference;
}

task<FlatBuffers::MessageReference_V1> PartitionTreeWriter::WriteRows()
{
    auto& iterator = m_writeRowsResult.resumptionRow;

    auto earliestSequenceNumber = SequenceNumber::Latest;
    auto latestSequenceNumber = SequenceNumber::Earliest;

    m_treeNodeStack.push_back(StackEntry());
    RawData currentKey;
    SequenceNumber lowestSequenceNumberForCurrentKey;
    PartitionDataValueOffset currentKeyOffset;
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
            row.Key->size()
            + row.Value->size()
            + row.TransactionId->size()
            + 100;

        if ((co_await m_dataWriter->CurrentOffset() + partitionTreeNodeBuilder.GetSize() + approximateRowSize) > m_writeRowsRequest.targetExtentSize)
        {
            // We need enough space left in the extent to write the bloom filter,
            // root, and remaining stack entries, 
            // so finish up here and stop this extent.
            FinishKey(
                currentKey,
                lowestSequenceNumberForCurrentKey,
                currentValues
            );

            co_await Flush(0);
            break;
        }
        if (partitionTreeNodeBuilder.GetSize() + approximateRowSize > m_writeRowsRequest.targetMessageSize)
        {
            // We need enough space left in the message to write the key entry,
            // so finish up this message.
            FinishKey(
                currentKey,
                lowestSequenceNumberForCurrentKey,
                currentValues
            );

            co_await Flush(0);
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

        if (!currentKey->data() || !std::ranges::equal(*currentKey, *row.Key))
        {
            FinishKey(
                currentKey,
                lowestSequenceNumberForCurrentKey,
                currentValues
            );

            currentKey = std::move(row.Key);
        }

        lowestSequenceNumberForCurrentKey = row.WriteSequenceNumber;

        auto valueDataOffset = WriteRawData(
            partitionTreeNodeBuilder,
            row.Value);

        Offset<flatbuffers::Vector<int8_t>> transactionIdOffset;
        if (row.TransactionId->data())
        {
            transactionIdOffset = partitionTreeNodeBuilder.CreateVector<int8_t>(
                get_int8_t_span(*row.TransactionId).data(),
                row.TransactionId->size());
        }

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
        currentKey,
        lowestSequenceNumberForCurrentKey,
        currentValues);

    FlatBuffers::MessageReference_V1 root;

    // Flush the lowest level to ensure that all the necessary higher levels are updated.
    root = co_await Flush(0);

    // The tree node stack is now complete.
    // Flush each of the intermediate levels up to the first level that has < 2 entries;
    // when we 
    for (auto level = 1; m_treeNodeStack[level].keyOffsets.size() > 1; level++)
    {
        if (!m_treeNodeStack[level].keyOffsets.empty())
        {
            root = co_await Flush(level);
        }
    }

    co_return root;
}

PartitionWriter::PartitionWriter(
    shared_ptr<ISequentialMessageWriter> dataWriter,
    shared_ptr<ISequentialMessageWriter> headerWriter
) :
    PartitionWriterBase { std::move(dataWriter) },
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

    co_await m_headerWriter->Write(
        partitionHeaderFlatMessage.data(),
        FlushBehavior::Flush
    );

    co_return writeRowsResult;
}
}
