#pragma once

#include "PartitionWriterImpl.h"
#include "MessageStore.h"
#include <vector>
#include "src/ProtoStoreInternal.pb.h"
#include "BloomFilter.h"

namespace Phantom::ProtoStore
{

PartitionTreeWriter::PartitionTreeWriter(
    shared_ptr<ISequentialMessageWriter> dataWriter
) : m_dataWriter(dataWriter)
{
    m_treeNodeStack.push_back(StackEntry());

    // Ensure there is a partition tree node in the first message we create,
    // in case the partition ends up empty.
    m_treeNodeStack[0].Message.mutable_partitiontreenode();
}

void PartitionTreeWriter::StartTreeEntry(
    string key)
{
    auto treeEntry = m_treeNodeStack[0].Message.mutable_partitiontreenode()->mutable_treeentries()->Add();
    treeEntry->set_key(move(key));

    m_treeNodeStack[0].EstimatedSize += treeEntry->ByteSizeLong();
}

void PartitionTreeWriter::AddTreeEntryValue(
    PartitionTreeEntryValue partitionTreeEntryValue
)
{
    *m_treeNodeStack[0].Message.mutable_partitiontreenode()->mutable_treeentries(
        m_treeNodeStack[0].Message.mutable_partitiontreenode()->treeentries_size() - 1
    )->mutable_valueset()->add_values() = move(partitionTreeEntryValue);

    m_treeNodeStack[0].EstimatedSize += partitionTreeEntryValue.ByteSizeLong();
}

bool PartitionTreeWriter::IsCurrentKey(
    const string& key
)
{
    auto treeEntriesSize = m_treeNodeStack[0].Message.partitiontreenode().treeentries_size();
    return treeEntriesSize > 0
        && m_treeNodeStack[0].Message.partitiontreenode().treeentries(treeEntriesSize - 1).key() == key;
}

task<WriteMessageResult> PartitionTreeWriter::Write(
    uint32_t level)
{
    return Write(
        level,
        true);
}

task<WriteMessageResult> PartitionTreeWriter::Write(
    uint32_t level,
    bool createNewLevel)
{
    auto treeNode = m_treeNodeStack[level].Message.mutable_partitiontreenode();
    treeNode->set_level(level);

    auto writeMessageResult = co_await m_dataWriter->Write(
        m_treeNodeStack[level].Message,
        FlushBehavior::DontFlush);

    if (m_treeNodeStack.size() < level + 2)
    {
        if (!createNewLevel)
        {
            co_return writeMessageResult;
        }

        m_treeNodeStack.resize(level + 2);
    }

    auto& lastChild = *(treeNode->mutable_treeentries()->end() - 1);
    auto higherLevelTreeEntry = m_treeNodeStack[level + 1].Message.mutable_partitiontreenode()->add_treeentries();
    higherLevelTreeEntry->set_key(
        move(*lastChild.mutable_key())
    );
    higherLevelTreeEntry->mutable_child()->set_treenodeoffset(
        writeMessageResult.DataRange.Beginning
    );
    higherLevelTreeEntry->mutable_child()->set_lowestwritesequencenumberforkey(
        GetLowestSequenceNumber(lastChild)
    );
    m_treeNodeStack[level + 1].EstimatedSize += higherLevelTreeEntry->ByteSizeLong();

    m_treeNodeStack[level].Message.Clear();
    m_treeNodeStack[level].EstimatedSize = 0;

    co_return writeMessageResult;
}

google::protobuf::uint64 PartitionTreeWriter::GetLowestSequenceNumber(
    const PartitionTreeEntry& treeEntry
)
{
    if (treeEntry.has_child())
    {
        return treeEntry.child().lowestwritesequencenumberforkey();
    }
    return (treeEntry.valueset().values().end() - 1)->writesequencenumber();
}

task<WriteMessageResult> PartitionTreeWriter::WriteRoot()
{
    WriteMessageResult result;
    for (auto level = 0; level < m_treeNodeStack.size(); level++)
    {
        // Only write the node if it is non-empty
        // or is the highest level.
        if (m_treeNodeStack[level].Message.partitiontreenode().treeentries_size() > 0
            ||
            level == m_treeNodeStack.size() - 1)
        {
            result = co_await Write(
                level,
                false);
        }
    }

    co_return result;
}

int PartitionTreeWriter::GetKeyCount(
    uint32_t level)
{
    return level >= m_treeNodeStack.size()
        ? 0
        : m_treeNodeStack[level].Message.partitiontreenode().treeentries_size();
}

uint32_t PartitionTreeWriter::GetLevelCount()
{
    return m_treeNodeStack.size();
}

size_t PartitionTreeWriter::GetEstimatedSizeForNewTreeEntryValue(
    const PartitionTreeEntryValue& partitionTreeEntryValue
)
{
    return m_treeNodeStack[0].EstimatedSize + partitionTreeEntryValue.ByteSizeLong();
}

size_t PartitionTreeWriter::GetEstimatedSizeForNewTreeEntry(
    const string& key,
    const PartitionTreeEntryValue& partitionTreeEntryValue
)
{
    return m_treeNodeStack[0].EstimatedSize + partitionTreeEntryValue.ByteSizeLong() + key.size();
}

size_t PartitionTreeWriter::GetEstimatedSizeForWriteOfParent(
    uint32_t level
)
{
    auto parentLevelEstimatedSize =
        level >= m_treeNodeStack.size()
        ? 0
        : m_treeNodeStack[level].EstimatedSize;

    auto keySize = m_treeNodeStack[level].Message.partitiontreenode().treeentries(0).key().size();

    return parentLevelEstimatedSize + keySize;
}

PartitionWriter::PartitionWriter(
    PartitionWriterParameters parameters,
    shared_ptr<ISequentialMessageWriter> dataWriter,
    shared_ptr<ISequentialMessageWriter> headerWriter
)
    :
    m_parameters(parameters),
    m_dataWriter(dataWriter),
    m_headerWriter(headerWriter),
    m_partitionTreeWriter(dataWriter)
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
    PartitionHeader partitionHeader
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
    PartitionRoot partitionRoot
)
{
    PartitionMessage message;
    *message.mutable_partitionroot() = move(partitionRoot);
    co_return co_await Write(message);
}

task<WriteMessageResult> PartitionWriter::Write(
    PartitionTreeNode partitionTreeNode
)
{
    PartitionMessage message;
    *message.mutable_partitiontreenode() = move(partitionTreeNode);
    co_return co_await Write(message);
}

task<WriteMessageResult> PartitionWriter::Write(
    PartitionBloomFilter partitionBloomFilter
)
{
    PartitionMessage message;
    *message.mutable_partitionbloomfilter() = move(partitionBloomFilter);
    co_return co_await Write(message);
}

task<WriteMessageResult> PartitionWriter::Write(
    string value
)
{
    PartitionMessage message;
    message.set_value(move(value));
    co_return co_await Write(message);
}

task<> PartitionWriter::AddValueToTreeEntry(
    string key,
    PartitionTreeEntryValue partitionTreeEntryValue)
{
    bool needNewTreeEntry = !m_partitionTreeWriter.IsCurrentKey(
        key);

    bool flushLevel0 = 
        m_partitionTreeWriter.GetKeyCount(0) >= m_parameters.MinimumKeysPerTreeNode;

    if (flushLevel0)
    {
        size_t estimatedNewSizeOfLevel0;

        if (needNewTreeEntry)
        {
            estimatedNewSizeOfLevel0 = m_partitionTreeWriter.GetEstimatedSizeForNewTreeEntry(
                key,
                partitionTreeEntryValue);
        }
        else
        {
            estimatedNewSizeOfLevel0 = m_partitionTreeWriter.GetEstimatedSizeForNewTreeEntryValue(
                partitionTreeEntryValue);
        }

        flushLevel0 = estimatedNewSizeOfLevel0 > m_parameters.DesiredTreeNodeSize;
    }

    if (flushLevel0)
    {
        co_await FlushCompleteTreeNode(0);
    }

    if (needNewTreeEntry)
    {
        m_partitionTreeWriter.StartTreeEntry(
            move(key));
    }

    m_partitionTreeWriter.AddTreeEntryValue(
        move(partitionTreeEntryValue));
}

task<> PartitionWriter::FlushCompleteTreeNode(
    uint32_t level)
{
    bool flushNextLevel = 
        m_partitionTreeWriter.GetKeyCount(level + 1) > m_parameters.MinimumKeysPerTreeNode;
    
    if (flushNextLevel)
    {
        auto estimatedNewSizeOfNextLevel = m_partitionTreeWriter.GetEstimatedSizeForWriteOfParent(
            level);

        flushNextLevel = estimatedNewSizeOfNextLevel > m_parameters.DesiredTreeNodeSize;
    }

    if (flushNextLevel)
    {
        co_await FlushCompleteTreeNode(
            level + 1);
    }

    co_await m_partitionTreeWriter.Write(
        level);
}

task<WriteMessageResult> PartitionWriter::WriteLeftoverTreeEntries()
{
    return m_partitionTreeWriter.WriteRoot();
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
        else if (row.Value->ByteSizeLong() > m_parameters.MaxEmbeddedValueSize)
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
