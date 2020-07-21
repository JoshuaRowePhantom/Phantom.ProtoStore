#pragma once

#include "PartitionWriterImpl.h"
#include "MessageStore.h"
#include <vector>
#include "src/ProtoStoreInternal.pb.h"
#include "BloomFilter.h"

namespace Phantom::ProtoStore
{

PartitionWriter::PartitionWriter(
    shared_ptr<ISequentialMessageWriter> dataWriter,
    shared_ptr<ISequentialMessageWriter> headerWriter
)
    :
    m_dataWriter(dataWriter),
    m_headerWriter(headerWriter)
{}

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
    std::vector<shared_ptr<PartitionTreeNode>> treeNodeStack;

    for (auto iterator = co_await rows.begin();
        iterator != rows.end();
        co_await ++iterator)
    {
        ++discoveredRowCount;
        auto& row = *iterator;
        
        auto treeEntry = std::optional(
            PartitionTreeEntry());

        row.Key->SerializeToString(
            treeEntry->mutable_key());

        bloomFilter.add(
            *treeEntry->mutable_key()
        );

        treeEntry->set_writesequencenumber(
            ToUint64(row.WriteSequenceNumber));

        if (!row.Value)
        {
            treeEntry->set_deleted(true);
        }
        else if (row.Value->ByteSizeLong() > 20)
        {
            auto largeValueWrite = co_await m_dataWriter->Write(
                *row.Value,
                FlushBehavior::DontFlush);
            treeEntry->set_valueoffset(
                largeValueWrite.DataRange.Beginning);
        }
        else
        {
            row.Value->SerializeToString(
                treeEntry->mutable_value());
        }

        for (int index = 0; index < treeNodeStack.size(); index++)
        {
            auto treeNode = treeNodeStack[index];

            if (treeNode->ByteSizeLong() + treeEntry->ByteSizeLong() < 65500
                ||
                treeNode->treeentries_size() < 2)
            {
                *(treeNode->add_treeentries()) = move(*treeEntry);
                treeEntry.reset();
                break;
            }
            else
            {
                auto treeNodeWriteResult = co_await m_dataWriter->Write(
                    *treeNode,
                    FlushBehavior::DontFlush);

                treeNodeStack[index] = treeNode = make_shared<PartitionTreeNode>();
                *(treeNode->add_treeentries()) = move(*treeEntry);

                treeEntry->Clear();
                row.Key->SerializeToString(
                    treeEntry->mutable_key());
                treeEntry->set_treenodeoffset(
                    treeNodeWriteResult.DataRange.Beginning);
            }
        }

        if (treeEntry.has_value())
        {
            auto treeNode = make_shared<PartitionTreeNode>();
            *(treeNode->add_treeentries()) = move(*treeEntry);
            treeNodeStack.push_back(treeNode);
        }
    }

    assert(rowCount == discoveredRowCount);

    for (size_t index = 0; index < treeNodeStack.size() - 1; index++)
    {
        auto treeNode = treeNodeStack[index];

        auto treeNodeWriteResult = co_await m_dataWriter->Write(
            *treeNode,
            FlushBehavior::DontFlush);

        auto treeNodeEntry = treeNodeStack[index + 1]->add_treeentries();
        treeNodeEntry->set_key(
            treeNode->treeentries(treeNode->treeentries_size() - 1).key());
        treeNodeEntry->set_treenodeoffset(
            treeNodeWriteResult.DataRange.Beginning);
    }

    auto rootTreeNodeWriteResult = co_await m_dataWriter->Write(
        *treeNodeStack[treeNodeStack.size() - 1],
        FlushBehavior::DontFlush);

    auto bloomFilterWriteResult = co_await m_dataWriter->Write(
        partitionBloomFilter,
        FlushBehavior::DontFlush);

    PartitionRoot partitionRoot;
    partitionRoot.set_roottreenodeoffset(
        rootTreeNodeWriteResult.DataRange.Beginning);
    partitionRoot.set_rowcount(
        rowCount);
    partitionRoot.set_bloomfilteroffset(
        bloomFilterWriteResult.DataRange.Beginning
    );

    auto partitionRootWriteResult = co_await m_dataWriter->Write(
        partitionRoot,
        FlushBehavior::DontFlush);

    PartitionHeader partitionHeader;
    partitionHeader.set_partitionrootoffset(
        partitionRootWriteResult.DataRange.Beginning);

    co_await m_dataWriter->Write(
        partitionHeader,
        FlushBehavior::Flush);

    co_await m_headerWriter->Write(
        partitionHeader,
        FlushBehavior::Flush);
}

}
