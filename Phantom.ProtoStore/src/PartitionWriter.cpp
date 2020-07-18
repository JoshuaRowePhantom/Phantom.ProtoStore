#pragma once

#include "PartitionWriterImpl.h"
#include "MessageStore.h"
#include <vector>
#include "src/ProtoStoreInternal.pb.h"

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
    row_generator rows
)
{
    std::vector<shared_ptr<PartitionTreeNode>> treeNodeStack;

    for (auto iterator = co_await rows.begin();
        iterator != rows.end();
        co_await ++iterator)
    {
        auto& row = *iterator;
        
        auto treeEntry = make_shared<PartitionTreeEntry>();

        row.Key->SerializeToString(
            treeEntry->mutable_key());

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
                treeEntry = nullptr;
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

        if (treeEntry)
        {
            auto treeNode = make_shared<PartitionTreeNode>();
            *(treeNode->add_treeentries()) = move(*treeEntry);
            treeNodeStack.push_back(treeNode);
        }
    }

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

    PartitionRoot partitionRoot;
    partitionRoot.set_roottreenodeoffset(
        rootTreeNodeWriteResult.DataRange.Beginning);

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
