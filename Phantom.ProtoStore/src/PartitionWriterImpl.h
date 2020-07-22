#pragma once

#include "PartitionWriter.h"
#include "src/ProtoStoreInternal.pb.h"
#include "MessageStore.h"

namespace Phantom::ProtoStore
{
struct PartitionWriterParameters
{
    size_t DesiredTreeNodeSize = 1024 * 64 - 4096;
    size_t MinimumKeysPerTreeNode = 2;
    size_t MinimumValuesPerTreeNodeEntry = 10;
};

class PartitionWriter
    :
    public IPartitionWriter
{
    shared_ptr<ISequentialMessageWriter> m_dataWriter;
    shared_ptr<ISequentialMessageWriter> m_headerWriter;
    std::vector<PartitionTreeNode> m_treeNodeStack;
    optional<PartitionTreeEntry> m_currentLeafTreeEntry;
    const PartitionWriterParameters m_parameters;

    task<> AddValueToTreeEntry(
        string&& key,
        PartitionTreeEntryValue&& partitionTreeEntryValue);

    task<WriteMessageResult> Write(
        const PartitionMessage& partitionMessage,
        FlushBehavior flushBehavior = FlushBehavior::DontFlush
    );

    task<WriteMessageResult> Write(
        PartitionHeader&& partitionHeader
    );

    task<WriteMessageResult> Write(
        PartitionRoot&& partitionRoot
    );

    task<WriteMessageResult> Write(
        PartitionTreeNode&& partitionTreeNode
    );

    task<WriteMessageResult> Write(
        PartitionBloomFilter&& partitionBloomFilter
    );

    task<WriteMessageResult> Write(
        string&& value
    );

    task<WriteMessageResult> WriteLeftoverTreeEntries();

public:
    PartitionWriter(
        PartitionWriterParameters parameters,
        shared_ptr<ISequentialMessageWriter> dataWriter,
        shared_ptr<ISequentialMessageWriter> headerWriter
    );

    virtual task<> WriteRows(
        size_t rowCount,
        row_generator rows
    ) override;
};

}
