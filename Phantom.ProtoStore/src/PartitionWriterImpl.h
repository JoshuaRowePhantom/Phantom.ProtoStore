#pragma once

#include "PartitionWriter.h"
#include "ProtoStoreInternal.pb.h"
#include "MessageStore.h"

namespace Phantom::ProtoStore
{
struct PartitionWriterParameters
{
    size_t DesiredTreeNodeSize = 1024 * 64 - 4096;
    size_t MinimumKeysPerTreeNode = 2;
    size_t MaxEmbeddedValueSize = 100;
};

class PartitionTreeWriter
{
    struct StackEntry
    {
        PartitionMessage Message;
        size_t EstimatedSize = 0;
    };

    std::vector<StackEntry> m_treeNodeStack;
    
    shared_ptr<ISequentialMessageWriter> m_dataWriter;

    google::protobuf::uint64 GetLowestSequenceNumber(
        const PartitionTreeEntry& treeEntry
    );

    task<WriteMessageResult> Write(
        uint32_t level,
        bool createNewLevel);

public:
    PartitionTreeWriter(
        shared_ptr<ISequentialMessageWriter> dataWriter
    );

    size_t GetEstimatedSizeForNewTreeEntryValue(
        const PartitionTreeEntryValue& partitionTreeEntryValue
    );

    size_t GetEstimatedSizeForNewTreeEntry(
        const string& key,
        const PartitionTreeEntryValue& partitionTreeEntryValue
    );

    size_t GetEstimatedSizeForWriteOfParent(
        uint32_t level
    );

    void StartTreeEntry(
        string key);

    void AddTreeEntryValue(
        PartitionTreeEntryValue partitionTreeEntryValue);

    bool IsCurrentKey(
        const string& key
    );

    int GetKeyCount(
        uint32_t level);

    uint32_t GetLevelCount();

    task<WriteMessageResult> Write(
        uint32_t level);

    task<WriteMessageResult> WriteRoot();
};

class PartitionWriter
    :
    public IPartitionWriter
{
    shared_ptr<ISequentialMessageWriter> m_dataWriter;
    shared_ptr<ISequentialMessageWriter> m_headerWriter;
    const PartitionWriterParameters m_parameters;
    PartitionTreeWriter m_partitionTreeWriter;

    task<> AddValueToTreeEntry(
        string key,
        PartitionTreeEntryValue partitionTreeEntryValue);

    task<WriteMessageResult> Write(
        const PartitionMessage& partitionMessage,
        FlushBehavior flushBehavior = FlushBehavior::DontFlush
    );

    task<WriteMessageResult> Write(
        PartitionHeader partitionHeader
    );

    task<WriteMessageResult> Write(
        PartitionRoot partitionRoot
    );

    task<WriteMessageResult> Write(
        PartitionTreeNode partitionTreeNode
    );

    task<WriteMessageResult> Write(
        PartitionBloomFilter partitionBloomFilter
    );

    task<WriteMessageResult> Write(
        string value
    );

    task<WriteMessageResult> WriteLeftoverTreeEntries();

    task<> FlushCompleteTreeNode(
        uint32_t level);

public:
    PartitionWriter(
        PartitionWriterParameters parameters,
        shared_ptr<ISequentialMessageWriter> dataWriter,
        shared_ptr<ISequentialMessageWriter> headerWriter
    );

    virtual task<WriteRowsResult> WriteRows(
        WriteRowsRequest writeRowsRequest
    ) override;
};

}
