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

class PartitionTreeWriter : SerializationTypes
{
private:
    struct StackEntry
    {
        PartitionMessage Message;
        size_t EstimatedSize = 0;
    };

    std::vector<StackEntry> m_treeNodeStack;
    
    shared_ptr<ISequentialMessageWriter> m_dataWriter;

    google::protobuf::uint64 GetLowestSequenceNumber(
        const Serialization::PartitionTreeEntry& treeEntry
    );

    task<DataReference<StoredMessage>> Write(
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

    task<DataReference<StoredMessage>> Write(
        uint32_t level);

    task<DataReference<StoredMessage>> WriteRoot();
};

class PartitionWriter : 
    SerializationTypes,
    public IPartitionWriter
{
    shared_ptr<ISequentialMessageWriter> m_dataWriter;
    shared_ptr<ISequentialMessageWriter> m_headerWriter;
    const PartitionWriterParameters m_parameters;
    PartitionTreeWriter m_partitionTreeWriter;

    task<> AddValueToTreeEntry(
        string key,
        PartitionTreeEntryValue partitionTreeEntryValue);

    task<DataReference<StoredMessage>> Write(
        const PartitionMessage& partitionMessage,
        FlushBehavior flushBehavior = FlushBehavior::DontFlush
    );

    task<DataReference<StoredMessage>> Write(
        PartitionHeader partitionHeader
    );

    task<DataReference<StoredMessage>> Write(
        PartitionRoot partitionRoot
    );

    task<DataReference<StoredMessage>> Write(
        PartitionTreeNode partitionTreeNode
    );

    task<DataReference<StoredMessage>> Write(
        PartitionBloomFilter partitionBloomFilter
    );

    task<DataReference<StoredMessage>> Write(
        string value
    );

    task<DataReference<StoredMessage>> WriteLeftoverTreeEntries();

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
