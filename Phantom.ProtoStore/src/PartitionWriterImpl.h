#pragma once

#include "PartitionWriter.h"
#include "src/ProtoStoreInternal_generated.h"
#include "MessageStore.h"
#include "PartitionImpl.h"

namespace Phantom::ProtoStore
{
struct PartitionWriterParameters
{
    size_t DesiredTreeNodeSize = 1024 * 64 - 4096;
    size_t MinimumKeysPerTreeNode = 2;
    size_t MaxEmbeddedValueSize = 100;
};

class PartitionWriterBase : SerializationTypes
{
protected:
    shared_ptr<ISequentialMessageWriter> m_dataWriter;

    PartitionWriterBase(
        shared_ptr<ISequentialMessageWriter> dataWriter);

    task<FlatBuffers::MessageReference_V1> Write(
        const FlatMessage<FlatBuffers::PartitionMessage>&,
        FlushBehavior flushBehavior
    );
};

class PartitionTreeWriter : PartitionWriterBase
{
private:
    struct StackEntry
    {
        flatbuffers::FlatBufferBuilder partitionTreeNodeBuilder;
        RawData highestKey;
        SequenceNumber lowestSequenceNumberForKey;
        std::vector<flatbuffers::Offset<FlatBuffers::PartitionTreeEntryKey>> keyOffsets;
    };

    std::vector<StackEntry> m_treeNodeStack;
    
    WriteRowsRequest& m_writeRowsRequest;
    WriteRowsResult& m_writeRowsResult;
    BloomFilterVersion1<std::span<char>>& m_bloomFilter;

    using PartitionTreeEntryValueOffsetVector = std::vector<flatbuffers::Offset<FlatBuffers::PartitionTreeEntryValue>>;
    using PartitionDataValueOffset = Offset<FlatBuffers::PartitionDataValue>;
    using FlatBufferBuilder = flatbuffers::FlatBufferBuilder;

    void FinishKey(
        RawData& currentKey,
        SequenceNumber lowestSequenceNumberForKey,
        PartitionTreeEntryValueOffsetVector& treeEntryValues
        );

    PartitionDataValueOffset WriteRawData(
        FlatBufferBuilder& builder,
        const RawData& rawData);

    task<FlatBuffers::MessageReference_V1> Flush(
        size_t level);

public:
    PartitionTreeWriter(
        shared_ptr<ISequentialMessageWriter> dataWriter,
        WriteRowsRequest& writeRowsRequest,
        WriteRowsResult& writeRowsResult,
        BloomFilterVersion1<std::span<char>>& bloomFilter
    );

    task<FlatBuffers::MessageReference_V1> WriteRows();
};

class PartitionWriter : 
    PartitionWriterBase,
    public IPartitionWriter
{
    shared_ptr<ISequentialMessageWriter> m_headerWriter;
    const PartitionWriterParameters m_parameters;

public:
    PartitionWriter(
        PartitionWriterParameters parameters,
        shared_ptr<ISequentialMessageWriter> dataWriter,
        shared_ptr<ISequentialMessageWriter> headerWriter
    );

    virtual task<WriteRowsResult> WriteRows(
        WriteRowsRequest& writeRowsRequest
    ) override;
};

}
