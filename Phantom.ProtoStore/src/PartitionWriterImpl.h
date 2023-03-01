#pragma once

#include "PartitionWriter.h"
#include "src/ProtoStoreInternal_generated.h"
#include "MessageStore.h"
#include "PartitionImpl.h"

namespace Phantom::ProtoStore
{
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
    
    StackEntry& current();

    WriteRowsRequest& m_writeRowsRequest;
    WriteRowsResult& m_writeRowsResult;
    BloomFilterVersion1<std::span<char>>& m_bloomFilter;

    using PartitionTreeEntryValueOffsetVector = std::vector<flatbuffers::Offset<FlatBuffers::PartitionTreeEntryValue>>;
    using PartitionDataValueOffset = Offset<FlatBuffers::PartitionDataValue>;
    using FlatBufferBuilder = flatbuffers::FlatBufferBuilder;

    void FinishKey(
        PartitionTreeEntryValueOffsetVector& treeEntryValues
        );

    PartitionDataValueOffset WriteRawData(
        FlatBufferBuilder& builder,
        const RawData& rawData);

    task<FlatBuffers::MessageReference_V1> Flush(
        uint8_t level,
        bool isFinishing);

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

public:
    PartitionWriter(
        shared_ptr<ISequentialMessageWriter> dataWriter,
        shared_ptr<ISequentialMessageWriter> headerWriter
    );

    virtual task<WriteRowsResult> WriteRows(
        WriteRowsRequest& writeRowsRequest
    ) override;
};

}
