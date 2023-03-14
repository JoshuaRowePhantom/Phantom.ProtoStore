#pragma once

#include "PartitionWriter.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"
#include "MessageStore.h"
#include "PartitionImpl.h"

namespace Phantom::ProtoStore
{
class PartitionWriterBase : SerializationTypes
{
protected:
    const shared_ptr<const Schema> m_schema;
    const shared_ptr<const KeyComparer> m_keyComparer;
    const shared_ptr<const KeyComparer> m_valueComparer;
    shared_ptr<ISequentialMessageWriter> m_dataWriter;

    PartitionWriterBase(
        const shared_ptr<const Schema> schema,
        const shared_ptr<const KeyComparer> keyComparer,
        const shared_ptr<const KeyComparer> valueComparer,
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
        ProtoValue highestKey;
        SequenceNumber lowestSequenceNumberForKey;
        std::vector<flatbuffers::Offset<FlatBuffers::PartitionTreeEntryKey>> keyOffsets;
    };

    std::vector<StackEntry> m_treeNodeStack;
    
    StackEntry& current();

    WriteRowsRequest& m_writeRowsRequest;
    WriteRowsResult& m_writeRowsResult;
    BloomFilterVersion1<std::span<char>>& m_bloomFilter;

    using PartitionTreeEntryValueOffsetVector = std::vector<flatbuffers::Offset<FlatBuffers::PartitionTreeEntryValue>>;
    using DataValueOffset = Offset<FlatBuffers::DataValue>;
    using FlatBufferBuilder = flatbuffers::FlatBufferBuilder;

    void FinishKey(
        PartitionTreeEntryValueOffsetVector& treeEntryValues
        );

    DataValueOffset WriteAlignedMessage(
        FlatBufferBuilder& builder,
        const AlignedMessage& rawData);

    task<FlatBuffers::MessageReference_V1> Flush(
        uint8_t level,
        bool isFinishing);

public:
    PartitionTreeWriter(
        shared_ptr<const Schema> schema,
        shared_ptr<const KeyComparer> keyComparer,
        shared_ptr<const KeyComparer> valueComparer,
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
        shared_ptr<const Schema> schema,
        shared_ptr<const KeyComparer> keyComparer,
        shared_ptr<const KeyComparer> valueComparer,
        shared_ptr<ISequentialMessageWriter> dataWriter,
        shared_ptr<ISequentialMessageWriter> headerWriter
    );

    virtual task<WriteRowsResult> WriteRows(
        WriteRowsRequest& writeRowsRequest
    ) override;
};

}
