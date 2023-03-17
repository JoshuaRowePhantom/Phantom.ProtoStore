#pragma once

#include "PartitionWriter.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"
#include "MessageStore.h"
#include "PartitionImpl.h"
#include "ValueComparer.h"

namespace Phantom::ProtoStore
{
class PartitionWriterBase : SerializationTypes
{
protected:
    const shared_ptr<const Schema> m_schema;
    const shared_ptr<const ValueComparer> m_keyComparer;
    const shared_ptr<const ValueComparer> m_valueComparer;
    shared_ptr<ISequentialMessageWriter> m_dataWriter;

    PartitionWriterBase(
        const shared_ptr<const Schema> schema,
        const shared_ptr<const ValueComparer> keyComparer,
        const shared_ptr<const ValueComparer> valueComparer,
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
        std::shared_ptr<ValueBuilder> partitionTreeNodeValueBuilder = std::make_shared<ValueBuilder>();
        ProtoValue highestKey;
        int32_t estimatedHighestKeySize;
        SequenceNumber lowestSequenceNumberForKey;
        std::vector<flatbuffers::Offset<FlatBuffers::PartitionTreeEntryKey>> keyOffsets;
    };

    std::vector<StackEntry> m_treeNodeStack;
    
    StackEntry& current();

    WriteRowsRequest& m_writeRowsRequest;
    WriteRowsResult& m_writeRowsResult;
    BloomFilterVersion1<std::span<char>>& m_bloomFilter;

    struct WrittenValueEntry
    {
        uint64_t writeSequenceNumber;
        flatbuffers::Offset<FlatBuffers::DataValue> dataValueOffset;
        flatbuffers::Offset<FlatBuffers::ValuePlaceholder> flatValueOffset;
        std::optional<FlatBuffers::MessageReference_V1> bigDataReference;
        flatbuffers::Offset<flatbuffers::String> distributedTransactionIdOffset;
    };

    using PartitionTreeEntryVector = std::vector<WrittenValueEntry>;
    using DataValueOffset = Offset<FlatBuffers::DataValue>;
    using FlatBufferBuilder = flatbuffers::FlatBufferBuilder;

    void FinishKey(
        PartitionTreeEntryVector& treeEntryValues
        );

    struct WrittenValue
    {
        flatbuffers::Offset<FlatBuffers::DataValue> dataValueOffset;
        flatbuffers::Offset<FlatBuffers::ValuePlaceholder> placeholderOffset;
    };

    WrittenValue WriteValue(
        ValueBuilder& valueBuilder,
        const ValueComparer& keyComparer,
        const ProtoValue& value);

    DataValueOffset WriteAlignedMessage(
        FlatBufferBuilder& builder,
        const AlignedMessage& rawData);

    task<FlatBuffers::MessageReference_V1> Flush(
        uint8_t level,
        bool isFinishing);

public:
    PartitionTreeWriter(
        shared_ptr<const Schema> schema,
        shared_ptr<const ValueComparer> keyComparer,
        shared_ptr<const ValueComparer> valueComparer,
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
        shared_ptr<const ValueComparer> keyComparer,
        shared_ptr<const ValueComparer> valueComparer,
        shared_ptr<ISequentialMessageWriter> dataWriter,
        shared_ptr<ISequentialMessageWriter> headerWriter
    );

    virtual task<WriteRowsResult> WriteRows(
        WriteRowsRequest& writeRowsRequest
    ) override;
};

}
