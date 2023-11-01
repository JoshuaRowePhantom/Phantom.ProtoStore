#pragma once

#include "StandardTypes.h"
#include "MemoryTable.h"
#include "PartitionWriter.h"

namespace Phantom::ProtoStore
{

class IIndexDataSourcesSelector;

class IIndexData : public SerializationTypes
{
public:
    using CreateLoggedRowWrite = std::function<task<FlatMessage<LoggedRowWrite>>(PartitionNumber)>;

    virtual operation_task<PartitionNumber> AddRow(
        SequenceNumber readSequenceNumber,
        CreateLoggedRowWrite loggedRowWrite,
        shared_ptr<DelayedMemoryTableTransactionOutcome> delayedTransactionOutcome
    ) = 0;

    virtual const shared_ptr<const ValueComparer>& GetKeyComparer(
    ) = 0;

    virtual const shared_ptr<const ValueComparer>& GetValueComparer(
    ) = 0;

    virtual operation_task<ReadResult> Read(
        shared_ptr<DelayedMemoryTableTransactionOutcome> originatingTransactionOutcome,
        const ReadRequest& readRequest
    ) = 0;

    virtual EnumerateResultGenerator Enumerate(
        shared_ptr<DelayedMemoryTableTransactionOutcome> originatingTransactionOutcome,
        EnumerateRequest readRequest
    ) = 0;

    virtual EnumerateResultGenerator EnumeratePrefix(
        shared_ptr<DelayedMemoryTableTransactionOutcome> originatingTransactionOutcome,
        EnumeratePrefixRequest readRequest
    ) = 0;

    virtual IndexNumber GetIndexNumber(
    ) const = 0;

    virtual const IndexName& GetIndexName(
    ) const = 0;

    virtual const shared_ptr<const Schema>& GetSchema() const = 0;

    virtual const FlatValue<FlatBuffers::Metadata>& GetMetadata(
    ) const = 0;
};

class IIndex : public IIndexData
{
public:
    virtual task<> Join(
    ) = 0;

    virtual task<WriteRowsResult> WriteMemoryTables(
        const shared_ptr<IPartitionWriter>& partitionWriter,
        const vector<shared_ptr<IMemoryTable>>& memoryTablesToCheckpoint
    ) = 0;

    virtual task<> ReplayRow(
        shared_ptr<IMemoryTable> memoryTable,
        FlatMessage<LoggedRowWrite> loggedRowWrite
        ) = 0;

    virtual task<> SetDataSources(
        std::shared_ptr<IIndexDataSourcesSelector> indexDataSourcesSelector
    ) = 0;
};

std::shared_ptr<IIndex> MakeIndex(
    IndexName indexName,
    IndexNumber indexNumber,
    SequenceNumber createSequenceNumber,
    shared_ptr<const ValueComparer> keyComparer,
    shared_ptr<const ValueComparer> valueComparer,
    IUnresolvedTransactionsTracker* unresolvedTransactionsTracker,
    std::shared_ptr<const Schema> schema,
    FlatValue<FlatBuffers::Metadata> metadata
);

}
