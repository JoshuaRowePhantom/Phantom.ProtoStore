#pragma once

#include "Index.h"
#include "Schema.h"
#include "MemoryTable.h"
#include "Partition.h"
#include "Phantom.System/async_reader_writer_lock.h"
#include <atomic>

namespace Phantom::ProtoStore
{

class Index
    : public IIndex
{
    const Schedulers m_schedulers;
    const shared_ptr<const Schema> m_schema;
    const shared_ptr<const ValueComparer> m_keyComparer;
    const shared_ptr<const ValueComparer> m_valueComparer;
    const IndexName m_indexName;
    const FlatValue<FlatBuffers::Metadata> m_metadata;
    const IndexNumber m_indexNumber;
    const SequenceNumber m_createSequenceNumber;
    const shared_ptr<RowMerger> m_rowMerger;
    IUnresolvedTransactionsTracker* const m_unresolvedTransactionsTracker;

    // This lock control access to the following members:
    // vvvvvvvvvvvvvvvvv
    async_reader_writer_lock m_dataSourcesLock;
    std::shared_ptr<IIndexDataSourcesSelector> m_indexDataSourcesSelector;
    // ^^^^^^^^^^^^^^^^^
    // The above members are locked with m_dataSourcesLock

    std::unexpected<FailedResult> MakeUnresolvedTransactionFailedResult(
        TransactionId unresolvedTransactionId);

public:
    Index(
        Schedulers m_schedulers,
        IndexName indexName,
        IndexNumber indexNumber,
        SequenceNumber createSequenceNumber,
        shared_ptr<const ValueComparer> keyComparer,
        shared_ptr<const ValueComparer> valueComparer,
        IUnresolvedTransactionsTracker* unresolvedTransactionsTracker,
        std::shared_ptr<const Schema> schema,
        FlatValue<FlatBuffers::Metadata> metadata
        );

    virtual const shared_ptr<const ValueComparer>& GetKeyComparer(
    ) override;
    
    virtual const shared_ptr<const ValueComparer>& GetValueComparer(
    ) override;

    virtual operation_task<PartitionNumber> AddRow(
        SequenceNumber readSequenceNumber,
        CreateLoggedRowWrite loggedRowWrite,
        const shared_ptr<DelayedMemoryTableTransactionOutcome>& delayedTransactionOutcome
    ) override;

    virtual task<> ReplayRow(
        shared_ptr<IMemoryTable> memoryTable,
        FlatMessage<LoggedRowWrite> loggedRowWrite
    ) override;

    virtual operation_task<ReadResult> Read(
        shared_ptr<DelayedMemoryTableTransactionOutcome> originatingTransactionOutcome,
        const ReadRequest& readRequest
    ) override;

    virtual cppcoro::async_generator<OperationResult<EnumerateResult>> Enumerate(
        shared_ptr<DelayedMemoryTableTransactionOutcome> originatingTransactionOutcome,
        EnumerateRequest readRequest
    ) override;

    virtual cppcoro::async_generator<OperationResult<EnumerateResult>> EnumeratePrefix(
        shared_ptr<DelayedMemoryTableTransactionOutcome> originatingTransactionOutcome,
        EnumeratePrefixRequest readRequest
    ) override;

    virtual IndexNumber GetIndexNumber(
    ) const override;

    virtual const IndexName& GetIndexName(
    ) const override;

    virtual task<WriteRowsResult> WriteMemoryTables(
        const shared_ptr<IPartitionWriter>& partitionWriter,
        const vector<shared_ptr<IMemoryTable>>& memoryTablesToCheckpoint
    ) override;

    virtual task<> SetDataSources(
        std::shared_ptr<IIndexDataSourcesSelector> indexDataSourcesSelector
    ) override;

    virtual const shared_ptr<const Schema>& GetSchema(
    ) const override;

    virtual const FlatValue<FlatBuffers::Metadata>& GetMetadata(
    ) const override;

    virtual task<> Join(
    ) override;
};


}
