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
    const shared_ptr<const Schema> m_schema;
    const shared_ptr<const ValueComparer> m_keyComparer;
    const shared_ptr<const ValueComparer> m_valueComparer;
    const IndexName m_indexName;
    const IndexNumber m_indexNumber;
    const SequenceNumber m_createSequenceNumber;
    const shared_ptr<RowMerger> m_rowMerger;
    IUnresolvedTransactionsTracker* const m_unresolvedTransactionsTracker;

    // This lock control access to the following members:
    // vvvvvvvvvvvvvvvvv
    async_reader_writer_lock m_dataSourcesLock;

    shared_ptr<IMemoryTable> m_activeMemoryTable;
    CheckpointNumber m_activeCheckpointNumber;

    typedef shared_ptr<vector<shared_ptr<IMemoryTable>>> MemoryTablesEnumeration;
    typedef shared_ptr<vector<shared_ptr<IPartition>>> PartitionsEnumeration;

    MemoryTablesEnumeration m_inactiveMemoryTables;
    MemoryTablesEnumeration m_memoryTablesToEnumerate;
    PartitionsEnumeration m_partitions;
    // ^^^^^^^^^^^^^^^^^
    // The above members are locked with m_dataSourcesLock

    void UpdateMemoryTablesToEnumerate();

    task<> GetEnumerationDataSources(
        MemoryTablesEnumeration& memoryTables,
        PartitionsEnumeration& partitions);

    task<vector<shared_ptr<IMemoryTable>>> StartCheckpoint(
        const LoggedCheckpoint& loggedCheckpoint
    );

    std::unexpected<FailedResult> MakeUnresolvedTransactionFailedResult(
        TransactionId unresolvedTransactionId);

public:
    Index(
        IndexName indexName,
        IndexNumber indexNumber,
        SequenceNumber createSequenceNumber,
        shared_ptr<const ValueComparer> keyComparer,
        shared_ptr<const ValueComparer> valueComparer,
        IUnresolvedTransactionsTracker* unresolvedTransactionsTracker,
        std::shared_ptr<const Schema> schema
    );

    virtual const shared_ptr<const ValueComparer>& GetKeyComparer(
    ) override;
    
    virtual const shared_ptr<const ValueComparer>& GetValueComparer(
    ) override;

    virtual operation_task<CheckpointNumber> AddRow(
        SequenceNumber readSequenceNumber,
        CreateLoggedRowWrite loggedRowWrite,
        shared_ptr<DelayedMemoryTableTransactionOutcome> delayedTransactionOutcome
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
        const EnumerateRequest& readRequest
    ) override;

    virtual cppcoro::async_generator<OperationResult<EnumerateResult>> EnumeratePrefix(
        shared_ptr<DelayedMemoryTableTransactionOutcome> originatingTransactionOutcome,
        const EnumeratePrefixRequest& readRequest
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
        shared_ptr<IMemoryTable> activeMemoryTable,
        CheckpointNumber activeCheckpointNumber,
        vector<shared_ptr<IMemoryTable>> inactiveMemoryTables,
        vector<shared_ptr<IPartition>> partitions
    ) override;

    virtual const shared_ptr<const Schema>& GetSchema(
    ) const override;

    virtual task<> Join(
    ) override;


};

}
