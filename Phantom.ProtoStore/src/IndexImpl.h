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
    IndexName m_indexName;
    IndexNumber m_indexNumber;
    SequenceNumber m_createSequenceNumber;
    shared_ptr<KeyComparer> m_keyComparer;
    shared_ptr<RowMerger> m_rowMerger;
    shared_ptr<IMessageFactory> m_keyFactory;
    shared_ptr<IMessageFactory> m_valueFactory;

    std::atomic_flag m_dontNeedCheckpoint;

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


public:
    Index(
        const string& indexName,
        IndexNumber indexNumber,
        SequenceNumber createSequenceNumber,
        shared_ptr<IMessageFactory> keyFactory,
        shared_ptr<IMessageFactory> valueFactory
    );

    virtual shared_ptr<KeyComparer> GetKeyComparer(
    ) override;

    virtual shared_ptr<IMessageFactory> GetKeyFactory(
    ) override;

    virtual shared_ptr<IMessageFactory> GetValueFactory(
    ) override;

    virtual task<CheckpointNumber> AddRow(
        SequenceNumber readSequenceNumber,
        const ProtoValue& key,
        const ProtoValue& value,
        SequenceNumber writeSequenceNumber,
        MemoryTableOperationOutcomeTask operationOutcomeTask
    ) override;

    virtual task<> ReplayRow(
        shared_ptr<IMemoryTable> memoryTable,
        const string& key,
        const string& value,
        SequenceNumber writeSequenceNumber
    ) override;

    virtual task<ReadResult> Read(
        const ReadRequest& readRequest
    ) override;

    virtual cppcoro::async_generator<EnumerateResult> Enumerate(
        const EnumerateRequest& readRequest
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
        vector<shared_ptr<IMemoryTable>> memoryTablesToEnumerate,
        vector<shared_ptr<IPartition>> partitions
    ) override;

    virtual task<> Join(
    ) override;

};

}
