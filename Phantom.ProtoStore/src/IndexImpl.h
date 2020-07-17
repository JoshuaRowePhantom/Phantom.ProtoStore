#pragma once

#include "Index.h"
#include "Schema.h"
#include "MemoryTable.h"
#include "Partition.h"
#include "Phantom.System/async_reader_writer_lock.h"

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
    
    shared_ptr<IMemoryTable> m_currentMemoryTable;
    CheckpointNumber m_currentCheckpointNumber;

    map<CheckpointNumber, shared_ptr<IMemoryTable>> m_activeMemoryTables;
    map<CheckpointNumber, shared_ptr<IMemoryTable>> m_checkpointingMemoryTables;

    typedef shared_ptr<vector<shared_ptr<IMemoryTable>>> MemoryTablesEnumeration;
    typedef shared_ptr<vector<shared_ptr<IPartition>>> PartitionsEnumeration;

    MemoryTablesEnumeration m_memoryTablesToEnumerate;
    PartitionsEnumeration m_partitions;
    async_reader_writer_lock m_partitionsLock;

    void UpdateMemoryTablesToEnumerate();
    task<> GetItemsToEnumerate(
        MemoryTablesEnumeration& memoryTables,
        PartitionsEnumeration& partitions);

public:
    Index(
        const string& indexName,
        IndexNumber indexNumber,
        SequenceNumber createSequenceNumber,
        shared_ptr<IMessageFactory> keyFactory,
        shared_ptr<IMessageFactory> valueFactory
    );

    virtual task<CheckpointNumber> AddRow(
        SequenceNumber readSequenceNumber,
        const ProtoValue& key,
        const ProtoValue& value,
        SequenceNumber writeSequenceNumber,
        MemoryTableOperationOutcomeTask operationOutcomeTask
    ) override;

    virtual task<CheckpointNumber> Replay(
        const LoggedRowWrite& rowWrite
    ) override;

    virtual task<ReadResult> Read(
        const ReadRequest& readRequest
    ) override;

    virtual IndexNumber GetIndexNumber(
    ) const override;

    virtual const IndexName& GetIndexName(
    ) const override;

    virtual task<> Join(
    ) override;
};

}
