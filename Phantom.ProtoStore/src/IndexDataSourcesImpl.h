#pragma once

#include "IndexDataSources.h"
#include "MemoryTable.h"
#include "Partition.h"
#include <cppcoro/async_mutex.hpp>

namespace Phantom::ProtoStore
{

class IndexDataSources
    : 
    public IIndexDataSources
{
    IInternalProtoStore* m_protoStore;
    cppcoro::async_mutex m_dataSourcesLock;

    vector<shared_ptr<IPartition>> m_partitions;
    shared_ptr<IMemoryTable> m_activeMemoryTable;
    PartitionNumber m_activeMemoryTablePartitionNumber;
    
    std::map<PartitionNumber, shared_ptr<IMemoryTable>> m_checkpointingMemoryTables;
    std::map<PartitionNumber, shared_ptr<IMemoryTable>> m_replayedMemoryTables;

    shared_ptr<IIndex> m_index;

    task<> UpdateIndexDataSources();

public:
    IndexDataSources(
        IInternalProtoStore* protoStore,
        shared_ptr<IIndex> index
    );

    virtual task<FlatBuffers::LoggedCheckpointT> StartCheckpoint(
    ) override;

    virtual task<WriteRowsResult> Checkpoint(
        const LoggedCheckpointT& loggedCheckpoint,
        shared_ptr<IPartitionWriter> partitionWriter
    ) override;

    virtual task<> UpdatePartitions(
        const LoggedCheckpointT& loggedCheckpoint,
        vector<shared_ptr<IPartition>> partitions
    ) override;

    virtual task<> EnsureHasActiveMemoryTable(
    ) override;

    virtual task<std::shared_ptr<IMemoryTable>> ReplayCreateMemoryTable(
        PartitionNumber partitionNumber
    ) override;
};

class IndexDataSourcesSelector :
    public IIndexDataSourcesSelector
{
    std::shared_ptr<IMemoryTable> m_activeMemoryTable;
    PartitionNumber m_activeMemoryTablePartitionNumber;

    std::shared_ptr<IndexDataSourcesSelection> m_readAndEnumerateSelection;
    std::shared_ptr<IndexDataSourcesSelection> m_checkConflictSelection;

public:
    IndexDataSourcesSelector(
        std::shared_ptr<IMemoryTable> activeMemoryTable,
        PartitionNumber activeMemoryTablePartitionNumber,
        std::vector<std::shared_ptr<IMemoryTable>> inactiveMemoryTables,
        std::vector<std::shared_ptr<IPartition>> partitions
    );

    virtual std::shared_ptr<const IndexDataSourcesSelection> SelectForCheckConflict(
        const ProtoValue& key,
        SequenceNumber readSequenceNumber
    ) const override;

    virtual std::shared_ptr<const IndexDataSourcesSelection> SelectForRead(
        const ProtoValue& key,
        SequenceNumber readSequenceNumber
    ) const override;

    virtual std::shared_ptr<const IndexDataSourcesSelection> SelectForEnumerate(
        const ProtoValue& keyLow,
        const ProtoValue& keyHigh,
        SequenceNumber readSequenceNumber
    ) const override;

    virtual std::shared_ptr<const IndexDataSourcesSelection> SelectForEnumeratePrefix(
        const Prefix& prefix,
        SequenceNumber readSequenceNumber
    ) const override;

    virtual const std::shared_ptr<IMemoryTable>& ActiveMemoryTable(
    ) const override;

    virtual PartitionNumber ActiveMemoryTablePartitionNumber(
    ) const override;
};

}
