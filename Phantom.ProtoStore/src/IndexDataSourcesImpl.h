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
    cppcoro::async_mutex m_dataSourcesLock;

    vector<shared_ptr<IPartition>> m_partitions;
    shared_ptr<IMemoryTable> m_activeMemoryTable;
    PartitionNumber m_currentPartitionNumber;
    
    std::map<PartitionNumber, shared_ptr<IMemoryTable>> m_checkpointingMemoryTables;
    std::map<PartitionNumber, shared_ptr<IMemoryTable>> m_replayedMemoryTables;

    shared_ptr<IIndex> m_index;
    function<shared_ptr<IMemoryTable>()> m_makeMemoryTable;

    task<> UpdateIndexDataSources();

public:
    IndexDataSources(
        shared_ptr<IIndex> index,
        function<shared_ptr<IMemoryTable>()> makeMemoryTable
    );

    virtual task<> Replay(
        FlatMessage<LoggedRowWrite> rowWrite
    ) override;

    virtual task<> Replay(
        const LoggedCheckpoint* loggedCheckpoint
    ) override;

    virtual task<> FinishReplay(
    ) override;

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
};
}
