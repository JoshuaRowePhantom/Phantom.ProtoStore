#include "IndexDataSourcesImpl.h"
#include "Index.h"
#include "src/ProtoStoreInternal.pb.h"

namespace Phantom::ProtoStore
{

IndexDataSources::IndexDataSources(
    shared_ptr<IIndex> index,
    function<shared_ptr<IMemoryTable>()> makeMemoryTable
) :
    m_index(index),
    m_makeMemoryTable(makeMemoryTable),
    m_currentCheckpointNumber(1),
    m_activeMemoryTable(makeMemoryTable())
{
}

task<> IndexDataSources::Replay(
    const LoggedRowWrite& rowWrite
)
{
    auto memoryTable = m_replayedMemoryTables[rowWrite.checkpointnumber()];
    if (!memoryTable)
    {
        memoryTable = m_replayedMemoryTables[rowWrite.checkpointnumber()] = m_makeMemoryTable();
        
        m_currentCheckpointNumber = std::max(
            m_currentCheckpointNumber,
            rowWrite.checkpointnumber() + 1
        );

        co_await UpdateIndexDataSources();
    }

    co_await m_index->ReplayRow(
        memoryTable,
        rowWrite.key(),
        rowWrite.value(),
        ToSequenceNumber(rowWrite.sequencenumber())
    );
}

task<> IndexDataSources::Replay(
    const LoggedCheckpoint& loggedCheckpoint
)
{
    for (auto checkpointNumber : loggedCheckpoint.checkpointnumber())
    {
        m_replayedMemoryTables.erase(
            checkpointNumber);
    }

    co_return;
}

task<> IndexDataSources::FinishReplay()
{
    co_await UpdateIndexDataSources();
}

task<LoggedCheckpoint> IndexDataSources::StartCheckpoint()
{
    auto lock = co_await m_dataSourcesLock.scoped_lock_async();

    LoggedCheckpoint loggedCheckpoint;

    loggedCheckpoint.set_indexnumber(
        m_index->GetIndexNumber());

    for (auto replayedMemoryTable : m_replayedMemoryTables)
    {
        loggedCheckpoint.add_checkpointnumber(
            replayedMemoryTable.first
        );
        m_checkpointingMemoryTables[replayedMemoryTable.first] = replayedMemoryTable.second;
    }
    m_replayedMemoryTables.clear();

    loggedCheckpoint.add_checkpointnumber(m_currentCheckpointNumber);
    m_checkpointingMemoryTables[m_currentCheckpointNumber] = m_activeMemoryTable;

    m_currentCheckpointNumber++;
    m_activeMemoryTable = m_makeMemoryTable();

    co_await UpdateIndexDataSources();

    // Only return the loggedCheckpoint if there are in fact rows to checkpoint.
    for (auto checkpoint : loggedCheckpoint.checkpointnumber())
    {
        if (co_await m_checkpointingMemoryTables[checkpoint]->GetRowCount() > 0)
        {
            co_return loggedCheckpoint;
        }
    }

    // If there are no rows to checkpoint, return an empty LoggedCheckpoint,
    // and forget about the empty memory tables.
    for (auto checkpoint : loggedCheckpoint.checkpointnumber())
    {
        co_await m_checkpointingMemoryTables[checkpoint]->Join();
        m_checkpointingMemoryTables.erase(
            checkpoint);
    }

    co_return LoggedCheckpoint();
}

task<WriteRowsResult> IndexDataSources::Checkpoint(
    const LoggedCheckpoint& loggedCheckpoint,
    shared_ptr<IPartitionWriter> partitionWriter)
{

    vector<shared_ptr<IMemoryTable>> memoryTablesToCheckpoint;

    {
        auto lock = co_await m_dataSourcesLock.scoped_lock_async();

        for (auto checkpointNumber : loggedCheckpoint.checkpointnumber())
        {
            auto memoryTable = m_checkpointingMemoryTables[checkpointNumber];
            assert(memoryTable);

            memoryTablesToCheckpoint.push_back(
                memoryTable);
        }
    }

    co_return co_await m_index->WriteMemoryTables(
        partitionWriter,
        memoryTablesToCheckpoint);
}

task<> IndexDataSources::UpdatePartitions(
    const LoggedCheckpoint& loggedCheckpoint,
    vector<shared_ptr<IPartition>> partitions) 
{
    // Copy the old data sources so that we can release them outside the lock.
    auto oldMemoryTables = m_checkpointingMemoryTables;
    auto oldPartitions = m_partitions;

    {
        auto lock = co_await m_dataSourcesLock.scoped_lock_async();

        for (auto checkpointNumber : loggedCheckpoint.checkpointnumber())
        {
            m_checkpointingMemoryTables.erase(checkpointNumber);
        }

        m_partitions = partitions;

        co_await UpdateIndexDataSources();
    }

    for (auto& oldMemoryTable : oldMemoryTables)
    {
        co_await oldMemoryTable.second->Join();
    }
}

task<> IndexDataSources::UpdateIndexDataSources()
{
    vector<shared_ptr<IMemoryTable>> inactiveMemoryTables;

    for (auto memoryTable : m_checkpointingMemoryTables)
    {
        inactiveMemoryTables.push_back(
            memoryTable.second);
    }

    for (auto memoryTable : m_replayedMemoryTables)
    {
        inactiveMemoryTables.push_back(
            memoryTable.second);
    }

    co_await m_index->SetDataSources(
        m_activeMemoryTable,
        m_currentCheckpointNumber,
        inactiveMemoryTables,
        m_partitions);
}

}