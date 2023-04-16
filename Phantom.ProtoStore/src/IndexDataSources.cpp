#include "IndexDataSourcesImpl.h"
#include "Index.h"
#include "InternalProtoStore.h"
#include "ProtoStoreInternal.pb.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"

namespace Phantom::ProtoStore
{

IndexDataSources::IndexDataSources(
    IInternalProtoStore* protoStore,
    shared_ptr<IIndex> index
) :
    m_protoStore(protoStore),
    m_index(std::move(index)),
    m_activeMemoryTablePartitionNumber(0)
{
}

task<> IndexDataSources::Replay(
    FlatMessage<LoggedRowWrite> rowWrite
)
{
    auto iterator = m_replayedMemoryTables.find(rowWrite->partition_number());
    if (iterator == m_replayedMemoryTables.end())
    {
        m_activeMemoryTablePartitionNumber = co_await m_protoStore->CreateMemoryTable(
            m_index,
            rowWrite->partition_number(),
            m_activeMemoryTable);
        m_replayedMemoryTables[m_activeMemoryTablePartitionNumber] = m_activeMemoryTable;

        co_await UpdateIndexDataSources();
    }

    co_await m_index->ReplayRow(
        iterator->second,
        std::move(rowWrite)
    );
}

task<> IndexDataSources::Replay(
    const LoggedCreateMemoryTable* loggedCreateMemoryTable
)
{
    //if (m_replayedMemoryTables.find(loggedCreateMemoryTable->partition_number()) != m_replayedMemoryTables.end())
    //{
    //    co_return;
    //}

    //m_activeMemoryTablePartitionNumber = co_await m_protoStore->CreateMemoryTable(
    //    m_index,
    //    loggedCreateMemoryTable->partition_number(),
    //    m_activeMemoryTable);
    //m_replayedMemoryTables[m_activeMemoryTablePartitionNumber] = m_activeMemoryTable;

    co_await UpdateIndexDataSources();

    co_return;
}

task<> IndexDataSources::Replay(
    const LoggedCheckpoint* loggedCheckpoint
)
{
    for (auto partitionNumber : *loggedCheckpoint->partition_number())
    {
        m_replayedMemoryTables.erase(
            partitionNumber);
    }

    co_return;
}

task<> IndexDataSources::FinishReplay()
{
    co_await UpdateIndexDataSources();
}

task<FlatBuffers::LoggedCheckpointT> IndexDataSources::StartCheckpoint()
{
    auto lock = co_await m_dataSourcesLock.scoped_lock_async();

    FlatBuffers::LoggedCheckpointT loggedCheckpoint;

    loggedCheckpoint.index_number =
        m_index->GetIndexNumber();

    for (auto replayedMemoryTable : m_replayedMemoryTables)
    {
        loggedCheckpoint.partition_number.push_back(
            replayedMemoryTable.first
        );
        m_checkpointingMemoryTables[replayedMemoryTable.first] = replayedMemoryTable.second;
    }
    m_replayedMemoryTables.clear();

    // Only checkpoint the active memory table if it has rows in it.
    // This might race with the actual addition of rows to the memory table,
    // but that's ok. In that case, we may not checkpoint the memory table,
    // but that's ok because the memory table will be checkpointed on the next
    // checkpoint.
    // It's also possible that we'll actually write zero rows, if all the rows
    // in the memory table are transaction aborts. This is also okay and desirable:
    // The aborted rows use in-process memory, and checkpointing will free that memory.
    if (m_activeMemoryTable->GetApproximateRowCount() > 0)
    {
        loggedCheckpoint.partition_number.push_back(m_activeMemoryTablePartitionNumber);
        m_checkpointingMemoryTables[m_activeMemoryTablePartitionNumber] = std::move(m_activeMemoryTable);

        m_activeMemoryTablePartitionNumber = co_await m_protoStore->CreateMemoryTable(
            m_index,
            0,
            m_activeMemoryTable);
    }

    co_await UpdateIndexDataSources();

    co_return loggedCheckpoint;
}

task<WriteRowsResult> IndexDataSources::Checkpoint(
    const LoggedCheckpointT& loggedCheckpoint,
    shared_ptr<IPartitionWriter> partitionWriter)
{

    vector<shared_ptr<IMemoryTable>> memoryTablesToCheckpoint;

    {
        auto lock = co_await m_dataSourcesLock.scoped_lock_async();

        for (auto partitionNumber : loggedCheckpoint.partition_number)
        {
            auto memoryTable = m_checkpointingMemoryTables[partitionNumber];
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
    const LoggedCheckpointT& loggedCheckpoint,
    vector<shared_ptr<IPartition>> partitions) 
{
    // Copy the old data sources so that we can release them outside the lock.
    auto oldMemoryTables = m_checkpointingMemoryTables;
    auto oldPartitions = m_partitions;

    {
        auto lock = co_await m_dataSourcesLock.scoped_lock_async();

        for (auto partitionNumber : loggedCheckpoint.partition_number)
        {
            m_checkpointingMemoryTables.erase(partitionNumber);
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
        std::make_shared<IndexDataSourcesSelector>(
            m_activeMemoryTable,
            m_activeMemoryTablePartitionNumber,
            inactiveMemoryTables,
            m_partitions));
}

task<> IndexDataSources::EnsureHasActiveMemoryTable(
)
{
    if (!m_activeMemoryTable)
    {
        m_activeMemoryTablePartitionNumber = co_await m_protoStore->CreateMemoryTable(
            m_index,
            0,
            m_activeMemoryTable);

        co_await UpdateIndexDataSources();
    }
}

IndexDataSourcesSelector::IndexDataSourcesSelector(
    std::shared_ptr<IMemoryTable> activeMemoryTable,
    PartitionNumber activeMemoryTablePartitionNumber,
    std::vector<std::shared_ptr<IMemoryTable>> inactiveMemoryTables,
    std::vector<std::shared_ptr<IPartition>> partitions
)
{
    m_activeMemoryTable = std::move(activeMemoryTable);
    m_activeMemoryTablePartitionNumber = activeMemoryTablePartitionNumber;

    auto readAndEnumerateMemoryTables = inactiveMemoryTables;
    if (m_activeMemoryTable)
    {
        readAndEnumerateMemoryTables.push_back(m_activeMemoryTable);
    }

    m_readAndEnumerateSelection = std::make_shared<IndexDataSourcesSelection>(
        std::move(readAndEnumerateMemoryTables),
        partitions
    );

    m_checkConflictSelection = std::make_shared<IndexDataSourcesSelection>(
        std::move(inactiveMemoryTables),
        std::move(partitions)
    );
}

std::shared_ptr<const IndexDataSourcesSelection> IndexDataSourcesSelector::SelectForCheckConflict(
    const ProtoValue& key,
    SequenceNumber readSequenceNumber
) const
{
    return m_checkConflictSelection;
}

std::shared_ptr<const IndexDataSourcesSelection> IndexDataSourcesSelector::SelectForRead(
    const ProtoValue& key,
    SequenceNumber readSequenceNumber
) const
{
    return m_readAndEnumerateSelection;
}

std::shared_ptr<const IndexDataSourcesSelection> IndexDataSourcesSelector::SelectForEnumerate(
    const ProtoValue& keyLow,
    const ProtoValue& keyHigh,
    SequenceNumber readSequenceNumber
) const
{
    return m_readAndEnumerateSelection;
}

std::shared_ptr<const IndexDataSourcesSelection> IndexDataSourcesSelector::SelectForEnumeratePrefix(
    const Prefix& prefix,
    SequenceNumber readSequenceNumber
) const
{
    return m_readAndEnumerateSelection;
}

const std::shared_ptr<IMemoryTable>& IndexDataSourcesSelector::ActiveMemoryTable(
) const
{
    return m_activeMemoryTable;
}

PartitionNumber IndexDataSourcesSelector::ActiveMemoryTablePartitionNumber(
) const
{
    return m_activeMemoryTablePartitionNumber;
}

std::shared_ptr<IIndexDataSources> MakeIndexDataSources(
    IInternalProtoStore* protoStore,
    shared_ptr<IIndex> index
)
{
    return std::make_shared<IndexDataSources>(
        protoStore,
        std::move(index));
}

}