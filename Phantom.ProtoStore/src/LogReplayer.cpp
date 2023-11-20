#include "Phantom.Coroutines/async_scope.h"
#include "ExtentName.h"
#include "LogReplayer.h"
#include "MemoryTable.h"

namespace Phantom::ProtoStore
{

LogReplayer::LogReplayer(
    Schedulers& schedulers,
    IProtoStoreReplayTarget& protoStore,
    IHeaderAccessor& headerAccessor,
    IMessageStore& logMessageStore
)
    :
    m_schedulers(schedulers),
    m_protoStore(protoStore),
    m_headerAccessor(headerAccessor),
    m_logMessageStore(logMessageStore)
{
}

task<> LogReplayer::ReplayLog()
{
    auto header = co_await m_headerAccessor.ReadHeader();

    m_globalSequenceNumbers.m_nextIndexNumber.ReplayNextSequenceNumber(
        header->next_index_number);
    m_globalSequenceNumbers.m_nextPartitionNumber.ReplayNextSequenceNumber(
        header->next_partition_number);

    Phantom::Coroutines::async_scope<> readLogExtentsScope;
    for (const auto& logExtent : header->log_replay_extent_names)
    {
        readLogExtentsScope.spawn(
            ReadLogExtent(logExtent->log_extent_sequence_number)
        );
    }
    co_await readLogExtentsScope.join();

    Phantom::Coroutines::async_scope<> tasksScope;
    tasksScope.spawn([&]() -> task<>
        {
            co_await m_handleLoggedCheckpointScope.join();
            m_handleLoggedCheckpointScopeComplete.set();
        });

    tasksScope.spawn([&]() -> task<>
        {
            co_await m_handleLoggedCommitTransactionScope.join();
            m_handleLoggedCommitTransactionScopeComplete.set();
        });

    tasksScope.spawn([&]()->task<>
        {
            co_await m_handleLoggedRowWriteScope.join();
            m_handleLoggedRowWriteScopeComplete.set();
        });
    
    tasksScope.spawn([&]()->task<>
        {
            co_await m_handleLoggedPartitionsDataScope.join();
            m_handleLoggedPartitionsDataScopeComplete.set();
        });
    
    tasksScope.spawn([&]()->task<>
        {
            co_await m_handleLoggedRowWriteScopeComplete;
            co_await m_handleLoggedRowWrite_System_Indexes_Scope.join();
            co_await m_handleLoggedPartitionsDataScopeComplete;
            
            co_await m_protoStore.ReplayPartitionsData(
                m_lastLoggedPartitionsData
            );

            co_await m_protoStore.ReplayPartitionsForOpenedIndexes();
            m_handleLoggedRowWrite_System_Indexes_ScopeComplete.set();
        });
    
    tasksScope.spawn([&]()->task<>
        {
            co_await m_handleLoggedRowWriteScopeComplete;
            co_await m_handleLoggedRowWrite_User_Indexes_Scope.join();
            m_handleLoggedRowWrite_User_Indexes_ScopeComplete.set();
        });

    tasksScope.spawn([&]()->task<>
        {
            co_await m_handleLoggedPartitionsDataScopeComplete;
            co_await m_handleLoggedRowWriteScopeComplete;
            co_await m_protoStore.ReplayPartitionsForOpenedIndexes();
            co_await m_protoStore.ReplayLogExtentUsageMap(
                std::move(m_logExtentUsageMap)
            );
        });

    co_await tasksScope.join();
}

task<> LogReplayer::ReadLogExtent(
    LogExtentSequenceNumber logExtentSequenceNumber)
{
    co_await m_schedulers.ComputeScheduler->schedule();

    if (!m_logExtentUsageMap.HandleNewLogExtent(
        logExtentSequenceNumber))
    {
        // If we've already started processing this log extent,
        // don't do it again!
        co_return;
    }

    Phantom::Coroutines::async_scope<> messagesScope;

    auto logExtentNameT = MakeLogExtentName(logExtentSequenceNumber);
    auto logExtentName = FlatValue{ &logExtentNameT };

    auto logExtentMessages = co_await m_logMessageStore.OpenExtentForSequentialReadAccess(
        logExtentName);

    while (true)
    {
        auto message = co_await logExtentMessages->Read();
        if (!message)
        {
            break;
        }

        messagesScope.spawn(
            HandleLogMessage(
                logExtentSequenceNumber,
                std::move(message))
        );
    }

    co_await messagesScope.join();
}

task<> LogReplayer::HandleLogMessage(
    LogExtentSequenceNumber logExtentSequenceNumber,
    DataReference<StoredMessage> message
)
{
    co_await m_schedulers.ComputeScheduler->schedule();

    message->VerifyChecksum();

    FlatMessage<FlatBuffers::LogRecord> logRecord{ std::move(message) };

    if (!logRecord->log_entries())
    {
        co_return;
    }

    for (auto logEntry : *logRecord->log_entries())
    {
        switch (logEntry->log_entry_type())
        {
        case FlatBuffers::LogEntryUnion::LoggedCreateIndex:
        case FlatBuffers::LogEntryUnion::LoggedCreateExtent:
        case FlatBuffers::LogEntryUnion::LoggedCommitExtent:
        case FlatBuffers::LogEntryUnion::LoggedDeleteExtent:

        case FlatBuffers::LogEntryUnion::LoggedUpdatePartitions:
            break;

        case FlatBuffers::LogEntryUnion::LoggedCommitLocalTransaction:
            m_handleLoggedCommitTransactionScope.spawn(
                HandleLoggedCommitTransaction(FlatMessage{ logRecord, logEntry->log_entry_as_LoggedCommitLocalTransaction() })
            );
            break;

        case FlatBuffers::LogEntryUnion::LoggedCheckpoint:
            m_handleLoggedCheckpointScope.spawn(
                HandleLoggedCheckpoint(FlatMessage{ logRecord, logEntry->log_entry_as_LoggedCheckpoint() })
            );
            break;

        case FlatBuffers::LogEntryUnion::LoggedPartitionsData:
            m_handleLoggedPartitionsDataScope.spawn(
                HandleLoggedPartitionsData(FlatMessage{ logRecord, logEntry->log_entry_as_LoggedPartitionsData() })
            );
            break;

        case FlatBuffers::LogEntryUnion::LoggedRowWrite:
            m_handleLoggedRowWriteScope.spawn(
                HandleLoggedRowWrite(
                    logExtentSequenceNumber,
                    FlatMessage{ logRecord, logEntry->log_entry_as_LoggedRowWrite() })
            );
            break;

        case FlatBuffers::LogEntryUnion::LoggedNewLogExtent:
            co_await ReadLogExtent(
                logEntry->log_entry_as_LoggedNewLogExtent()->new_log_extent_name()->log_extent_sequence_number()
            );
            break;

        default:
            throw std::runtime_error("Unknown log entry type");
        }
    }
}

task<> LogReplayer::HandleLoggedCheckpoint(
    FlatMessage<FlatBuffers::LoggedCheckpoint> checkpoint
)
{
    co_await m_schedulers.ComputeScheduler->schedule();

    if (checkpoint->partition_number())
    {
        for (auto partitionNumber : *checkpoint->partition_number())
        {
            m_checkpointedMemoryTables.emplace(
                partitionNumber,
                std::monostate{});
        }
    }
}

task<> LogReplayer::HandleLoggedPartitionsData(
    FlatMessage<FlatBuffers::LoggedPartitionsData> loggedPartitionsData
)
{
    co_await m_schedulers.ComputeScheduler->schedule();

    auto lock = co_await m_lastLoggedPartitionsData_Mutex.scoped_lock_async();

    if (!m_lastLoggedPartitionsData
        ||
        m_lastLoggedPartitionsData->partitions_table_partition_number() < loggedPartitionsData->partitions_table_partition_number())
    {
        m_lastLoggedPartitionsData = loggedPartitionsData;
    }

    m_globalSequenceNumbers.m_nextPartitionNumber.ReplayUsedSequenceNumber(
        loggedPartitionsData->partitions_table_partition_number());

    if (loggedPartitionsData->header_extent_names())
    {
        for (auto headerExtentName : *loggedPartitionsData->header_extent_names())
        {
            m_globalSequenceNumbers.m_nextIndexNumber.ReplayUsedSequenceNumber(
                headerExtentName->index_extent_name()->index_number());
            m_globalSequenceNumbers.m_nextPartitionNumber.ReplayUsedSequenceNumber(
                headerExtentName->index_extent_name()->partition_number());
        }
    }
}

task<> LogReplayer::HandleLoggedCommitTransaction(
    FlatMessage<FlatBuffers::LoggedCommitLocalTransaction> loggedCommitLocalTransaction
)
{
    co_await m_schedulers.ComputeScheduler->schedule();

    m_committedLocalTransactions.emplace(
        loggedCommitLocalTransaction->local_transaction_id(),
        std::monostate{});
}

task<> LogReplayer::HandleLoggedRowWrite(
    LogExtentSequenceNumber logExtentSequenceNumber,
    FlatMessage<FlatBuffers::LoggedRowWrite> loggedRowWrite
)
{
    co_await m_handleLoggedCommitTransactionScopeComplete;
    co_await m_handleLoggedCheckpointScopeComplete;
    co_await m_schedulers.ComputeScheduler->schedule();

    m_globalSequenceNumbers.m_nextIndexNumber.ReplayUsedSequenceNumber(loggedRowWrite->index_number());
    m_globalSequenceNumbers.m_nextPartitionNumber.ReplayUsedSequenceNumber(loggedRowWrite->partition_number());
    m_globalSequenceNumbers.m_nextLocalTransactionNumber.ReplayUsedSequenceNumber(loggedRowWrite->local_transaction_id());

    if (m_checkpointedMemoryTables.contains(loggedRowWrite->partition_number()))
    {
        co_return;
    }

    if (!m_committedLocalTransactions.contains(loggedRowWrite->local_transaction_id()))
    {
        co_return;
    }

    m_logExtentUsageMap.HandleLoggedRowWrite(
        logExtentSequenceNumber,
        loggedRowWrite.get());

    if (SystemIndexNumbers::IsSystemIndex(loggedRowWrite->index_number()))
    {
        m_handleLoggedRowWrite_System_Indexes_Scope.spawn(
            HandleLoggedRowWrite_System_Index(
                std::move(loggedRowWrite))
        );
    }
    else
    {
        m_handleLoggedRowWrite_User_Indexes_Scope.spawn(
            HandleLoggedRowWrite_User_Index(
                std::move(loggedRowWrite))
        );
    }
}

shared_task<const LogReplayer::ReplayedMemoryTable>& LogReplayer::ReplayMemoryTable(
    IndexNumber indexNumber,
    PartitionNumber partitionNumber
)
{
    static auto doReplayMemoryTable = [](
        IProtoStoreReplayTarget& protoStore,
        IndexNumber indexNumber,
        PartitionNumber partitionNumber
        )
        ->
        shared_task<const LogReplayer::ReplayedMemoryTable>
    {
        ReplayedMemoryTable result;
        result.m_index = co_await protoStore.GetIndex(indexNumber);
        result.m_memoryTable = co_await protoStore.ReplayCreateMemoryTable(
            indexNumber,
            partitionNumber);
        co_return std::move(result);
    };

    shared_task<const LogReplayer::ReplayedMemoryTable>* replayedMemoryTable;

    if (m_replayedMemoryTables.cvisit(partitionNumber, [&](const auto& existingItem)
        {
            replayedMemoryTable = existingItem.second.get();
        }))
    {
        return *replayedMemoryTable;
    }
    
    auto replayedMemoryTableSharedPointer = std::make_shared<shared_task<const LogReplayer::ReplayedMemoryTable>>(
        doReplayMemoryTable(
            m_protoStore,
            indexNumber,
            partitionNumber));

    replayedMemoryTable = replayedMemoryTableSharedPointer.get();

    m_replayedMemoryTables.emplace_or_cvisit(
        partitionNumber,
        replayedMemoryTableSharedPointer,
        [&](const auto& item)
        {
            replayedMemoryTable = item.second.get();
        });

    return *replayedMemoryTable;
}

task<> LogReplayer::HandleLoggedRowWrite_System_Index(
    FlatMessage<FlatBuffers::LoggedRowWrite> loggedRowWrite
)
{
    co_await m_schedulers.ComputeScheduler->schedule();

    auto& replayedMemoryTable = co_await ReplayMemoryTable(
        loggedRowWrite->index_number(),
        loggedRowWrite->partition_number());

    co_await m_schedulers.ComputeScheduler->schedule();

    co_await replayedMemoryTable.m_memoryTable->ReplayRow(
        std::move(loggedRowWrite));
}

task<> LogReplayer::HandleLoggedRowWrite_User_Index(
    FlatMessage<FlatBuffers::LoggedRowWrite> loggedRowWrite
)
{
    // The only difference between replay of user index rows and system index rows
    // is that user index rows need the metadata in the system indexes that might need to be replayed.
    // We therefore wait for all system index rows to be replayed before replaying user index rows.
    co_await m_handleLoggedRowWrite_System_Indexes_ScopeComplete;

    co_await m_schedulers.ComputeScheduler->schedule();
    co_await HandleLoggedRowWrite_System_Index(
        std::move(loggedRowWrite));
}

}
