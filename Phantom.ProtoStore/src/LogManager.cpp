#include "ExtentName.h"
#include "LogManager.h"
#include "ExtentStore.h"
#include "MessageStore.h"
#include "Phantom.Coroutines/suspend_result.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"

namespace Phantom::ProtoStore
{

size_t LogManager::LogExtentUsageHasher::operator() (
    const LogManager::LogExtentUsage& logExtentUsage
) const 
{
    return
        (logExtentUsage.IndexNumber << 7)
        ^ (logExtentUsage.IndexNumber >> 25)
        + (logExtentUsage.PartitionNumber << 3)
        ^ (logExtentUsage.PartitionNumber >> 29)
        + (logExtentUsage.LogExtentSequenceNumber << 17)
        ^ (logExtentUsage.LogExtentSequenceNumber >> 15);
}

LogManager::LogManager(
    Schedulers schedulers,
    shared_ptr<IExtentStore> logExtentStore,
    shared_ptr<IMessageStore> logMessageStore,
    const DatabaseHeaderT* header
)
    :
    m_schedulers(schedulers),
    m_logExtentStore(logExtentStore),
    m_logMessageStore(logMessageStore),
    m_nextLogExtentSequenceNumber(0),
    m_currentLogExtentSequenceNumber(0)
{
    for (auto& logReplayExtentName : header->log_replay_extent_names)
    {
        auto logExtentSequenceNumber = logReplayExtentName->log_extent_sequence_number;

        m_existingLogExtentSequenceNumbers.insert(
            logExtentSequenceNumber);

        m_nextLogExtentSequenceNumber = std::max(
            m_nextLogExtentSequenceNumber,
            logExtentSequenceNumber + 1);
    }
}

task<> LogManager::Replay(
    const FlatBuffers::LogExtentNameT* extentName,
    const LogRecord* logRecord
)
{
    auto logExtentSequenceNumber = extentName->log_extent_sequence_number;

    if (!logRecord->log_entries())
    {
        co_return;
    }

    for (uoffset_t logEntryIndex = 0; logEntryIndex < logRecord->log_entries()->size(); ++logEntryIndex)
    {
        auto logEntry = logRecord->log_entries()->Get(logEntryIndex);
        switch (logEntry->log_entry_type())
        {
        case LogEntryUnion::LoggedRowWrite:
        {
            auto loggedRowWrite = logEntry->log_entry_as<LoggedRowWrite>();

            LogExtentUsage logExtentUsage =
            {
                .LogExtentSequenceNumber = logExtentSequenceNumber,
                .IndexNumber = loggedRowWrite->index_number(),
                .PartitionNumber = loggedRowWrite->partition_number(),
            };

            m_logExtentUsage.insert(
                logExtentUsage);
            break;
        }

        case LogEntryUnion::LoggedCreateExtent:
        {
            auto loggedCreateExtent = logEntry->log_entry_as<LoggedCreateExtent>();
            FlatBuffers::ExtentNameT createdExtent;
            loggedCreateExtent->extent_name()->UnPackTo(&createdExtent);
            m_uncommittedExtentToLogExtentSequenceNumber[createdExtent] = logExtentSequenceNumber;
            break;
        }

        case LogEntryUnion::LoggedDeleteExtentPendingPartitionsUpdated:
        {
            auto loggedDeleteExtentPendingPartitionsUpdated = logEntry->log_entry_as<LoggedDeleteExtentPendingPartitionsUpdated>();
            FlatBuffers::ExtentNameT extentToDeletePendingPartitionsUpdated;
            loggedDeleteExtentPendingPartitionsUpdated->extent_name()->UnPackTo(
                &extentToDeletePendingPartitionsUpdated);

            m_partitionsPartitionNumberToExtentsToDelete.emplace(
                loggedDeleteExtentPendingPartitionsUpdated->partitions_table_partition_number(),
                extentToDeletePendingPartitionsUpdated);
            break;
        }

        case LogEntryUnion::LoggedCommitExtent:
        {
            auto loggedCommitExtent = logEntry->log_entry_as<LoggedCommitExtent>();
            FlatBuffers::ExtentNameT committedExtent;
            m_uncommittedExtentToLogExtentSequenceNumber.erase(committedExtent);
            break;
        }

        case LogEntryUnion::LoggedCreateMemoryTable:
        {
            auto loggedCreateMemoryTable = logEntry->log_entry_as<LoggedCreateMemoryTable>();
            LogExtentUsage logExtentUsage =
            {
                .LogExtentSequenceNumber = logExtentSequenceNumber,
                .IndexNumber = loggedCreateMemoryTable->index_number(),
                .PartitionNumber = loggedCreateMemoryTable->partition_number(),
            };

            m_logExtentUsage.insert(
                logExtentUsage);
            break;
        }

        case LogEntryUnion::LoggedCheckpoint:
        {
            auto loggedCheckpoint = logEntry->log_entry_as<LoggedCheckpoint>();

            auto loggedPartitionNumbers = std::set(
                loggedCheckpoint->partition_number()->cbegin(),
                loggedCheckpoint->partition_number()->cend());

            std::erase_if(
                m_logExtentUsage,
                [&](const LogExtentUsage& logExtentUsage)
            {
                return
                logExtentUsage.IndexNumber == loggedCheckpoint->index_number()
                && loggedPartitionNumbers.contains(logExtentUsage.PartitionNumber);
            });
            break;
        }

        case LogEntryUnion::LoggedPartitionsData:
        {
            auto loggedPartitionsData = logEntry->log_entry_as<LoggedPartitionsData>();
            m_partitionsDataLogExtentSequenceNumber = logExtentSequenceNumber;
            loggedPartitionsData->UnPackTo(&m_latestLoggedPartitionsData);

            if (m_logExtentSequenceNumberToLowestPartitionsDataPartitionNumber.contains(logExtentSequenceNumber))
            {
                m_logExtentSequenceNumberToLowestPartitionsDataPartitionNumber[logExtentSequenceNumber] = std::min(
                    m_logExtentSequenceNumberToLowestPartitionsDataPartitionNumber[logExtentSequenceNumber],
                    loggedPartitionsData->partitions_table_partition_number()
                );
            }
            else
            {
                m_logExtentSequenceNumberToLowestPartitionsDataPartitionNumber[logExtentSequenceNumber] =
                    loggedPartitionsData->partitions_table_partition_number();
            }

        }
        }
    }

    co_return;
}

task<task<>> LogManager::FinishReplay(
    DatabaseHeaderT* header
)
{
    co_return co_await DelayedOpenNewLogWriter(
        header);
}

bool LogManager::NeedToUpdateMaps(
    LogExtentSequenceNumber logExtentSequenceNumber,
    const LogRecord* logRecord
)
{
    for(int logEntryIndex = 0; logEntryIndex < logRecord->log_entries()->size(); logEntryIndex++)
    {
        auto logEntry = logRecord->log_entries()->Get(logEntryIndex);
        switch (logEntry->log_entry_type())
        {
        case LogEntryUnion::LoggedCreateExtent:
        case LogEntryUnion::LoggedDeleteExtentPendingPartitionsUpdated:
        case LogEntryUnion::LoggedCommitExtent:
        case LogEntryUnion::LoggedCheckpoint:
        case LogEntryUnion::LoggedCreateMemoryTable:
            return true;

        case LogEntryUnion::LoggedRowWrite:
        {
            auto loggedRowWrite = logEntry->log_entry_as<LoggedRowWrite>();

            LogExtentUsage logExtentUsage =
            {
                .LogExtentSequenceNumber = logExtentSequenceNumber,
                .IndexNumber = loggedRowWrite->index_number(),
                .PartitionNumber = loggedRowWrite->partition_number(),
            };

            if (!m_logExtentUsage.contains(logExtentUsage))
            {
                return true;
            }
        }
        }
    }

    return false;
}

task<FlatMessage<FlatBuffers::LogRecord>> LogManager::WriteLogRecord(
    const FlatMessage<FlatBuffers::LogRecord>& logRecord,
    FlushBehavior flushBehavior
)
{
    RetryWithReadLock:
    {
        Phantom::Coroutines::suspend_result suspendResult;
        auto readlock = co_await(suspendResult << m_logExtentUsageLock.reader().scoped_lock_async());
        if (suspendResult.did_suspend())
        {
            co_await *m_schedulers.IoScheduler;
        }

        if (!NeedToUpdateMaps(
            m_currentLogExtentSequenceNumber,
            logRecord.get()))
        {
            auto writeResult = co_await m_logMessageWriter->Write(
                logRecord.data(),
                flushBehavior);

            co_return writeResult;
        }
    }

    {
        auto writeLock = co_await m_logExtentUsageLock.writer().scoped_lock_async();
        co_await *m_schedulers.IoScheduler;

        if (!NeedToUpdateMaps(
            m_currentLogExtentSequenceNumber,
            logRecord.get()))
        {
            goto RetryWithReadLock;
        }

        FlatBuffers::LogExtentNameT logExtentName;
        logExtentName.log_extent_sequence_number = m_currentLogExtentSequenceNumber;

        co_await Replay(
            &logExtentName,
            logRecord.get());

        auto writeResult = co_await m_logMessageWriter->Write(
            logRecord.data(),
            flushBehavior);

        co_return writeResult;
    }
}

task<task<>> LogManager::Checkpoint(
    DatabaseHeaderT* header
)
{
    std::unordered_set<LogExtentSequenceNumber> logExtentSequenceNumbersToDelete;

    {
        auto lock = co_await m_logExtentUsageLock.writer().scoped_lock_async();

        for (auto existingLogExtentSequenceNumber : m_existingLogExtentSequenceNumbers)
        {
            logExtentSequenceNumbersToDelete.insert(
                existingLogExtentSequenceNumber);
        }

        LogExtentSequenceNumber lowestLogExtentSequenceNumberInUse = std::numeric_limits<LogExtentSequenceNumber>::max();

        for (auto& extentUsage : m_logExtentUsage)
        {
            lowestLogExtentSequenceNumberInUse = std::min(
                lowestLogExtentSequenceNumberInUse,
                extentUsage.LogExtentSequenceNumber
            );
        }

        for (auto& uncommittedExtentToLogExtentSequenceNumber : m_uncommittedExtentToLogExtentSequenceNumber)
        {
            lowestLogExtentSequenceNumberInUse = std::min(
                lowestLogExtentSequenceNumberInUse,
                uncommittedExtentToLogExtentSequenceNumber.second
            );
        }

        if (m_partitionsDataLogExtentSequenceNumber.has_value())
        {
            lowestLogExtentSequenceNumberInUse = std::min(
                lowestLogExtentSequenceNumberInUse,
                *m_partitionsDataLogExtentSequenceNumber
            );
        }

        lowestLogExtentSequenceNumberInUse = std::min(
            lowestLogExtentSequenceNumberInUse,
            m_currentLogExtentSequenceNumber
        );

        erase_if(
            logExtentSequenceNumbersToDelete,
            [lowestLogExtentSequenceNumberInUse](auto extent)
        {
            return extent >= lowestLogExtentSequenceNumberInUse;
        });

        for (auto removedExtent : logExtentSequenceNumbersToDelete)
        {
            m_existingLogExtentSequenceNumbers.erase(
                removedExtent);
            m_logExtentSequenceNumbersToRemove.insert(
                removedExtent);
        }

        co_return co_await DelayedOpenNewLogWriter(
            header);
    }
}

task<task<>> LogManager::DelayedOpenNewLogWriter(
    DatabaseHeaderT* header
)
{
    auto newExtentName = MakeLogExtentName(
        m_nextLogExtentSequenceNumber);

    // This ensures that after the header is written,
    // we will replay the new log, even though it starts out empty.
    m_existingLogExtentSequenceNumbers.insert(
        m_nextLogExtentSequenceNumber);

    header->log_replay_extent_names.clear();

    for (auto existingExtentSequenceNumber : m_existingLogExtentSequenceNumbers)
    {
        header->log_replay_extent_names.emplace_back(
            std::make_unique<FlatBuffers::LogExtentNameT>(
                FlatBuffers::LogExtentNameT
            {
                .log_extent_sequence_number = existingExtentSequenceNumber
            }));
    }

    co_return OpenNewLogWriter();
}

task<> LogManager::DeleteExtents()
{
    for (auto logExtentSequenceNumberToRemove : m_logExtentSequenceNumbersToRemove)
    {
        FlatValue extentNameToRemove = MakeLogExtentName(
            logExtentSequenceNumberToRemove);

        co_await m_logExtentStore->DeleteExtent(
            move(extentNameToRemove));

        m_logExtentSequenceNumberToLowestPartitionsDataPartitionNumber.erase(
            logExtentSequenceNumberToRemove);

        std::optional<PartitionNumber> minPartitionsDataPartitionNumber;

        for (auto lowestPartitionsDataPartitionNumber : m_logExtentSequenceNumberToLowestPartitionsDataPartitionNumber)
        {
            if (minPartitionsDataPartitionNumber)
            {
                minPartitionsDataPartitionNumber = std::min(
                    *minPartitionsDataPartitionNumber,
                    lowestPartitionsDataPartitionNumber.second);
            }
            else
            {
                minPartitionsDataPartitionNumber = lowestPartitionsDataPartitionNumber.second;
            }
        }

        while (
            minPartitionsDataPartitionNumber
            &&
            !m_partitionsPartitionNumberToExtentsToDelete.empty()
            &&
            m_partitionsPartitionNumberToExtentsToDelete.begin()->first < *minPartitionsDataPartitionNumber)
        {
            auto extentName = m_partitionsPartitionNumberToExtentsToDelete.begin()->second;
            m_partitionsPartitionNumberToExtentsToDelete.erase(
                m_partitionsPartitionNumberToExtentsToDelete.begin());
            co_await m_logExtentStore->DeleteExtent(
                FlatValue{ extentName });
        }
    }

    m_logExtentSequenceNumbersToRemove.clear();
}

task<> LogManager::OpenNewLogWriter()
{
    auto lock = co_await m_logExtentUsageLock.writer().scoped_lock_async();

    // Ensure the previous log is flushed by writing an empty message.
    if (m_logMessageWriter)
    {
        FlatBuffers::LogRecordT emptyMessage;

        co_await m_logMessageWriter->Write(
            FlatMessage<FlatBuffers::LogRecord>(&emptyMessage).data(),
            FlushBehavior::Flush);
    }

    m_currentLogExtentSequenceNumber = m_nextLogExtentSequenceNumber++;
    
    FlatValue logExtentName = MakeLogExtentName(
        m_currentLogExtentSequenceNumber);

    m_logMessageWriter = co_await m_logMessageStore->OpenExtentForSequentialWriteAccess(
        logExtentName);

    // Writing the last partitions checkpoint as the very first record
    // ensures that all replay actions will have a set of partitions
    // to start from.
    FlatBuffers::LogRecordT firstLogRecord;
    FlatBuffers::LogEntryT firstLogEntry;
    firstLogEntry.log_entry.Set(m_latestLoggedPartitionsData);
    firstLogRecord.log_entries.push_back(copy_unique(firstLogEntry));
    FlatMessage flatFirstLogRecord{ &firstLogRecord };

    FlatBuffers::LogExtentNameT fbLogExtentName;
    fbLogExtentName.log_extent_sequence_number = m_currentLogExtentSequenceNumber;

    co_await Replay(
        &fbLogExtentName,
        flatFirstLogRecord.get());

    co_await m_logMessageWriter->Write(
        flatFirstLogRecord.data(),
        FlushBehavior::Flush);

    co_await DeleteExtents();
}

}
