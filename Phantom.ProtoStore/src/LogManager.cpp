#include "ExtentName.h"
#include "LogManager.h"
#include "ExtentStore.h"
#include "MessageStore.h"
#include "Phantom.Coroutines/suspend_result.h"
#include "src/ProtoStoreInternal_generated.h"

namespace Phantom::ProtoStore
{

size_t LogManager::LogExtentUsageHasher::operator() (
    const LogManager::LogExtentUsage& logExtentUsage
) const 
{
    return
        (logExtentUsage.IndexNumber << 7)
        ^ (logExtentUsage.IndexNumber >> 25)
        + (logExtentUsage.CheckpointNumber << 3)
        ^ (logExtentUsage.CheckpointNumber >> 29)
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

    if (!logRecord->log_entry())
    {
        co_return;
    }

    for (uoffset_t logEntryIndex = 0; logEntryIndex < logRecord->log_entry()->size(); ++logEntryIndex)
    {
        switch (logRecord->log_entry_type()->Get(logEntryIndex))
        {
        case LogEntry::LoggedRowWrite:
        {
            auto loggedRowWrite = logRecord->log_entry()->GetAs<LoggedRowWrite>(logEntryIndex);

            LogExtentUsage logExtentUsage =
            {
                .LogExtentSequenceNumber = logExtentSequenceNumber,
                .IndexNumber = loggedRowWrite->index_number(),
                .CheckpointNumber = loggedRowWrite->checkpoint_number(),
            };

            m_logExtentUsage.insert(
                logExtentUsage);
            break;
        }

        case LogEntry::LoggedCreateExtent:
        {
            auto loggedCreateExtent = logRecord->log_entry()->GetAs<LoggedCreateExtent>(logEntryIndex);
            FlatBuffers::ExtentNameT createdExtent;
            loggedCreateExtent->extent_name()->UnPackTo(&createdExtent);
            m_uncommittedExtentToLogExtentSequenceNumber[createdExtent] = logExtentSequenceNumber;
            break;
        }

        case LogEntry::LoggedDeleteExtentPendingPartitionsUpdated:
        {
            auto loggedDeleteExtentPendingPartitionsUpdated = logRecord->log_entry()->GetAs<LoggedDeleteExtentPendingPartitionsUpdated>(logEntryIndex);
            FlatBuffers::ExtentNameT extentToDeletePendingPartitionsUpdated;
            loggedDeleteExtentPendingPartitionsUpdated->extent_name()->UnPackTo(
                &extentToDeletePendingPartitionsUpdated);

            m_partitionsCheckpointNumberToExtentsToDelete.emplace(
                loggedDeleteExtentPendingPartitionsUpdated->partitions_table_checkpoint_number(),
                extentToDeletePendingPartitionsUpdated);
            break;
        }

        case LogEntry::LoggedCommitExtent:
        {
            auto loggedCommitExtent = logRecord->log_entry()->GetAs<LoggedCommitExtent>(logEntryIndex);
            FlatBuffers::ExtentNameT committedExtent;
            m_uncommittedExtentToLogExtentSequenceNumber.erase(committedExtent);
            break;
        }

        case LogEntry::LoggedCheckpoint:
        {
            auto loggedCheckpoint = logRecord->log_entry()->GetAs<LoggedCheckpoint>(logEntryIndex);

            auto loggedCheckpointNumbers = std::set(
                loggedCheckpoint->checkpoint_number()->cbegin(),
                loggedCheckpoint->checkpoint_number()->cend());

            std::erase_if(
                m_logExtentUsage,
                [&](const LogExtentUsage& logExtentUsage)
            {
                return
                logExtentUsage.IndexNumber == loggedCheckpoint->index_number()
                && loggedCheckpointNumbers.contains(logExtentUsage.CheckpointNumber);
            });
            break;
        }

        case LogEntry::LoggedPartitionsData:
        {
            auto loggedPartitionsData = logRecord->log_entry()->GetAs<LoggedPartitionsData>(logEntryIndex);
            m_partitionsDataLogExtentSequenceNumber = logExtentSequenceNumber;
            loggedPartitionsData->UnPackTo(&m_latestLoggedPartitionsData);

            if (m_logExtentSequenceNumberToLowestPartitionsDataCheckpointNumber.contains(logExtentSequenceNumber))
            {
                m_logExtentSequenceNumberToLowestPartitionsDataCheckpointNumber[logExtentSequenceNumber] = std::min(
                    m_logExtentSequenceNumberToLowestPartitionsDataCheckpointNumber[logExtentSequenceNumber],
                    loggedPartitionsData->partitions_table_checkpoint_number()
                );
            }
            else
            {
                m_logExtentSequenceNumberToLowestPartitionsDataCheckpointNumber[logExtentSequenceNumber] =
                    loggedPartitionsData->partitions_table_checkpoint_number();
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
    for(int logEntryIndex = 0; logEntryIndex < logRecord->log_entry()->size(); logEntryIndex++)
    {
        switch (logRecord->log_entry_type()->Get(logEntryIndex))
        {
        case LogEntry::LoggedCreateExtent:
        case LogEntry::LoggedDeleteExtentPendingPartitionsUpdated:
        case LogEntry::LoggedCommitExtent:
        case LogEntry::LoggedCheckpoint:
            return true;

        case LogEntry::LoggedRowWrite:
        {
            auto loggedRowWrite = logRecord->log_entry()->GetAs<LoggedRowWrite>(logEntryIndex);

            LogExtentUsage logExtentUsage =
            {
                .LogExtentSequenceNumber = logExtentSequenceNumber,
                .IndexNumber = loggedRowWrite->index_number(),
                .CheckpointNumber = loggedRowWrite->checkpoint_number(),
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
        auto extentNameToRemove = MakeLogExtentName(
            logExtentSequenceNumberToRemove);

        co_await m_logExtentStore->DeleteExtent(
            move(extentNameToRemove));

        m_logExtentSequenceNumberToLowestPartitionsDataCheckpointNumber.erase(
            logExtentSequenceNumberToRemove);

        std::optional<CheckpointNumber> minPartitionsDataCheckpointNumber;

        for (auto lowestPartitionsDataCheckpointNumber : m_logExtentSequenceNumberToLowestPartitionsDataCheckpointNumber)
        {
            if (minPartitionsDataCheckpointNumber)
            {
                minPartitionsDataCheckpointNumber = std::min(
                    *minPartitionsDataCheckpointNumber,
                    lowestPartitionsDataCheckpointNumber.second);
            }
            else
            {
                minPartitionsDataCheckpointNumber = lowestPartitionsDataCheckpointNumber.second;
            }
        }

        while (
            minPartitionsDataCheckpointNumber
            &&
            !m_partitionsCheckpointNumberToExtentsToDelete.empty()
            &&
            m_partitionsCheckpointNumberToExtentsToDelete.begin()->first < *minPartitionsDataCheckpointNumber)
        {
            auto extentName = m_partitionsCheckpointNumberToExtentsToDelete.begin()->second;
            m_partitionsCheckpointNumberToExtentsToDelete.erase(
                m_partitionsCheckpointNumberToExtentsToDelete.begin());
            co_await m_logExtentStore->DeleteExtent(
                MakeExtentName(extentName));
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
    
    auto logExtentName = MakeLogExtentName(
        m_currentLogExtentSequenceNumber);

    m_logMessageWriter = co_await m_logMessageStore->OpenExtentForSequentialWriteAccess(
        logExtentName);

    // Writing the last partitions checkpoint as the very first record
    // ensures that all replay actions will have a set of partitions
    // to start from.
    FlatBuffers::LogRecordT firstLogRecord;
    FlatBuffers::LogEntryUnion firstLogEntry;
    firstLogEntry.Set(m_latestLoggedPartitionsData);
    firstLogRecord.log_entry.push_back(
        std::move(firstLogEntry)
    );
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
