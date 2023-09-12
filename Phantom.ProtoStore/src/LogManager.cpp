#include "ExtentName.h"
#include "LogManager.h"
#include "ExtentStore.h"
#include "MessageStore.h"
#include "Phantom.Coroutines/suspend_result.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"
#include "Phantom.System/hash.h"

namespace Phantom::ProtoStore
{

size_t LogManager::LogExtentUsageHasher::operator() (
    const LogManager::LogExtentUsage& logExtentUsage
) const 
{
    return hash(
        logExtentUsage.IndexNumber,
        logExtentUsage.PartitionNumber,
        logExtentUsage.LogExtentSequenceNumber
    );
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

void LogManager::Replay(
    const FlatBuffers::LogExtentName* extentName,
    const LogEntry* logEntry
)
{
    auto logExtentSequenceNumber = extentName->log_extent_sequence_number();

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

    case LogEntryUnion::LoggedDeleteExtent:
    {
        auto loggedDeleteExtent = logEntry->log_entry_as<LoggedDeleteExtent>();
        FlatBuffers::ExtentNameT extentToDelete;
        loggedDeleteExtent->extent_name()->UnPackTo(
            &extentToDelete);

        m_partitionExtentsToDelete.emplace(
            std::move(extentToDelete));
        break;
    }

    case LogEntryUnion::LoggedCommitExtent:
    {
        auto loggedCommitExtent = logEntry->log_entry_as<LoggedCommitExtent>();
        FlatBuffers::ExtentNameT committedExtent;
        m_uncommittedExtentToLogExtentSequenceNumber.erase(committedExtent);
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
        break;
    }
    }
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
        case LogEntryUnion::LoggedDeleteExtent:
        case LogEntryUnion::LoggedCommitExtent:
        case LogEntryUnion::LoggedCheckpoint:
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
    DataReference<StoredMessage> writeResult;

    co_await execute_conditional_read_unlikely_write_operation(
        m_logExtentUsageLock,
        *m_schedulers.IoScheduler,
        [&](bool hasWriteLock) -> task<bool>
    {
        if (
            logRecord->log_entries()
            &&
            NeedToUpdateMaps(
                m_currentLogExtentSequenceNumber,
                logRecord.get()
            ))
        {
            if (!hasWriteLock)
            {
                co_return false;
            }

            for (const auto* logEntry : *logRecord->log_entries())
            {
                Replay(
                    m_currentLogExtentName.get(),
                    logEntry);
            }
        }

        writeResult = co_await m_logMessageWriter->Write(
            logRecord.data(),
            flushBehavior);

        co_return true;
    });

    co_return writeResult;
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
    // We want to remove partition extents before removing the log extents
    // that might ask for their deletion.
    for (auto& partitionExtentToDelete : m_partitionExtentsToDelete)
    {
        co_await m_logExtentStore->DeleteExtent(
            FlatValue{ partitionExtentToDelete });
    }
    m_partitionExtentsToDelete.clear();

    for (auto logExtentSequenceNumberToRemove : m_logExtentSequenceNumbersToRemove)
    {
        FlatValue extentNameToRemove = MakeLogExtentName(
            logExtentSequenceNumberToRemove);

        co_await m_logExtentStore->DeleteExtent(
            move(extentNameToRemove));

        m_logExtentSequenceNumberToLowestPartitionsDataPartitionNumber.erase(
            logExtentSequenceNumberToRemove);
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
    m_currentLogExtentName = logExtentName.SubValue(
        logExtentName->extent_name_as_LogExtentName()
    );

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

    Replay(
        m_currentLogExtentName,
        flatFirstLogRecord->log_entries()->Get(0));

    co_await m_logMessageWriter->Write(
        flatFirstLogRecord.data(),
        FlushBehavior::Flush);

    co_await DeleteExtents();
}

}
