#include "ExtentName.h"
#include "LogManager.h"
#include "ExtentStore.h"
#include "MessageStore.h"
#include "Phantom.Coroutines/suspend_result.h"

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
    const Header& header
)
    :
    m_schedulers(schedulers),
    m_logExtentStore(logExtentStore),
    m_logMessageStore(logMessageStore),
    m_nextLogExtentSequenceNumber(0),
    m_currentLogExtentSequenceNumber(0)
{
    for (auto& logReplayExtentName : header.logreplayextentnames())
    {
        auto logExtentSequenceNumber = logReplayExtentName.logextentname().logextentsequencenumber();

        m_existingLogExtentSequenceNumbers.insert(
            logExtentSequenceNumber);

        m_nextLogExtentSequenceNumber = std::max(
            m_nextLogExtentSequenceNumber,
            logExtentSequenceNumber + 1);
    }
}

task<> LogManager::Replay(
    ExtentName extentName,
    const LogRecord& logRecord
)
{
    auto logExtentSequenceNumber = extentName.logextentname().logextentsequencenumber();

    for (const auto& rowRecord : logRecord.rows())
    {
        LogExtentUsage logExtentUsage =
        {
            .LogExtentSequenceNumber = logExtentSequenceNumber,
            .IndexNumber = rowRecord.indexnumber(),
            .CheckpointNumber = rowRecord.checkpointnumber(),
        };

        m_logExtentUsage.insert(
            logExtentUsage);
    }

    for (const auto& loggedAction : logRecord.extras().loggedactions())
    {
        if (loggedAction.has_loggedcreateextent())
        {
            m_uncommittedExtentToLogExtentSequenceNumber[loggedAction.loggedcreateextent().extentname()] = logExtentSequenceNumber;
        }
        if (loggedAction.has_loggeddeleteextentpendingpartitionsupdated())
        {
            m_partitionsCheckpointNumberToExtentsToDelete.emplace(
                loggedAction.loggeddeleteextentpendingpartitionsupdated().partitionstablecheckpointnumber(),
                loggedAction.loggeddeleteextentpendingpartitionsupdated().extentname());
        }
        if (loggedAction.has_loggedcommitextent())
        {
            m_uncommittedExtentToLogExtentSequenceNumber.erase(loggedAction.loggedcommitextent().extentname());
        }
        if (loggedAction.has_loggedcheckpoints())
        {
            auto& loggedCheckpoint = loggedAction.loggedcheckpoints();
            auto& logExtentUsage = m_logExtentUsage;

            auto loggedCheckpointNumbers = std::set(
                loggedCheckpoint.checkpointnumber().cbegin(),
                loggedCheckpoint.checkpointnumber().cend());

            std::erase_if(
                m_logExtentUsage,
                [&](const LogExtentUsage& logExtentUsage)
            {
                return
                    logExtentUsage.IndexNumber == loggedCheckpoint.indexnumber()
                    && loggedCheckpointNumbers.contains(logExtentUsage.CheckpointNumber);
            });
        }
        if (loggedAction.has_loggedpartitionsdata())
        {
            m_partitionsDataLogExtentSequenceNumber = logExtentSequenceNumber;
            m_latestLoggedPartitionsData = loggedAction.loggedpartitionsdata();

            if (m_logExtentSequenceNumberToLowestPartitionsDataCheckpointNumber.contains(logExtentSequenceNumber))
            {
                m_logExtentSequenceNumberToLowestPartitionsDataCheckpointNumber[logExtentSequenceNumber] = std::min(
                    m_logExtentSequenceNumberToLowestPartitionsDataCheckpointNumber[logExtentSequenceNumber],
                    loggedAction.loggedpartitionsdata().partitionstablecheckpointnumber()
                );
            }
            else
            {
                m_logExtentSequenceNumberToLowestPartitionsDataCheckpointNumber[logExtentSequenceNumber] =
                    loggedAction.loggedpartitionsdata().partitionstablecheckpointnumber();
            }
        }
    }

    co_return;
}

task<task<>> LogManager::FinishReplay(
    Header& header
)
{
    co_return co_await DelayedOpenNewLogWriter(
        header);
}

bool LogManager::NeedToUpdateMaps(
    LogExtentSequenceNumber logExtentSequenceNumber,
    const LogRecord& logRecord
)
{
    for (const auto& rowRecord : logRecord.rows())
    {
        LogExtentUsage logExtentUsage =
        {
            .LogExtentSequenceNumber = logExtentSequenceNumber,
            .IndexNumber = rowRecord.indexnumber(),
            .CheckpointNumber = rowRecord.checkpointnumber(),
        };

        if (!m_logExtentUsage.contains(logExtentUsage))
        {
            return true;
        }
    }

    for (const auto& loggedAction : logRecord.extras().loggedactions())
    {
        if (loggedAction.has_loggedcreateextent())
        {
            return true;
        }
        if (loggedAction.has_loggeddeleteextentpendingpartitionsupdated())
        {
            return true;
        }
        if (loggedAction.has_loggedcommitextent())
        {
            return true;
        }
        if (loggedAction.has_loggedcheckpoints())
        {
            return true;
        }
    }

    return false;
}

task<DataReference<StoredMessage>> LogManager::WriteLogRecord(
    const LogRecord& logRecord
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
            logRecord))
        {
            auto writeResult = co_await m_logMessageWriter->Write(
                logRecord,
                FlushBehavior::Flush);

            co_return writeResult;
        }
    }

    {
        auto writeLock = co_await m_logExtentUsageLock.writer().scoped_lock_async();
        co_await *m_schedulers.IoScheduler;

        if (!NeedToUpdateMaps(
            m_currentLogExtentSequenceNumber,
            logRecord))
        {
            goto RetryWithReadLock;
        }

        auto extentName = MakeLogExtentName(
            m_currentLogExtentSequenceNumber);

        co_await Replay(
            extentName,
            logRecord);

        auto writeResult = co_await m_logMessageWriter->Write(
            logRecord,
            FlushBehavior::Flush);

        co_return writeResult;
    }
}

task<task<>> LogManager::Checkpoint(
    Header& header
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
    Header& header
)
{
    auto newExtentName = MakeLogExtentName(
        m_nextLogExtentSequenceNumber);

    // This ensures that after the header is written,
    // we will replay the new log, even though it starts out empty.
    m_existingLogExtentSequenceNumbers.insert(
        m_nextLogExtentSequenceNumber);

    header.mutable_logreplayextentnames()->Clear();

    for (auto existingExtentSequenceNumber : m_existingLogExtentSequenceNumbers)
    {
        ExtentName extentName;
        extentName.mutable_logextentname()->set_logextentsequencenumber(
            existingExtentSequenceNumber);
        *header.add_logreplayextentnames() = move(extentName);
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
                extentName);
        }
    }

    m_logExtentSequenceNumbersToRemove.clear();
}

task<> LogManager::OpenNewLogWriter()
{
    auto lock = co_await m_logExtentUsageLock.writer().scoped_lock_async();

    m_currentLogExtentSequenceNumber = m_nextLogExtentSequenceNumber++;
    
    auto logExtentName = MakeLogExtentName(
        m_currentLogExtentSequenceNumber);

    m_logMessageWriter = co_await m_logMessageStore->OpenExtentForSequentialWriteAccess(
        logExtentName);

    // Writing the last partitions checkpoint as the very first record
    // ensures that all replay actions will have a set of partitions
    // to start from.
    LogRecord firstLogRecord;
    firstLogRecord.mutable_extras()->add_loggedactions()->mutable_loggedpartitionsdata()->CopyFrom(
        m_latestLoggedPartitionsData);

    co_await Replay(
        logExtentName,
        firstLogRecord);

    co_await m_logMessageWriter->Write(
        firstLogRecord,
        FlushBehavior::Flush);

    co_await DeleteExtents();
}

}
