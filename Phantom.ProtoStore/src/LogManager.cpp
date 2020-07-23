#include "LogManager.h"
#include "ExtentStore.h"
#include "MessageStore.h"

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
        + (logExtentUsage.LogExtentNumber << 17)
        ^ (logExtentUsage.LogExtentNumber >> 15);
}

LogManager::LogManager(
    shared_ptr<IExtentStore> logExtentStore,
    shared_ptr<IMessageStore> logMessageStore,
    const Header& header
)
    :
    m_logExtentStore(logExtentStore),
    m_logMessageStore(logMessageStore),
    m_existingExtents(
        header.logreplayextentnumbers().cbegin(),
        header.logreplayextentnumbers().cend())
{
    for (auto existingExtent : m_existingExtents)
    {
        m_nextLogExtentNumber = std::max(
            m_nextLogExtentNumber,
            existingExtent + 1);
    }
}

task<> LogManager::Replay(
    ExtentNumber logExtentNumber,
    const LogRecord& logRecord
)
{
    for (const auto& rowRecord : logRecord.rows())
    {
        LogExtentUsage logExtentUsage =
        {
            .LogExtentNumber = logExtentNumber,
            .IndexNumber = rowRecord.indexnumber(),
            .CheckpointNumber = rowRecord.checkpointnumber(),
        };

        m_logExtentUsage.insert(
            logExtentUsage);
    }

    for (const auto& loggedAction : logRecord.extras().loggedactions())
    {
        if (loggedAction.has_loggedcreatedataextents())
        {
            m_uncommitedDataExtentNumberToLogExtentNumber[loggedAction.loggedcreatedataextents().extentnumber()] = logExtentNumber;
        }
        if (loggedAction.has_loggeddeletedataextents())
        {
            m_uncommitedDataExtentNumberToLogExtentNumber.erase(loggedAction.loggeddeletedataextents().extentnumber());
        }
        if (loggedAction.has_loggedcommitdataextents())
        {
            m_uncommitedDataExtentNumberToLogExtentNumber.erase(loggedAction.loggedcommitdataextents().extentnumber());
        }
        if (loggedAction.has_loggedcheckpoints())
        {
            auto loggedCheckpointNumbers = std::set(
                loggedAction.loggedcheckpoints().checkpointnumber().cbegin(),
                loggedAction.loggedcheckpoints().checkpointnumber().cend());

            std::erase_if(
                m_logExtentUsage,
                [&](const LogExtentUsage& logExtentUsage)
            {
                return
                    logExtentUsage.IndexNumber == loggedAction.loggedcheckpoints().indexnumber()
                    && loggedCheckpointNumbers.contains(logExtentUsage.CheckpointNumber);
            });
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
    ExtentNumber logExtentNumber,
    const LogRecord& logRecord
)
{
    for (const auto& rowRecord : logRecord.rows())
    {
        LogExtentUsage logExtentUsage =
        {
            .LogExtentNumber = logExtentNumber,
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
        if (loggedAction.has_loggedcreatedataextents())
        {
            return true;
        }
        if (loggedAction.has_loggeddeletedataextents())
        {
            return true;
        }
        if (loggedAction.has_loggedcommitdataextents())
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

task<> LogManager::WriteLogRecord(
    const LogRecord& logRecord
)
{
    {
        auto readLock = co_await m_logExtentUsageLock.reader().scoped_lock_async();
        if (!NeedToUpdateMaps(
            m_currentLogExtentNumber,
            logRecord))
        {
            co_await m_logMessageWriter->Write(
                logRecord,
                FlushBehavior::Flush);
        }
    }

    {
        auto writeLock = co_await m_logExtentUsageLock.writer().scoped_lock_async();
        
        co_await Replay(
            m_currentLogExtentNumber,
            logRecord);

        co_await m_logMessageWriter->Write(
            logRecord,
            FlushBehavior::Flush);
    }
}

task<task<>> LogManager::Checkpoint(
    Header& header
)
{
    std::unordered_set<ExtentNumber> extentsToDelete;

    {
        auto lock = co_await m_logExtentUsageLock.writer().scoped_lock_async();

        for (auto& extentUsage : m_logExtentUsage)
        {
            extentsToDelete.erase(
                extentUsage.LogExtentNumber);
        }

        for (auto& uncommittedDataExtent : m_uncommitedDataExtentNumberToLogExtentNumber)
        {
            extentsToDelete.erase(
                uncommittedDataExtent.second);
        }

        extentsToDelete.erase(
            m_currentLogExtentNumber);

        for (auto removedExtent : extentsToDelete)
        {
            m_existingExtents.erase(
                removedExtent);
            m_extentsToRemove.insert(
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
    m_existingExtents.insert(
        m_nextLogExtentNumber);

    header.mutable_logreplayextentnumbers()->Clear();

    for (auto existingExtent : m_existingExtents)
    {
        header.mutable_logreplayextentnumbers()->Add(
            existingExtent);
    }

    co_return OpenNewLogWriter();
}

task<> LogManager::OpenNewLogWriter()
{
    auto lock = co_await m_logExtentUsageLock.writer().scoped_lock_async();

    for (auto removedExtent : m_extentsToRemove)
    {
        co_await m_logExtentStore->DeleteExtent(
            removedExtent);
    }

    m_extentsToRemove.clear();

    m_currentLogExtentNumber = m_nextLogExtentNumber++;
    m_logMessageWriter = co_await m_logMessageStore->OpenExtentForSequentialWriteAccess(
        m_currentLogExtentNumber);
}

}
