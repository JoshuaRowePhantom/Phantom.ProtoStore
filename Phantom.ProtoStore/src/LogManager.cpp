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
        + (logExtentUsage.LogExtentName.logextentsequencenumber() << 17)
        ^ (logExtentUsage.LogExtentName.logextentsequencenumber() >> 15);
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
    m_existingExtents(
        header.logreplayextentnames().cbegin(),
        header.logreplayextentnames().cend())
{
    for (auto existingExtent : m_existingExtents)
    {
        m_nextLogExtentSequenceNumber = std::max(
            m_nextLogExtentSequenceNumber,
            existingExtent.logextentname().logextentsequencenumber() + 1);
    }
}

task<> LogManager::Replay(
    ExtentName extentName,
    const LogRecord& logRecord
)
{
    LogExtentName logExtentName = extentName.logextentname();

    for (const auto& rowRecord : logRecord.rows())
    {
        LogExtentUsage logExtentUsage =
        {
            .LogExtentName = logExtentName,
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
            m_uncommitedExtentToLogExtent[loggedAction.loggedcreateextent().extentname()] = logExtentName;
        }
        if (loggedAction.has_loggeddeleteextent())
        {
            m_uncommitedExtentToLogExtent.erase(loggedAction.loggeddeleteextent().extentname());
        }
        if (loggedAction.has_loggedcommitextent())
        {
            m_uncommitedExtentToLogExtent.erase(loggedAction.loggedcommitextent().extentname());
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
            m_partitionsDataLogExtentName = logExtentName;
            m_latestLoggedPartitionsData = loggedAction.loggedpartitionsdata();
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
    ExtentName logExtentName,
    const LogRecord& logRecord
)
{
    for (const auto& rowRecord : logRecord.rows())
    {
        LogExtentUsage logExtentUsage =
        {
            .LogExtentName = logExtentName,
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
        if (loggedAction.has_loggeddeleteextent())
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

task<WriteMessageResult> LogManager::WriteLogRecord(
    const LogRecord& logRecord
)
{
    while (true)
    {
        {
            auto readLock = m_logExtentUsageLock.reader().scoped_try_lock();
            if (!readLock)
            {
                readLock = co_await m_logExtentUsageLock.reader().scoped_lock_async();
                co_await *m_schedulers.IoScheduler;
            }

            if (!NeedToUpdateMaps(
                m_currentLogExtentNumber,
                logRecord))
            {
                auto writeResult = co_await m_logMessageWriter->Write(
                    logRecord,
                    FlushBehavior::Flush);

                co_return writeResult;
            }
        }

        // Most of the time, the reason the write lock can't be acquired
        // is because someone else is adding the same entry,
        // so just go back to the read path if we don't think we'll be able to acquire the lock.
        if (!m_logExtentUsageLock.writer().has_owner()
            &&
            !m_logExtentUsageLock.writer().has_waiter()
            )
        {
            auto writeLock = m_logExtentUsageLock.writer().scoped_try_lock();

            co_await Replay(
                m_currentLogExtentNumber,
                logRecord);

            auto writeResult = co_await m_logMessageWriter->Write(
                logRecord,
                FlushBehavior::Flush);

            co_return writeResult;
        }
    }
}

task<task<>> LogManager::Checkpoint(
    Header& header
)
{
    std::unordered_set<ExtentName> extentNamesToDelete;

    {
        auto lock = co_await m_logExtentUsageLock.writer().scoped_lock_async();

        for (auto extent : m_existingExtents)
        {
            extentNamesToDelete.insert(
                extent);
        }

        LogExtentSequenceNumber lowestLogExtentSequenceNumberInUse = std::numeric_limits<LogExtentSequenceNumber>::max();

        for (auto& extentUsage : m_logExtentUsage)
        {
            lowestLogExtentSequenceNumberInUse = std::min(
                lowestLogExtentSequenceNumberInUse,
                extentUsage.LogExtentName.logextentname().logextentsequencenumber()
            );
        }

        for (auto& uncommittedDataExtent : m_uncommitedExtentToLogExtent)
        {
            lowestLogExtentSequenceNumberInUse = std::min(
                lowestLogExtentSequenceNumberInUse,
                uncommittedDataExtent.second.logextentsequencenumber()
            );
        }

        if (m_partitionsData.has_value())
        {
            lowestLogExtentSequenceNumberInUse = std::min(
                lowestLogExtentSequenceNumberInUse,
                m_partitionsData->partitionsDataLogExtentName.logextentsequencenumber()
            );
        }

        lowestLogExtentSequenceNumberInUse = std::min(
            lowestLogExtentSequenceNumberInUse,
            m_currentLogExtentSequenceNumber
        );

        erase_if(
            extentsToDelete,
            [lowestLogExtentSequenceNumberInUse](auto extent)
        {
            return extent >= lowestLogExtentSequenceNumberInUse;
        });

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
    ExtentName newExtentName;
    newExtentName.mutable_logextentname()->set_logextentsequencenumber(
        m_nextLogExtentSequenceNumber);

    m_existingExtents.push_back(
        newExtentName);

    header.mutable_logreplayextentnames()->Clear();

    for (auto existingExtent : m_existingExtents)
    {
        *header.add_logreplayextentnames() = existingExtent;
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

    m_currentLogExtentSequenceNumber = m_nextLogExtentSequenceNumber++;
    
    ExtentName logExtentName;
    logExtentName.mutable_logextentname()->set_logextentsequencenumber(
        m_currentLogExtentSequenceNumber);
    m_logMessageWriter = co_await m_logMessageStore->OpenExtentForSequentialWriteAccess(
        logExtentName);

    // Writing the last partitions checkpoint as the very first record
    // ensures that all replay actions will have a set of partitions
    // to start from.
    LogRecord lastPartitionsCheckpointLogRecord;
    lastPartitionsCheckpointLogRecord.mutable_extras()->add_loggedactions()->mutable_loggedpartitionsdata()->CopyFrom(
        m_latestLoggedPartitionsData);

    co_await Replay(
        logExtentName,
        lastPartitionsCheckpointLogRecord);

    co_await m_logMessageWriter->Write(
        lastPartitionsCheckpointLogRecord,
        FlushBehavior::Flush);
}

}
