#include "ExtentName.h"
#include "LogManager.h"
#include "ExtentStore.h"
#include "MessageStore.h"
#include "Phantom.Coroutines/suspend_result.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"
#include "Phantom.System/hash.h"

namespace Phantom::ProtoStore
{

LogManager::LogManager(
    Schedulers schedulers,
    shared_ptr<IExtentStore> logExtentStore,
    shared_ptr<IMessageStore> logMessageStore
)
    :
    m_schedulers(schedulers),
    m_logExtentStore(logExtentStore),
    m_logMessageStore(logMessageStore),
    m_nextLogExtentSequenceNumber(0)
{
}

task<> LogManager::ReplayLogExtentUsageMap(
    LogExtentUsageMap logExtentUsageMap
)
{
    m_logExtentUsageMap = std::move(logExtentUsageMap);
    
    for (auto logExtentSequenceNumber : logExtentUsageMap.GetExtentsToReplay())
    {
        m_nextLogExtentSequenceNumber.ReplayUsedSequenceNumber(
            logExtentSequenceNumber);
    }
    
    for (auto logExtentSequenceNumber : logExtentUsageMap.GetExtentsToDelete())
    {
        m_nextLogExtentSequenceNumber.ReplayUsedSequenceNumber(
            logExtentSequenceNumber);
    }

    co_return;
}

task<FlatMessage<FlatBuffers::LogRecord>> LogManager::WriteLogRecord(
    const FlatMessage<FlatBuffers::LogRecord>& logRecord,
    FlushBehavior flushBehavior
)
{
    auto lock = co_await m_logLock.reader().scoped_lock_async();

    for (auto logEntry : *logRecord->log_entries())
    {
        m_logExtentUsageMap.HandleLogEntry(
            m_nextLogExtentSequenceNumber.GetCurrentSequenceNumber(),
            logEntry
        );
    }

    co_return co_await m_logMessageWriter->Write(
        logRecord.data(),
        flushBehavior);
}

task<> LogManager::Checkpoint(
    DatabaseHeaderT* header
)
{
    auto extentsToReplay = m_logExtentUsageMap.GetExtentsToReplay();

    extentsToReplay.push_back(m_nextLogExtentSequenceNumber.GetCurrentSequenceNumber());

    header->log_replay_extent_names.clear();
    for (auto logExtentSequenceNumber : extentsToReplay)
    {
        FlatBuffers::LogExtentNameT logExtentNameT;
        logExtentNameT.log_extent_sequence_number = logExtentSequenceNumber;
        header->log_replay_extent_names.push_back(
            copy_unique(std::move(logExtentNameT)));
    }

    co_return;
}

task<> LogManager::OpenNewLogWriter()
{
    auto lock = co_await m_logLock.writer().scoped_lock_async();

    auto nextLogExtentSequenceNumber = m_nextLogExtentSequenceNumber.AllocateNextSequenceNumber();

    // Ensure the previous log is flushed by flushing a new message
    // with the new log extent name.
    if (m_logMessageWriter)
    {
        FlatBuffers::LogRecordT newLogRecordT;
        FlatBuffers::LogEntryT newLogEntryT;
        FlatBuffers::LoggedNewLogExtentT loggedNewLogExtentT;
        FlatBuffers::LogExtentNameT newLogExtentNameT;
        newLogExtentNameT.log_extent_sequence_number = nextLogExtentSequenceNumber;
        loggedNewLogExtentT.new_log_extent_name = copy_unique(std::move(newLogExtentNameT));
        newLogEntryT.log_entry.Set(loggedNewLogExtentT);
        newLogRecordT.log_entries.push_back(
            copy_unique(std::move(newLogEntryT)));

        co_await m_logMessageWriter->Write(
            FlatMessage{ newLogRecordT }.data(),
            FlushBehavior::Flush);
    }

    auto extentName = FlatMessage
    {
        MakeLogExtentName(
            nextLogExtentSequenceNumber)
    };

    m_logMessageWriter = co_await m_logMessageStore->OpenExtentForSequentialWriteAccess(
        extentName.get()
    );

    m_logExtentUsageMap.HandleNewLogExtent(
        nextLogExtentSequenceNumber);
}

}
