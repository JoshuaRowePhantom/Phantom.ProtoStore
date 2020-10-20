#pragma once

#include "StandardTypes.h"
#include "src/ProtoStoreInternal.pb.h"
#include "Phantom.System/async_reader_writer_lock.h"
#include "MessageStore.h"

namespace Phantom::ProtoStore
{

class LogManager
{
    Schedulers m_schedulers;
    shared_ptr<IExtentStore> m_logExtentStore;
    shared_ptr<IMessageStore> m_logMessageStore;

    struct LogExtentUsage
    {
        LogExtentSequenceNumber LogExtentSequenceNumber;
        IndexNumber IndexNumber;
        CheckpointNumber CheckpointNumber;

        auto operator <=>(const LogExtentUsage&) const = default;
    };

    struct LogExtentUsageHasher
    {
        size_t operator()(const LogExtentUsage& value) const;
    };

    async_reader_writer_lock m_logExtentUsageLock;
    std::set<LogExtentSequenceNumber> m_existingLogExtentSequenceNumbers;
    std::set<LogExtentSequenceNumber> m_logExtentSequenceNumbersToRemove;
    std::unordered_set<LogExtentUsage, LogExtentUsageHasher> m_logExtentUsage;
    std::unordered_map<ExtentName, LogExtentSequenceNumber> m_uncommitedExtentToLogExtentSequenceNumber;
    optional<LogExtentSequenceNumber> m_partitionsDataLogExtentSequenceNumber;
    LoggedPartitionsData m_latestLoggedPartitionsData;

    shared_ptr<ISequentialMessageWriter> m_logMessageWriter;
    LogExtentSequenceNumber m_currentLogExtentSequenceNumber;

    LogExtentSequenceNumber m_nextLogExtentSequenceNumber;

    bool NeedToUpdateMaps(
        LogExtentSequenceNumber logExtentName,
        const LogRecord& logRecord
    );

    task<task<>> DelayedOpenNewLogWriter(
        Header& header);

    task<> OpenNewLogWriter();

public:
    LogManager(
        Schedulers schedulers,
        shared_ptr<IExtentStore> logExtentStore,
        shared_ptr<IMessageStore> logMessageStore,
        const Header& header
    );

    task<> Replay(
        ExtentName logExtentName,
        const LogRecord& logRecord
    );

    task<task<>> FinishReplay(
        Header& header);

    task<WriteMessageResult> WriteLogRecord(
        const LogRecord& logRecord
    );

    task<task<>> Checkpoint(
        Header& header
    );
};
}