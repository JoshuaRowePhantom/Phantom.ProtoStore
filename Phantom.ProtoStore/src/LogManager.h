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
        ExtentNumber LogExtentNumber;
        IndexNumber IndexNumber;
        CheckpointNumber CheckpointNumber;

        auto operator <=>(const LogExtentUsage&) const = default;
    };

    struct LogExtentUsageHasher
    {
        size_t operator()(const LogExtentUsage& value) const;
    };

    async_reader_writer_lock m_logExtentUsageLock;
    std::set<ExtentNumber> m_existingExtents;
    std::set<ExtentNumber> m_extentsToRemove;
    std::unordered_set<LogExtentUsage, LogExtentUsageHasher> m_logExtentUsage;
    std::unordered_map<ExtentNumber, ExtentNumber> m_uncommitedDataExtentNumberToLogExtentNumber;
    optional<ExtentNumber> m_partitionsDataExtentNumber;
    LoggedPartitionsData m_latestLoggedPartitionsData;

    shared_ptr<ISequentialMessageWriter> m_logMessageWriter;
    ExtentNumber m_currentLogExtentNumber;

    ExtentNumber m_nextLogExtentNumber;

    bool NeedToUpdateMaps(
        ExtentNumber logExtentNumber,
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
        ExtentNumber logExtentNumber,
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