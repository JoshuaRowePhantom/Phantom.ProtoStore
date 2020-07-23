#pragma once

#include "StandardTypes.h"
#include "src/ProtoStoreInternal.pb.h"
#include "Phantom.System/async_reader_writer_lock.h"

namespace Phantom::ProtoStore
{

class LogManager
{
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

    task<> WriteLogRecord(
        const LogRecord& logRecord
    );

    task<task<>> Checkpoint(
        Header& header
    );
};
}