#pragma once

#include "StandardTypes.h"
#include "ProtoStoreInternal.pb.h"
#include "Phantom.System/async_reader_writer_lock.h"
#include "MessageStore.h"

namespace Phantom::ProtoStore
{

class LogManager : SerializationTypes
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
    std::unordered_map<ExtentName, LogExtentSequenceNumber> m_uncommittedExtentToLogExtentSequenceNumber;
    // For each log extent, maps the log extent sequence number to the lowest partitions data checkpoint number
    // referenced by the log extent.
    std::unordered_map<LogExtentSequenceNumber, CheckpointNumber> m_logExtentSequenceNumberToLowestPartitionsDataCheckpointNumber;
    // For each partitions table checkpoint number, records the extents to delete when
    // that checkpoint number becomes the lowest available.
    std::multimap<CheckpointNumber, ExtentName> m_partitionsCheckpointNumberToExtentsToDelete;
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
        DatabaseHeaderT* header);

    task<> DeleteExtents();
    task<> OpenNewLogWriter();

public:
    LogManager(
        Schedulers schedulers,
        shared_ptr<IExtentStore> logExtentStore,
        shared_ptr<IMessageStore> logMessageStore,
        const DatabaseHeaderT* header
    );

    task<> Replay(
        const LogExtentNameT* logExtentName,
        const LogRecord& logRecord
    );

    task<task<>> FinishReplay(
        DatabaseHeaderT* header);

    task<DataReference<StoredMessage>> WriteLogRecord(
        const LogRecord& logRecord
    );

    task<task<>> Checkpoint(
        DatabaseHeaderT* header
    );
};
}