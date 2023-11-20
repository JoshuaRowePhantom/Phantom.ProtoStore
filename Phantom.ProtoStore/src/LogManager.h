#pragma once

#include "StandardTypes.h"
#include "ProtoStoreInternal.pb.h"
#include "Phantom.Coroutines/async_sharded_reader_writer_lock.h"
#include "LogExtentUsageMap.h"
#include "MessageStore.h"
#include "SequenceNumber.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"

namespace Phantom::ProtoStore
{

class LogManager : SerializationTypes
{
    Schedulers m_schedulers;
    shared_ptr<IExtentStore> m_logExtentStore;
    shared_ptr<IMessageStore> m_logMessageStore;

    LogExtentUsageMap m_logExtentUsageMap;

    Phantom::Coroutines::async_sharded_reader_writer_lock<> m_logLock;
    shared_ptr<ISequentialMessageWriter> m_logMessageWriter;
    AtomicSequenceNumber<LogExtentSequenceNumber> m_nextLogExtentSequenceNumber;

    task<> DeleteExtents();

public:
    LogManager(
        Schedulers schedulers,
        shared_ptr<IExtentStore> logExtentStore,
        shared_ptr<IMessageStore> logMessageStore
    );

    task<> ReplayLogExtentUsageMap(
        LogExtentUsageMap logExtentUsageMap
    );

    task<FlatMessage<FlatBuffers::LogRecord>> WriteLogRecord(
        const FlatMessage<FlatBuffers::LogRecord>& logRecord,
        FlushBehavior flushBehavior = FlushBehavior::Flush
    );

    task<> Checkpoint(
        DatabaseHeaderT* header
    );

    task<> OpenNewLogWriter();
};
}