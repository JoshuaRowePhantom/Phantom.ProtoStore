#pragma once

#include "Phantom.ProtoStore/Payloads.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"

namespace Phantom::ProtoStore
{

// Base class for replaying log streams.
class LogReplayTarget
{
    static task<> getCompletedTask();

public:
    virtual task<> BeginReplay(
    );

    virtual task<> Replay(
        int phase,
        const ExtentName* extentName);

    virtual task<> Replay(
        const FlatMessage<FlatBuffers::LogEntry>& logEntry
    );

    virtual task<> Replay(
        const FlatMessage<FlatBuffers::LoggedRowWrite>& loggedRowWrite
    );

    virtual task<> Replay(
        const FlatMessage<FlatBuffers::LoggedCommitLocalTransaction>& loggedCommitLocalTransaction
    );
    
    virtual task<> Replay(
        const FlatMessage<FlatBuffers::LoggedCreateIndex>& loggedCreateIndex
    );
    
    virtual task<> Replay(
        const FlatMessage<FlatBuffers::LoggedCreateExtent>& loggedCreateExtent
    );

    virtual task<> Replay(
        const FlatMessage<FlatBuffers::LoggedCommitExtent>& loggedCommitExtent
    );

    virtual task<> Replay(
        const FlatMessage<FlatBuffers::LoggedCheckpoint>& loggedCheckpoint
    );

    virtual task<> Replay(
        const FlatMessage<FlatBuffers::LoggedDeleteExtent>& loggedDeleteExtent
    );

    virtual task<> Replay(
        const FlatMessage<FlatBuffers::LoggedUpdatePartitions>& loggedUpdatePartitions
    );

    virtual task<> Replay(
        const FlatMessage<FlatBuffers::LoggedPartitionsData>& loggedPartitionsData
    );

    virtual task<> FinishReplay(
    );
};

}