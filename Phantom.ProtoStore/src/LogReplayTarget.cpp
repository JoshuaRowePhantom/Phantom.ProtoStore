#include "LogReplayTarget.h"

namespace Phantom::ProtoStore
{

task<> LogReplayTarget::getCompletedTask()
{
    co_return;
}

task<> LogReplayTarget::BeginReplay(
)
{
    return getCompletedTask();
}

task<> LogReplayTarget::Replay(
    int phase,
    const ExtentName* extentName)
{
    std::ignore = phase;
    std::ignore = extentName;
    return getCompletedTask();
}

task<> LogReplayTarget::Replay(
    const FlatMessage<FlatBuffers::LogEntry>& logEntry
)
{
    std::ignore = logEntry;
    return getCompletedTask();
}

task<> LogReplayTarget::Replay(
    const FlatMessage<FlatBuffers::LoggedRowWrite>& loggedRowWrite
)
{
    std::ignore = loggedRowWrite;
    return getCompletedTask();
}

task<> LogReplayTarget::Replay(
    const FlatMessage<FlatBuffers::LoggedCommitLocalTransaction>& loggedCommitLocalTransaction
)
{
    std::ignore = loggedCommitLocalTransaction;
    return getCompletedTask();
}

task<> LogReplayTarget::Replay(
    const FlatMessage<FlatBuffers::LoggedCreateIndex>& loggedCreateIndex
)
{
    std::ignore = loggedCreateIndex;
    return getCompletedTask();
}

task<> LogReplayTarget::Replay(
    const FlatMessage<FlatBuffers::LoggedCreateExtent>& loggedCreateExtent
)
{
    std::ignore = loggedCreateExtent;
    return getCompletedTask();
}

task<> LogReplayTarget::Replay(
    const FlatMessage<FlatBuffers::LoggedCommitExtent>& loggedCommitExtent
)
{
    std::ignore = loggedCommitExtent;
    return getCompletedTask();
}

task<> LogReplayTarget::Replay(
    const FlatMessage<FlatBuffers::LoggedCheckpoint>& loggedCheckpoint
)
{
    std::ignore = loggedCheckpoint;
    return getCompletedTask();
}

task<> LogReplayTarget::Replay(
    const FlatMessage<FlatBuffers::LoggedDeleteExtent>& loggedDeleteExtent
)
{
    std::ignore = loggedDeleteExtent;
    return getCompletedTask();
}

task<> LogReplayTarget::Replay(
    const FlatMessage<FlatBuffers::LoggedUpdatePartitions>& loggedUpdatePartitions
)
{
    std::ignore = loggedUpdatePartitions;
    return getCompletedTask();
}

task<> LogReplayTarget::Replay(
    const FlatMessage<FlatBuffers::LoggedPartitionsData>& loggedPartitionsData
)
{
    std::ignore = loggedPartitionsData;
    return getCompletedTask();
}

task<> LogReplayTarget::FinishReplay(
)
{
    return getCompletedTask();
}


}