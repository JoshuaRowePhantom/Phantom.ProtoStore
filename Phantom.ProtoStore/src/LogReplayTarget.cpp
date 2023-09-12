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
    return getCompletedTask();
}

task<> LogReplayTarget::Replay(
    const FlatMessage<FlatBuffers::LogEntry>& logEntry
)
{
    return getCompletedTask();
}

task<> LogReplayTarget::Replay(
    const FlatMessage<FlatBuffers::LoggedRowWrite>& loggedRowWrite
)
{
    return getCompletedTask();
}

task<> LogReplayTarget::Replay(
    const FlatMessage<FlatBuffers::LoggedCommitLocalTransaction>& loggedCommitLocalTransaction
)
{
    return getCompletedTask();
}

task<> LogReplayTarget::Replay(
    const FlatMessage<FlatBuffers::LoggedAbortLocalTransaction>& loggedAbortLocalTransaction
)
{
    return getCompletedTask();
}

task<> LogReplayTarget::Replay(
    const FlatMessage<FlatBuffers::LoggedCreateIndex>& loggedCreateIndex
)
{
    return getCompletedTask();
}

task<> LogReplayTarget::Replay(
    const FlatMessage<FlatBuffers::LoggedCreateExtent>& loggedCreateExtent
)
{
    return getCompletedTask();
}

task<> LogReplayTarget::Replay(
    const FlatMessage<FlatBuffers::LoggedCommitExtent>& loggedCommitExtent
)
{
    return getCompletedTask();
}

task<> LogReplayTarget::Replay(
    const FlatMessage<FlatBuffers::LoggedCheckpoint>& loggedCheckpoint
)
{
    return getCompletedTask();
}

task<> LogReplayTarget::Replay(
    const FlatMessage<FlatBuffers::LoggedDeleteExtent>& loggedDeleteExtent
)
{
    return getCompletedTask();
}

task<> LogReplayTarget::Replay(
    const FlatMessage<FlatBuffers::LoggedUpdatePartitions>& loggedUpdatePartitions
)
{
    return getCompletedTask();
}

task<> LogReplayTarget::Replay(
    const FlatMessage<FlatBuffers::LoggedPartitionsData>& loggedPartitionsData
)
{
    return getCompletedTask();
}

task<> LogReplayTarget::FinishReplay(
)
{
    return getCompletedTask();
}


}