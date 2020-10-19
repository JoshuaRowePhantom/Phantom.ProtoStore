#pragma once

#include "StandardTypes.h"

namespace Phantom::ProtoStore
{

class IExtentManager
{
public:
    virtual ExtentName LogAllocateExtent(
        LogRecord& logRecord
    ) = 0;

    virtual void LogDeleteExtent(
        LogRecord& logRecord,
        ExtentName extentName
    ) = 0;

    virtual void LogCommitExtent(
        LogRecord& logRecord,
        ExtentName extentName
    ) = 0;

    virtual Task<> CommitCheckpoint(
        const LogRecord& logRecord
    ) = 0;

    virtual Task<> Replay(
        const LogRecord& logRecord
    ) = 0;

    virtual Task<> FinishReplay(
    ) = 0;

    virtual Task<> Checkpoint(
        LogRecord& logRecord
    ) = 0;
};
}