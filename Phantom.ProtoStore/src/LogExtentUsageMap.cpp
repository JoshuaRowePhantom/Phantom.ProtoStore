#include "LogExtentUsageMap.h"

namespace Phantom::ProtoStore
{

size_t LogExtentUsageHash::operator()(const LogExtentUsage& usage) const noexcept
{
    size_t result = 0;
    boost::hash_combine(result, usage.logExtentSequenceNumber);
    boost::hash_combine(result, usage.indexNumber);
    boost::hash_combine(result, usage.partitionNumber);
    return result;
}

LogExtentUsageMap::LogExtentUsageMap()
{
}

void LogExtentUsageMap::SetCurrentLogExtent(
    LogExtentSequenceNumber logExtentSequenceNumber
)
{
    currentLogExtentSequenceNumber = logExtentSequenceNumber;
    HandleNewLogExtent(
        logExtentSequenceNumber);
}

void LogExtentUsageMap::HandleDatabaseHeader(
    const FlatBuffers::DatabaseHeader* databaseHeader
)
{
    // add all log extents to the map
    if (databaseHeader && databaseHeader->log_replay_extent_names())
    {
        for (auto extent : *databaseHeader->log_replay_extent_names())
        {
            HandleNewLogExtent(
                extent->log_extent_sequence_number()
            );
        }
    }
}

bool LogExtentUsageMap::HandleNewLogExtent(
    LogExtentSequenceNumber logExtentSequenceNumber
)
{
    return extents.emplace(
        logExtentSequenceNumber,
        std::monostate{}
    );
}

void LogExtentUsageMap::HandleDeletedLogExtent(
    LogExtentSequenceNumber logExtentSequenceNumber
)
{
    extents.erase(
        logExtentSequenceNumber
    );
}

void LogExtentUsageMap::HandleLoggedRowWrite(
    LogExtentSequenceNumber logExtentSequenceNumber,
    const FlatBuffers::LoggedRowWrite* logRecord
)
{
    usages.emplace(
        LogExtentUsage
        {
            logExtentSequenceNumber,
            logRecord->index_number(),
            logRecord->partition_number()
        },
        std::monostate{}
    );
}

void LogExtentUsageMap::HandleLoggedCheckpoint(
    const FlatBuffers::LoggedCheckpoint* logRecord
)
{
    auto indexNumber = logRecord->index_number();
    
    usages.erase_if(
        [&](const auto& usage)
        {
            for (auto partitionNumber : *logRecord->partition_number())
            {
                if (usage.first.indexNumber == indexNumber &&
                    usage.first.partitionNumber == partitionNumber)
                {
                    return true;
                }
            }

            return false;
        }
    );
}

void LogExtentUsageMap::HandleLoggedPartitionsData(
    LogExtentSequenceNumber logExtentSequenceNumber,
    const FlatBuffers::LoggedPartitionsData* logRecord
)
{
    std::ignore = logRecord;
    partitionsDataLogExtentSequenceNumber = logExtentSequenceNumber;
}

void LogExtentUsageMap::HandleLogEntry(
    LogExtentSequenceNumber logExtentSequenceNumber,
    const FlatBuffers::LogEntry* logRecord
)
{
    switch (logRecord->log_entry_type())
    {
    case FlatBuffers::LogEntryUnion::LoggedRowWrite:
        HandleLoggedRowWrite(
            logExtentSequenceNumber,
            logRecord->log_entry_as_LoggedRowWrite()
        );
        break;

    case FlatBuffers::LogEntryUnion::LoggedCheckpoint:
        HandleLoggedCheckpoint(
            logRecord->log_entry_as_LoggedCheckpoint()
        );
        break;

    case FlatBuffers::LogEntryUnion::LoggedPartitionsData:
        HandleLoggedPartitionsData(
            logExtentSequenceNumber,
            logRecord->log_entry_as_LoggedPartitionsData()
        );
        break;
    }
}

std::vector<LogExtentSequenceNumber> LogExtentUsageMap::GetExtentsToDelete() const
{
    // We store the result in a set first because we need the extents to be sorted
    // and we want the unique list.
    std::set<LogExtentSequenceNumber> result;

    extents.cvisit_all([&](const auto& logExtentSequenceNumber)
        {
            result.insert(logExtentSequenceNumber.first);
        });

    for (auto extentToReplay : GetExtentsToReplay())
    {
        result.erase(extentToReplay);
    }

    return { result.begin(), result.end() };
}

std::vector<LogExtentSequenceNumber> LogExtentUsageMap::GetExtentsToReplay() const
{
    // We store the result in a set first because we need the extents to be sorted
    // and we want the unique list.
    std::set<LogExtentSequenceNumber> result;

    if (currentLogExtentSequenceNumber)
    {
        result.insert(*currentLogExtentSequenceNumber);
    }

    if (partitionsDataLogExtentSequenceNumber)
    {
        result.insert(*partitionsDataLogExtentSequenceNumber);
    }

    usages.cvisit_all([&](const auto& usage)
        {
            result.insert(usage.first.logExtentSequenceNumber);
        });

    return { result.begin(), result.end() };
}

}
