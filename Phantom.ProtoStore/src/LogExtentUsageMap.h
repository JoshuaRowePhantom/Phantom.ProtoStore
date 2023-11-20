#pragma once

#include<set>
#include<vector>

#include "StandardTypes.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"
#include "boost/unordered/concurrent_flat_map.hpp"

namespace Phantom::ProtoStore
{

struct LogExtentUsage
{
    LogExtentSequenceNumber logExtentSequenceNumber;
    IndexNumber indexNumber;
    PartitionNumber partitionNumber;

    friend bool operator ==(const LogExtentUsage&, const LogExtentUsage&) = default;
};

struct LogExtentUsageHash
{
    size_t operator()(const LogExtentUsage&) const noexcept;
};

class LogExtentUsageMap
{
public:

    boost::unordered::concurrent_flat_map<LogExtentSequenceNumber, std::monostate> extents;
    boost::unordered::concurrent_flat_map<LogExtentUsage, std::monostate, LogExtentUsageHash> usages;

    std::optional<LogExtentSequenceNumber> partitionsDataLogExtentSequenceNumber;
    std::optional<LogExtentSequenceNumber> currentLogExtentSequenceNumber;

public:
    LogExtentUsageMap(
    );

    void SetCurrentLogExtent(
        LogExtentSequenceNumber logExtentSequenceNumber
    );

    void HandleDatabaseHeader(
        const FlatBuffers::DatabaseHeader* databaseHeader
    );

    bool HandleNewLogExtent(
        LogExtentSequenceNumber logExtentSequenceNumber
    );
    
    void HandleDeletedLogExtent(
        LogExtentSequenceNumber logExtentSequenceNumber
    );

    void HandleLoggedRowWrite(
        LogExtentSequenceNumber logExtentSequenceNumber,
        const FlatBuffers::LoggedRowWrite* logRecord
    );
    
    void HandleLoggedCheckpoint(
        const FlatBuffers::LoggedCheckpoint* logRecord
    );
    
    void HandleLoggedPartitionsData(
        LogExtentSequenceNumber logExtentSequenceNumber,
        const FlatBuffers::LoggedPartitionsData* logRecord
    );
    
    void HandleLogEntry(
        LogExtentSequenceNumber logExtentSequenceNumber,
        const FlatBuffers::LogEntry* logEntry
    );

    std::vector<LogExtentSequenceNumber> GetExtentsToDelete() const;
    std::vector<LogExtentSequenceNumber> GetExtentsToReplay() const;
};

}
