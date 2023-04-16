#pragma once

#include "StandardTypes.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"

namespace Phantom::ProtoStore
{

class IPartition;
class IMemoryTable;

class IIndexDataSources : public SerializationTypes
{
public:
    virtual task<> Replay(
        FlatMessage<LoggedRowWrite> rowWrite
    ) = 0;

    virtual task<> Replay(
        const LoggedCheckpoint* loggedCheckpoint
    ) = 0;

    virtual task<> Replay(
        const LoggedCreateMemoryTable* loggedCreateMemoryTable
    ) = 0;

    virtual task<> FinishReplay(
    ) = 0;

    virtual task<LoggedCheckpointT> StartCheckpoint(
    ) = 0;

    virtual task<WriteRowsResult> Checkpoint(
        const LoggedCheckpointT& loggedCheckpoint,
        std::shared_ptr<IPartitionWriter> partitionWriter
    ) = 0;

    virtual task<> UpdatePartitions(
        const LoggedCheckpointT& loggedCheckpoint,
        std::vector<std::shared_ptr<IPartition>> partitions
    ) = 0;

    virtual task<> EnsureHasActiveMemoryTable(
    ) = 0;
};

struct IndexDataSourcesSelection
{
    std::vector<std::shared_ptr<IMemoryTable>> memoryTables;
    std::vector<std::shared_ptr<IPartition>> partitions;
};

class IIndexDataSourcesSelector
{
public:
    virtual std::shared_ptr<const IndexDataSourcesSelection> SelectForCheckConflict(
        const ProtoValue& key,
        SequenceNumber readSequenceNumber
    ) const = 0;

    virtual std::shared_ptr<const IndexDataSourcesSelection> SelectForRead(
        const ProtoValue& key,
        SequenceNumber readSequenceNumber
    ) const = 0;

    virtual std::shared_ptr<const IndexDataSourcesSelection> SelectForEnumerate(
        const ProtoValue& keyLow,
        const ProtoValue& keyHigh,
        SequenceNumber readSequenceNumber
    ) const = 0;

    virtual std::shared_ptr<const IndexDataSourcesSelection> SelectForEnumeratePrefix(
        const Prefix& prefix,
        SequenceNumber readSequenceNumber
    ) const = 0;

    virtual const std::shared_ptr<IMemoryTable>& ActiveMemoryTable() const = 0;
    virtual PartitionNumber ActiveMemoryTablePartitionNumber() const = 0;
};

std::shared_ptr<IIndexDataSources> MakeIndexDataSources(
    IInternalProtoStore* protoStore,
    shared_ptr<IIndex> index
);

}
