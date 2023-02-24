#pragma once

#include "StandardTypes.h"
#include "MemoryTable.h"
#include "PartitionWriter.h"

namespace Phantom::ProtoStore
{

class IIndex : public SerializationTypes
{
public:
    virtual operation_task<CheckpointNumber> AddRow(
        SequenceNumber readSequenceNumber,
        FlatMessage<LoggedRowWrite> loggedRowWrite,
        shared_ptr<DelayedMemoryTableTransactionOutcome> delayedTransactionOutcome
    ) = 0;

    virtual shared_ptr<KeyComparer> GetKeyComparer(
    ) = 0;

    virtual shared_ptr<IMessageFactory> GetKeyFactory(
    ) = 0;

    virtual shared_ptr<IMessageFactory> GetValueFactory(
    ) = 0;

    virtual operation_task<ReadResult> Read(
        shared_ptr<DelayedMemoryTableTransactionOutcome> originatingTransactionOutcome,
        const ReadRequest& readRequest
    ) = 0;

    virtual cppcoro::async_generator<OperationResult<EnumerateResult>> Enumerate(
        shared_ptr<DelayedMemoryTableTransactionOutcome> originatingTransactionOutcome,
        const EnumerateRequest& readRequest
    ) = 0;

    virtual IndexNumber GetIndexNumber(
    ) const = 0;

    virtual const IndexName& GetIndexName(
    ) const = 0;

    virtual task<> Join(
    ) = 0;

    virtual task<WriteRowsResult> WriteMemoryTables(
        const shared_ptr<IPartitionWriter>& partitionWriter,
        const vector<shared_ptr<IMemoryTable>>& memoryTablesToCheckpoint
    ) = 0;

    virtual task<> ReplayRow(
        shared_ptr<IMemoryTable> memoryTable,
        FlatMessage<LoggedRowWrite> loggedRowWrite
        ) = 0;

    virtual task<> SetDataSources(
        shared_ptr<IMemoryTable> activeMemoryTable,
        CheckpointNumber activeCheckpointNumber,
        vector<shared_ptr<IMemoryTable>> inactiveMemoryTables,
        vector<shared_ptr<IPartition>> partitions
    ) = 0;
};

}
