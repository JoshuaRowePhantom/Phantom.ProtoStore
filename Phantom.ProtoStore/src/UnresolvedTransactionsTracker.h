#pragma once

#include "StandardTypes.h"
#include "InternalProtoStore.h"
#include "ExistingPartitions.h"

namespace Phantom::ProtoStore
{

class IUnresolvedTransactionsTracker : 
    public SerializationTypes
{
public:
    virtual task<TransactionOutcome> GetTransactionOutcome(
        TransactionId transactionId
    ) = 0;

    virtual task<> ResolveTransaction(
        LogRecord& logRecord,
        TransactionId transactionId,
        const TransactionOutcome outcome
    ) = 0;

    virtual task<> Replay(
        const LogRecord& logRecord
    ) = 0;

    // Filter out transactions from the DistributedTransactions table
    // that have no references in DistributedTransactionReferences.
    virtual row_generator MergeDistributedTransactionsTable(
        PartitionNumber partitionNumber,
        row_generator source
    ) = 0;

    // Filter out transactions from the DistributedTransactionReferences table
    // for partitions that are no longer in existence.
    virtual row_generator MergeDistributedTransactionReferencesTable(
        PartitionNumber partitionNumber,
        row_generator source
    ) = 0;

    // Filter out transactions that have been aborted,
    // and add still-unresolved transactions to the DistributedTransactionReferences table.
    virtual row_generator HandleDistributedTransactionsDuringMerge(
        PartitionNumber partitionNumber,
        row_generator source
    ) = 0;
};

shared_ptr<IUnresolvedTransactionsTracker> MakeUnresolvedTransactionsTracker(
    IIndex* distributedTransactionsIndex,
    IIndex* distributedTransactionReferencesIndex,
    IInternalProtoStoreTransactionFactory* transactionFactory,
    ExistingPartitions* existingPartitions
);

}