#pragma once

#include "StandardTypes.h"

namespace Phantom::ProtoStore
{

class IUnresolvedTransactionsTracker : 
    public SerializationTypes
{
public:
    virtual task<TransactionOutcome> GetTransactionOutcome(
        const TransactionId& transactionId
    ) = 0;

    virtual task<> ResolveTransaction(
        LogRecord& logRecord,
        const TransactionId& transactionId,
        const TransactionOutcome outcome
    ) = 0;

    virtual task<> Replay(
        const LogRecord& logRecord
    ) = 0;

    // Filter out transactions from the DistributedTransactions table
    // that have no referencing partitions.
    virtual row_generator MergeDistributedTransactionsTable(
        PartitionNumber partitionNumber,
        row_generator source
    ) = 0;

    // Filter out transactions from the DistributedTransactions table
    // that have no referencing partitions.
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
    IInternalProtoStore* protoStore
);

}