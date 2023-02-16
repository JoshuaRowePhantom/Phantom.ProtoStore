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

    // Logs an unresolved transaction into the log record for a committed extent
    // during the partition write process.
    virtual task<> LogUnresolvedTransaction(
        IInternalTransaction* transaction,
        const LoggedRowWrite& loggedRowWrite
    ) = 0;
};

shared_ptr<IUnresolvedTransactionsTracker> MakeUnresolvedTransactionsTracker(
    IInternalProtoStore* protoStore
);

}