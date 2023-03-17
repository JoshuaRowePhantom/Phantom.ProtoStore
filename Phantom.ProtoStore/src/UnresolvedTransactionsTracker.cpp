#include "UnresolvedTransactionsTracker.h"
#include "InternalProtoStore.h"
#include "ProtoStoreInternal.pb.h"
#include "Index.h"

namespace Phantom::ProtoStore
{

class UnresolvedTransactionsTracker :
    public IUnresolvedTransactionsTracker
{
    shared_ptr<IIndex> m_unresolvedTransactionsIndex;

public:
    UnresolvedTransactionsTracker(
        shared_ptr<IIndex> unresolvedTransactionsIndex
    ) : m_unresolvedTransactionsIndex { std::move(unresolvedTransactionsIndex) }
    {}

    // Inherited via IUnresolvedTransactionsTracker
    virtual task<TransactionOutcome> GetTransactionOutcome(
        const TransactionId& transactionId
    ) override
    {
        #if 0
        UnresolvedTransactionKey unresolvedTransactionKey;
        unresolvedTransactionKey.set_transactionid(transactionId);

        ReadRequest readRequest =
        {
            .Index = m_unresolvedTransactionsIndex,
            .Key = &unresolvedTransactionKey,
            .ReadValueDisposition = ReadValueDisposition::ReadValue,
        };

        auto readResult = co_await m_unresolvedTransactionsIndex->Read(
            0,
            readRequest);
        
        // We should always be able to read from unresolved transactions.
        if (!readResult)
        {
            std::move(readResult).error().throw_exception();
        }

        if (readResult->ReadStatus == ReadStatus::NoValue)
        {
            co_return TransactionOutcome::Committed;
        }
        auto unresolvedTransactionValue = readResult->Value.cast_if<UnresolvedTransactionValue>();

        if (unresolvedTransactionValue->status() == Serialization::UnresolvedTransactionStatus::Unresolved)
        {
            co_return TransactionOutcome::Unknown;
        }
        co_return TransactionOutcome::Aborted;
#else
        co_return TransactionOutcome::Committed;
#endif
    }

    virtual task<> ResolveTransaction(
        LogRecord& logRecord, 
        const TransactionId& transactionId, 
        const TransactionOutcome outcome
    ) override
    {
        return task<>();
    }

    virtual task<> Replay(
        const LogRecord& logRecord
    ) override
    {
        //if (logRecord.has_extras())
        //{
        //    for (const auto& loggedUnresolvedTransaction : logRecord.extras().loggedactions())
        //    {
        //        if (loggedUnresolvedTransaction.has_loggedunresolvedtransactions())
        //        {
        //            for (const auto& transactionId : loggedUnresolvedTransaction.loggedunresolvedtransactions().unresolvedtransactions())
        //            {
        //            }
        //        }
        //    }
        //}

        co_return;
    }


    // Filter out transactions from the DistributedTransactions table
    // that have no referencing partitions.
    virtual row_generator MergeDistributedTransactionsTable(
        PartitionNumber partitionNumber,
        row_generator source
    ) override
    {
        co_return;
    }

    // Filter out transactions from the DistributedTransactions table
    // that have no referencing partitions.
    virtual row_generator MergeDistributedTransactionReferencesTable(
        PartitionNumber partitionNumber,
        row_generator source
    ) override
    {
        co_return;
    }

    // Filter out transactions that have been aborted,
    // and add still-unresolved transactions to the DistributedTransactionReferences table.
    virtual row_generator HandleDistributedTransactionsDuringMerge(
        PartitionNumber partitionNumber,
        row_generator source
    ) override
    {
        co_return;
    }
};

shared_ptr<IUnresolvedTransactionsTracker> MakeUnresolvedTransactionsTracker(
    IInternalProtoStore* protoStore
)
{
    return std::make_shared<UnresolvedTransactionsTracker>(
        protoStore->GetUnresolvedTransactionsIndex());
}

}