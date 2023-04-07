#include "UnresolvedTransactionsTracker.h"
#include "InternalProtoStore.h"
#include "ProtoStoreInternal.pb.h"
#include "Index.h"
#include <limits>

namespace Phantom::ProtoStore
{

class UnresolvedTransactionsTracker :
    public IUnresolvedTransactionsTracker
{
    IIndex* m_distributedTransactionsIndex;
    IIndex* m_distributedTransactionReferencesIndex;
    IInternalProtoStoreTransactionFactory* m_transactionFactory;

public:
    UnresolvedTransactionsTracker(
        IIndex* distributedTransactionsIndex,
        IIndex* distributedTransactionReferencesIndex,
        IInternalProtoStoreTransactionFactory* transactionFactory,
        ExistingPartitions* existingPartitions
    ) :
        m_transactionFactory{ transactionFactory },
        m_distributedTransactionsIndex{ distributedTransactionsIndex },
        m_distributedTransactionReferencesIndex{ distributedTransactionReferencesIndex }
    {}

    bool PartitionNumberExists(
        PartitionNumber)
    {
        return true;
    }

    // Inherited via IUnresolvedTransactionsTracker
    virtual task<TransactionOutcome> GetTransactionOutcome(
        TransactionId transactionId
    ) override
    {
        flatbuffers::FlatBufferBuilder keyBuilder;
        auto transactionIdOffset = keyBuilder.CreateString(
            transactionId);
        auto keyOffset = FlatBuffers::CreateDistributedTransactionsKey(
            keyBuilder,
            transactionIdOffset);

        auto key = flatbuffers::GetRoot<FlatBuffers::DistributedTransactionsKey>(
            keyBuilder.GetCurrentBufferPointer());

        ReadRequest readRequest =
        {
            .Index = m_distributedTransactionsIndex,
            .Key = key,
            .ReadValueDisposition = ReadValueDisposition::ReadValue,
        };

        auto readResult = throw_if_failed(co_await m_distributedTransactionsIndex->Read(
            nullptr,
            readRequest));
        
        if (readResult->ReadStatus == ReadStatus::NoValue)
        {
            co_return TransactionOutcome::Committed;
        }

        auto unresolvedTransactionValue = readResult->Value.cast_if<FlatBuffers::DistributedTransactionsValue>();
        if (unresolvedTransactionValue->distributed_transaction_state() == FlatBuffers::DistributedTransactionState::Unknown)
        {
            co_return TransactionOutcome::Unknown;
        }

        co_return TransactionOutcome::Aborted;
    }

    virtual task<> ResolveTransaction(
        LogRecord& logRecord, 
        TransactionId transactionId, 
        const TransactionOutcome outcome
    ) override
    {
        co_return;
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
    {        // Reuse the key builders over and over.
        flatbuffers::FlatBufferBuilder keyLowBuilder;
        flatbuffers::FlatBufferBuilder keyHighBuilder;

        for (auto iterator = co_await source.begin();
            iterator != source.end();
            co_await ++iterator)
        {
            if (!iterator->TransactionId)
            {
                co_yield *iterator;
                continue;
            }

            const FlatBuffers::DistributedTransactionsKey* key = iterator->Key.cast_if<FlatBuffers::DistributedTransactionsKey>();

            {
                auto keyLowTransactionIdOffset = keyLowBuilder.CreateString(
                    key->distributed_transaction_id());

                auto keyLowOffset = FlatBuffers::CreateDistributedTransactionReferencesKey(
                    keyLowBuilder,
                    keyLowTransactionIdOffset,
                    0);

                keyLowBuilder.Finish(
                    keyLowOffset);
            }

            {
                auto keyHighTransactionIdOffset = keyHighBuilder.CreateString(
                    key->distributed_transaction_id());

                auto keyHighOffset = FlatBuffers::CreateDistributedTransactionReferencesKey(
                    keyHighBuilder,
                    keyHighTransactionIdOffset,
                    std::numeric_limits<PartitionNumber>::max());

                keyHighBuilder.Finish(
                    keyHighOffset);
            }

            auto distributedTransactionReferencesEnumeration = m_distributedTransactionReferencesIndex->Enumerate(
                nullptr,
                EnumerateRequest
                {
                    .Index = m_distributedTransactionReferencesIndex,
                    .KeyLow = flatbuffers::GetRoot<FlatBuffers::DistributedTransactionReferencesKey>(
                        keyLowBuilder.GetBufferPointer()),
                    .KeyLowInclusivity = Inclusivity::Inclusive,
                    .KeyHigh = flatbuffers::GetRoot<FlatBuffers::DistributedTransactionReferencesKey>(
                        keyHighBuilder.GetBufferPointer()),
                    .KeyHighInclusivity = Inclusivity::Inclusive,
                    .ReadValueDisposition = ReadValueDisposition::DontReadValue,
                });

            // If there is at least DistributedTransactionReferences row for this transaction where the partition exists,
            // then we keep this row in the merged partition.
            for (auto distributedTransactionReferencesIterator = co_await distributedTransactionReferencesEnumeration.begin();
                distributedTransactionReferencesIterator != distributedTransactionReferencesEnumeration.end();
                co_await ++distributedTransactionReferencesIterator)
            {
                const FlatBuffers::DistributedTransactionReferencesKey* enumeratedKey
                    = (*distributedTransactionReferencesIterator)->Key.cast_if<FlatBuffers::DistributedTransactionReferencesKey>();

                if (PartitionNumberExists(enumeratedKey->partition_number()))
                {
                    co_yield *iterator;
                    break;
                }
            }

            keyLowBuilder.Reset();
            keyHighBuilder.Reset();
        }
    }

    // Filter out rows from the DistributedTransactionReferences table
    // where the partition does not exist.
    virtual row_generator MergeDistributedTransactionReferencesTable(
        PartitionNumber partitionNumber,
        row_generator source
    ) override
    {
        for (auto iterator = co_await source.begin();
            iterator != source.end();
            co_await ++iterator)
        {
            const FlatBuffers::DistributedTransactionReferencesKey* key = iterator->Key.cast_if<FlatBuffers::DistributedTransactionReferencesKey>();

            if (PartitionNumberExists(key->partition_number()))
            {
                co_yield *iterator;
            }
        }
    }

    // Filter out transactions that have been aborted,
    // and add still-unresolved transactions to the DistributedTransactionReferences table.
    virtual row_generator HandleDistributedTransactionsDuringMerge(
        PartitionNumber partitionNumber,
        row_generator source
    ) override
    {
        // All the values are identical, so build it now.
        FlatBuffers::DistributedTransactionReferencesValueT valueT;
        FlatValue value{ &valueT };

        // Reuse the keybuilder over and over.
        flatbuffers::FlatBufferBuilder keyBuilder;

        for (auto iterator = co_await source.begin();
            iterator != source.end();
            co_await ++iterator)
        {
            if (!iterator->TransactionId)
            {
                co_yield *iterator;
                continue;
            }

            auto transactionOutcome = co_await GetTransactionOutcome(
                flatbuffers::GetStringView(iterator->TransactionId.get()));

            if (transactionOutcome == TransactionOutcome::Committed)
            {
                // The transaction has been committed.
                // We can drop the information about the transaction from the row.
                iterator->TransactionId = nullptr;
                co_yield *iterator;
            }
            else if (transactionOutcome == TransactionOutcome::Unknown)
            { 
                co_await m_transactionFactory->InternalExecuteTransaction({},
                    [&](IInternalTransaction* transaction) -> status_task<>
                {
                    auto transactionIdOffset = keyBuilder.CreateString(
                        iterator->TransactionId.get());

                    auto keyOffset = FlatBuffers::CreateDistributedTransactionReferencesKey(
                        keyBuilder,
                        transactionIdOffset,
                        partitionNumber);

                    keyBuilder.Finish(keyOffset);

                    auto key = flatbuffers::GetRoot<FlatBuffers::DistributedTransactionReferencesKey>(
                        keyBuilder.GetBufferPointer());

                    co_await transaction->AddRowInternal(
                        {},
                        m_distributedTransactionReferencesIndex,
                        key,
                        value);

                    keyBuilder.Reset();

                    co_return {};
                });

                // The transaction is still unresolved.
                // We need to add it to the DistributedTransactionReferences table.
                // We also need to keep it as an unresolved transaction in the merged partition.
                co_yield *iterator;
            }
            else
            {
                assert(transactionOutcome == TransactionOutcome::Aborted);
                // Skip this row, since it belongs to an aborted transaction.
            }
        }
    }
};

shared_ptr<IUnresolvedTransactionsTracker> MakeUnresolvedTransactionsTracker(
    IIndex* distributedTransactionsIndex,
    IIndex* distributedTransactionReferencesIndex,
    IInternalProtoStoreTransactionFactory* transactionFactory,
    ExistingPartitions* existingPartitions
)
{
    return std::make_shared<UnresolvedTransactionsTracker>(
        distributedTransactionsIndex,
        distributedTransactionReferencesIndex,
        transactionFactory,
        existingPartitions);
}

}