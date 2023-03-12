#include <algorithm>
#include "IndexImpl.h"
#include "ProtoStoreInternal.pb.h"
#include "KeyComparer.h"
#include "MemoryTableImpl.h"
#include "RowMerger.h"
#include "PartitionWriter.h"
#include "UnresolvedTransactionsTracker.h"

namespace Phantom::ProtoStore
{

Index::Index(
    const string& indexName,
    IndexNumber indexNumber,
    SequenceNumber createSequenceNumber,
    shared_ptr<KeyComparer> keyComparer,
    IUnresolvedTransactionsTracker* unresolvedTransactionsTracker,
    std::shared_ptr<const Schema> schema
)
    :
    m_indexName(indexName),
    m_indexNumber(indexNumber),
    m_createSequenceNumber(createSequenceNumber),
    m_keyComparer(std::move(keyComparer)),
    m_rowMerger(make_shared<RowMerger>(
        m_schema,
        m_keyComparer)),
    m_unresolvedTransactionsTracker(unresolvedTransactionsTracker),
    m_schema(std::move(schema))
{
}

shared_ptr<KeyComparer> Index::GetKeyComparer()
{
    return m_keyComparer;
}

IndexNumber Index::GetIndexNumber() const
{
    return m_indexNumber;
}

const IndexName& Index::GetIndexName() const
{
    return m_indexName;
}

operation_task<CheckpointNumber> Index::AddRow(
    SequenceNumber readSequenceNumber,
    CreateLoggedRowWrite createLoggedRowWrite,
    shared_ptr<DelayedMemoryTableTransactionOutcome> delayedTransactionOutcome)
{
    auto lock = co_await m_dataSourcesLock.reader().scoped_lock_async();

    auto row = co_await createLoggedRowWrite(m_activeCheckpointNumber);;

    shared_ptr<IMemoryTable> activeMemoryTable;
    MemoryTablesEnumeration inactiveMemoryTables;
    PartitionsEnumeration partitions;
    auto writeSequenceNumber = ToSequenceNumber(row->sequence_number());
    
    auto makeWriteConflict = [&](SequenceNumber conflictingSequenceNumber)
    {
        return std::unexpected
        {
            FailedResult
            {
                .ErrorCode = make_error_code(ProtoStoreErrorCode::WriteConflict),
                .ErrorDetails = WriteConflict
                {
                    .Index = this,
                    .ConflictingSequenceNumber = conflictingSequenceNumber,
                },
            },
        };
    };

    for (auto& memoryTable : *m_inactiveMemoryTables)
    {
        if (memoryTable->GetLatestSequenceNumber() < writeSequenceNumber
            &&
            memoryTable->GetLatestSequenceNumber() < readSequenceNumber)
        {
            continue;
        }
    }

    for (auto& partition : *m_partitions)
    {
        if (partition->GetLatestSequenceNumber() < writeSequenceNumber
            &&
            partition->GetLatestSequenceNumber() < readSequenceNumber)
        {
            continue;
        }

        auto conflictingSequenceNumber = co_await partition->CheckForWriteConflict(
            readSequenceNumber,
            writeSequenceNumber,
            SchemaDescriptions::MakeProtoValueKey(
                *m_schema,
                row->key()));

        if (conflictingSequenceNumber.has_value())
        {
            co_return makeWriteConflict(*conflictingSequenceNumber);
        }
    }

    auto conflictingSequenceNumber = co_await m_activeMemoryTable->AddRow(
        readSequenceNumber,
        std::move(row),
        std::move(delayedTransactionOutcome));

    if (conflictingSequenceNumber.has_value())
    {
        co_return makeWriteConflict(*conflictingSequenceNumber);
    }

    co_return m_activeCheckpointNumber;
}

task<> Index::ReplayRow(
    shared_ptr<IMemoryTable> memoryTable,
    FlatMessage<LoggedRowWrite> loggedRowWrite
)
{
    co_await memoryTable->ReplayRow(
        std::move(loggedRowWrite));
}

std::unexpected<FailedResult> Index::MakeUnresolvedTransactionFailedResult(
    TransactionId unresolvedTransactionId)
{
    return std::unexpected
    {
        FailedResult
        {
            .ErrorCode = make_error_code(ProtoStoreErrorCode::UnresolvedTransaction),
            .ErrorDetails = UnresolvedTransaction
            {
                .UnresolvedTransactionId = std::move(unresolvedTransactionId),
            }
        }
    };
}

task<> Index::GetEnumerationDataSources(
    MemoryTablesEnumeration& memoryTables,
    PartitionsEnumeration& partitions)
{
    auto lock = co_await m_dataSourcesLock.reader().scoped_lock_async();
    memoryTables = m_memoryTablesToEnumerate;
    partitions = m_partitions;
}

operation_task<ReadResult> Index::Read(
    shared_ptr<DelayedMemoryTableTransactionOutcome> originatingTransactionOutcome,
    const ReadRequest& readRequest
)
{
    ProtoValue unowningKey = readRequest.Key.pack_unowned();

    KeyRangeEnd keyLow
    {
        .Key = unowningKey,
        .Inclusivity = Inclusivity::Inclusive,
    };

    MemoryTablesEnumeration memoryTablesEnumeration;
    PartitionsEnumeration partitionsEnumeration;

    co_await GetEnumerationDataSources(
        memoryTablesEnumeration,
        partitionsEnumeration);

    auto enumerateAllItemsLambda = [&]() -> row_generators
    {
        for (auto& memoryTable : *memoryTablesEnumeration)
        {
            co_yield memoryTable->Enumerate(
                originatingTransactionOutcome,
                readRequest.SequenceNumber,
                keyLow,
                keyLow
            );
        }

        for (auto& partition : *partitionsEnumeration)
        {
            co_yield partition->Read(
                readRequest.SequenceNumber,
                keyLow.Key,
                ReadValueDisposition::ReadValue);
        }
    };

    auto enumeration = m_rowMerger->Merge(
        enumerateAllItemsLambda());

    for (auto iterator = co_await enumeration.begin();
        iterator != enumeration.end();
        co_await ++iterator)
    {
        ResultRow& resultRow = *iterator;

        //if (resultRow->TransactionId.data())
        //{
        //    auto transactionOutcome = co_await m_unresolvedTransactionsTracker->GetTransactionOutcome(
        //        resultRow->TransactionId
        //    );

        //    if (transactionOutcome == TransactionOutcome::Unknown)
        //    {
        //        co_return MakeUnresolvedTransactionFailedResult(
        //            *resultRow.TransactionId);
        //    }
        //    else if (transactionOutcome == TransactionOutcome::Aborted)
        //    {
        //        continue;
        //    }
        //}

        if (!resultRow.Value)
        {
            break;
        }

        co_return ReadResult
        {
            .WriteSequenceNumber = resultRow.WriteSequenceNumber,
            .Value = std::move(resultRow.Value),
            .ReadStatus = ReadStatus::HasValue,
        };
    }

    co_return ReadResult
    {
        .ReadStatus = ReadStatus::NoValue,
    };
}

cppcoro::async_generator<OperationResult<EnumerateResult>> Index::Enumerate(
    shared_ptr<DelayedMemoryTableTransactionOutcome> originatingTransactionOutcome,
    const EnumerateRequest& enumerateRequest
)
{
    ProtoValue unowningKeyLow = enumerateRequest.KeyLow.pack_unowned();
    ProtoValue unowningKeyHigh = enumerateRequest.KeyHigh.pack_unowned();

    KeyRangeEnd keyLow
    {
        .Key = unowningKeyLow,
        .Inclusivity = enumerateRequest.KeyLowInclusivity,
    };

    KeyRangeEnd keyHigh
    {
        .Key = unowningKeyHigh,
        .Inclusivity = enumerateRequest.KeyHighInclusivity,
    };

    MemoryTablesEnumeration memoryTablesEnumeration;
    PartitionsEnumeration partitionsEnumeration;

    co_await GetEnumerationDataSources(
        memoryTablesEnumeration,
        partitionsEnumeration);

    auto enumerateAllItemsLambda = [&]() -> row_generators
    {
        for (auto& memoryTable : *memoryTablesEnumeration)
        {
            co_yield memoryTable->Enumerate(
                originatingTransactionOutcome,
                enumerateRequest.SequenceNumber,
                keyLow,
                keyHigh
            );
        }

        for (auto& partition : *partitionsEnumeration)
        {
            co_yield partition->Enumerate(
                enumerateRequest.SequenceNumber,
                keyLow,
                keyHigh,
                ReadValueDisposition::ReadValue);
        }
    };

    auto enumeration = m_rowMerger->Enumerate(
        enumerateAllItemsLambda());

    for (auto iterator = co_await enumeration.begin();
        iterator != enumeration.end();
        co_await ++iterator)
    {
        auto& resultRow = *iterator;

        co_yield EnumerateResult
        {
            {
                .WriteSequenceNumber = resultRow.WriteSequenceNumber,
                .Value = std::move(resultRow.Value),
                .ReadStatus = ReadStatus::HasValue,
            },
            std::move(resultRow.Key),
        };
    }
}

task<WriteRowsResult> Index::WriteMemoryTables(
    const shared_ptr<IPartitionWriter>& partitionWriter,
    const vector<shared_ptr<IMemoryTable>>& memoryTablesToCheckpoint
)
{
    size_t rowCount = 0;
    for (auto& memoryTable : memoryTablesToCheckpoint)
    {
        rowCount += co_await memoryTable->GetRowCount();
    }

    auto rows = m_rowMerger->Merge([&]() -> row_generators
    {
        for (auto& memoryTable : memoryTablesToCheckpoint)
        {
            co_yield memoryTable->Checkpoint();
        }
    }());

    WriteRowsRequest writeRowsRequest =
    {
        .approximateRowCount = rowCount,
        .rows = &rows,
        .targetExtentSize = std::numeric_limits<ExtentOffset>::max(),
        .targetMessageSize = 1024*1024*1024,
    };

    co_return co_await partitionWriter->WriteRows(
        writeRowsRequest);
}

task<> Index::SetDataSources(
    shared_ptr<IMemoryTable> activeMemoryTable,
    CheckpointNumber activeCheckpointNumber,
    vector<shared_ptr<IMemoryTable>> inactiveMemoryTables,
    vector<shared_ptr<IPartition>> partitions
)
{
    co_await m_dataSourcesLock.writer().scoped_lock_async();

    m_activeMemoryTable = activeMemoryTable;
    m_activeCheckpointNumber = activeCheckpointNumber;
    m_inactiveMemoryTables = make_shared<vector<shared_ptr<IMemoryTable>>>(
        inactiveMemoryTables);
    m_memoryTablesToEnumerate = make_shared<vector<shared_ptr<IMemoryTable>>>(
        inactiveMemoryTables);
    m_memoryTablesToEnumerate->push_back(
        activeMemoryTable);
    m_partitions = make_shared<vector<shared_ptr<IPartition>>>(
        partitions);
}

const shared_ptr<const Schema>& Index::GetSchema() const
{
    return m_schema;
}

task<> Index::Join()
{
    co_return;
}

}