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
    shared_ptr<IMessageFactory> keyFactory,
    shared_ptr<IMessageFactory> valueFactory,
    IUnresolvedTransactionsTracker* unresolvedTransactionsTracker
)
    :
    m_indexName(indexName),
    m_indexNumber(indexNumber),
    m_createSequenceNumber(createSequenceNumber),
    m_keyFactory(keyFactory),
    m_valueFactory(valueFactory),
    m_keyComparer(make_shared<KeyComparer>(keyFactory->GetDescriptor())),
    m_rowMerger(make_shared<RowMerger>(&*m_keyComparer)),
    m_unresolvedTransactionsTracker(unresolvedTransactionsTracker)
{
}

shared_ptr<KeyComparer> Index::GetKeyComparer()
{
    return m_keyComparer;
}

shared_ptr<IMessageFactory> Index::GetKeyFactory()
{
    return m_keyFactory;
}

shared_ptr<IMessageFactory> Index::GetValueFactory()
{
    return m_valueFactory;
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
    const ProtoValue& key,
    const ProtoValue& value,
    SequenceNumber writeSequenceNumber,
    const TransactionId* transactionId,
    MemoryTableOperationOutcomeTask operationOutcomeTask)
{
    unique_ptr<Message> keyMessage(
        m_keyFactory->GetPrototype()->New());
    key.unpack<>(keyMessage.get());

    unique_ptr<Message> valueMessage;
    if (value)
    {
        valueMessage.reset(
            m_valueFactory->GetPrototype()->New());
        value.unpack<>(valueMessage.get());
    }

    MemoryTableRow row
    {
        .Key = move(keyMessage),
        .WriteSequenceNumber = writeSequenceNumber,
        .Value = move(valueMessage),
        .TransactionId = transactionId ? std::optional { *transactionId } : std::optional<TransactionId>{},
    };

    shared_ptr<IMemoryTable> activeMemoryTable;
    CheckpointNumber activeCheckpointNumber;
    MemoryTablesEnumeration inactiveMemoryTables;
    PartitionsEnumeration partitions;

    auto lock = co_await m_dataSourcesLock.reader().scoped_lock_async();
    
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

        auto conflictingSequenceNumber = co_await memoryTable->CheckForWriteConflict(
            readSequenceNumber,
            writeSequenceNumber,
            row.Key.get());

        if (conflictingSequenceNumber.has_value())
        {
            co_return makeWriteConflict(*conflictingSequenceNumber);
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
            row.Key.get());

        if (conflictingSequenceNumber.has_value())
        {
            co_return makeWriteConflict(*conflictingSequenceNumber);
        }
    }

    auto conflictingSequenceNumber = co_await m_activeMemoryTable->AddRow(
        readSequenceNumber,
        row, 
        operationOutcomeTask);

    if (conflictingSequenceNumber.has_value())
    {
        co_return makeWriteConflict(*conflictingSequenceNumber);
    }

    co_return m_activeCheckpointNumber;
}

task<> Index::ReplayRow(
    shared_ptr<IMemoryTable> memoryTable,
    const string& key,
    const string& value,
    SequenceNumber writeSequenceNumber,
    const TransactionId* transactionId
)
{
    unique_ptr<Message> keyMessage(
        m_keyFactory->GetPrototype()->New());
    keyMessage->ParseFromString(
        key);

    unique_ptr<Message> valueMessage(
        m_valueFactory->GetPrototype()->New());
    valueMessage->ParseFromString(
        value);

    MemoryTableRow row
    {
        .Key = move(keyMessage),
        .WriteSequenceNumber = writeSequenceNumber,
        .Value = move(valueMessage),
        .TransactionId = transactionId ? std::optional{ *transactionId } : std::optional<TransactionId>{},
    };

    co_await memoryTable->ReplayRow(
        row);
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
    const ReadRequest& readRequest
)
{
    unique_ptr<Message> unpackedKey;

    KeyRangeEnd keyLow
    {
        .Key = readRequest.Key.as_message_if(),
        .Inclusivity = Inclusivity::Inclusive,
    };

    if (!keyLow.Key)
    {
        unpackedKey.reset(
            m_keyFactory->GetPrototype()->New());
        readRequest.Key.unpack(
            unpackedKey.get());
        keyLow.Key = unpackedKey.get();
    }

    MemoryTablesEnumeration memoryTablesEnumeration;
    PartitionsEnumeration partitionsEnumeration;

    co_await GetEnumerationDataSources(
        memoryTablesEnumeration,
        partitionsEnumeration);

    auto enumerateAllItemsLambda = [&]() -> cppcoro::generator<cppcoro::async_generator<ResultRow>>
    {
        for (auto& memoryTable : *memoryTablesEnumeration)
        {
            co_yield memoryTable->Enumerate(
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

        if (resultRow.TransactionId)
        {
            auto transactionOutcome = co_await m_unresolvedTransactionsTracker->GetTransactionOutcome(
                *resultRow.TransactionId
            );

            if (transactionOutcome == TransactionOutcome::Unknown)
            {
                co_return MakeUnresolvedTransactionFailedResult(
                    *resultRow.TransactionId);
            }
            else if (transactionOutcome == TransactionOutcome::Aborted)
            {
                continue;
            }
        }

        if (!resultRow.Value)
        {
            break;
        }

        unique_ptr<Message> value(resultRow.Value->New());
        value->CopyFrom(*resultRow.Value);

        ReadResult readResult
        {
            .WriteSequenceNumber = resultRow.WriteSequenceNumber,
            .Value = move(value),
            .ReadStatus = ReadStatus::HasValue,
        };
        
        co_return readResult;
    }

    co_return ReadResult
    {
        .ReadStatus = ReadStatus::NoValue,
    };
}

cppcoro::async_generator<OperationResult<EnumerateResult>> Index::Enumerate(
    const EnumerateRequest& enumerateRequest
)
{
    unique_ptr<Message> unpackedKeyLow;
    unique_ptr<Message> unpackedKeyHigh;

    KeyRangeEnd keyLow
    {
        .Key = enumerateRequest.KeyLow.as_message_if(),
        .Inclusivity = enumerateRequest.KeyLowInclusivity,
    };

    if (!keyLow.Key
        &&
        enumerateRequest.KeyLow.has_value())
    {
        unpackedKeyLow.reset(
            m_keyFactory->GetPrototype()->New());
        enumerateRequest.KeyLow.unpack(
            unpackedKeyLow.get());
        keyLow.Key = unpackedKeyLow.get();
    }

    KeyRangeEnd keyHigh
    {
        .Key = enumerateRequest.KeyHigh.as_message_if(),
        .Inclusivity = enumerateRequest.KeyHighInclusivity,
    };

    if (!keyHigh.Key
        &&
        enumerateRequest.KeyHigh.has_value())
    {
        unpackedKeyHigh.reset(
            m_keyFactory->GetPrototype()->New());
        enumerateRequest.KeyHigh.unpack(
            unpackedKeyHigh.get());
        keyHigh.Key = unpackedKeyHigh.get();
    }

    assert(keyLow.Key);
    assert(keyHigh.Key);

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
                .Value = resultRow.Value,
                .ReadStatus = ReadStatus::HasValue,
            },
            resultRow.Key,
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
    };

    co_return co_await partitionWriter->WriteRows(
        move(writeRowsRequest));
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

task<> Index::Join()
{
    co_return;
}

}