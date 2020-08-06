#include <algorithm>
#include "IndexImpl.h"
#include "src/ProtoStoreInternal.pb.h"
#include "KeyComparer.h"
#include "MemoryTableImpl.h"
#include "RowMerger.h"
#include "PartitionWriter.h"

namespace Phantom::ProtoStore
{

Index::Index(
    const string& indexName,
    IndexNumber indexNumber,
    SequenceNumber createSequenceNumber,
    shared_ptr<IMessageFactory> keyFactory,
    shared_ptr<IMessageFactory> valueFactory
)
    :
    m_indexName(indexName),
    m_indexNumber(indexNumber),
    m_createSequenceNumber(createSequenceNumber),
    m_keyFactory(keyFactory),
    m_valueFactory(valueFactory),
    m_keyComparer(make_shared<KeyComparer>(keyFactory->GetDescriptor())),
    m_rowMerger(make_shared<RowMerger>(&*m_keyComparer))
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

task<CheckpointNumber> Index::AddRow(
    SequenceNumber readSequenceNumber,
    const ProtoValue& key,
    const ProtoValue& value,
    SequenceNumber writeSequenceNumber,
    MemoryTableOperationOutcomeTask operationOutcomeTask)
{
    MemoryTableRow row;

    unique_ptr<Message> keyMessage(
        m_keyFactory->GetPrototype()->New());
    key.unpack<>(keyMessage.get());

    unique_ptr<Message> valueMessage(
        m_valueFactory->GetPrototype()->New());
    value.unpack<>(valueMessage.get());

    row.Key = move(keyMessage);
    row.Value = move(valueMessage);
    row.WriteSequenceNumber = writeSequenceNumber;

    shared_ptr<IMemoryTable> activeMemoryTable;
    CheckpointNumber activeCheckpointNumber;
    MemoryTablesEnumeration inactiveMemoryTables;
    PartitionsEnumeration partitions;

    auto lock = co_await m_dataSourcesLock.reader().scoped_lock_async();

    for (auto& memoryTable : *m_inactiveMemoryTables)
    {
        if (memoryTable->GetLatestSequenceNumber() < readSequenceNumber)
        {
            continue;
        }

        auto conflictingSequenceNumber = co_await memoryTable->CheckForWriteConflict(
            readSequenceNumber,
            row.Key.get());

        if (conflictingSequenceNumber.has_value())
        {
            throw WriteConflict();
        }
    }

    for (auto& partition : *m_partitions)
    {
        if (partition->GetLatestSequenceNumber() < readSequenceNumber)
        {
            continue;
        }

        auto conflictingSequenceNumber = co_await partition->CheckForWriteConflict(
            readSequenceNumber,
            row.Key.get());

        if (conflictingSequenceNumber.has_value())
        {
            throw WriteConflict();
        }
    }

    co_await m_activeMemoryTable->AddRow(
        readSequenceNumber,
        row, 
        operationOutcomeTask);

    co_return m_activeCheckpointNumber;
}

task<> Index::ReplayRow(
    shared_ptr<IMemoryTable> memoryTable,
    const string& key,
    const string& value,
    SequenceNumber writeSequenceNumber
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

    MemoryTableRow row;
    row.Key = move(keyMessage);
    row.Value = move(valueMessage);
    row.WriteSequenceNumber = writeSequenceNumber;

    co_await memoryTable->ReplayRow(
        row);
}

task<> Index::GetEnumerationDataSources(
    MemoryTablesEnumeration& memoryTables,
    PartitionsEnumeration& partitions)
{
    auto lock = co_await m_dataSourcesLock.reader().scoped_lock_async();
    memoryTables = m_memoryTablesToEnumerate;
    partitions = m_partitions;
}

task<ReadResult> Index::Read(
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

    for co_await(auto resultRow : enumeration)
    {
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

cppcoro::async_generator<EnumerateResult> Index::Enumerate(
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

    if (!keyLow.Key)
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

    if (!keyHigh.Key)
    {
        unpackedKeyHigh.reset(
            m_keyFactory->GetPrototype()->New());
        enumerateRequest.KeyHigh.unpack(
            unpackedKeyHigh.get());
        keyHigh.Key = unpackedKeyHigh.get();
    }

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

    auto enumeration = m_rowMerger->Merge(
        enumerateAllItemsLambda());

    for co_await(auto resultRow : enumeration)
    {
        co_yield EnumerateResult
        {
            .Key = resultRow.Key,
            .WriteSequenceNumber = resultRow.WriteSequenceNumber,
            .Value = resultRow.Value,
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