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
    m_rowMerger(make_shared<RowMerger>(&*m_keyComparer)),
    m_currentCheckpointNumber(1),
    m_partitions(make_shared<vector<shared_ptr<IPartition>>>())
{
    m_dontNeedCheckpoint.test_and_set();

    m_currentMemoryTable = make_shared<MemoryTable>(
        &*m_keyComparer);

    UpdateMemoryTablesToEnumerate();
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

    auto lock = co_await m_partitionsLock.scoped_nonrecursive_lock_read_async();

    m_dontNeedCheckpoint.clear();

    co_await m_currentMemoryTable->AddRow(
        readSequenceNumber,
        row, 
        operationOutcomeTask);

    co_return m_currentCheckpointNumber;
}

task<CheckpointNumber> Index::Replay(
    const LoggedRowWrite& loggedRowWrite
)
{
    if (!m_activeMemoryTables.contains(loggedRowWrite.checkpointnumber()))
    {
        m_activeMemoryTables[loggedRowWrite.checkpointnumber()] = make_shared<MemoryTable>(
            &*m_keyComparer);

        UpdateMemoryTablesToEnumerate();

        m_currentCheckpointNumber = std::max(
            loggedRowWrite.checkpointnumber() + 1,
            m_currentCheckpointNumber
        );
    }

    unique_ptr<Message> keyMessage(
        m_keyFactory->GetPrototype()->New());
    keyMessage->ParseFromString(
        loggedRowWrite.key());

    unique_ptr<Message> valueMessage(
        m_valueFactory->GetPrototype()->New());
    valueMessage->ParseFromString(
        loggedRowWrite.value());

    MemoryTableRow row;
    row.Key = move(keyMessage);
    row.Value = move(valueMessage);
    row.WriteSequenceNumber = ToSequenceNumber(
        loggedRowWrite.sequencenumber());

    co_await m_activeMemoryTables[loggedRowWrite.checkpointnumber()]->ReplayRow(
        row);

    m_dontNeedCheckpoint.clear();

    co_return loggedRowWrite.checkpointnumber();
}

void Index::UpdateMemoryTablesToEnumerate()
{
    auto newVector = std::make_shared<vector<shared_ptr<IMemoryTable>>>();

    newVector->push_back(
        m_currentMemoryTable
    );

    for (auto& activeMemoryTable : m_activeMemoryTables)
    {
        newVector->push_back(
            activeMemoryTable.second);
    }

    for (auto& checkpointingMemoryTable : m_checkpointingMemoryTables)
    {
        newVector->push_back(
            checkpointingMemoryTable.second);
    }

    m_memoryTablesToEnumerate = newVector;
}

task<> Index::GetItemsToEnumerate(
    MemoryTablesEnumeration& memoryTables,
    PartitionsEnumeration& partitions)
{
    auto lock = co_await m_partitionsLock.scoped_nonrecursive_lock_read_async();
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

    co_await GetItemsToEnumerate(
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
                keyLow.Key);
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

    co_await GetItemsToEnumerate(
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
                keyHigh);
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

task<LoggedCheckpoint> Index::StartCheckpoint()
{
    auto lock = co_await m_partitionsLock.scoped_nonrecursive_lock_write_async();

    LoggedCheckpoint loggedCheckpoint;

    if (m_dontNeedCheckpoint.test_and_set())
    {
        co_return loggedCheckpoint;
    }

    loggedCheckpoint.add_checkpointnumber(
        m_currentCheckpointNumber);
    m_checkpointingMemoryTables[m_currentCheckpointNumber] = m_currentMemoryTable;

    m_currentCheckpointNumber++;
    m_currentMemoryTable = make_shared<MemoryTable>(
        &*m_keyComparer);

    for (auto activeMemoryTable : m_activeMemoryTables)
    {
        m_checkpointingMemoryTables[activeMemoryTable.first] = activeMemoryTable.second;

        loggedCheckpoint.add_checkpointnumber(
            activeMemoryTable.first);
    }

    m_activeMemoryTables.clear();

    UpdateMemoryTablesToEnumerate();

    co_return loggedCheckpoint;
}

task<vector<shared_ptr<IMemoryTable>>> Index::StartCheckpoint(
    const LoggedCheckpoint& loggedCheckpoint)
{
    vector<shared_ptr<IMemoryTable>> memoryTablesToCheckpoint;

    auto lock = co_await m_partitionsLock.scoped_nonrecursive_lock_read_async();

    for (auto checkpointNumber : loggedCheckpoint.checkpointnumber())
    {
        auto memoryTable = m_checkpointingMemoryTables[checkpointNumber];
        assert(memoryTable);

        memoryTablesToCheckpoint.push_back(
            memoryTable);
    }

    co_return memoryTablesToCheckpoint;
}

task<> Index::WriteMemoryTables(
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

    co_await partitionWriter->WriteRows(
        rowCount,
        move(rows));
}

task<> Index::Checkpoint(
    const LoggedCheckpoint& loggedCheckpoint,
    shared_ptr<IPartitionWriter> partitionWriter
)
{
    auto memoryTablesToCheckpoint = co_await StartCheckpoint(
        loggedCheckpoint);

    co_await WriteMemoryTables(
        partitionWriter,
        memoryTablesToCheckpoint);
}

task<> Index::Replay(
    const LoggedCheckpoint& loggedCheckpoint
)
{
    for (auto checkpointNumber : loggedCheckpoint.checkpointnumber())
    {
        m_activeMemoryTables.erase(
            checkpointNumber);
    }

    co_return;
}

task<> Index::UpdatePartitions(
    const LoggedCheckpoint& loggedCheckpoint,
    vector<shared_ptr<IPartition>> partitions
)
{
    vector<shared_ptr<IMemoryTable>> memoryTablesToRemove;

    {
        auto lock = co_await m_partitionsLock.scoped_nonrecursive_lock_write_async();

        for (auto checkpointNumber : loggedCheckpoint.checkpointnumber())
        {
            if (m_checkpointingMemoryTables.contains(checkpointNumber))
            {
                // Copy the memory table out of the checkpointing memory tables list
                // so that the eventual delete operation happens outside of the lock,
                // and hence out of the request processing path.
                memoryTablesToRemove.push_back(m_checkpointingMemoryTables[checkpointNumber]);
                m_checkpointingMemoryTables.erase(checkpointNumber);
            }
        }

        m_partitions = make_shared<vector<shared_ptr<IPartition>>>(
            partitions);

        UpdateMemoryTablesToEnumerate();
    }

    // Now the memory tables will be deleted, maybe;
    // some enumerations might still be caught up in the delete.
}

task<> Index::Join()
{
    co_await m_currentMemoryTable->Join();
    for (auto activeMemoryTable : m_activeMemoryTables)
    {
        co_await activeMemoryTable.second->Join();
    }
    for (auto checkpointingMemoryTable : m_checkpointingMemoryTables)
    {
        co_await checkpointingMemoryTable.second->Join();
    }
}

}