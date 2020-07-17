#include "IndexImpl.h"
#include "src/ProtoStoreInternal.pb.h"
#include "KeyComparer.h"
#include "MemoryTableImpl.h"
#include "RowMerger.h"

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
    m_currentCheckpointNumber(1)
{
    m_currentMemoryTable = make_shared<MemoryTable>(
        &*m_keyComparer);

    UpdateMemoryTablesToEnumerate();
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
        keyLow.Key = unpackedKey.get();
    }

    MemoryTablesEnumeration memoryTablesEnumeration;
    PartitionsEnumeration partitionsEnumeration;

    co_await GetItemsToEnumerate(
        memoryTablesEnumeration,
        partitionsEnumeration);

    auto enumerateAllItemsLambda = [&]() -> cppcoro::generator<cppcoro::async_generator<const MemoryTableRow*>>
    {
        for (auto& memoryTable : *memoryTablesEnumeration)
        {
            co_yield memoryTable->Enumerate(
                readRequest.SequenceNumber,
                keyLow,
                keyLow
            );
        }
    };

    auto enumeration = m_rowMerger->Merge(
        enumerateAllItemsLambda());

    for co_await(auto memoryTableRow : enumeration)
    {
        if (!memoryTableRow->Value)
        {
            break;
        }

        co_return ReadResult
        {
            .WriteSequenceNumber = memoryTableRow->WriteSequenceNumber,
            .Value = memoryTableRow->Value.get(),
            .ReadStatus = ReadStatus::HasValue,
        };
    }

    co_return ReadResult
    {
        .ReadStatus = ReadStatus::NoValue,
    };
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