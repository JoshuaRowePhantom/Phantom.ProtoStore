#include "IndexImpl.h"
#include "src/ProtoStoreInternal.pb.h"
#include "KeyComparer.h"
#include "MemoryTableImpl.h"

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
    m_keyComparer(make_shared<KeyComparer>(keyFactory->GetDescriptor()))
{
    m_currentMemoryTable = make_shared<MemoryTable>(
        &*m_keyComparer);
}

IndexNumber Index::GetIndexNumber() const
{
    return m_indexNumber;
}

const IndexName& Index::GetIndexName() const
{
    return m_indexName;
}

task<> Index::AddRow(
    const ProtoValue& key,
    const ProtoValue& value,
    SequenceNumber writeSequenceNumber)
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

    co_await m_currentMemoryTable->AddRow(
        SequenceNumber::Latest,
        row,
        [=]()->MemoryTableOperationOutcomeTask
    {
        co_return MemoryTableOperationOutcome
        {
            .Outcome = OperationOutcome::Committed,
            .WriteSequenceNumber = writeSequenceNumber,
        };
    }());
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

    auto enumeration = m_currentMemoryTable->Enumerate(
        readRequest.SequenceNumber,
        keyLow,
        keyLow
    );

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
}

}