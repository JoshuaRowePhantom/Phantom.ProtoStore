// Phantom.ProtoStore.h : Include file for standard system include files,
// or project specific include files.

#pragma once

#include <any>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <type_traits>
#include <variant>
#include <cppcoro/task.hpp>
#include <cppcoro/async_generator.hpp>
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include <Phantom.System/pooled_ptr.h>

namespace Phantom::ProtoStore 
{
using cppcoro::async_generator;
using cppcoro::task;
using google::protobuf::Message;
using std::shared_ptr;
using std::unique_ptr;

template<typename T>
concept IsMessage = std::is_convertible_v<T*, Message*>;

typedef std::string IndexName;
enum class SequenceNumber : std::uint64_t
{
    Earliest = 0,
    Latest = std::numeric_limits<std::uint64_t>::max(),
    LatestCommitted = Latest - 1,
};

inline SequenceNumber ToSequenceNumber(
    std::uint64_t sequenceNumber)
{
    if (sequenceNumber >= 0x4000000000000000ULL)
    {
        throw std::out_of_range(
            "SequenceNumber was out of range.");
    }

    return static_cast<SequenceNumber>(sequenceNumber << 2);
}

inline std::uint64_t ToUint64(
    SequenceNumber sequenceNumber)
{
    return static_cast<std::uint64_t>(sequenceNumber) >> 2;
}

typedef std::string TransactionId;

class IMessageStore;
class IIndex;

class ProtoIndex
{
    friend class ProtoStore;
    ProtoStore* m_protoStore;
    IIndex* m_index;

    ProtoIndex(
        ProtoStore* protoStore,
        IIndex* index)
        :
        m_protoStore(protoStore),
        m_index(index)
    {
    }

public:
    ProtoIndex()
        :
        m_protoStore(nullptr),
        m_index(nullptr)
    {}

    ProtoStore* ProtoStore() const;
    const IndexName& IndexName() const;
};

struct GetIndexRequest
{
    SequenceNumber SequenceNumber = SequenceNumber::Latest;
    IndexName IndexName;
};

struct KeySchema
{
    const google::protobuf::Descriptor* KeyDescriptor;
};

struct ValueSchema
{
    const google::protobuf::Descriptor* ValueDescriptor;
};

struct CreateIndexRequest
    : GetIndexRequest
{
    KeySchema KeySchema;
    ValueSchema ValueSchema;
};

class ProtoValue
{
    typedef std::variant<
        std::monostate,
        std::span<const std::byte>,
        std::vector<std::byte>
    > message_data_type;

    typedef std::variant<
        std::monostate,
        const Message*,
        unique_ptr<const Message>
    > message_type;

public:
    message_data_type message_data;
    message_type message;

    ProtoValue()
    {}

    ProtoValue(
        std::vector<std::byte> bytes)
        :
        message_data(move(bytes))
    {
    }

    ProtoValue(
        std::vector<std::byte>&& bytes)
        :
        message_data(move(bytes))
    {
    }

    ProtoValue(
        std::span<const std::byte> bytes)
        :
        message_data(bytes)
    {
    }

    ProtoValue(
        const Message* message)
        :
        message(message)
    {
    }

    const Message* as_message_if() const
    {
        {
            auto source = std::get_if<const Message*>(&message);
            if (source)
            {
                return *source;
            }
        }

        {
            auto source = std::get_if<unique_ptr<const Message>>(&message);
            if (source)
            {
                return source->get();
            }
        }

        return nullptr;
    }

    template<
        typename TMessage
    > const TMessage* cast_if() const
    {
        auto message = as_message_if();
        if (message
            &&
            message->GetDescriptor() == TMessage::descriptor())
        {
            return static_cast<const TMessage*>(message);
        }

        return nullptr;
    }

    template<
        typename TMessage
    > void unpack(
        TMessage* destination)
    {
        {
            const Message** source;
            if (source = std::get_if<const Message*>(&message))
            {
                destination->CopyFrom(**source);
            }
            return;
        }

        {
            unique_ptr<const Message>* source;
            if (source = std::get_if<unique_ptr<const Message>>(&message))
            {
                destination->CopyFrom(**source);
            }
            return;
        }

        {
            std::span<const std::byte>* source;
            if (source = std::get_if<std::span<const std::byte>>(&message_data))
            {
                destination->ParseFromArray(
                    source->data(),
                    source->size_bytes()
                );
            }
            return;
        }

        {
            std::vector<byte>* source;
            if (source = std::get_if<std::vector<byte>>(&message_data))
            {
                destination->ParseFromArray(
                    source->data(),
                    source->size()
                );
            }
            return;
        }

        destination->Clear();
    }
};

struct WriteOperation
{
    ProtoIndex Index;
    ProtoValue Key;
    ProtoValue Value;
    std::optional<SequenceNumber> OriginalSequenceNumber;
    std::optional<SequenceNumber> ExpirationSequenceNumber;
};

enum ReadValueDisposition
{
    ReadValue = 0,
    DontReadValue = 1,
};

struct ReadRequest
{
    ProtoIndex Index;
    SequenceNumber SequenceNumber = SequenceNumber::Latest;
    ProtoValue Key;
    ReadValueDisposition ReadValueDisposition = ReadValueDisposition::ReadValue;
};

enum ReadStatus
{
    HasValue = 0,
    NoValue = 1,
};

struct ReadResult
{
    SequenceNumber WriteSequenceNumber;
    ProtoValue Value;
    ReadStatus ReadStatus;
};

struct CommitTransactionRequest
{
    SequenceNumber SequenceNumber;
};

struct CommitTransactionResult
{
};

struct AbortTransactionRequest
{
    SequenceNumber SequenceNumber;
};

struct AbortTransactionResult
{
};

struct BeginTransactionRequest
{
    SequenceNumber MinimumWriteSequenceNumber = SequenceNumber::Earliest;
    SequenceNumber MinimumReadSequenceNumber = SequenceNumber::Latest;
};

struct CommitResult
{};

class IReadableProtoStore
{
public:
    virtual task<ProtoIndex> GetIndex(
        const GetIndexRequest& getIndexRequest
    ) = 0;

    virtual task<ReadResult> Read(
        ReadRequest& readRequest
    ) = 0;
};

enum class OperationOutcome {
    Unknown = 0,
    Committed = 1,
    Aborted = 2,
};

enum class TransactionOutcome {
    Committed = 0,
    Aborted = 1,
};

enum class LoggedOperationDisposition {
    Processed = 0,
    Unprocessed = 1,
};

enum class Inclusivity {
    Inclusive = 0,
    Exclusive = 1,
};

struct WriteOperationMetadata
{
    const TransactionId* TransactionId = nullptr;
    LoggedOperationDisposition LoggedOperationDisposition = LoggedOperationDisposition::Unprocessed;
};

class ProtoStoreException
    : public std::runtime_error 
{
public:
    ProtoStoreException(
        const char* message)
        : std::runtime_error(message)
    {}
};

class WriteConflict
    : public ProtoStoreException
{
public:
    WriteConflict()
        : ProtoStoreException("A write conflict occurred")
    {}
};

class UnresolvedTransactionConflict
    : public ProtoStoreException
{
public:
    UnresolvedTransactionConflict()
        : ProtoStoreException("An unresolved transaction was discovered")
    {}
};

class IWritableOperation
{
public:
    virtual task<> AddLoggedAction(
        const WriteOperationMetadata& writeOperationMetadata,
        const Message* loggedAction,
        LoggedOperationDisposition disposition
    ) = 0;

    virtual task<> AddRow(
        const WriteOperationMetadata& writeOperationMetadata,
        SequenceNumber readSequenceNumber,
        const ProtoValue& key,
        const ProtoValue& value
    ) = 0;

    virtual task<> ResolveTransaction(
        const WriteOperationMetadata& writeOperationMetadata,
        TransactionOutcome outcome
    ) = 0;
};

class IOperation
    :
    public IWritableOperation,
    public IReadableProtoStore
{
public:
};

class IOperationTransaction
    :
    public IOperation
{
    virtual task<CommitResult> Commit(
    ) = 0;
};

typedef std::function<task<>(IWritableOperation*)> WritableOperationVisitor;
typedef std::function<task<>(IOperation*)> OperationVisitor;

class IOperationProcessor
{
public:
    virtual task<> ProcessOperation(
        IOperation* resultOperation,
        WritableOperationVisitor sourceOperation
    ) = 0;
};

struct OperationResult
{
};

class IProtoStore
    : public IReadableProtoStore
{
public:
    virtual task<OperationResult> ExecuteOperation(
        const BeginTransactionRequest beginRequest,
        OperationVisitor visitor
    ) = 0;

    virtual task<ProtoIndex> CreateIndex(
        const CreateIndexRequest& createIndexRequest
    ) = 0;

    virtual task<> Join(
    ) = 0;
};

class IExtentStore;

struct OpenProtoStoreRequest
{
    std::function<task<shared_ptr<IExtentStore>>()> HeaderExtentStore;
    std::function<task<shared_ptr<IExtentStore>>()> LogExtentStore;
    std::function<task<shared_ptr<IExtentStore>>()> DataExtentStore;
    std::vector<shared_ptr<IOperationProcessor>> OperationProcessors;
};

struct CreateProtoStoreRequest
    : public OpenProtoStoreRequest
{
    size_t LogAlignment = 0;
};

class IProtoStoreFactory
{
public:
    virtual task<shared_ptr<IProtoStore>> Open(
        const OpenProtoStoreRequest& openRequest
    ) = 0;

    virtual task<shared_ptr<IProtoStore>> Create(
        const CreateProtoStoreRequest& openRequest
    ) = 0;
};

shared_ptr<IProtoStoreFactory> MakeProtoStoreFactory();
}
