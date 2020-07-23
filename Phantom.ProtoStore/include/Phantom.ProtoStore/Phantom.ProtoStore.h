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
#include <cppcoro/async_generator.hpp>
#include <cppcoro/task.hpp>
#include <cppcoro/static_thread_pool.hpp>
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
    Latest = 0x3fffffffffffffffULL,
    LatestCommitted = Latest - 1,
};

inline SequenceNumber ToSequenceNumber(
    std::uint64_t sequenceNumber)
{
    if (sequenceNumber > static_cast<uint64_t>(SequenceNumber::Latest))
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

inline std::weak_ordering operator <=>(
    SequenceNumber s1,
    SequenceNumber s2
    )
{
    return ToUint64(s1) <=> ToUint64(s2);
}

typedef std::string TransactionId;

class IMessageStore;
class IIndex;

class ProtoIndex
{
    friend class ProtoStore;
    friend class Operation;
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
        std::string
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
        std::unique_ptr<Message>&& other)
    {
        if (other)
        {
            message = move(other);
        }
    }

    ProtoValue(
        std::string bytes)
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
        const Message* other)
    {
        if (other)
        {
            message = other;
        }
    }

    explicit operator bool() const
    {
        return has_value();
    }

    bool operator !() const
    {
        return message.index() == 0
            && message_data.index() == 0;
    }

    bool has_value() const
    {
        return message.index() != 0
            || message_data.index() != 0;
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
        typename TMessage = Message
    > void unpack(
        TMessage* destination
    ) const
    {
        {
            auto source = std::get_if<const Message*>(&message);
            if (source)
            {
                destination->CopyFrom(**source);
                return;
            }
        }

        {
            auto source = std::get_if<unique_ptr<const Message>>(&message);
            if (source)
            {
                destination->CopyFrom(**source);
                return;
            }
        }

        {
            auto source = std::get_if<std::string>(&message_data);
            if (source)
            {
                destination->ParseFromString(
                    *source
                );
                return;
            }
        }

        {
            auto source = std::get_if<std::span<const std::byte>>(&message_data);
            if (source)
            {
                destination->ParseFromArray(
                    source->data(),
                    source->size_bytes()
                );
                return;
            }
        }

        destination->Clear();
    }

    bool pack(
        std::string* destination
    ) const
    {
        {
            auto source = std::get_if<std::string>(&message_data);
            if (source)
            {
                *destination = *source;
                return true;
            }
        }

        {
            auto source = std::get_if<std::span<const std::byte>>(&message_data);
            if (source)
            {
                destination->resize(source->size());
                std::copy(
                    source->begin(),
                    source->end(),
                    reinterpret_cast<std::byte*>(destination->data())
                );
                return true;
            }
        }

        {
            auto source = std::get_if<const Message*>(&message);
            if (source)
            {
                (*source)->SerializeToString(
                    destination);
                return true;
            }
        }

        {
            auto source = std::get_if<unique_ptr<const Message>>(&message);
            if (source)
            {
                (*source)->SerializeToString(
                    destination);
                return true;
            }
        }

        return false;
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
    SequenceNumber SequenceNumber = SequenceNumber::LatestCommitted;
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

enum class Inclusivity {
    Inclusive = 0,
    Exclusive = 1,
};

struct EnumerateRequest
{
    ProtoIndex Index;
    SequenceNumber SequenceNumber = SequenceNumber::LatestCommitted;
    ProtoValue KeyLow;
    Inclusivity KeyLowInclusivity;
    ProtoValue KeyHigh;
    Inclusivity KeyHighInclusivity;
};

struct EnumerateResult
{
    ProtoValue Key;
    SequenceNumber WriteSequenceNumber;
    ProtoValue Value;
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
    SequenceNumber MinimumReadSequenceNumber = SequenceNumber::LatestCommitted;
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
        ProtoIndex protoIndex,
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

class IJoinable
{
public:
    virtual task<> Join(
    ) = 0;
};

class IScheduler
{
public:
    class schedule_operation
    {
        friend class IScheduler;
        IScheduler* m_scheduler;
        std::any m_value;

        schedule_operation(
            IScheduler* scheduler,
            std::any&& value)
            : m_scheduler(scheduler),
            m_value(move(value))
        {}

    public:
        bool await_ready() noexcept
        {
            return m_scheduler->await_ready(
                &m_value);
        }

        void await_suspend(std::experimental::coroutine_handle<> awaitingCoroutine) noexcept
        {
            m_scheduler->await_suspend(
                &m_value,
                awaitingCoroutine);
        }

        void await_resume()
        {
            return m_scheduler->await_resume(
                &m_value);
        }

    };
    
    virtual std::any create_schedule_operation_value(
    ) = 0;

    virtual bool await_ready(
        std::any* scheduleOperationValue
    ) noexcept = 0;

    virtual void await_suspend(
        std::any* scheduleOperationValue,
        std::experimental::coroutine_handle<> awaitingCoroutine
    ) noexcept = 0;

    virtual void await_resume(
        std::any* scheduleOperationValue
    ) noexcept = 0;

    schedule_operation schedule()
    {
        return schedule_operation(
            this,
            create_schedule_operation_value());
    }

    schedule_operation operator co_await()
    {
        return schedule();
    }
};

template<
    typename TScheduler
> class DefaultScheduler
    :
    public IScheduler
{
    TScheduler m_scheduler;
    typedef decltype(m_scheduler.schedule()) underlying_schedule_operation;

public:
    template<
        typename ... TArgs
    >
    DefaultScheduler(
        TArgs&& ... args
    ) : m_scheduler(std::forward<TArgs>(args)...)
    {}

    virtual std::any create_schedule_operation_value(
    ) override
    {
        return m_scheduler.schedule();
    }

    virtual bool await_ready(
        std::any* value
    ) noexcept override
    {
        return std::any_cast<underlying_schedule_operation>(value)->await_ready();
    }

    virtual void await_suspend(
        std::any* value,
        std::experimental::coroutine_handle<> awaitingCoroutine
    ) noexcept override
    {
        return std::any_cast<underlying_schedule_operation>(value)->await_suspend(
            awaitingCoroutine);
    }

    virtual void await_resume(
        std::any* value
    ) noexcept override
    {
        return std::any_cast<underlying_schedule_operation>(value)->await_resume();
    }
};

class IProtoStore
    : 
    public IReadableProtoStore,
    public virtual IJoinable
{
public:
    virtual task<OperationResult> ExecuteOperation(
        const BeginTransactionRequest beginRequest,
        OperationVisitor visitor
    ) = 0;

    virtual task<ProtoIndex> CreateIndex(
        const CreateIndexRequest& createIndexRequest
    ) = 0;

    virtual task<> Checkpoint(
    ) = 0;

};

class IExtentStore;

struct Schedulers
{
    std::shared_ptr<IScheduler> LockScheduler;
    std::shared_ptr<IScheduler> IoScheduler;
    std::shared_ptr<IScheduler> ComputeScheduler;

    static Schedulers Default();
};

struct OpenProtoStoreRequest
{
    std::function<task<shared_ptr<IExtentStore>>()> HeaderExtentStore;
    std::function<task<shared_ptr<IExtentStore>>()> LogExtentStore;
    std::function<task<shared_ptr<IExtentStore>>()> DataExtentStore;
    std::function<task<shared_ptr<IExtentStore>>()> DataHeaderExtentStore;
    std::vector<shared_ptr<IOperationProcessor>> OperationProcessors;
    Schedulers Schedulers = Schedulers::Default();
    uint64_t CheckpointLogSize = 10 * 1024 * 1024;
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
