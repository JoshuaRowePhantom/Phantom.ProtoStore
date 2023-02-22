// Phantom.ProtoStore.h : Include file for standard system include files,
// or project specific include files.

#pragma once

#include <any>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <system_error>
#include <type_traits>
#include <variant>
#include <cppcoro/async_generator.hpp>
#include <cppcoro/static_thread_pool.hpp>
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include <Phantom.System/pooled_ptr.h>
#include "Phantom.ProtoStore/ProtoStore.pb.h"
#include "Phantom.Coroutines/early_termination_task.h"
#include "Phantom.Coroutines/expected_early_termination.h"
#include "Phantom.Coroutines/reusable_task.h"
#include "flatbuffers/flatbuffers.h"

namespace Phantom::ProtoStore
{
namespace FlatBuffers
{
enum class ExtentFormatVersion : uint8_t;
}

using cppcoro::async_generator;

template<
    typename Result = void
> using StatusResult = std::expected<Result, std::error_code>;

template<
    typename Result = void
> using status_task =
Phantom::Coroutines::basic_reusable_task
<
    Phantom::Coroutines::derived_promise
    <
        Phantom::Coroutines::reusable_task_promise
        <
            StatusResult<Result>
        >,
        Phantom::Coroutines::expected_early_termination_transformer,
        Phantom::Coroutines::await_all_await_transform
    >
>;

struct FailedResult;

template<
    typename Result
>
using OperationResult = std::expected<Result, FailedResult>;

template<
    typename Result = void
> using operation_task =
Phantom::Coroutines::basic_reusable_task
<
    Phantom::Coroutines::derived_promise
    <
        Phantom::Coroutines::reusable_task_promise
        <
            OperationResult<Result>
        >,
        Phantom::Coroutines::expected_early_termination_transformer,
        Phantom::Coroutines::await_all_await_transform
    >
>;

extern const std::error_category& ProtoStoreErrorCategory();

enum class ProtoStoreErrorCode
{
    AbortedTransaction,
    WriteConflict,
    UnresolvedTransaction,
};

std::error_code make_error_code(
    ProtoStoreErrorCode errorCode
);

std::unexpected<std::error_code> make_unexpected(
    ProtoStoreErrorCode errorCode
);

// Operation processors can return this error code
// to generically abort the operation.
std::unexpected<std::error_code> abort_transaction();

template<
    typename Result = void
> using task =
Phantom::Coroutines::reusable_task<Result>;

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
    friend class LocalTransaction;
    IIndex* m_index;

public:
    ProtoIndex()
        :
        m_index(nullptr)
    {}

    ProtoIndex(
        IIndex* index)
        :
        m_index(index)
    {
    }

    ProtoIndex(
        shared_ptr<IIndex> index)
        :
        m_index(index.get())
    {
    }

    ProtoStore* ProtoStore() const;
    const IndexName& IndexName() const;

    friend bool operator==(
        const ProtoIndex&,
        const ProtoIndex&
        ) = default;
};

struct GetIndexRequest
{
    SequenceNumber SequenceNumber = SequenceNumber::Latest;
    IndexName IndexName;

    friend bool operator==(
        const GetIndexRequest&,
        const GetIndexRequest&
        ) = default;
};

struct KeySchema
{
    const google::protobuf::Descriptor* KeyDescriptor;

    friend bool operator==(
        const KeySchema&,
        const KeySchema&
        ) = default;
};

struct ValueSchema
{
    const google::protobuf::Descriptor* ValueDescriptor;

    friend bool operator==(
        const ValueSchema&,
        const ValueSchema&
        ) = default;
};

struct CreateIndexRequest
    : GetIndexRequest
{
    KeySchema KeySchema;
    ValueSchema ValueSchema;

    friend bool operator==(
        const CreateIndexRequest&,
        const CreateIndexRequest&
        ) = default;
};

template<
    typename Data
>
class DataReference
{
    template<
        typename Data
    >
    friend class DataReference;

    std::shared_ptr<void> m_dataHolder;
    Data m_data;

public:
    DataReference(
        nullptr_t = nullptr
    )
    {}

    DataReference(
        std::shared_ptr<void> dataHolder,
        Data data
    ) noexcept :
        m_dataHolder { std::move(dataHolder) },
        m_data{ std::move(data) }
    {}

    DataReference(
        const DataReference&
    ) = default;

    DataReference(
        DataReference&& other
    ) : 
        m_dataHolder { std::move(other.m_dataHolder) },
        m_data{ std::move(other.m_data) }
    {
        other.m_data = {};
    }

    template<
        typename Other
    >
    explicit DataReference(
        DataReference<Other>&& other,
        Data data
    ) :
        m_dataHolder{ std::move(other.m_dataHolder) },
        m_data{ data }
    {
        other.m_data = Other{};
    }

    DataReference& operator=(const DataReference& other) = default;

    auto& operator=(DataReference&& other)
    {
        if (&other != this)
        {
            m_dataHolder = std::move(other.m_dataHolder);
            m_data = other.m_data;
            other.m_dataHolder = nullptr;
            other.m_data = {};
        }

        return *this;
    }

    operator bool() const noexcept
        requires std::convertible_to<Data, bool>
    {
        return static_cast<bool>(m_data);
    }

    const Data& data() const noexcept
    {
        return m_data;
    }

    const Data* operator->() const noexcept
    {
        return std::addressof(m_data);
    }

    const Data& operator*() const noexcept
    {
        return m_data;
    }
};

using RawData = DataReference<std::span<const std::byte>>;
using WritableRawData = DataReference<std::span<std::byte>>;

typedef std::uint64_t ExtentOffset;

struct ExtentOffsetRange
{
    ExtentOffset Beginning;
    ExtentOffset End;
};


struct StoredMessage
{
    // The format version of the extent the message was stored in.
    FlatBuffers::ExtentFormatVersion ExtentFormatVersion;
    
    // The alignment requirement of the message in bytes.
    uint8_t MessageAlignment = 0;

    // The message body itself.
    std::span<const std::byte> Message;
    // The header for the message.
    std::span<const std::byte> Header;

    // The range the message was stored in.
    ExtentOffsetRange DataRange;
};

template<
    typename Table
> class FlatMessage
{
    DataReference<StoredMessage> m_storedMessage;

public:
    FlatMessage()
    {}

    explicit FlatMessage(
        DataReference<StoredMessage> storedMessage
    ) :
        m_storedMessage{ std::move(storedMessage) }
    {}

    explicit FlatMessage(
        uint8_t messageAlignment,
        std::span<const std::byte> message
    ) :
        m_storedMessage
    {
        nullptr,
        {
            .ExtentFormatVersion = FlatBuffers::ExtentFormatVersion::None,
            .MessageAlignment = messageAlignment,
            .Message = message,
        },
    }
    {
    }

    explicit FlatMessage(
        const flatbuffers::FlatBufferBuilder& builder
    ) :
        m_storedMessage
    {
        nullptr,
        {
            .ExtentFormatVersion = FlatBuffers::ExtentFormatVersion::None,
            .MessageAlignment = static_cast<uint8_t>(builder.GetBufferMinAlignment()),
            .Message = 
            { 
                reinterpret_cast<const std::byte*>(builder.GetBufferPointer()), 
                builder.GetSize() 
            },
        },
    }
    {
    }

    const StoredMessage& data() const noexcept
    {
        return *m_storedMessage;
    }

    const Table* get() const noexcept
    {
        if (!*this)
        {
            return nullptr;
        }

        return flatbuffers::GetRoot<Table>(
            m_storedMessage->Message.data()
        );
    }

    explicit operator bool() const noexcept
    {
        return m_storedMessage->Message.data();
    }

    const Table* operator->() const noexcept
    {
        return get();
    }
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

    static ProtoValue KeyMin();
    static ProtoValue KeyMax();

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

    bool IsKeyMin() const;
    bool IsKeyMax() const;
};

struct WriteConflict
{
    ProtoIndex Index;
    SequenceNumber ConflictingSequenceNumber;

    friend bool operator ==(
        const WriteConflict&,
        const WriteConflict&
        ) = default;
};

struct UnresolvedTransaction
{
    TransactionId UnresolvedTransactionId;

    friend bool operator ==(
        const UnresolvedTransaction&,
        const UnresolvedTransaction&
        ) = default;
};

enum class TransactionOutcome {
    Unknown = 0,
    Committed = 1,
    Aborted = 2,
};

struct TransactionFailedResult
{
    // The transaction outcome,
    // which will be either Aborted or Unknown.
    TransactionOutcome TransactionOutcome;

    // The failures that were encountered during
    // execution of the transaction. These failures
    // may have been ignored by the actual transaction
    // processor.
    std::vector<FailedResult> Failures;

    friend bool operator==(
        const TransactionFailedResult&,
        const TransactionFailedResult&
        ) = default;
};

struct FailedResult
{
    std::error_code ErrorCode;

    using error_details_type = std::variant<
        std::monostate,
        WriteConflict,
        UnresolvedTransaction,
        TransactionFailedResult
    >;

    error_details_type ErrorDetails;

    operator std::error_code() const
    {
        return ErrorCode;
    }

    operator std::unexpected<std::error_code>() const
    {
        return std::unexpected{ ErrorCode };
    }

    operator std::unexpected<FailedResult>() const &
    {
        return std::unexpected{ *this };
    }

    operator std::unexpected<FailedResult>() &&
    {
        return std::unexpected{ std::move(*this) };
    }

    friend bool operator ==(
        const FailedResult&,
        const FailedResult&
        ) = default;

    [[noreturn]]
    void throw_exception(
        this auto&& self);
};

class ProtoStoreException : std::exception
{
public:
    const FailedResult Result;

    ProtoStoreException(
        FailedResult result
    ) : Result(std::move(result))
    {
    }
};

[[noreturn]]
void FailedResult::throw_exception(
    this auto&& self)
{
    throw ProtoStoreException{ std::forward<decltype(self)>(self) };
}

struct WriteOperation
{
    ProtoIndex Index;
    ProtoValue Key;
    ProtoValue Value;
    std::optional<SequenceNumber> OriginalSequenceNumber;
    std::optional<SequenceNumber> ExpirationSequenceNumber;

    friend bool operator==(
        const WriteOperation&,
        const WriteOperation&
        ) = default;
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

    friend bool operator==(
        const ReadRequest&,
        const ReadRequest&
        ) = default;
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

    friend bool operator==(
        const ReadResult&,
        const ReadResult&
        ) = default;
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

    friend bool operator==(
        const EnumerateRequest&,
        const EnumerateRequest&
        ) = default;
};

struct EnumerateResult : ReadResult
{
    ProtoValue Key;
};

struct CommitTransactionRequest
{
    SequenceNumber SequenceNumber;

    friend bool operator==(
        const CommitTransactionRequest&,
        const CommitTransactionRequest&
        ) = default;
};

struct CommitTransactionResult
{
    friend bool operator==(
        const CommitTransactionResult&,
        const CommitTransactionResult&
        ) = default;
};

struct AbortTransactionRequest
{
    SequenceNumber SequenceNumber;

    friend bool operator==(
        const AbortTransactionRequest&,
        const AbortTransactionRequest&
        ) = default;
};

struct AbortTransactionResult
{
    friend bool operator==(
        const AbortTransactionResult&,
        const AbortTransactionResult&
        ) = default;
};

struct BeginTransactionRequest
{
    SequenceNumber MinimumWriteSequenceNumber = SequenceNumber::Earliest;
    SequenceNumber MinimumReadSequenceNumber = SequenceNumber::LatestCommitted;
    
    friend bool operator==(
        const BeginTransactionRequest&,
        const BeginTransactionRequest&
        ) = default;
};

struct CommitResult
{
    TransactionOutcome Outcome;

    friend bool operator==(
        const CommitResult&,
        const CommitResult&
        ) = default;
};

class IReadableProtoStore
{
public:
    virtual task<ProtoIndex> GetIndex(
        const GetIndexRequest& getIndexRequest
    ) = 0;

    virtual operation_task<ReadResult> Read(
        const ReadRequest& readRequest
    ) = 0;

    virtual async_generator<OperationResult<EnumerateResult>> Enumerate(
        const EnumerateRequest& enumerateRequest
    ) = 0;
};

enum class LoggedOperationDisposition {
    Processed = 0,
    Unprocessed = 1,
};

struct WriteOperationMetadata
{
    const TransactionId* TransactionId = nullptr;
    LoggedOperationDisposition LoggedOperationDisposition = LoggedOperationDisposition::Unprocessed;
    std::optional<SequenceNumber> ReadSequenceNumber;
    std::optional<SequenceNumber> WriteSequenceNumber;
};

}

namespace std
{
template<>
struct is_error_code_enum<Phantom::ProtoStore::ProtoStoreErrorCode> : true_type
{};
}

namespace Phantom::ProtoStore
{

class IWritableTransaction
{
public:
    virtual operation_task<> AddLoggedAction(
        const WriteOperationMetadata& writeOperationMetadata,
        const Message* loggedAction,
        LoggedOperationDisposition disposition
    ) = 0;

    virtual operation_task<> AddRow(
        const WriteOperationMetadata& writeOperationMetadata,
        ProtoIndex protoIndex,
        const ProtoValue& key,
        const ProtoValue& value
    ) = 0;

    virtual operation_task<> ResolveTransaction(
        const WriteOperationMetadata& writeOperationMetadata,
        TransactionOutcome outcome
    ) = 0;
};

class ITransaction
    :
    public IWritableTransaction,
    public IReadableProtoStore
{
public:
};

class ICommittableTransaction
    :
    public ITransaction
{
public:
    virtual operation_task<CommitResult> Commit(
    ) = 0;
};

typedef std::function<status_task<>(IWritableTransaction*)> WritableTransactionVisitor;
typedef std::function<status_task<>(ICommittableTransaction*)> TransactionVisitor;

class IOperationProcessor
{
public:
    //virtual task<> ProcessOperation(
    //    ITransaction* resultOperation,
    //    WritableOperationVisitor sourceOperation
    //) = 0;
};

struct TransactionSucceededResult
{
    // The transaction outcome,
    // which will be either Committed or ReadOnly.
    TransactionOutcome m_transactionOutcome;

    friend bool operator==(
        const TransactionSucceededResult&,
        const TransactionSucceededResult&
        ) = default;
};

using TransactionResult = std::expected<
    TransactionSucceededResult,
    FailedResult
>;

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

        void await_suspend(std::coroutine_handle<> awaitingCoroutine) noexcept
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
        std::coroutine_handle<> awaitingCoroutine
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
        std::coroutine_handle<> awaitingCoroutine
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
    virtual operation_task<TransactionSucceededResult> ExecuteTransaction(
        const BeginTransactionRequest beginRequest,
        TransactionVisitor visitor
    ) = 0;

    virtual operation_task<ProtoIndex> CreateIndex(
        const CreateIndexRequest& createIndexRequest
    ) = 0;

    virtual task<> Checkpoint(
    ) = 0;

    virtual task<> Merge(
    ) = 0;
};

class IExtentStore;

struct Schedulers
{
    std::shared_ptr<IScheduler> LockScheduler;
    std::shared_ptr<IScheduler> IoScheduler;
    std::shared_ptr<IScheduler> ComputeScheduler;

    static Schedulers Default();
    static Schedulers Inline();
};

enum class IntegrityCheck
{
    CheckPartitionOnOpen = 1,
    CheckPartitionOnWrite = 2,
};

struct OpenProtoStoreRequest
{
    std::function<task<shared_ptr<IExtentStore>>()> ExtentStore;
    std::vector<shared_ptr<IOperationProcessor>> OperationProcessors;
    Schedulers Schedulers = Schedulers::Default();
    uint64_t CheckpointLogSize = 10 * 1024 * 1024;
    MergeParameters DefaultMergeParameters;

    std::set<IntegrityCheck> IntegrityChecks = 
    {
#ifndef NDEBUG
        IntegrityCheck::CheckPartitionOnOpen,
#endif
    };

    OpenProtoStoreRequest();

    friend bool operator==(
        const OpenProtoStoreRequest&,
        const OpenProtoStoreRequest&
        ) = default;
};

struct CreateProtoStoreRequest
    : public OpenProtoStoreRequest
{
    size_t LogAlignment = 0;

    friend bool operator==(
        const CreateProtoStoreRequest&,
        const CreateProtoStoreRequest&
        ) = default;
};

enum class IntegrityCheckErrorCode
{
    Partition_KeyNotInBloomFilter = 1,
    Partition_MissingTreeNode = 2,
    Partition_MissingTreeNodeEntryContent = 4,
    Partition_OutOfOrderKey = 5,
    Partition_OutOfOrderSequenceNumber = 6,
    Partition_SequenceNumberOutOfMinRange = 7,
    Partition_SequenceNumberOutOfMaxRange = 8,
    Partition_KeyOutOfMinRange = 9,
    Partition_KeyOutOfMaxRange = 10,
    Partition_NonLeafNodeNeedsChild = 11,
    Partition_LeafNodeNeedsValueOrValueSet = 12,
    Partition_LeafNodeHasChild = 13,
    Partition_NoContentInTreeEntry = 14,
};

struct ExtentLocation
{
    ExtentName extentName;
    ExtentOffset extentOffset;

    friend bool operator==(
        const ExtentLocation&,
        const ExtentLocation&
        ) = default;
};

struct IntegrityCheckError
{
    IntegrityCheckErrorCode Code;
    std::optional<std::string> Key;
    std::optional<ExtentLocation> Location;
    std::optional<int> TreeNodeEntryIndex;
    std::optional<int> TreeNodeValueIndex;

    friend bool operator==(
        const IntegrityCheckError&,
        const IntegrityCheckError&
        ) = default;
};

typedef std::vector<IntegrityCheckError> IntegrityCheckErrorList;

class IntegrityException : std::exception
{
public:
    const IntegrityCheckErrorList Errors;

    IntegrityException(
        IntegrityCheckErrorList errors
    ) : exception("Integrity check error"),
        Errors(errors)
    {}
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
