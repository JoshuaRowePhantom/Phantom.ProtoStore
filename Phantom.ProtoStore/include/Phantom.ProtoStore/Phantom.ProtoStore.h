// Phantom.ProtoStore.h : Include file for standard system include files,
// or project specific include files.

#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <system_error>
#include <type_traits>
#include <variant>
#include <cppcoro/async_generator.hpp>
#include <google/protobuf/message.h>
#include <Phantom.System/concepts.h>
#include "Phantom.ProtoStore/ProtoStore.pb.h"
#include "Phantom.ProtoStore/ProtoStore_generated.h"
#include "Async.h"
#include "Errors.h"
#include "Payloads.h"
#include "Primitives.h"
#include "Scheduler.h"
#include "Schema.h"

namespace Phantom::ProtoStore
{

namespace FlatBuffersSchemas
{
extern const reflection::Schema* const ProtoStoreSchema;
extern const reflection::Object* const ExtentName_Object;
extern const reflection::Object* const IndexHeaderExtentName_Object;
extern const ProtoValueComparers ExtentNameComparers;
}

using FlatBuffers::ExtentNameT;
using FlatBuffers::ExtentName;

template<typename T>
concept IsMessage = std::is_convertible_v<T*, google::protobuf::Message*>;

class IMessageStore;

struct GetIndexRequest
{
    SequenceNumber SequenceNumber = SequenceNumber::Latest;
    IndexName IndexName;

    friend bool operator==(
        const GetIndexRequest&,
        const GetIndexRequest&
        ) = default;
};

struct CreateIndexRequest
    : GetIndexRequest
{
    Schema Schema;

    friend bool operator==(
        const CreateIndexRequest&,
        const CreateIndexRequest&
        ) = default;
};

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

using EnumerateResultGenerator = async_generator<OperationResult<EnumerateResult>>;

class IReadableProtoStore
{
public:
    virtual task<ProtoIndex> GetIndex(
        const GetIndexRequest& getIndexRequest
    ) = 0;

    virtual operation_task<ReadResult> Read(
        const ReadRequest& readRequest
    ) = 0;

    virtual EnumerateResultGenerator Enumerate(
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
        const google::protobuf::Message* loggedAction,
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

enum class IntegrityCheck
{
    CheckPartitionOnOpen = 1,
    CheckPartitionOnWrite = 2,
};

struct OpenProtoStoreRequest
{
    std::function<task<std::shared_ptr<IExtentStore>>()> ExtentStore;
    std::vector<std::shared_ptr<IOperationProcessor>> OperationProcessors;
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
    FlatValue<FlatBuffers::ExtentName> extentName;
    ExtentOffset extentOffset;

    friend bool operator==(
        const ExtentLocation&,
        const ExtentLocation&
        ) = default;
};

struct IntegrityCheckError
{
    IntegrityCheckErrorCode Code;
    ProtoValue Key;
    ExtentLocation Location;
    std::optional<int> TreeNodeEntryIndex;
    std::optional<int> TreeNodeValueIndex;
    // std::shared_ptr<FlatMessage<flatbuffers::Table>>
    std::shared_ptr<void> PartitionMessage;

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
    virtual task<std::shared_ptr<IProtoStore>> Open(
        const OpenProtoStoreRequest& openRequest
    ) = 0;

    virtual task<std::shared_ptr<IProtoStore>> Create(
        const CreateProtoStoreRequest& openRequest
    ) = 0;
};

std::shared_ptr<IProtoStoreFactory> MakeProtoStoreFactory();
}
