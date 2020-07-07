﻿// Phantom.ProtoStore.h : Include file for standard system include files,
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
    Latest = std::numeric_limits<std::uint64_t>::max(),
    LatestCommitted = Latest - 1,
};

typedef std::string TransactionId;

class ProtoStore;
class IMessageStore;

class ProtoIndex
{
    class Impl;
    Impl* m_pImpl;

public:
    ProtoStore* ProtoStore() const;
    const IndexName& IndexName() const;
};

struct GetIndexRequest
{
    SequenceNumber SequenceNumber = SequenceNumber::Latest;
    IndexName IndexName;
};

typedef std::vector<const google::protobuf::FieldDescriptor*> DescendingFieldPath;
typedef std::set<DescendingFieldPath> DescendingFields;

struct KeySchema
{
    google::protobuf::Descriptor* KeyDescriptor;

    DescendingFields DescendingFields;
};

struct ValueSchema
{
    google::protobuf::Descriptor* ValueDescriptor;
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
        Message*,
        unique_ptr<Message>
    > message_type;

public:
    message_data_type message_data;
    message_type message;

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
        Message* message)
        :
        message(message)
    {
    }

    operator Message && ();
};

struct WriteOperation
{
    ProtoIndex Index;
    ProtoValue Key;
    ProtoValue Value;
    std::optional<SequenceNumber> OriginalSequenceNumber;
    std::optional<SequenceNumber> ExpirationSequenceNumber;
};

enum class WriteRequestCommitBehavior
{
    Prepare,
    Commit,
};

struct ReadOperation
{
    ProtoIndex Index;
    SequenceNumber SequenceNumber = SequenceNumber::Latest;
    ProtoValue Key;
};

struct ReadRequest
{
};

struct ReadOperationResult
{
};

struct ReadResult
{
    SequenceNumber SequenceNumber;
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
    SequenceNumber MinimumWriteSequenceNumber;
    SequenceNumber MinimumReadSequenceNumber;
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
    Commit = 0,
    Abort = 1,
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
    const TransactionId* TransactionId;
    LoggedOperationDisposition LoggedOperationDisposition;
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

    virtual task<ProtoIndex> CreateIndex(
        const WriteOperationMetadata& writeOperationMetadata,
        const CreateIndexRequest& createIndexRequest
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
typedef std::function<task<OperationOutcome>(IOperation*)> OperationVisitor;

class IOperationProcessor
{
public:
    virtual task<> ProcessOperation(
        IOperation* resultOperation,
        WritableOperationVisitor sourceOperation
    ) = 0;
};

class IProtoStore
    : public IReadableProtoStore
{
public:
    virtual task<OperationOutcome> ExecuteOperation(
        const BeginTransactionRequest beginRequest,
        OperationVisitor visitor
    ) = 0;
};

class IExtentStore;

struct OpenProtoStoreRequest
{
    std::function<task<shared_ptr<IExtentStore>>()> ExtentStore;
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
