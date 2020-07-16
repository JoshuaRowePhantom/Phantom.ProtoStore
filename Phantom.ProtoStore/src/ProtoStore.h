#pragma once

#include <Phantom.ProtoStore/Phantom.ProtoStore.h>
#include "StandardTypes.h"
#include "Phantom.System/async_reader_writer_lock.h"
#include <cppcoro/async_mutex.hpp>
#include <cppcoro/async_scope.hpp>
#include <cppcoro/inline_scheduler.hpp>
#include <cppcoro/sequence_barrier.hpp>
#include <cppcoro/shared_task.hpp>

namespace Phantom::ProtoStore
{

class IIndex;

class ProtoStore
    :
    public IProtoStore
{
    const shared_ptr<IExtentStore> m_headerExtentStore;
    const shared_ptr<IExtentStore> m_logExtentStore;
    const shared_ptr<IExtentStore> m_dataExtentStore;
    const shared_ptr<IMessageStore> m_headerMessageStore;
    const shared_ptr<IMessageStore> m_logMessageStore;
    const shared_ptr<IMessageStore> m_dataMessageStore;
    const shared_ptr<IRandomMessageAccessor> m_headerMessageAccessor;
    const shared_ptr<IRandomMessageAccessor> m_dataMessageAccessor;
    const shared_ptr<IHeaderAccessor> m_headerAccessor;

    cppcoro::async_scope m_asyncScope;
    cppcoro::inline_scheduler m_inlineScheduler;
    cppcoro::sequence_barrier<uint64_t> m_writeSequenceNumberBarrier;
    std::atomic<uint64_t> m_nextWriteSequenceNumber;

    cppcoro::async_mutex m_headerMutex;
    async_reader_writer_lock m_indexesByNumberLock;

    shared_ptr<IIndex> m_indexesByNumberIndex;
    shared_ptr<IIndex> m_indexesByNameIndex;
    shared_ptr<IIndex> m_nextIndexNumberIndex;

    shared_ptr<ISequentialMessageWriter> m_logWriter;

    cppcoro::shared_task<> m_joinTask;

    typedef unordered_map<google::protobuf::uint64, shared_ptr<IIndex>> IndexesByNumberMap;
    IndexesByNumberMap m_indexesByNumber;

    task<shared_ptr<IIndex>> GetIndexInternal(
        const string& indexName,
        SequenceNumber sequenceNumber
    );

    task<shared_ptr<IIndex>> GetIndexInternal(
        google::protobuf::uint64 indexNumber
    );

    void MakeIndexesByNumberRow(
        IndexesByNumberKey& indexesByNumberKey,
        IndexesByNumberValue& indexesByNumberValue,
        const IndexName& indexName,
        IndexNumber indexNumber,
        SequenceNumber createSequenceNumber,
        const Descriptor* keyDescriptor,
        const Descriptor* valueDescriptor
    );

    shared_ptr<IIndex> MakeIndex(
        const IndexesByNumberKey& indexesKey,
        const IndexesByNumberValue& indexesValue
    );

    task<IndexNumber> AllocateIndexNumber();
    cppcoro::shared_task<> InternalJoinTask();
    
    task<> Replay(
        ExtentNumber logExtent);

    task<> Replay(
        const LogRecord& logRecord);

    task<> WriteLogRecord(
        const LogRecord& logRecord);

    task<> OpenLogWriter();

    task<> UpdateHeader(
        std::function<task<>(Header&)> modifier
    );

    friend class Operation;

public:

    // Noncopyable, nonmovable.
    ProtoStore(
        const ProtoStore&
    ) = delete;

    ProtoStore(
        ProtoStore&&
    ) = delete;

    ProtoStore& operator=(
        const ProtoStore&
        ) = delete;

    ProtoStore& operator=(
        ProtoStore&&
        ) = delete;

    ProtoStore(
        shared_ptr<IExtentStore> headerStore,
        shared_ptr<IExtentStore> logStore,
        shared_ptr<IExtentStore> dataStore);

    ~ProtoStore();

    task<> Open(
        const OpenProtoStoreRequest& openRequest
    );

    task<> Create(
        const CreateProtoStoreRequest& openRequest
    );

    virtual task<OperationResult> ExecuteOperation(
        const BeginTransactionRequest beginRequest,
        OperationVisitor visitor
    ) override;

    virtual task<ProtoIndex> CreateIndex(
        const CreateIndexRequest& createIndexRequest
    ) override;

    virtual task<ProtoIndex> GetIndex(
        const GetIndexRequest& getIndexRequest
    ) override;

    virtual task<ReadResult> Read(
        ReadRequest& readRequest
    ) override;

    virtual task<> Join(
    ) override;
};
}