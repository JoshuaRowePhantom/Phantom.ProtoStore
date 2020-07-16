#pragma once

#include <Phantom.ProtoStore/Phantom.ProtoStore.h>
#include "StandardTypes.h"
#include "Phantom.System/async_reader_writer_lock.h"

namespace Phantom::ProtoStore
{

class IIndex;

class ProtoStore
    :
    public IProtoStore
{
    const shared_ptr<IExtentStore> m_extentStore;
    const shared_ptr<IMessageStore> m_messageStore;
    const shared_ptr<IRandomMessageAccessor> m_messageAccessor;
    const shared_ptr<IHeaderAccessor> m_headerAccessor;

    async_reader_writer_lock m_indexesByNumberLock;

    shared_ptr<IIndex> m_indexesByNumberIndex;
    shared_ptr<IIndex> m_indexesByNameIndex;
    shared_ptr<IIndex> m_nextIndexNumberIndex;

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
        shared_ptr<IExtentStore> extentStore);

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