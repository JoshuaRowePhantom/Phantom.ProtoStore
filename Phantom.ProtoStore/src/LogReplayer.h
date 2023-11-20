#pragma once
#include "InternalProtoStore.h"
#include "HeaderAccessor.h"
#include "LogExtentUsageMap.h"
#include "Phantom.Coroutines/async_scope.h"
#include "Phantom.Coroutines/async_manual_reset_event.h"
#include "boost/unordered/concurrent_flat_map.hpp"

namespace Phantom::ProtoStore
{

class LogReplayer
{
    Schedulers& m_schedulers;
    IProtoStoreReplayTarget& m_protoStore;
    IHeaderAccessor& m_headerAccessor;
    IMessageStore& m_logMessageStore;
    
    Phantom::Coroutines::async_scope<> m_handleLoggedCommitTransactionScope;
    Phantom::Coroutines::async_manual_reset_event<> m_handleLoggedCommitTransactionScopeComplete;
    Phantom::Coroutines::async_scope<> m_handleLoggedCheckpointScope;
    Phantom::Coroutines::async_manual_reset_event<> m_handleLoggedCheckpointScopeComplete;
    Phantom::Coroutines::async_scope<> m_handleLoggedPartitionsDataScope;
    Phantom::Coroutines::async_manual_reset_event<> m_handleLoggedPartitionsDataScopeComplete;
    Phantom::Coroutines::async_scope<> m_handleLoggedRowWriteScope;
    Phantom::Coroutines::async_manual_reset_event<> m_handleLoggedRowWriteScopeComplete;

    Phantom::Coroutines::async_scope<> m_handleLoggedRowWrite_System_Indexes_Scope;
    Phantom::Coroutines::async_manual_reset_event<> m_handleLoggedRowWrite_System_Indexes_ScopeComplete;
    Phantom::Coroutines::async_scope<> m_handleLoggedRowWrite_User_Indexes_Scope;
    Phantom::Coroutines::async_manual_reset_event<> m_handleLoggedRowWrite_User_Indexes_ScopeComplete;

    Phantom::Coroutines::async_mutex<> m_lastLoggedPartitionsData_Mutex;
    FlatMessage<FlatBuffers::LoggedPartitionsData> m_lastLoggedPartitionsData;

    // The set of memory tables that have been checkpointed
    // and therefore shouldn't have their row writes replayed.
    boost::concurrent_flat_map<PartitionNumber, std::monostate> m_checkpointedMemoryTables;
    
    // The set of local transactions that have been committed,
    // and therefore should have their row writes replayed.
    boost::concurrent_flat_map<LocalTransactionNumber, std::monostate> m_committedLocalTransactions;
    
    struct ReplayedMemoryTable
    {
        shared_ptr<IIndex> m_index;
        shared_ptr<IMemoryTable> m_memoryTable;
    };

    boost::concurrent_flat_map<PartitionNumber, shared_ptr<shared_task<const ReplayedMemoryTable>>> m_replayedMemoryTables;

    GlobalSequenceNumbers m_globalSequenceNumbers;
    LogExtentUsageMap m_logExtentUsageMap;

    task<> ReadLogExtent(
        LogExtentSequenceNumber logExtentSequenceNumber
    );

    task<> HandleLogMessage(
        LogExtentSequenceNumber logExtentSequenceNumber,
        DataReference<StoredMessage> message
    );

    task<> HandleLoggedCommitTransaction(
        FlatMessage<FlatBuffers::LoggedCommitLocalTransaction> loggedCommitLocalTransaction
    );

    task<> HandleLoggedCheckpoint(
        FlatMessage<FlatBuffers::LoggedCheckpoint> loggedCheckpoint
    );
    
    task<> HandleLoggedPartitionsData(
        FlatMessage<FlatBuffers::LoggedPartitionsData> loggedPartitionsData
    );

    task<> HandleLoggedRowWrite(
        LogExtentSequenceNumber logExtentSequenceNumber,
        FlatMessage<FlatBuffers::LoggedRowWrite> loggedRowWrite
    );

    shared_task<const ReplayedMemoryTable>& ReplayMemoryTable(
        IndexNumber,
        PartitionNumber
    );

    task<> HandleLoggedRowWrite_System_Index(
        FlatMessage<FlatBuffers::LoggedRowWrite> loggedRowWrite
    );

    task<> HandleLoggedRowWrite_User_Index(
        FlatMessage<FlatBuffers::LoggedRowWrite> loggedRowWrite
    );

public:
    LogReplayer(
        Schedulers& schedulers,
        IProtoStoreReplayTarget& protoStore,
        IHeaderAccessor& headerAccessor,
        IMessageStore& logMessageStore
    );

    task<> ReplayLog();
};

}