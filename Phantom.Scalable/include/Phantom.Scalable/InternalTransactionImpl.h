#pragma once

#include "Phantom.System/async_value_source.h"
#include "Phantom.System/service_provider.h"
#include "InternalTransaction.h"

namespace Phantom::Scalable
{

class InternalTransactionOperation
{
    const InternalTransactionServiceProvider m_serviceProvider;
    
    shared_task<const Grpc::Internal::InternalTransactionInformation&> m_internalTransactionInformation;
    shared_task<const Grpc::TransactionOutcome> m_internalTransactionOutcome;
    Grpc::Internal::InternalOperationInformation m_internalOperationInformation;

public:
    InternalTransactionOperation(
        InternalTransactionServiceProvider serviceProvider,
        shared_task<const Grpc::Internal::InternalTransactionInformation&> internalTransactionInformation,
        shared_task<Grpc::TransactionOutcome> internalTransactionOutcome,
        Grpc::Internal::InternalOperationInformation internalOperationInformation
    );

    void Start(
        shared_task<Grpc::Internal::InternalOperationResult>& operationPrepareResultTask,
        shared_task<>& operationCompletionTask
    );
};

class InternalTransaction
    :
    public IInternalTransactionBuilder
{
    const InternalTransactionServiceProvider m_serviceProvider;

    Grpc::Internal::InternalTransactionInformation m_internalTransactionInformation;
    cppcoro::async_manual_reset_event m_internalTransactionInformationComplete;
    shared_task<const Grpc::Internal::InternalTransactionInformation&> m_internalTransactionInformationTask;
    shared_task<const Grpc::Internal::InternalTransactionInformation&> WaitForInternalTransactionInformation();

    shared_task<Grpc::TransactionOutcome> m_internalTransactionOutcomeTask;
    shared_task<Grpc::TransactionOutcome> WaitForTransactionOutcome();

    std::vector<task<Grpc::TransactionOutcome>> m_internalOperationPrepareOutcomeTasks;
    std::vector<shared_task<>> m_internalOperationCompletionTasks;

    std::vector<InternalTransactionOperation> m_internalTransactionOperations;

    task<Grpc::TransactionOutcome> ToPrepareOutcome(
        shared_task<Grpc::Internal::InternalOperationResult> operationResultTask
    );

public:
    InternalTransaction(
        InternalTransactionServiceProvider serviceProvider,
        Grpc::Internal::InternalTransactionIdentifier internalTransactionIdentifier
    );

    InternalTransactionAddOperationResult AddOperation(
        Grpc::Internal::InternalOperationInformation operationInformation
    );

    shared_task<Grpc::TransactionOutcome> Commit();
};

}
