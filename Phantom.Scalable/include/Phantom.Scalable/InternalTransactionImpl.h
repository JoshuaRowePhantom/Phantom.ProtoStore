#pragma once

#include "Phantom.System/async_value_source.h"
#include "Phantom.System/service_provider.h"
#include "InternalTransaction.h"
#include <optional>
#include <cppcoro/async_generator.hpp>

namespace Phantom::Scalable
{

class InternalTransactionOperation
    :
    public BaseBackgoundWorker<InternalTransactionOperation>,
    public std::enable_shared_from_this<InternalTransactionOperation>
{
    const InternalTransactionServiceProvider m_serviceProvider;
    
    const shared_task<const Grpc::Internal::InternalTransactionInformation&> m_internalTransactionInformation;
    const shared_task<Grpc::TransactionOutcome> m_internalTransactionOutcome;
    
    const Grpc::Internal::InternalOperationInformation m_partialInternalOperationInformation;
    const shared_task<const Grpc::Internal::InternalOperationInformation*> m_fullInternalOperationInformation;
    unique_ptr<Grpc::Internal::InternalOperationInformation> m_fullInternalOperationInformationHolder;
    shared_task<Grpc::Internal::InternalOperationResult> m_prepareTask;

    task<> NotifyCommitAbortDecision(
        Grpc::Internal::EpochNumber epochNumber);

    Grpc::Internal::InternalOperationInformation MakePartialInternalOperationInformation(
        const Grpc::Internal::InternalOperationInformation& internalOperationInformation
    );

    shared_task<const Grpc::Internal::InternalOperationInformation*> GetFullInternalOperationInformation();
    shared_task<Grpc::Internal::InternalOperationResult> DelayedPrepare();
    
    cppcoro::async_generator<Grpc::Internal::ProcessOperationResponse> SendProcessOperationRequest(
        Grpc::Address destination,
        const Grpc::Internal::ProcessOperationRequest& request
    );

    shared_task<Grpc::Internal::ProcessOperationRequest> AddOperationInformationToRequest(
        const Grpc::Internal::ProcessOperationRequest& originalRequest
    );

    cppcoro::async_generator<Grpc::Internal::ProcessOperationResponse> SendProcessOperationRequestWithNeedOperationInformationFaultHandling(
        Grpc::Address destination,
        const Grpc::Internal::ProcessOperationRequest& requestWithoutOperationInformation,
        shared_task<Grpc::Internal::ProcessOperationRequest>& requestWithOperationInformation
    );

    cppcoro::async_generator<
        std::tuple
        <
            Grpc::Internal::ParticipantNode,
            Grpc::Internal::ProcessOperationResponse
        >
    > SendProcessOperationRequestToParticipants(
        const std::vector<Grpc::Internal::ParticipantNode> nodes,
        const Grpc::Internal::ProcessOperationRequest& request
    );

    //cppcoro::async_generator<
    //    std::tuple<
    //        Grpc::>> SendProcessOperationRequest(
    //    Grpc::Internal::EpochNumber epochNumber,
    //    Grpc::Internal::ProcessOperationRequest requestWithoutOperationInformation
    //);

public:
    InternalTransactionOperation(
        InternalTransactionServiceProvider serviceProvider,
        shared_task<const Grpc::Internal::InternalTransactionInformation&> internalTransactionInformation,
        shared_task<Grpc::TransactionOutcome> internalTransactionOutcome,
        Grpc::Internal::InternalOperationInformation internalOperationInformation
    );

    shared_task<Grpc::Internal::InternalOperationResult> Prepare();
};

class InternalTransaction
    :
    public IInternalTransactionBuilder,
    public BaseBackgoundWorker<InternalTransaction>,
    public std::enable_shared_from_this<InternalTransaction>
{
    const InternalTransactionServiceProvider m_serviceProvider;

    Grpc::Internal::InternalTransactionInformation m_internalTransactionInformation;
    cppcoro::async_manual_reset_event m_internalTransactionInformationComplete;
    shared_task<const Grpc::Internal::InternalTransactionInformation&> m_internalTransactionInformationTask;
    shared_task<const Grpc::Internal::InternalTransactionInformation&> WaitForInternalTransactionInformation();

    shared_task<Grpc::TransactionOutcome> m_internalTransactionOutcomeTask;
    shared_task<Grpc::TransactionOutcome> WaitForTransactionOutcome();

    std::vector<shared_ptr<InternalTransactionOperation>> m_internalTransactionOperations;
    std::vector<task<Grpc::TransactionOutcome>> m_internalOperationPrepareOutcomeTasks;

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
