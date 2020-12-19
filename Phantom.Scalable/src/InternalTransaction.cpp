#include <cppcoro/async_latch.hpp>
#include <cppcoro/async_manual_reset_event.hpp>
#include <cppcoro/async_scope.hpp>
#include <cppcoro/when_all.hpp>
#include "Phantom.Scalable/InternalResourceManager.h"
#include "Phantom.Scalable/InternalTransactionImpl.h"
#include "Phantom.Scalable/PeerToPeerClient.h"
#include "Phantom.Scalable/ScalablePaxos.h"
#include "Phantom.System/async_ring_buffer.h"
#include "Phantom.System/async_utility.h"

namespace Phantom::Scalable
{

TransactionFactory::TransactionFactory(
    InternalTransactionServiceProvider serviceProvider
)
    : m_serviceProvider(serviceProvider)
{}

shared_ptr<IInternalTransactionBuilder> TransactionFactory::CreateTransactionBuilder(
    Grpc::Internal::InternalTransactionIdentifier internalTransactionIdentifier
)
{
    auto result = make_shared<InternalTransaction>(
        m_serviceProvider,
        internalTransactionIdentifier);

    spawn(result->join());

    return result;
}

shared_task<Grpc::TransactionOutcome> TransactionFactory::ResolveTransaction(
    Grpc::Internal::InternalTransactionInformation transactionInformation
)
{
    auto transaction = CreateTransactionBuilder(
        move(*transactionInformation.mutable_internaltransactionidentifier()));

    for (auto& participantResource : *transactionInformation.mutable_participantresources())
    {
        Grpc::Internal::InternalOperationInformation operationInformation;
        *operationInformation.mutable_internaloperationidentifier() = move(*participantResource.mutable_operationidentifier());

        // Note that we leave the internaloperation field clear.

        // We don't actually care about waiting for the operation result, so we discard the result.
        transaction->AddOperation(
            operationInformation);
    }

    auto commitTask = transaction->Commit();
    co_return co_await commitTask;
};

InternalTransaction::InternalTransaction(
    InternalTransactionServiceProvider serviceProvider,
    Grpc::Internal::InternalTransactionIdentifier internalTransactionIdentifier
) : m_serviceProvider(
        serviceProvider),
    m_internalTransactionInformationTask(
        WaitForInternalTransactionInformation()),
    m_internalTransactionOutcomeTask(
        WaitForTransactionOutcome())
{
    *m_internalTransactionInformation.mutable_internaltransactionidentifier()
        = move(internalTransactionIdentifier);
}

shared_task<const Grpc::Internal::InternalTransactionInformation&> InternalTransaction::WaitForInternalTransactionInformation()
{
    co_await m_internalTransactionInformationComplete;
    co_return m_internalTransactionInformation;
}

task<Grpc::TransactionOutcome> InternalTransaction::ToPrepareOutcome(
    shared_task<Grpc::Internal::InternalOperationResult> operationResultTask
)
{
    co_return (co_await operationResultTask).transactionoutcome();
}

shared_task<Grpc::TransactionOutcome> InternalTransaction::WaitForTransactionOutcome()
{
    co_await m_internalTransactionInformationTask;

    auto prepareOutcomes = co_await cppcoro::when_all(
        move(m_internalOperationPrepareOutcomeTasks));

    for (auto prepareOutcome : prepareOutcomes)
    {
        if (prepareOutcome != Grpc::TransactionOutcome::Succeeded)
        {
            co_return prepareOutcome;
        }
    }

    co_return Grpc::TransactionOutcome::Succeeded;
}

InternalTransactionAddOperationResult InternalTransaction::AddOperation(
    Grpc::Internal::InternalOperationInformation operationInformation
)
{
    auto& internalTransactionOperation = m_internalTransactionOperations.emplace_back(std::make_shared<InternalTransactionOperation>(
        m_serviceProvider,
        m_internalTransactionInformationTask,
        m_internalTransactionOutcomeTask,
        std::move(operationInformation)));

    auto operationPrepareResultTask = internalTransactionOperation->Prepare();

    m_internalOperationPrepareOutcomeTasks.emplace_back(
        ToPrepareOutcome(
            operationPrepareResultTask));

    return
    {
        std::move(operationPrepareResultTask)
    };
}

shared_task<Grpc::TransactionOutcome> InternalTransaction::Commit()
{
    // This triggers all the pending operations to execute their Prepare actions.
    m_internalTransactionInformationComplete.set();

    // This causes all the prepare actions to complete and gives us the result of the transaction.
    co_return co_await m_internalTransactionOutcomeTask;
}

InternalTransactionOperation::InternalTransactionOperation(
    InternalTransactionServiceProvider serviceProvider,
    shared_task<const Grpc::Internal::InternalTransactionInformation&> internalTransactionInformation,
    shared_task<Grpc::TransactionOutcome> internalTransactionOutcome,
    Grpc::Internal::InternalOperationInformation internalOperationInformation
)
    : 
    m_serviceProvider(
        move(serviceProvider)),
    m_internalTransactionInformation(
        move(internalTransactionInformation)),
    m_internalTransactionOutcome(
        move(internalTransactionOutcome)),
    m_partialInternalOperationInformation(
        MakePartialInternalOperationInformation(
            internalOperationInformation)),
    m_fullInternalOperationInformation(
        GetFullInternalOperationInformation()),
    m_nodeSelectorTask(
        m_serviceProvider
            .get<IInternalResourceManagerSelector*>()
            ->GetNodeSelector(internalOperationInformation.internaloperationidentifier().participantresource()))
{
    if (internalOperationInformation.has_internaloperation())
    {
        m_fullInternalOperationInformationHolder = std::make_unique<Grpc::Internal::InternalOperationInformation>(
            move(internalOperationInformation));
    }

    m_prepareTask = DelayedPrepare();
}

Grpc::Internal::InternalOperationInformation InternalTransactionOperation::MakePartialInternalOperationInformation(
    const Grpc::Internal::InternalOperationInformation& internalOperationInformation
)
{
    Grpc::Internal::InternalOperationInformation partialInternalOperationInformation;
    *partialInternalOperationInformation.mutable_internaloperationidentifier() = internalOperationInformation.internaloperationidentifier();
    return partialInternalOperationInformation;
}

shared_task<const Grpc::Internal::InternalOperationInformation*> InternalTransactionOperation::GetFullInternalOperationInformation()
{
    while (!m_fullInternalOperationInformationHolder)
    {
        throw 0;
    }

    co_return m_fullInternalOperationInformationHolder.get();
}

shared_task<Grpc::Internal::InternalOperationResult> InternalTransactionOperation::DelayedPrepare()
{

    // Spawn a task that waits for the outcome of the distributed transaction to be known,
    // then notifies this operation's participants of the distributed outcome.
    spawn(
        NotifyCommitAbortDecision(
            Grpc::Internal::EpochNumber{}));

    throw 0;
    co_return Grpc::Internal::InternalOperationResult{};
}

shared_task<Grpc::Internal::InternalOperationResult> InternalTransactionOperation::Prepare()
{
    return m_prepareTask;
}

task<> InternalTransactionOperation::NotifyCommitAbortDecision(
    Grpc::Internal::EpochNumber epochNumber)
{
    auto transactionOutcome = co_await m_internalTransactionOutcome;

    Grpc::Internal::ProcessOperationRequest processOperationRequest;
    
    processOperationRequest.mutable_committransactionoutcome()->set_transactionoutcome(
        transactionOutcome);

    auto enumeration = SendProcessOperationRequestToParticipants(
        epochNumber,
        processOperationRequest);

    co_await async_skip(
        co_await enumeration.begin(),
        enumeration.end());
}

cppcoro::async_generator<Grpc::Internal::ProcessOperationResponse> InternalTransactionOperation::SendProcessOperationRequest(
    const NodeSelection& nodes,
    ParticipantNode participantNode,
    const Grpc::Internal::ProcessOperationRequest& request
)
{
    auto nodeResolver = m_serviceProvider.get<INodeResolver*>();
    auto nodeTask = nodeResolver->ResolveNode(
        nodes.EpochNumber,
        nodes[participantNode]
    );
    auto& node = co_await nodeTask;

    auto& address = node.addresses(0);

    auto clientFactory = m_serviceProvider.get<IPeerToPeerClientFactory*>();
    auto client = co_await clientFactory->Open(
        address);

    auto enumeration = client->ProcessOperation(
        request);

    for (auto iterator = co_await enumeration.begin();
        iterator != enumeration.end();
        co_await ++iterator)
    {
        co_yield move(*iterator);
    }
}

cppcoro::async_generator<Grpc::Internal::ProcessOperationResponse> InternalTransactionOperation::SendProcessOperationRequestWithNeedOperationInformationFaultHandling(
    const NodeSelection& nodes,
    ParticipantNode participantNode,
    const Grpc::Internal::ProcessOperationRequest& requestWithoutOperationInformation,
    shared_task<Grpc::Internal::ProcessOperationRequest>& requestWithOperationInformation
)
{
    const Grpc::Internal::ProcessOperationRequest* requestToSend = &requestWithoutOperationInformation;

    bool needResend;

    do
    {
        needResend = false;

        auto enumeration = SendProcessOperationRequest(
            nodes,
            participantNode,
            *requestToSend);

        for (auto iterator = co_await enumeration.begin();
            iterator != enumeration.end();
            co_await ++iterator)
        {
            auto& response = *iterator;

            if (response.has_needoperationinformationfault())
            {
                const auto& materializedRequestWithOperationInformation = co_await requestWithOperationInformation;
                requestToSend = &materializedRequestWithOperationInformation;
                needResend = true;
            }

            co_yield move(response);
        }
    } while (needResend);
}

shared_task<Grpc::Internal::ProcessOperationRequest> 
InternalTransactionOperation::AddOperationInformationToRequest(
    const Grpc::Internal::ProcessOperationRequest& originalRequest
)
{
    Grpc::Internal::ProcessOperationRequest newRequest
    {
        originalRequest
    };

    *newRequest.mutable_internaloperationinformation() = *co_await m_fullInternalOperationInformation;

    co_return newRequest;
}

cppcoro::async_generator<
    InternalTransactionOperation::ParticipantResponse
> InternalTransactionOperation::SendProcessOperationRequestToParticipants(
    const NodeSelection& nodes,
    const Grpc::Internal::ProcessOperationRequest& request
)
{
    async_ring_buffer<ParticipantResponse> buffer(nodes.Members.size());
    cppcoro::async_latch latch(nodes.Members.size() + 1);
    cppcoro::async_scope asyncScope;

    auto addOperationInformationToRequestTask = AddOperationInformationToRequest(
        request);

    auto sendProcessOperationLambda = [&](
        ParticipantNode node
        ) -> task<>
    {
        auto enumeration = SendProcessOperationRequestWithNeedOperationInformationFaultHandling(
            nodes,
            node,
            request,
            addOperationInformationToRequestTask
        );

        for (auto iterator = co_await enumeration.begin();
            iterator != enumeration.end();
            co_await ++iterator)
        {
            co_await buffer.push(
                ParticipantResponse
                {
                    node,
                    std::move(*iterator)
                }
            );
        }

        latch.count_down();
    };

    for (auto& participantNode : nodes.Participants())
    {
        asyncScope.spawn(
            sendProcessOperationLambda(
                participantNode));
    }

    asyncScope.spawn([&]() -> task<>
    {
        co_await latch;
        co_await buffer.complete();
    }());

    latch.count_down();

    for (auto iterator = co_await buffer.begin();
        iterator != buffer.end();
        co_await ++iterator)
    {
        co_yield move(*iterator);
    }
    
    co_await asyncScope.join();
}


cppcoro::async_generator<
    InternalTransactionOperation::ParticipantResponse
> InternalTransactionOperation::SendProcessOperationRequestToParticipants(
    const Grpc::Internal::EpochNumber& epochNumber,
    const Grpc::Internal::ProcessOperationRequest& request
)
{
    auto nodeSelectionTask = (co_await m_nodeSelectorTask)->GetNodeSelection(
        epochNumber,
        m_partialInternalOperationInformation.internaloperationidentifier().participantresource()
    );

    auto& nodeSelection = co_await nodeSelectionTask;

    auto enumeration = SendProcessOperationRequestToParticipants(
        nodeSelection,
        request
    );

    for (auto iterator = co_await enumeration.begin();
        iterator != enumeration.end();
        co_await ++iterator)
    {
        co_yield move(*iterator);
    }
}
}
