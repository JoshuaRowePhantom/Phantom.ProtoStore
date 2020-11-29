#include "Phantom.Scalable/InternalTransactionImpl.h"
#include <cppcoro/async_scope.hpp>
#include <cppcoro/async_manual_reset_event.hpp>
#include <cppcoro/when_all.hpp>

namespace Phantom::Scalable
{


shared_ptr<IInternalTransactionBuilder> TransactionFactory::CreateTransactionBuilder(
    Grpc::Internal::InternalTransactionIdentifier internalTransactionIdentifier
)
{
    return make_shared<InternalTransaction>(
        internalTransactionIdentifier);
}

shared_task<Grpc::TransactionOutcome> TransactionFactory::ResolveTransaction(
    Grpc::Internal::InternalTransactionInformation transactionInformation
)
{
    auto transaction = CreateTransactionBuilder(
        move(*transactionInformation.mutable_internaltransactionidentifier()));

    for (auto& operationIdentifier : *transactionInformation.mutable_operationidentifiers())
    {
        Grpc::Internal::InternalOperationInformation operationInformation;
        *operationInformation.mutable_internaloperationidentifier() = move(operationIdentifier);

        // Note that we leave the internaloperation field clear.

        // We don't actually care about waiting for the operation result, so we discard the result.
        transaction->AddOperation(
            operationInformation);
    }

    auto commitTask = transaction->Commit();
    co_return co_await commitTask;
};

InternalTransaction::InternalTransaction(
    Grpc::Internal::InternalTransactionIdentifier internalTransactionIdentifier
) : m_internalTransactionInformationTask(
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
    auto& internalTransactionOperation = m_internalTransactionOperations.emplace_back(
        m_internalTransactionInformationTask,
        m_internalTransactionOutcomeTask,
        operationInformation);

    shared_task<Grpc::Internal::InternalOperationResult> operationPrepareResultTask;
    shared_task<> operationCompletionTask;

    internalTransactionOperation.Start(
        operationPrepareResultTask,
        operationCompletionTask);

    m_internalOperationPrepareOutcomeTasks.emplace_back(
        ToPrepareOutcome(
            operationPrepareResultTask));

    m_internalOperationCompletionTasks.emplace_back(
        std::move(operationCompletionTask));

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
    auto transactionOutcome = co_await m_internalTransactionOutcomeTask;

    // This causes the commit / abort notifications to happen.
    co_await cppcoro::when_all(
        move(m_internalOperationCompletionTasks));

    co_return transactionOutcome;
}

}
