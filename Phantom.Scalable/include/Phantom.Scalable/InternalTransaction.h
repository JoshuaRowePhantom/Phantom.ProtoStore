#pragma once

#include "StandardIncludes.h"
#include "src/PhantomScalableGrpcInternal.grpc.pb.h"

namespace Phantom::Scalable
{

class IInternalResourceManagerSelector;

typedef service_provider<
    IInternalResourceManagerSelector*
> InternalTransactionServiceProvider;

struct InternalTransactionAddOperationResult
{
    shared_task<Grpc::Internal::InternalOperationResult> InternalOperationResult;
};

class IInternalTransactionBuilder
{
public:
    virtual InternalTransactionAddOperationResult AddOperation(
        Grpc::Internal::InternalOperationInformation internalOperationInformation
    ) = 0;

    virtual shared_task<Grpc::TransactionOutcome> Commit(
    ) = 0;
};

class ITransactionFactory
{
public:
    virtual shared_ptr<IInternalTransactionBuilder> CreateTransactionBuilder(
        Grpc::Internal::InternalTransactionIdentifier internalTransactionIdentifier
    ) = 0;
    
    virtual shared_task<Grpc::TransactionOutcome> ResolveTransaction(
        Grpc::Internal::InternalTransactionInformation transactionInformation
    ) = 0;
};

class TransactionFactory
    :
    public ITransactionFactory
{
    InternalTransactionServiceProvider m_serviceProvider;
public:
    TransactionFactory(
        InternalTransactionServiceProvider serviceProvider
    );

    virtual shared_ptr<IInternalTransactionBuilder> CreateTransactionBuilder(
        Grpc::Internal::InternalTransactionIdentifier internalTransactionIdentifier
    ) override;

    virtual shared_task<Grpc::TransactionOutcome> ResolveTransaction(
        Grpc::Internal::InternalTransactionInformation transactionInformation
    ) override;
};

}
