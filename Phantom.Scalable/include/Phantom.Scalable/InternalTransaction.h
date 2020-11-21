#pragma once

#include "StandardIncludes.h"
#include "src/PhantomScalableGrpcInternal.grpc.pb.h"

namespace Phantom::Scalable
{

class IInternalTransactionBuilder
{
public:
    virtual shared_task<Grpc::Internal::InternalOperationResult> AddOperation(
        Grpc::Internal::InternalOperationInformation internalOperationInformation
    ) = 0;

    virtual shared_task<Grpc::TransactionOutcome> Commit(
    ) = 0;
};

class ITransactionFactory
{
public:
    virtual shared_ptr<IInternalTransactionBuilder> CreateTransactionBuilder(
    ) = 0;
    
    virtual shared_ptr<Grpc::TransactionOutcome> ResolveTransaction(
        Grpc::Internal::InternalTransactionInformation transactionInformation
    ) = 0;
};

class TransactionFactory
    :
    public ITransactionFactory
{
public:
    virtual shared_ptr<IInternalTransactionBuilder> CreateTransactionBuilder(
    ) override;

    virtual shared_ptr<Grpc::TransactionOutcome> ResolveTransaction(
        Grpc::Internal::InternalTransactionInformation transactionInformation
    ) override;
};

}
