#pragma once

#include "StandardIncludes.h"
#include "PhantomScalableGrpcInternal.pb.h"
#include "Scheduling.h"

namespace Phantom::Scalable
{

class IInternalResourceManagerSelector;
class INodeResolver;
class INodeSelector;
class IPeerToPeerClientFactory;

typedef service_provider<
    IInternalResourceManagerSelector*,
    IPeerToPeerClientFactory*,
    INodeResolver*
> InternalTransactionServiceProvider;

struct InternalTransactionAddOperationResult
{
    shared_task<Grpc::Internal::InternalOperationResult> InternalOperationResult;
};

class IInternalTransactionBuilder
    :
    virtual public IJoinable
{
public:
    virtual InternalTransactionAddOperationResult AddOperation(
        Grpc::Internal::InternalOperationInformation internalOperationInformation
    ) = 0;

    virtual shared_task<Grpc::TransactionOutcome> Commit(
    ) = 0;
};

class ITransactionFactory
    :
    virtual public IJoinable
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
    public ITransactionFactory,
    public BackgroundWorker,
    public std::enable_shared_from_this<TransactionFactory>
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
