#pragma once

#include "StandardIncludes.h"
#include "src/PhantomScalableGrpcInternal.pb.h"
#include "Phantom.Scalable/PhantomScalableGrpc.pb.h"

namespace Phantom::Scalable
{
using namespace Phantom::Scalable::Grpc;
using namespace Phantom::Scalable::Grpc::Internal;

class IResourceManagerPerformContext
{
public:
    virtual shared_task<InternalOperationResult> AddOperation(
        const Grpc::Internal::InternalOperationIdentifier& internalOperationIdentifier,
        const Grpc::Internal::InternalOperation& internalOperation
    ) = 0;
};

class IResourceManager
{
public:
    virtual ~IResourceManager();

    virtual task<ScalablePerformResult> Perform(
        IResourceManagerPerformContext& context,
        const Grpc::ScalablePerformOperation& operation
    ) = 0;
};

}