#pragma once

#include "StandardIncludes.h"
#include "src/PhantomScalableGrpcInternal.pb.h"

namespace Phantom::Scalable
{
using namespace Phantom::Scalable::Grpc::Internal;

class IInternalResourceManagerOperationContext
{
public:
    virtual const InternalOperation& GetInternalOperation(
    ) const;
};

class IInternalResourceManagerProposeContext
    :
    public IInternalResourceManagerOperationContext
{
public:
};

class IInternalResourceManagerPrepareContext
    :
    public IInternalResourceManagerOperationContext
{
public:
};

class IInternalResourceManagerCommitContext
    :
    public IInternalResourceManagerOperationContext
{
public:
};

class IInternalResourceManagerAbortContext
    :
    public IInternalResourceManagerOperationContext
{
public:
};

class IInternalResourceManager
{
public:
    virtual ~IInternalResourceManager() = 0;

    virtual task<> Propose(
        IInternalResourceManagerProposeContext& context
    ) = 0;

    virtual task<> Prepare(
        IInternalResourceManagerPrepareContext& context
    ) = 0;

    virtual task<> Commit(
        IInternalResourceManagerCommitContext& context
    ) = 0;

    virtual task<> Abort(
        IInternalResourceManagerAbortContext& context
    ) = 0;
};

}
