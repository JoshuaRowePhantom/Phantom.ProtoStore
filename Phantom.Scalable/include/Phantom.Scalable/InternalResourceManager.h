#pragma once

#include "StandardIncludes.h"
#include "src/PhantomScalableGrpcInternal.pb.h"
#include "Phantom.ProtoStore/Phantom.ProtoStore.h"

namespace Phantom::Scalable
{
using namespace Phantom::Scalable::Grpc::Internal;

class IInternalResourceManagerOperationContext
{
public:
    virtual const InternalOperation& GetInternalOperation(
    ) const = 0;

    virtual ProtoStore::IOperation* GetStoreOperation(
    ) const = 0;
};

class IInternalResourceManagerResultContext
{
public:
    virtual task<> MarkReadOnly(
    ) = 0;

    virtual task<> SetResult(
        InternalOperationResult result
    ) = 0;

    virtual task<> Fail(
    ) = 0;

    virtual task<> Succeed(
    ) = 0;
};

class IInternalResourceManagerProposeContext
    :
    public IInternalResourceManagerOperationContext,
    public IInternalResourceManagerResultContext
{
public:
    virtual task<> AddInterferenceRelationship(
        const InternalOperationInterferenceRelationship& relationship
    ) = 0;
};

class IInternalResourceManagerPrepareContext
    :
    public IInternalResourceManagerOperationContext,
    public IInternalResourceManagerResultContext
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
    /// <summary>
    /// An operation has been proposed.  The resource manager should describe
    /// any operation dependencies the new operation has.
    /// </summary>
    virtual task<> Propose(
        IInternalResourceManagerProposeContext* context
    ) = 0;

    /// <summary>
    /// An operation should be prepared.  The resource manager should
    /// compute the outcome of the request, which may later be committed or aborted.
    /// </summary>
    virtual task<> Prepare(
        IInternalResourceManagerPrepareContext* context
    ) = 0;

    /// <summary>
    /// An operation should be committed.
    /// </summary>
    virtual task<> Commit(
        IInternalResourceManagerCommitContext* context
    ) = 0;

    /// <summary>
    /// An operation should be aborted.
    /// </summary>
    virtual task<> Abort(
        IInternalResourceManagerAbortContext* context
    ) = 0;
};

class INodeSelector
{
public:
    virtual task<std::vector<Grpc::Internal::ParticipantNode>> GetParticipantNodes(
        Grpc::Internal::EpochNumber epochNumber,
        Grpc::Internal::ParticipantResource participantResource
    ) = 0;
};

class IInternalResourceManagerSelector
{
public:
    virtual task<shared_ptr<IInternalResourceManager>> GetInternalResourceManager(
        Grpc::Internal::ParticipantResource participantResource
    );

    virtual task<INodeSelector*> GetNodeSelector(
        Grpc::Internal::ParticipantResource participantResource
    );
};

}
