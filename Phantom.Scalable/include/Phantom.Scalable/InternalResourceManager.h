#pragma once

#include "StandardIncludes.h"
#include "PhantomScalableGrpcInternal.pb.h"
#include "Phantom.ProtoStore/Phantom.ProtoStore.h"
#include <cppcoro/generator.hpp>

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

struct ParticipantNode
{
    size_t MemberNumber;
};

struct NodeSelection
{
    // The epoch number the node selection was generated at.
    Grpc::Internal::EpochNumber EpochNumber;

    // A list of nodes in the node selection,
    // sorted by the unique identifier string.
    std::vector<
        Grpc::NodeIdentifier
    > Members;

    cppcoro::generator<ParticipantNode> Participants() const;

    // A rectangular array of size Members * QuorumCount.
    std::vector<bool> QuorumMemberships;

    // The number of quorums represented in this node selection.
    size_t QuorumCount;
    
    bool IsMemberOfQuorum(
        size_t memberNumber,
        size_t quorumNumber
    ) const;

    const Grpc::NodeIdentifier& operator[](
        ParticipantNode participantNode
        ) const;
};

class INodeSelector
{
public:
    virtual shared_task<const NodeSelection> GetNodeSelection(
        const Grpc::Internal::EpochNumber& epochNumber,
        const Grpc::Internal::ParticipantResource& participantResource
    ) const = 0;
};

class IInternalResourceManagerSelector
{
public:
    virtual shared_task<shared_ptr<IInternalResourceManager>> GetInternalResourceManager(
        const Grpc::Internal::ParticipantResource& participantResource
    );

    virtual shared_task<shared_ptr<INodeSelector>> GetNodeSelector(
        const Grpc::Internal::ParticipantResource& participantResource
    );
};

}
