#pragma once

#include "StandardIncludes.h"
#include "Paxos.h"
#include "src/PhantomScalableGrpcInternal.pb.h"
#include <map>
#include <optional>
#include <vector>
#include "Phantom.Scalable/InternalResourceManager.h"

namespace Phantom::Scalable
{

class ScalableQuorumMapping
{
    std::vector<std::optional<size_t>> m_members;
    size_t m_memberCount;

public:
    ScalableQuorumMapping(
        const NodeSelection& nodeSelection,
        size_t quorumNumber
    );

    optional<size_t> operator()(
        ParticipantNode participantNode
        );

    size_t MemberCount() const;
};

typedef Consensus::VectorQuorumChecker ScalableVectorQuorumChecker;
typedef Consensus::ValidatingQuorumChecker<ScalableVectorQuorumChecker> ScalableValidatingQuorumChecker;
typedef Consensus::MappingQuorumChecker<ScalableQuorumMapping, ScalableValidatingQuorumChecker> ScalableMappingQuorumChecker;
typedef std::vector<ScalableMappingQuorumChecker> ScalableMappingQuorumCheckerVector;
typedef Consensus::UnionQuorumChecker<ScalableMappingQuorumChecker> ScalableQuorumChecker;

struct ScalableBallotNumber
{
    uint64_t EpochNumber;
    uint64_t BallotNumber;

    ScalableBallotNumber(
        const Grpc::Internal::PaxosBallotNumber& paxosBallotNumber
    );

    operator Grpc::Internal::PaxosBallotNumber() const;

    auto operator<=>(const ScalableBallotNumber&) const = default;
};

static_assert(Consensus::BallotNumber<
    ScalableBallotNumber
>);

static_assert(Consensus::QuorumChecker<
    ScalableVectorQuorumChecker,
    size_t
>);

static_assert(Consensus::QuorumChecker<
    ScalableValidatingQuorumChecker,
    optional<size_t>
>);

static_assert(Consensus::QuorumChecker<
    ScalableMappingQuorumChecker,
    ParticipantNode
>);

static_assert(Consensus::QuorumChecker<
    ScalableQuorumChecker,
    ParticipantNode
>);

class ScalableQuorumCheckerFactory
{
    INodeSelector& m_nodeSelector;
    const Grpc::Internal::ParticipantResource& m_participantResource;

public:
    ScalableQuorumCheckerFactory(
        INodeSelector& nodeSelector,
        const Grpc::Internal::ParticipantResource& participantResource
    );

    task<ScalableQuorumChecker> operator()(
        ScalableBallotNumber ballotNumber
        ) const;
};

static_assert(Consensus::QuorumCheckerFactory<
    ScalableQuorumCheckerFactory, 
    ScalableBallotNumber,
    ScalableQuorumChecker,
    ParticipantNode
>);

class ScalableResourceStateMutator
{
public:
    task<Grpc::Internal::ResourceState> operator()(
        const std::optional<Grpc::Internal::ResourceState>& oldResourceState
        );
};

static_assert(Consensus::Paxos::AsyncMutator<
    ScalableResourceStateMutator, 
    Grpc::Internal::ResourceState
>);

class ScalablePaxosBallotNumberFactory
{
public:
    task<ScalableBallotNumber> operator()();
    task<ScalableBallotNumber> operator()(
        const ScalableBallotNumber&
        );
};

static_assert(Consensus::AsyncBallotNumberFactory<
    ScalablePaxosBallotNumberFactory, 
    ScalableBallotNumber
>);

typedef Consensus::Paxos::StateMachines<
    ParticipantNode,
    ScalableQuorumChecker,
    ScalableQuorumCheckerFactory,
    ScalableBallotNumber,
    ScalablePaxosBallotNumberFactory,
    Grpc::Internal::ResourceState,
    ScalableResourceStateMutator,
    cppcoro::task
> ScalablePaxosStateMachines;

class ScalablePaxosMessageSender
    : public ScalablePaxosStateMachines
{
public:
    cppcoro::async_generator<Phase1bResponse> SendPhase1a(
        Phase1aMessage phase1aMessage);

    cppcoro::async_generator<Phase2bResponse> SendPhase2a(
        Phase2aMessage phase2aMessage);
};

static_assert(Consensus::Paxos::MessageSender<
    ScalablePaxosMessageSender,
    ScalablePaxosStateMachines
>);

typedef Consensus::Paxos::StaticProposer<
    ScalablePaxosStateMachines,
    ScalablePaxosMessageSender,
    cppcoro::task
> ScalablePaxosProposer;

}