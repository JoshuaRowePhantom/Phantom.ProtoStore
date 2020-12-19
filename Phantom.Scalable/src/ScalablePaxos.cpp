#include "Phantom.Scalable/ScalablePaxos.h"

namespace Phantom::Scalable
{
ScalableQuorumMapping::ScalableQuorumMapping(
    const NodeSelection& nodeSelection,
    size_t quorumNumber
) : m_memberCount(0)
{
    for (
        size_t memberNumber = 0;
        memberNumber < nodeSelection.Members.size(); 
        memberNumber++
        )
    {
        if (nodeSelection.IsMemberOfQuorum(
            memberNumber,
            quorumNumber))
        {
            m_members[memberNumber] = m_memberCount++;
        }
    }
}

optional<size_t> ScalableQuorumMapping::operator()(
    ParticipantNode participantNode
    )
{
    return m_members[participantNode.MemberNumber];
}

size_t ScalableQuorumMapping::MemberCount() const
{
    return m_memberCount;
}

ScalableQuorumCheckerFactory::ScalableQuorumCheckerFactory(
    INodeSelector& nodeSelector,
    const Grpc::Internal::ParticipantResource& participantResource
) :
    m_nodeSelector(
        nodeSelector),
    m_participantResource(
        m_participantResource)
{

}

task<ScalableQuorumChecker> ScalableQuorumCheckerFactory::operator()(
    ScalableBallotNumber ballotNumber
    ) const
{
    Grpc::Internal::EpochNumber epochNumber;
    epochNumber.set_value(
        ballotNumber.EpochNumber
    );

    auto nodeSelectionTask = m_nodeSelector.GetNodeSelection(
        epochNumber,
        m_participantResource
    );

    auto& nodeSelection = co_await nodeSelectionTask;

    ScalableMappingQuorumCheckerVector mappingQuorumCheckers;
    mappingQuorumCheckers.reserve(
        nodeSelection.QuorumCount);

    for (size_t quorumNumber = 0;
        quorumNumber < nodeSelection.QuorumCount;
        quorumNumber++)
    {
        ScalableQuorumMapping mapping(
            nodeSelection,
            quorumNumber);

        ScalableVectorQuorumChecker vectorQuorumChecker(
            mapping.MemberCount(),
            mapping.MemberCount() + 1 / 2);

        ScalableValidatingQuorumChecker validatingQuorumChecker(
            move(vectorQuorumChecker));

        ScalableMappingQuorumChecker mappingQuorumChecker(
            move(mapping),
            move(validatingQuorumChecker));

        mappingQuorumCheckers.push_back(
            move(mappingQuorumChecker));
    }

    co_return ScalableQuorumChecker(
        move(mappingQuorumCheckers));
}

}