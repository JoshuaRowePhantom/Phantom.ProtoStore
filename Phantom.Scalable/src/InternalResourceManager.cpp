#include "Phantom.Scalable/InternalResourceManager.h"

namespace Phantom::Scalable
{


bool NodeSelection::IsMemberOfQuorum(
    size_t memberNumber,
    size_t quorumNumber
) const
{
    return QuorumMemberships[memberNumber + quorumNumber * Members.size()];
}

cppcoro::generator<ParticipantNode> NodeSelection::Participants() const
{
    for (size_t memberNumber = 0; memberNumber < Members.size(); memberNumber++)
    {
        co_yield ParticipantNode
        {
            memberNumber,
        };
    }
}

const Grpc::NodeIdentifier& NodeSelection::operator[](
    ParticipantNode participantNode
    ) const
{
    return Members[participantNode.MemberNumber];
}

}