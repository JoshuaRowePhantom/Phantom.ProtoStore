#include "StandardIncludes.h"
#include "Phantom.Scalable/Consensus.h"

namespace Phantom::Consensus
{
VectorQuorumChecker::VectorQuorumChecker(
    size_t totalMemberCount,
    size_t requiredMemberCount
) : m_members(
    totalMemberCount,
    false
)
{
    m_missingMemberCount = requiredMemberCount;
}

VectorQuorumChecker::operator bool() const
{
    return m_missingMemberCount == 0;
}

VectorQuorumChecker& VectorQuorumChecker::operator+=(
    size_t member)
{
    if (m_missingMemberCount > 0
        && !m_members[member])
    {
        m_members[member] = true;
        m_missingMemberCount--;
    }

    return *this;
}
}