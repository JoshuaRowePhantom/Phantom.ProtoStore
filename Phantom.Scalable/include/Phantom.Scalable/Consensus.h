#pragma once

#include "Phantom.System/async_utility.h"
#include <vector>

namespace Phantom::Consensus
{
template<
    typename TQuorumChecker
    , typename TMember
>
concept QuorumChecker = requires (
    TQuorumChecker q,
    TMember m)
{
    { q += m };
}
&&
std::convertible_to<TQuorumChecker, bool>;

template<
    typename TBallotNumber
>
concept BallotNumber = requires (
    TBallotNumber b1,
    TBallotNumber b2
    )
{
    { b1 <=> b2 } -> std::convertible_to<std::strong_ordering>;
};

template<
    typename TBallotNumberFactory,
    typename TBallotNumber
> concept AsyncBallotNumberFactory = requires(
    TBallotNumberFactory ballotNumberFactory,
    TBallotNumber ballotNumber
    )
{
    { co_await as_awaitable(ballotNumberFactory()) } -> std::convertible_to<TBallotNumber>;
    { co_await as_awaitable(ballotNumberFactory(ballotNumber)) } -> std::convertible_to<TBallotNumber>;
};

class VectorQuorumChecker
{
    std::vector<bool> m_members;
    size_t m_missingMemberCount;

public:
    VectorQuorumChecker(
        size_t totalMemberCount,
        size_t requiredMemberCount
    );

    VectorQuorumChecker& operator +=(
        size_t member);

    operator bool() const;
};



}
