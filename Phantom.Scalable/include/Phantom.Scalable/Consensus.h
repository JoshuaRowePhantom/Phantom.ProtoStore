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
    { b1 == b2 } -> std::convertible_to<bool>;
    { b1 != b2 } -> std::convertible_to<bool>;
    { b1 < b2 } -> std::convertible_to<bool>;
    { b1 > b2 } -> std::convertible_to<bool>;
    { b1 <= b2 } -> std::convertible_to<bool>;
    { b1 >= b2 } -> std::convertible_to<bool>;
};

template<
    typename TQuorumCheckerFactory,
    typename TBallotNumber,
    typename TQuorumChecker,
    typename TMember
>
concept QuorumCheckerFactory =
QuorumChecker<TQuorumChecker, TMember>
&&
BallotNumber<TBallotNumber>
&&
requires (
    TQuorumCheckerFactory quorumCheckerFactory,
    TBallotNumber ballotNumber
    )
{
    { quorumCheckerFactory(ballotNumber) };
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

template<
    typename TBallotNumber
> class NumericBallotNumberFactory
{
public:
    typedef TBallotNumber ballot_number_type;

    ballot_number_type operator()() const 
    {
        return 0; 
    }

    ballot_number_type operator()(
        ballot_number_type value
        ) const
    {
        return value + 1;
    }
};

template<
    typename TValue
>
concept AsyncProposer =
requires (TValue value)
{
    { Propose(value) };
};

template<
    typename TValue,
    template<typename> typename TFuture
>
class IAsyncProposer
{
public:
    virtual TFuture<TValue> Propose(
        TValue value
    ) = 0;

    virtual ~IAsyncProposer();
};
}
