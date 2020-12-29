#pragma once

#include "Phantom.System/async_utility.h"
#include <optional>
#include <vector>
#include <type_traits>

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
    { static_cast<bool>(q) };
};

template<
    typename TBallotNumber
>
concept BallotNumber = std::totally_ordered<TBallotNumber>;

template<
    typename TBallotNumber
>
concept BallotNumberWithDefault =
BallotNumber<TBallotNumber>
&&
std::convertible_to<TBallotNumber, bool>;

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
    { quorumCheckerFactory(ballotNumber) } -> as_awaitable_convertible_to<TQuorumChecker>;
};

template<
    typename TBallotNumberFactory,
    typename TBallotNumber
> concept AsyncBallotNumberFactory = requires(
    TBallotNumberFactory ballotNumberFactory,
    TBallotNumber ballotNumber
    )
{
    { ballotNumberFactory() } -> as_awaitable_convertible_to<TBallotNumber>;
    { ballotNumberFactory(ballotNumber) } -> as_awaitable_convertible_to<TBallotNumber>;
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

    explicit operator bool() const;
};

template<
    typename TUnderlyingQuorum
>
class ValidatingQuorumChecker
{
    TUnderlyingQuorum m_quorum;
public:
    template<
        typename TUnderlyingQuorum
    >
        ValidatingQuorumChecker(
            TUnderlyingQuorum&& quorum
        ) :
        m_quorum(
            std::forward<TUnderlyingQuorum>(quorum))
    {}

    template<
        typename TMember
    > ValidatingQuorumChecker& operator +=(
        TMember member
        )
    {
        if (member)
        {
            m_quorum += *member;
        }

        return *this;
    }

    explicit operator bool() const
    {
        return static_cast<bool>(m_quorum);
    }
};

template<
    typename TMapFunction,
    typename TUnderlyingQuorum
>
class MappingQuorumChecker
{
    TMapFunction m_mapFunction;
    TUnderlyingQuorum m_quorum;
public:
    template<
        typename TMapFunction,
        typename TUnderlyingQuorum
    > MappingQuorumChecker(
        TMapFunction&& mapFunction,
        TUnderlyingQuorum&& quorum
    ) : 
        m_mapFunction(
            std::forward<TMapFunction>(mapFunction)),
        m_quorum(
            std::forward<TUnderlyingQuorum>(quorum))
    {}

    template<
        typename TMember
    > MappingQuorumChecker& operator +=(
        TMember&& member
        )
    {
        m_quorum += m_mapFunction(
            std::forward<TMember>(
                member));

        return *this;
    }

    explicit operator bool() const
    {
        return static_cast<bool>(m_quorum);
    }
};

template<
    typename TUnderlyingQuorum
>
class UnionQuorumChecker
{
    std::vector<TUnderlyingQuorum> m_quorums;
public:
    UnionQuorumChecker(
        std::vector<TUnderlyingQuorum> quorums
    ) : m_quorums(
        std::move(quorums))
    {}

    template<
        typename TMember
    > UnionQuorumChecker& operator +=(
        TMember&& member)
    {
        for (auto& quorum : m_quorums)
        {
            quorum += std::forward<TMember>(
                member);
        }

        return *this;
    }

    explicit operator bool() const
    {
        for (auto& quorum : m_quorums)
        {
            if (!quorum)
            {
                return false;
            }
        }

        return true;
    }
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
    { Propose(value) } -> as_awaitable_convertible_to<TValue>;
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
