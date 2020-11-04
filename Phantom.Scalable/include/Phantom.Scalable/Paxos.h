#pragma once

#include "Phantom.System/async_utility.h"
#include <optional>
#include <variant>
#include <vector>
#include "Phantom.Scalable/Consensus.h"

namespace Phantom::Consensus
{

template
<
    typename TBallotNumber,
    typename TValue
>
class PaxosMessages
{
public:
    typedef TBallotNumber ballot_number_type;
    typedef TValue value_type;

    struct Phase1aMessage
    {
        ballot_number_type BallotNumber;
    };

    struct Phase1bVote
    {
        ballot_number_type VotedBallotNumber;
        value_type VotedValue;

        bool operator==(const Phase1bVote&) const = default;
    };

    struct Phase1bMessage
    {
        ballot_number_type BallotNumber;
        std::optional<Phase1bVote> Phase1bVote;

        bool operator==(const Phase1bMessage&) const = default;
    };

    struct Phase2aMessage
    {
        ballot_number_type BallotNumber;
        value_type Value;
    };

    struct Phase2bMessage
    {
        ballot_number_type BallotNumber;
        value_type Value;

        bool operator==(const Phase2bMessage&) const = default;
    };

    struct NakMessage
    {
        ballot_number_type BallotNumber;
        ballot_number_type MaxBallotNumber;

        bool operator==(const NakMessage&) const = default;
    };
};

template
<
    typename TMember,
    BallotNumber TBallotNumber,
    typename TValue,
    QuorumChecker<TMember> TQuorumChecker,
    template <typename> typename TFuture
>
class PaxosInterfaces
    : 
    public PaxosMessages<TBallotNumber, TValue>
{
public:
    typedef PaxosMessages<TBallotNumber, TValue> messages_type;
    using typename messages_type::ballot_number_type;
    using typename messages_type::value_type;
    using typename messages_type::Phase1aMessage;
    using typename messages_type::Phase1bMessage;
    using typename messages_type::Phase1bVote;
    using typename messages_type::Phase2aMessage;
    using typename messages_type::Phase2bMessage;
    using typename messages_type::NakMessage;

    typedef TQuorumChecker quorum_checker_type;
    typedef TMember member_type;

    struct LeaderState
    {
        value_type Proposal;
        std::optional<ballot_number_type> CurrentBallot;
        std::optional<ballot_number_type> MaxVotedBallotNumber;
        std::optional<quorum_checker_type> Phase1bQuorum;
    };

    struct Phase1aResult
    {
        Phase1aMessage Phase1aMessage;
    };

    enum class Phase2aResultAction
    {
        MismatchedBallot,
        QuorumOverreached,
        QuorumNotReached,
    };

    struct Phase2aResult
    {
        Phase2aResultAction Action;
        std::optional<Phase2aMessage> Phase2aMessage;
    };

    struct NakResult
    {
        std::optional<Phase1aResult> Phase1aResult;
    };

    class IAsyncLeader
    {
    public:
        virtual TFuture<Phase1aResult> Phase1a(
            LeaderState& leaderState,
            ballot_number_type ballotNumber,
            value_type value
        ) = 0;

        virtual TFuture<Phase2aResult> Phase2a(
            LeaderState& leaderState,
            member_type member,
            const Phase1bMessage& phase1bMessage
        ) = 0;

        virtual TFuture<NakResult> Nak(
            LeaderState& leaderState,
            const NakMessage& nakMessage
        ) = 0;
    };

    struct Phase1bResult
    {
        std::variant<
            Phase1bMessage,
            NakMessage
        > Phase1bResponseMessage;
    };

    struct Phase2bResult
    {
        std::variant<
            Phase2bMessage,
            NakMessage
        > Phase2bResponseMessage;
    };

    struct AcceptorState
    {
        std::optional<ballot_number_type> MaxBallotNumber;
        std::optional<Phase1bVote> Vote;
    };

    class IAsyncAcceptor
    {
    public:
        virtual TFuture<Phase1bResult> Phase1b(
            AcceptorState& acceptorState,
            const Phase1aMessage& phase1aMessage
        ) = 0;

        virtual TFuture<Phase2bResult> Phase2b(
            AcceptorState& acceptorState,
            const Phase2aMessage& phase1bMessage
        ) = 0;
    };

    struct LearnerState
    {
        std::optional<value_type> Value;
        std::optional<ballot_number_type> MaxBallotNumber;
        std::optional<quorum_checker_type> Quorum;
    };

    struct LearnedValue
    {
        value_type Value;
        bool IsNewlyLearned;
    };

    struct LearnResult
    {
        std::variant<
            std::monostate,
            LearnedValue,
            NakMessage
        > LearnedValue;

        bool has_value() const
        {
            return LearnedValue.index() == 1;
        }

        std::optional<value_type> value()
        {
            return has_value()
                ? std::make_optional(get<1>(LearnedValue).Value)
                : std::optional<value_type>();
        }
    };

    class IAsyncLearner
    {
    public:
        virtual TFuture<LearnResult> Learn(
            LearnerState& learnerState,
            member_type acceptor,
            Phase2bMessage phase2bMessage
        ) = 0;
    };
};

template
<
    typename TMember,
    typename TQuorumChecker,
    typename TQuorumCheckerFactory,
    BallotNumber TBallotNumber,
    typename TBallotNumberFactory,
    typename TValue,
    template<typename> typename TFuture
>
requires
QuorumCheckerFactory<TQuorumCheckerFactory, TBallotNumber, TQuorumChecker, TMember>
class Paxos
    :
    public PaxosInterfaces
    <
        TMember,
        TBallotNumber,
        TValue,
        TQuorumChecker,
        TFuture
    >
{
public:
    typedef PaxosInterfaces<
        TMember,
        TBallotNumber,
        TValue,
        TQuorumChecker,
        TFuture
    > paxos_interfaces_type;
    typedef TQuorumCheckerFactory quorum_checker_factory_type;
    using typename paxos_interfaces_type::ballot_number_type;
    typedef TBallotNumberFactory ballot_number_factory_type;
    using typename paxos_interfaces_type::member_type;
    using typename paxos_interfaces_type::LeaderState;
    using typename paxos_interfaces_type::AcceptorState;
    using typename paxos_interfaces_type::Phase1aResult;
    using typename paxos_interfaces_type::Phase1aMessage;
    using typename paxos_interfaces_type::Phase1bMessage;
    using typename paxos_interfaces_type::Phase1bVote;
    using typename paxos_interfaces_type::Phase1bResult;
    using typename paxos_interfaces_type::Phase2aResult;
    using typename paxos_interfaces_type::Phase2aResultAction;
    using typename paxos_interfaces_type::Phase2aMessage;
    using typename paxos_interfaces_type::Phase2bMessage;
    using typename paxos_interfaces_type::Phase2bResult;
    using typename paxos_interfaces_type::NakResult;
    using typename paxos_interfaces_type::NakMessage;
    using typename paxos_interfaces_type::LearnerState;
    using typename paxos_interfaces_type::LearnResult;
    using typename paxos_interfaces_type::LearnedValue;

    typedef TQuorumCheckerFactory quorum_checker_factory_type;

    class StaticAsyncLeader
    {
        quorum_checker_factory_type m_quorumCheckerFactory;
        quorum_checker_factory_type m_ballotNumberFactory;

    public:
        StaticAsyncLeader(
            quorum_checker_factory_type quorumCheckerFactory,
            ballot_number_factory_type ballotNumberFactory
        ) : m_quorumCheckerFactory(
            quorumCheckerFactory
        )
        {}

        TFuture<Phase1aResult> Phase1a(
            LeaderState& leaderState,
            TValue value
        )
        {
            co_return co_await Phase1a(
                leaderState,
                leaderState.CurrentBallot 
                    ? co_await as_awaitable(m_ballotNumberFactory(*leaderState.CurrentBallot))
                    : co_await as_awaitable(m_ballotNumberFactory()),
                move(value)
            );
        }

        TFuture<Phase1aResult> Phase1a(
            LeaderState& leaderState,
            ballot_number_type ballotNumber,
            TValue value
        )
        {
            leaderState.CurrentBallot = move(ballotNumber);
            leaderState.Proposal = move(value);
            leaderState.Phase1bQuorum = move(co_await as_awaitable(m_quorumFactory(
                ballotNumber)));
        }

        TFuture<Phase2aResult> Phase2a(
            LeaderState& leaderState,
            member_type member,
            const Phase1bMessage& phase1bMessage
        )
        {
            if (leaderState.BallotNumber != phase1bMessage.BallotNumber)
            {
                co_return Phase2aResult
                {
                    Phase2aResultAction::MismatchedBallot
                };
            }

            if (leaderState.Phase1bQuorum)
            {
                co_return Phase2aResult
                {
                    Phase2aResultAction::QuorumOverreached
                };
            }

            leaderState.Phase1bQuorum += member;

            if (phase1bMessage.Vote
                &&
                phase1bMessage.Vote->VotedBallotNumber > leaderState.MaxVotedBallotNumber)
            {
                leaderState.MaxVotedBallotNumber = phase1bMessage.Vote->VotedBallotNumber;
                leaderState.Proposal = phase1bMessage.Vote->VotedValue;
            }

            if (!leaderState.Phase1bQuorum)
            {
                co_return Phase2aResult
                {
                    Phase2aResultAction::QuorumNotReached
                };
            }

            co_return Phase2aResult
            {
                Phase2aResultAction::QuorumReached,
                Phase2aMessage
                {
                    .BallotNumber = leaderState.BallotNumber,
                    .Value = leaderState.Value,
                },
            };
        }

        TFuture<NakResult> Nak(
            LeaderState& leaderState,
            const NakMessage& nakMessage
        )
        {
            if (leaderState.BallotNumber > nakMessage.MaxBallotNumber)
            {
                co_return NakResult{};
            }

            co_return NakResult
            {
                .Phase1aResult = co_await Phase1a(
                    leaderState,
                    co_await as_awaitable(m_ballotNumberFactory(
                        nakMessage.MaxBallotNumber),
                    leaderState.Proposal)),
            };
        }
    };

    class StaticAcceptor
    {
    public:
        TFuture<Phase1bResult> Phase1b(
            AcceptorState& acceptorState,
            const Phase1aMessage& phase1aMessage
        )
        {
            if (phase1aMessage.BallotNumber <= acceptorState.MaxBallotNumber)
            {
                co_return Phase1bResult
                {
                    NakMessage
                    {
                        .BallotNumber = phase1aMessage.BallotNumber,
                        .MaxBallotNumber = *acceptorState.MaxBallotNumber,
                    },
                };
            }

            acceptorState.MaxBallotNumber = phase1aMessage.BallotNumber;

            co_return Phase1bResult
            {
                Phase1bMessage
                {
                    .BallotNumber = phase1aMessage.BallotNumber,
                    .Phase1bVote = acceptorState.Vote,
                },
            };
        }

        TFuture<Phase2bResult> Phase2b(
            AcceptorState& acceptorState,
            const Phase2aMessage& phase2aMessage
        )
        {
            if (phase2aMessage.BallotNumber < acceptorState.MaxBallotNumber)
            {
                co_return Phase2bResult
                {
                    NakMessage
                    {
                        .BallotNumber = phase2aMessage.BallotNumber,
                        .MaxBallotNumber = *acceptorState.MaxBallotNumber,
                    },
                };
            }

            acceptorState.MaxBallotNumber = phase2aMessage.BallotNumber;
            acceptorState.Vote = Phase1bVote
            {
                .VotedBallotNumber = phase2aMessage.BallotNumber,
                .VotedValue = phase2aMessage.Value,
            };

            co_return Phase2bResult
            {
                Phase2bMessage
                {
                    .BallotNumber = phase2aMessage.BallotNumber,
                    .Value = phase2aMessage.Value,
                },
            };
        }

    };

    class StaticLearner
    {
        quorum_checker_factory_type m_quorumCheckerFactory;
    public:
        StaticLearner(
            quorum_checker_factory_type quorumCheckerFactory
        ) : m_quorumCheckerFactory(
            quorumCheckerFactory
        )
        {}

        TFuture<LearnResult> Learn(
            LearnerState& learnerState,
            member_type acceptor,
            Phase2bMessage phase2bMessage
        )
        {
            if (learnerState.Value)
            {
                co_return LearnResult
                {
                    LearnedValue
                    {
                        .Value = *learnerState.Value,
                        .IsNewlyLearned = false,
                    },
                };
            }

            if (phase2bMessage.BallotNumber < learnerState.MaxBallotNumber)
            {
                co_return LearnResult
                {
                    .LearnedValue = NakMessage
                    {
                        .BallotNumber = phase2bMessage.BallotNumber,
                        .MaxBallotNumber = *learnerState.MaxBallotNumber
                    }
                };
            }

            if (phase2bMessage.BallotNumber > learnerState.MaxBallotNumber)
            {
                learnerState.Quorum = co_await as_awaitable(
                    m_quorumCheckerFactory(phase2bMessage.BallotNumber));
                learnerState.MaxBallotNumber = phase2bMessage.BallotNumber;
            }

            assert(learnerState.Quorum);
            assert(!learnerState.Value);
            assert(phase2bMessage.BallotNumber == learnerState.MaxBallotNumber);

            if (co_await as_awaitable(*learnerState.Quorum += acceptor))
            {
                learnerState.Value = phase2bMessage.Value;
                co_return LearnResult
                {
                    LearnedValue
                    {
                        .Value = *learnerState.Value,
                        .IsNewlyLearned = true,
                    },
                };
            }

            co_return LearnResult
            {
            };
        }
    };

    class StaticProposer
    {
    public:
        StaticProposer()
        {}

        TFuture<TValue> Propose(
            TValue value)
        {
        }
    };

    //class Proposer
    //    :
    //    public virtual IAsyncProposer<TValue>,
    //    public StaticProposer
    //{
    //};
};

}
