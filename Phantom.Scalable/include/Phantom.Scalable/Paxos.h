#pragma once

#include "Phantom.System/async_utility.h"
#include <optional>
#include <variant>
#include <vector>
#include "Phantom.Scalable/Consensus.h"

namespace Phantom::Consensus::Paxos
{

template
<
    typename TBallotNumber,
    typename TValue
>
class Messages
{
public:
    typedef TBallotNumber ballot_number_type;
    typedef TValue value_type;

    struct Phase1aMessage
    {
        ballot_number_type BallotNumber;

        bool operator==(const Phase1aMessage&) const = default;
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

        bool operator==(const Phase2aMessage&) const = default;
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

// An AsyncMutator accepts a value and returns a possibly awaitable value of the same type.
template<
    typename TMutator,
    typename TValue
> concept AsyncMutator =
requires (
    TMutator mutator,
    std::optional<TValue> value
    )
{
    { mutator(value) } -> as_awaitable_convertible_to<TValue>;
};

// PaxosMutator implements the original Paxos mutation
// function described in CASPaxos.
// value.
template<
    typename TValue
>
class PaxosMutator
{
    TValue m_value;

public:
    PaxosMutator() = default;

    template<
        typename T
    > PaxosMutator(
        T&& value)
    : m_value(std::forward<T>(value))
    {}

    TValue operator()(
        const std::optional<TValue>& original
        )
    {
        return original.value_or(
            m_value);
    }
};

template
<
    typename TMember,
    BallotNumber TBallotNumber,
    typename TValue,
    typename TMutator,
    QuorumChecker<TMember> TQuorumChecker,
    template <typename> typename TFuture
>
requires AsyncMutator<TMutator, TValue>
class StateMachineInterfaces
    : 
    public Messages<TBallotNumber, TValue>
{
public:
    typedef Messages<TBallotNumber, TValue> messages_type;
    using typename messages_type::ballot_number_type;
    using typename messages_type::value_type;
    using typename messages_type::Phase1aMessage;
    using typename messages_type::Phase1bMessage;
    using typename messages_type::Phase1bVote;
    using typename messages_type::Phase2aMessage;
    using typename messages_type::Phase2bMessage;
    using typename messages_type::NakMessage;

    typedef TMutator mutator_type;
    typedef TQuorumChecker quorum_checker_type;
    typedef TMember member_type;

    typedef std::tuple<member_type, Phase1bMessage> Phase1bResponse;
    typedef std::tuple<member_type, Phase2bMessage> Phase2bResponse;

    struct LeaderState
    {
        mutator_type Mutator;
        std::optional<value_type> Proposal;
        std::optional<ballot_number_type> CurrentBallotNumber;
        std::optional<ballot_number_type> MaxVotedBallotNumber;
        std::optional<quorum_checker_type> Phase1bQuorum;
    };

    struct Phase1aResult
    {
        Phase1aMessage Phase1aMessage;

        bool operator==(const Phase1aResult&) const = default;
    };

    enum class Phase2aResultAction
    {
        MismatchedBallot,
        QuorumOverreached,
        QuorumNotReached,
        QuorumReached,
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
            mutator_type mutator
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
        ballot_number_type LearnedBallotNumber;
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
    typename TMutator,
    template<typename> typename TFuture
>
requires
QuorumCheckerFactory<TQuorumCheckerFactory, TBallotNumber, TQuorumChecker, TMember>
&& AsyncMutator<TMutator, TValue>
class StateMachines
    :
    public StateMachineInterfaces
    <
    TMember,
    TBallotNumber,
    TValue,
    TMutator,
    TQuorumChecker,
    TFuture
    >
{
public:
    typedef StateMachineInterfaces<
        TMember,
        TBallotNumber,
        TValue,
        TMutator,
        TQuorumChecker,
        TFuture
    > paxos_interfaces_type;
    typedef TQuorumCheckerFactory quorum_checker_factory_type;
    using typename paxos_interfaces_type::ballot_number_type;
    typedef TBallotNumberFactory ballot_number_factory_type;
    using typename paxos_interfaces_type::member_type;
    using typename paxos_interfaces_type::mutator_type;
    using typename paxos_interfaces_type::value_type;
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

    class StaticLeader
    {
        quorum_checker_factory_type m_quorumCheckerFactory;
        ballot_number_factory_type m_ballotNumberFactory;

    public:
        StaticLeader(
            quorum_checker_factory_type quorumCheckerFactory,
            ballot_number_factory_type ballotNumberFactory
        ) : m_quorumCheckerFactory(quorumCheckerFactory),
            m_ballotNumberFactory(ballotNumberFactory)
        {}

        TFuture<Phase1aResult> Phase1a(
            LeaderState& leaderState,
            mutator_type mutator
        )
        {
            co_return co_await Phase1a(
                leaderState,
                leaderState.CurrentBallotNumber
                ? co_await as_awaitable(m_ballotNumberFactory(*leaderState.CurrentBallotNumber))
                : co_await as_awaitable(m_ballotNumberFactory()),
                move(mutator)
            );
        }

        TFuture<Phase1aResult> Phase1a(
            LeaderState& leaderState,
            ballot_number_type ballotNumber,
            mutator_type mutator
        )
        {
            leaderState.Mutator = std::move(mutator);
            leaderState.CurrentBallotNumber = std::move(ballotNumber);
            leaderState.Proposal.reset();
            leaderState.Phase1bQuorum = co_await as_awaitable(m_quorumCheckerFactory(
                ballotNumber));

            Phase1aResult result =
            {
                .Phase1aMessage = Phase1aMessage
                {
                    .BallotNumber = *leaderState.CurrentBallotNumber
                }
            };

            co_return result;
        }

        TFuture<Phase2aResult> Phase2a(
            LeaderState& leaderState,
            member_type member,
            const Phase1bMessage& phase1bMessage
        )
        {
            if (leaderState.CurrentBallotNumber != phase1bMessage.BallotNumber)
            {
                co_return Phase2aResult
                {
                    Phase2aResultAction::MismatchedBallot
                };
            }

            if (*leaderState.Phase1bQuorum)
            {
                co_return Phase2aResult
                {
                    Phase2aResultAction::QuorumOverreached
                };
            }

            *leaderState.Phase1bQuorum += member;

            if (phase1bMessage.Phase1bVote
                &&
                phase1bMessage.Phase1bVote->VotedBallotNumber > leaderState.MaxVotedBallotNumber)
            {
                leaderState.MaxVotedBallotNumber = phase1bMessage.Phase1bVote->VotedBallotNumber;
                leaderState.Proposal = phase1bMessage.Phase1bVote->VotedValue;
            }

            if (!*leaderState.Phase1bQuorum)
            {
                co_return Phase2aResult
                {
                    Phase2aResultAction::QuorumNotReached
                };
            }

            co_return Phase2aResult
            {
                .Action = Phase2aResultAction::QuorumReached,
                .Phase2aMessage = Phase2aMessage
                {
                    .BallotNumber = *leaderState.CurrentBallotNumber,
                    .Value = co_await as_awaitable(
                        leaderState.Mutator(
                            leaderState.Proposal)),
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
                        .LearnedBallotNumber = *learnerState.MaxBallotNumber,
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
                        .LearnedBallotNumber = *learnerState.MaxBallotNumber,
                    },
                };
            }

            co_return LearnResult
            {
            };
        }
    };
};

template<
    typename TMessageSender,
    typename TStateMachines
> concept MessageSender = requires (
    TMessageSender messageSender,
    typename TStateMachines::Phase1aMessage phase1aMessage,
    typename TStateMachines::Phase2aMessage phase2aMessage
    )
{
    { messageSender.SendPhase1a(phase1aMessage) } -> as_awaitable_async_enumerable; //_of<typename TStateMachines::Phase1bResponse>;
    { messageSender.SendPhase2a(phase2aMessage) } -> as_awaitable_async_enumerable; //_of<typename TStateMachines::Phase2bResponse>;
};

template<
    typename TStateMachines,
    typename TMessageSender,
    template <typename> typename TFuture
>
requires
MessageSender<TMessageSender, TStateMachines>
class StaticProposer
    : public TStateMachines
{
public:
    typedef TStateMachines paxos_type;
    typedef TMessageSender message_sender_type;
    using typename paxos_type::quorum_checker_factory_type;
    using typename paxos_type::ballot_number_factory_type;
    using typename paxos_type::member_type;
    using typename paxos_type::value_type;
    using typename paxos_type::mutator_type;
    using typename paxos_type::NakMessage;
    using typename paxos_type::Phase1aResult;
    using typename paxos_type::Phase1bMessage;
    using typename paxos_type::Phase2aResult;
    using typename paxos_type::Phase2bMessage;
    using typename paxos_type::LearnedValue;
private:
    typename paxos_type::StaticLeader m_leader;
    typename paxos_type::StaticLearner m_learner;
    message_sender_type m_messageSender;

public:

    StaticProposer(
        quorum_checker_factory_type quorumCheckerFactory,
        ballot_number_factory_type ballotNumberFactory,
        message_sender_type messageSender)
        : m_leader(
            quorumCheckerFactory,
            ballotNumberFactory
        ),
        m_learner(
            quorumCheckerFactory
        ),
        m_messageSender(
            std::move(messageSender))
    {
    }

    TFuture<LearnedValue> Propose(
        value_type value)
    {
        typename paxos_type::LeaderState leaderState;
        typename paxos_type::LearnerState learnerState;

    StartPhase1a:
        auto phase1aResult = co_await m_leader.Phase1a(
            leaderState,
            value);

    SendPhase1a:
        auto phase1bEnumeration = m_messageSender.SendPhase1a(
            phase1aResult.Phase1aMessage
        );

        typename paxos_type::Phase2aMessage phase2aMessage;

        for (
            auto phase1bIterator = co_await as_awaitable(phase1bEnumeration.begin());
            phase1bIterator != phase1bEnumeration.end();
            co_await as_awaitable(++phase1bIterator))
        {
            member_type member;
            typename paxos_type::Phase1bResult phase1bResult;

            tie(member, phase1bResult) = *phase1bIterator;

            if (has<NakMessage>(phase1bResult.Phase1bResponseMessage))
            {
                auto nakResult = co_await m_leader.Nak(
                    leaderState,
                    get<NakMessage>(phase1bResult.Phase1bResponseMessage));

                if (has<Phase1aResult>(nakResult.Phase1aResult))
                {
                    phase1aResult = get<Phase1aResult>(
                        nakResult.Phase1aResult);
                    goto SendPhase1a;
                }
            }

            if (has<Phase1bMessage>(phase1bResult.Phase1bResponseMessage))
            {
                auto phase2aResult = co_await m_leader.Phase2a(
                    leaderState,
                    member,
                    get<Phase1bMessage>(phase1bResult.Phase1bResponseMessage));

                if (phase2aResult.Phase2aMessage)
                {
                    phase2aMessage = std::move(
                        *phase2aResult.Phase2aMessage);

                    goto SendPhase2a;
                }
            }
        }
        // If we reached here,
        // no more messages are forthcoming, so start a new round of Paxos.
        goto StartPhase1a;

    SendPhase2a:
        auto phase2bEnumeration = m_messageSender.SendPhase2a(
            phase2aMessage);

        for (
            auto phase2bIterator = co_await as_awaitable(phase2bEnumeration.begin());
            phase2bIterator != phase2bEnumeration.end();
            co_await as_awaitable(++phase2bIterator)
            )
        {
            member_type member;
            typename paxos_type::Phase2bResult phase2bResult;

            tie(member, phase2bResult) = *phase2bIterator;

            if (has<NakMessage>(phase2bResult.Phase2bResponseMessage))
            {
                auto nakResult = co_await m_leader.Nak(
                    leaderState,
                    get<NakMessage>(phase2bResult.Phase2bResponseMessage));

                if (has<Phase1aResult>(nakResult.Phase1aResult))
                {
                    phase1aResult = get<Phase1aResult>(
                        nakResult.Phase1aResult);
                    goto SendPhase1a;
                }
            }

            if (has<Phase2bMessage>(phase2bResult.Phase2bResponseMessage))
            {
                auto learnResult = co_await m_learner.Learn(
                    learnerState,
                    member,
                    get<Phase2bMessage>(phase2bResult.Phase2bResponseMessage));

                if (learnResult.has_value())
                {
                    co_return get<LearnedValue>(
                        learnResult.LearnedValue);
                }
            }
        }
        // If we reach here, no more Phase2b messages are forthcoming,
        // so we have to start another round of Paxos.
        goto StartPhase1a;
    }
};

}
