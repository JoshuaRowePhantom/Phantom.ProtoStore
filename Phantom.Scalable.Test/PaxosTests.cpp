#include "StandardIncludes.h"
#include "Phantom.Scalable/Paxos.h"
#include <cppcoro/task.hpp>

namespace Phantom::Consensus
{
class PaxosTestsBase
{
public:
    typedef size_t member_type;
    typedef VectorQuorumChecker quorum_checker_type;
    typedef size_t ballot_number_type;
    typedef std::function<quorum_checker_type(ballot_number_type)> quorum_checker_factory_type;
    typedef NumericBallotNumberFactory<ballot_number_type> ballot_number_factory_type;
    typedef std::string value_type;

    typedef Paxos<
        member_type,
        quorum_checker_type,
        quorum_checker_factory_type,
        ballot_number_type,
        ballot_number_factory_type,
        value_type,
        cppcoro::task
    > paxos_type;
};

class PaxosTests
    : public testing::Test,
    public PaxosTestsBase,
    public PaxosTestsBase::paxos_type
{
public:
    using PaxosTestsBase::ballot_number_type;
    using PaxosTestsBase::quorum_checker_type;
    using PaxosTestsBase::quorum_checker_factory_type;
    using PaxosTestsBase::value_type;

    quorum_checker_factory_type CreateQuorumCheckerFactory(
        size_t totalMemberCount,
        size_t requiredMemberCount)
    {
        auto factory = [=](ballot_number_type ballotNumber) -> quorum_checker_type
        {
            quorum_checker_type quorumChecker(
                totalMemberCount,
                requiredMemberCount);

            return quorumChecker;
        };

        return factory;
    }
};

TEST_F(PaxosTests, Learner_returns_already_learned_value)
{
    run_async([=]() -> cppcoro::task<>
    {
        paxos_type::LearnerState state;
        state.Value = "hello world";

        paxos_type::StaticLearner learner(
            CreateQuorumCheckerFactory(5, 3));

        auto learnResult = co_await learner.Learn(
            state,
            0,
            {}
        );

        EXPECT_EQ(std::string("hello world"), get<LearnedValue>(learnResult.LearnedValue).Value);
        EXPECT_EQ(false, get<LearnedValue>(learnResult.LearnedValue).IsNewlyLearned);
    });
}

TEST_F(PaxosTests, Learner_returns_Nak_for_old_ballot)
{
    run_async([=]() -> cppcoro::task<>
    {
        paxos_type::LearnerState state;

        paxos_type::StaticLearner learner(
            CreateQuorumCheckerFactory(5, 3));

        {
            auto learnResult = co_await learner.Learn(
                state,
                0,
                {
                    .BallotNumber = 2,
                    .Value = "hello world", 
                }
            );

            EXPECT_EQ(std::monostate(), get<std::monostate>(learnResult.LearnedValue));
        }

        {
            auto learnResult = co_await learner.Learn(
                state,
                0,
                {
                    .BallotNumber = 1,
                    .Value = "hello world",
                }
            );

            EXPECT_EQ((
                NakMessage
                {
                    .BallotNumber = 1,
                    .MaxBallotNumber = 2,
                }), 
                get<NakMessage>(learnResult.LearnedValue));
        }
    });
}

TEST_F(PaxosTests, Learner_learns_when_quorum_commits_at_same_ballot_number)
{
    run_async([=]() -> cppcoro::task<>
    {
        paxos_type::LearnerState state;

        paxos_type::StaticLearner learner(
            CreateQuorumCheckerFactory(5, 3)
        );

        {
            auto learnResult = co_await learner.Learn(
                state,
                0,
                {
                    .BallotNumber = 1,
                    .Value = "hello world",
                }
            );
        
            EXPECT_EQ(std::nullopt, learnResult.value());
        }

        {
            auto learnResult = co_await learner.Learn(
                state,
                1,
                {
                    .BallotNumber = 1,
                    .Value = "hello world",
                }
            );

            EXPECT_EQ(std::nullopt, learnResult.value());
        }

        {
            auto learnResult = co_await learner.Learn(
                state,
                2,
                {
                    .BallotNumber = 1,
                    .Value = "hello world",
                }
            );

            value_type learnedValue("hello world");
            EXPECT_EQ(true, get<LearnedValue>(learnResult.LearnedValue).IsNewlyLearned);
            EXPECT_EQ(learnedValue, *learnResult.value());
            EXPECT_EQ(learnedValue, state.Value);
        }
    });
}

TEST_F(PaxosTests, Learner_resets_quorum_when_new_ballot_received)
{
    run_async([=]() -> cppcoro::task<>
    {
        paxos_type::LearnerState state;

        paxos_type::StaticLearner learner(
            CreateQuorumCheckerFactory(5, 3)
        );

        {
            auto learnResult = co_await learner.Learn(
                state,
                0,
                {
                    .BallotNumber = 1,
                    .Value = "hello world",
                }
            );

            EXPECT_EQ(std::nullopt, learnResult.value());
        }

        {
            auto learnResult = co_await learner.Learn(
                state,
                1,
                {
                    .BallotNumber = 1,
                    .Value = "hello world",
                }
            );

            EXPECT_EQ(std::nullopt, learnResult.value());
        }

        // This is a newer ballot, so should reset the quorum.
        {
            auto learnResult = co_await learner.Learn(
                state,
                2,
                {
                    .BallotNumber = 2,
                    .Value = "hello world",
                }
            );

            EXPECT_EQ(std::nullopt, learnResult.value());
        }

        {
            auto learnResult = co_await learner.Learn(
                state,
                0,
                {
                    .BallotNumber = 2,
                    .Value = "hello world",
                }
            );

            EXPECT_EQ(std::nullopt, learnResult.value());
        }

        {
            auto learnResult = co_await learner.Learn(
                state,
                1,
                {
                    .BallotNumber = 2,
                    .Value = "hello world",
                }
            );

            value_type learnedValue("hello world");
            EXPECT_EQ(learnedValue, *learnResult.value());
        }
    });
}

TEST_F(PaxosTests, Acceptor_Phase1b_accepts_increasing_ballots_from_Phase1b)
{
    run_async([=]()->cppcoro::task<>
    {
        AcceptorState state;
        StaticAcceptor acceptor;

        {
            auto phase1bResult = co_await acceptor.Phase1b(
                state,
                Phase1aMessage
                {
                    .BallotNumber = 1
                }
            );

            EXPECT_EQ((
                Phase1bMessage
                {
                    .BallotNumber = 1,
                }),
                get<Phase1bMessage>(phase1bResult.Phase1bResponseMessage));
        }

        {
            auto phase1bResult = co_await acceptor.Phase1b(
                state,
                Phase1aMessage
                {
                    .BallotNumber = 2
                }
            );

            EXPECT_EQ((
                Phase1bMessage
                {
                    .BallotNumber = 2,
                }),
                get<Phase1bMessage>(phase1bResult.Phase1bResponseMessage));
        }
    });
}

TEST_F(PaxosTests, Acceptor_Phase1b_rejects_same_ballot_from_Phase1b)
{
    run_async([=]()->cppcoro::task<>
    {
        AcceptorState state;
        StaticAcceptor acceptor;

        {
            auto phase1bResult = co_await acceptor.Phase1b(
                state,
                Phase1aMessage
                {
                    .BallotNumber = 1
                }
            );

            EXPECT_EQ((
                Phase1bMessage
                {
                    .BallotNumber = 1,
                }),
                get<Phase1bMessage>(phase1bResult.Phase1bResponseMessage));
        }

        {
            auto phase1bResult = co_await acceptor.Phase1b(
                state,
                Phase1aMessage
                {
                    .BallotNumber = 1
                }
            );

            EXPECT_EQ((
                NakMessage
                {
                    .BallotNumber = 1,
                    .MaxBallotNumber = 1,
                }),
                get<NakMessage>(phase1bResult.Phase1bResponseMessage));
        }
    });
}

TEST_F(PaxosTests, Acceptor_Phase1b_rejects_smaller_ballot_from_Phase1b)
{
    run_async([=]()->cppcoro::task<>
    {
        AcceptorState state;
        StaticAcceptor acceptor;

        {
            auto phase1bResult = co_await acceptor.Phase1b(
                state,
                Phase1aMessage
                {
                    .BallotNumber = 2
                }
            );

            EXPECT_EQ((
                Phase1bMessage
                {
                    .BallotNumber = 2,
                }),
                get<Phase1bMessage>(phase1bResult.Phase1bResponseMessage));
        }

        {
            auto phase1bResult = co_await acceptor.Phase1b(
                state,
                Phase1aMessage
                {
                    .BallotNumber = 1
                }
            );

            EXPECT_EQ((
                NakMessage
                {
                    .BallotNumber = 1,
                    .MaxBallotNumber = 2,
                }),
                get<NakMessage>(phase1bResult.Phase1bResponseMessage));
        }
    });
}

TEST_F(PaxosTests, Acceptor_Phase1b_accepts_increasing_ballots_from_Phase2b)
{
    run_async([=]()->cppcoro::task<>
    {
        AcceptorState state;
        StaticAcceptor acceptor;

        {
            auto phase2bResult = co_await acceptor.Phase2b(
                state,
                Phase2aMessage
                {
                    .BallotNumber = 1,
                    .Value = "hello world",
                }
            );

            EXPECT_EQ((
                Phase2bMessage
                {
                    .BallotNumber = 1,
                    .Value = "hello world",
                }),
                get<Phase2bMessage>(phase2bResult.Phase2bResponseMessage));
        }

        {
            auto phase1bResult = co_await acceptor.Phase1b(
                state,
                Phase1aMessage
                {
                    .BallotNumber = 2
                }
            );

            EXPECT_EQ((
                Phase1bMessage
                {
                    .BallotNumber = 2,
                    .Phase1bVote = Phase1bVote
                    {
                        .VotedBallotNumber = 1,
                        .VotedValue = "hello world",
                    },
                }),
                get<Phase1bMessage>(phase1bResult.Phase1bResponseMessage));
        }
    });
}

TEST_F(PaxosTests, Acceptor_Phase1b_rejects_same_ballot_from_Phase2b)
{
    run_async([=]()->cppcoro::task<>
    {
        AcceptorState state;
        StaticAcceptor acceptor;

        {
            auto phase2bResult = co_await acceptor.Phase2b(
                state,
                Phase2aMessage
                {
                    .BallotNumber = 1,
                    .Value = "hello world",
                }
            );

            EXPECT_EQ((
                Phase2bMessage
                {
                    .BallotNumber = 1,
                    .Value = "hello world",
                }),
                get<Phase2bMessage>(phase2bResult.Phase2bResponseMessage));
        }

        {
            auto phase1bResult = co_await acceptor.Phase1b(
                state,
                Phase1aMessage
                {
                    .BallotNumber = 1
                }
            );

            EXPECT_EQ((
                NakMessage
                {
                    .BallotNumber = 1,
                    .MaxBallotNumber = 1,
                }),
                get<NakMessage>(phase1bResult.Phase1bResponseMessage));
        }
    });
}

TEST_F(PaxosTests, Acceptor_Phase1b_rejects_smaller_ballot_from_Phase2b)
{
    run_async([=]()->cppcoro::task<>
    {
        AcceptorState state;
        StaticAcceptor acceptor;

        {
            auto phase2bResult = co_await acceptor.Phase2b(
                state,
                Phase2aMessage
                {
                    .BallotNumber = 2,
                    .Value = "hello world",
                }
            );

            EXPECT_EQ((
                Phase2bMessage
                {
                    .BallotNumber = 2,
                    .Value = "hello world",
                }),
                get<Phase2bMessage>(phase2bResult.Phase2bResponseMessage));
        }

        {
            auto phase1bResult = co_await acceptor.Phase1b(
                state,
                Phase1aMessage
                {
                    .BallotNumber = 1
                }
            );

            EXPECT_EQ((
                NakMessage
                {
                    .BallotNumber = 1,
                    .MaxBallotNumber = 2,
                }),
                get<NakMessage>(phase1bResult.Phase1bResponseMessage));
        }
    });
}


TEST_F(PaxosTests, Acceptor_Phase2b_accepts_increasing_ballots_from_Phase1b)
{
    run_async([=]()->cppcoro::task<>
    {
        AcceptorState state;
        StaticAcceptor acceptor;

        {
            auto phase1bResult = co_await acceptor.Phase1b(
                state,
                Phase1aMessage
                {
                    .BallotNumber = 1
                }
            );

            EXPECT_EQ((
                Phase1bMessage
                {
                    .BallotNumber = 1,
                }),
                get<Phase1bMessage>(phase1bResult.Phase1bResponseMessage));
        }

        {
            auto phase2bResult = co_await acceptor.Phase2b(
                state,
                Phase2aMessage
                {
                    .BallotNumber = 2,
                    .Value = "hello world",
                }
            );

            EXPECT_EQ((
                Phase2bMessage
                {
                    .BallotNumber = 2,
                    .Value = "hello world",
                }),
                get<Phase2bMessage>(phase2bResult.Phase2bResponseMessage));
        }
    });
}

TEST_F(PaxosTests, Acceptor_Phase2b_accepts_same_ballot_from_Phase1b)
{
    run_async([=]()->cppcoro::task<>
    {
        AcceptorState state;
        StaticAcceptor acceptor;

        {
            auto phase1bResult = co_await acceptor.Phase1b(
                state,
                Phase1aMessage
                {
                    .BallotNumber = 1
                }
            );

            EXPECT_EQ((
                Phase1bMessage
                {
                    .BallotNumber = 1,
                }),
                get<Phase1bMessage>(phase1bResult.Phase1bResponseMessage));
        }

        {
            auto phase2bResult = co_await acceptor.Phase2b(
                state,
                Phase2aMessage
                {
                    .BallotNumber = 1,
                    .Value = "hello world",
                }
            );

            EXPECT_EQ((
                Phase2bMessage
                {
                    .BallotNumber = 1,
                    .Value = "hello world",
                }),
                get<Phase2bMessage>(phase2bResult.Phase2bResponseMessage));
        }
    });
}

TEST_F(PaxosTests, Acceptor_Phase2b_rejects_smaller_ballot_from_Phase1b)
{
    run_async([=]()->cppcoro::task<>
    {
        AcceptorState state;
        StaticAcceptor acceptor;

        {
            auto phase1bResult = co_await acceptor.Phase1b(
                state,
                Phase1aMessage
                {
                    .BallotNumber = 2
                }
            );

            EXPECT_EQ((
                Phase1bMessage
                {
                    .BallotNumber = 2,
                }),
                get<Phase1bMessage>(phase1bResult.Phase1bResponseMessage));
        }

        {
            auto phase2bResult = co_await acceptor.Phase2b(
                state,
                Phase2aMessage
                {
                    .BallotNumber = 1,
                    .Value = "hello world",
                }
            );

            EXPECT_EQ((
                NakMessage
                {
                    .BallotNumber = 1,
                    .MaxBallotNumber = 2,
                }),
                get<NakMessage>(phase2bResult.Phase2bResponseMessage));
        }
    });
}

TEST_F(PaxosTests, Acceptor_Phase2b_accepts_increasing_ballots_from_Phase2b)
{
    run_async([=]()->cppcoro::task<>
    {
        AcceptorState state;
        StaticAcceptor acceptor;

        {
            auto phase2bResult = co_await acceptor.Phase2b(
                state,
                Phase2aMessage
                {
                    .BallotNumber = 1,
                    .Value = "hello world",
                }
            );

            EXPECT_EQ((
                Phase2bMessage
                {
                    .BallotNumber = 1,
                    .Value = "hello world",
                }),
                get<Phase2bMessage>(phase2bResult.Phase2bResponseMessage));
        }

        {
            auto phase2bResult = co_await acceptor.Phase2b(
                state,
                Phase2aMessage
                {
                    .BallotNumber = 2,
                    .Value = "hello world",
                }
            );

            EXPECT_EQ((
                Phase2bMessage
                {
                    .BallotNumber = 2,
                    .Value = "hello world",
                }),
                get<Phase2bMessage>(phase2bResult.Phase2bResponseMessage));
        }
    });
}

TEST_F(PaxosTests, Acceptor_Phase2b_accepts_same_ballot_from_Phase2b)
{
    run_async([=]()->cppcoro::task<>
    {
        AcceptorState state;
        StaticAcceptor acceptor;

        {
            auto phase2bResult = co_await acceptor.Phase2b(
                state,
                Phase2aMessage
                {
                    .BallotNumber = 1,
                    .Value = "hello world",
                }
            );

            EXPECT_EQ((
                Phase2bMessage
                {
                    .BallotNumber = 1,
                    .Value = "hello world",
                }),
                get<Phase2bMessage>(phase2bResult.Phase2bResponseMessage));
        }

        {
            auto phase2bResult = co_await acceptor.Phase2b(
                state,
                Phase2aMessage
                {
                    .BallotNumber = 1,
                    .Value = "hello world",
                }
            );

            EXPECT_EQ((
                Phase2bMessage
                {
                    .BallotNumber = 1,
                    .Value = "hello world",
                }),
                get<Phase2bMessage>(phase2bResult.Phase2bResponseMessage));
        }
    });
}

TEST_F(PaxosTests, Acceptor_Phase2b_rejects_smaller_ballot_from_Phase2b)
{
    run_async([=]()->cppcoro::task<>
    {
        AcceptorState state;
        StaticAcceptor acceptor;

        {
            auto phase2bResult = co_await acceptor.Phase2b(
                state,
                Phase2aMessage
                {
                    .BallotNumber = 2,
                    .Value = "hello world",
                }
            );

            EXPECT_EQ((
                Phase2bMessage
                {
                    .BallotNumber = 2,
                    .Value = "hello world",
                }),
                get<Phase2bMessage>(phase2bResult.Phase2bResponseMessage));
        }

        {
            auto phase2bResult = co_await acceptor.Phase2b(
                state,
                Phase2aMessage
                {
                    .BallotNumber = 1,
                    .Value = "hello world",
                }
            );

            EXPECT_EQ((
                NakMessage
                {
                    .BallotNumber = 1,
                    .MaxBallotNumber = 2,
                }),
                get<NakMessage>(phase2bResult.Phase2bResponseMessage));

        }
    });
}
}