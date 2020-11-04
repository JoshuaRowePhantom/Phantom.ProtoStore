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
            EXPECT_EQ(learnedValue, *learnResult.value());
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
}