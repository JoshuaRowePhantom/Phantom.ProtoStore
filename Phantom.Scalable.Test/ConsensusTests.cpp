#include "StandardIncludes.h"
#include "Phantom.Scalable/Consensus.h"

namespace Phantom::Consensus
{

TEST(VectorQuorumCheckerTests, IsCompleteWithZeroMembers)
{
    VectorQuorumChecker vectorQuorumChecker(0, 0);
    ASSERT_TRUE(vectorQuorumChecker);
}

TEST(VectorQuorumCheckerTests, Ignores_duplicate_member_adds)
{
    VectorQuorumChecker vectorQuorumChecker(3, 2);
    ASSERT_FALSE(vectorQuorumChecker);
    vectorQuorumChecker += 0;
    ASSERT_FALSE(vectorQuorumChecker);
    vectorQuorumChecker += 0;
    ASSERT_FALSE(vectorQuorumChecker);
}

TEST(VectorQuorumCheckerTests, Becomes_complete_at_required_number_of_distinct_adds)
{
    VectorQuorumChecker vectorQuorumChecker(3, 2);
    ASSERT_FALSE(vectorQuorumChecker);
    vectorQuorumChecker += 0;
    ASSERT_FALSE(vectorQuorumChecker);
    vectorQuorumChecker += 1;
    ASSERT_TRUE(vectorQuorumChecker);
}

TEST(NumericBallotNumberFactoryTests, DefaultIsZero)
{
    NumericBallotNumberFactory<int> factory;
    ASSERT_EQ(0, factory());
}

TEST(NumericBallotNumberFactoryTests, Returns_next_higher_value)
{
    NumericBallotNumberFactory<int> factory;
    ASSERT_EQ(1, factory(0));
    ASSERT_EQ(2, factory(1));
}
}