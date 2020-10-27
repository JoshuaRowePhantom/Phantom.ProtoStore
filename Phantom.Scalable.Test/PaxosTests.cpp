#include "StandardIncludes.h"
#include "Phantom.Scalable/Paxos.h"
#include <cppcoro/task.hpp>

namespace Phantom::Consensus
{
class PaxosTests
    : public testing::Test
{
public:
    typedef Paxos<
        VectorQuorumChecker,
        std::function<VectorQuorumChecker(int)>,
        int,
        int,
        NumericBallotNumberFactory<int>,
        std::string,
        cppcoro::task
    > paxos_type;

};
}