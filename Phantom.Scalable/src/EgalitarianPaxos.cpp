#include "StandardIncludes.h"
#include "Phantom.Scalable/EgalitarianPaxos.h"
#include <set>
#include <cppcoro/task.hpp>
#include <cppcoro/async_generator.hpp>

namespace Phantom::Consensus::EgalitarianPaxos
{
// Static tests.
class StaticPaxosTests
{
    typedef long ballot_number_type;
    typedef size_t member_type;
    typedef std::string instance_type;
    typedef std::set<std::string> instance_set_type;
    typedef std::string command_type;
    typedef std::size_t sequence_number_type;

    typedef VectorQuorumChecker quorum_checker_type;

    typedef Messages<
        ballot_number_type,
        member_type,
        instance_type,
        instance_set_type,
        command_type,
        sequence_number_type
    > messages_type;

    typedef Actions<
        messages_type,
        cppcoro::task,
        cppcoro::async_generator
    > actions_type;

    class CommandLog
    {};

    typedef AcceptorServices<
        actions_type,
        CommandLog,
        cppcoro::task
    > acceptor_services_type;

    typedef StaticAcceptor<
        acceptor_services_type,
        cppcoro::task
    > static_acceptor_type;

    static_assert(Acceptor<
        static_acceptor_type,
        actions_type
    >);
};
}
