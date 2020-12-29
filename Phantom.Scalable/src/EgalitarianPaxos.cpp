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

    typedef Services<
        actions_type,
        cppcoro::task,
        cppcoro::async_generator
    > services_type;

    class StaticTestCommandLog
        :
        public services_type,
        public services_type::IAsyncCommandLog
    {
    public:
        virtual cppcoro::task<std::optional<CommandLogEntry>> Get(
            const instance_type& instance
        ) ;

        virtual cppcoro::task<void> Put(
            const instance_type& instance,
            const std::optional<CommandLogEntry>& originalCommandLogEntry,
            const std::optional<CommandLogEntry>& newCommandLogEntry
        ) ;

        virtual cppcoro::task<UpdateDependenciesResult> UpdateDependencies(
            const PaxosInstanceState& instance,
            const CommandData& command
        ) ;
    };

    static_assert(EgalitarianPaxos::CommandLog<
        services_type::IAsyncCommandLog,
        services_type
    >);

    static_assert(EgalitarianPaxos::CommandLog<
        StaticTestCommandLog,
        services_type
    >);

    typedef StaticAcceptor<
        services_type,
        StaticTestCommandLog,
        cppcoro::task
    > static_acceptor_type;

    static_assert(Acceptor<
        static_acceptor_type,
        actions_type
    >);

    class StaticTestQuorumCheckerFactory
    {
    public:
        quorum_checker_type operator()(
            ballot_number_type,
            CommandLeaderMessageDestination destination
            );
    };

    static_assert(EgalitarianPaxosQuorumCheckerFactory<
        StaticTestQuorumCheckerFactory,
        ballot_number_type,
        quorum_checker_type,
        member_type
    >);

    class StaticTestMessageSender
        :
        public services_type,
        public services_type::IAsyncMessageSender
    {

    };

    typedef StaticCommandLeader<
        services_type,
        StaticTestQuorumCheckerFactory,
        quorum_checker_type,
        NumericBallotNumberFactory<size_t>,
        cppcoro::task
    > static_command_leader_type;

    static_assert(CommandLeader<
        services_type::IAsyncCommandLeader,
        actions_type
    >);

    static_assert(CommandLeader<
        static_command_leader_type,
        actions_type
    >);
};

template class StaticPaxosTests::static_command_leader_type;

}
