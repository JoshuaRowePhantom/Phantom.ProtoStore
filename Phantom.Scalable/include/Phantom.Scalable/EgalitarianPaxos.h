#pragma once

#include <optional>
#include "Consensus.h"

namespace Phantom::Scalable::EgalitarianPaxos
{
template<
    typename TBallotNumber,
    typename TMember,
    typename TInstance,
    typename TInstanceSet,
    typename TCommand,
    typename TSequenceNumber
> class Messages
{
public:
    typedef TBallotNumber ballot_number_type;
    typedef TMember member_type;
    typedef TInstance instance_type;
    typedef TInstanceSet instance_set_type;
    typedef TCommand command_type;
    typedef TSequenceNumber sequence_number_type;

    struct PaxosInstanceState
    {
        instance_type Instance;
        ballot_number_type BallotNumber;
    };

    struct CommandData
    {
        command_type Command;
        instance_set_type Dependencies;
        sequence_number_type SequenceNumber;
    };

    struct PreAccept
    {
        PaxosInstanceState PaxosInstanceState;
        CommandData CommandData;
    };

    struct Accept
    {
        PaxosInstanceState PaxosInstanceState;
        std::optional<CommandData> CommandData;
    };

    struct Commit
    {
        PaxosInstanceState PaxosInstanceState;
        CommandData CommandData;
    };

    struct Prepare
    {
        PaxosInstanceState PaxosInstanceState;
    };

    struct PreAcceptReply
    {
        PaxosInstanceState PaxosInstanceState;
        instance_set_type Dependencies;
        sequence_number_type SequenceNumber;
        instance_set_type CommittedDependencies;
    };

    struct AcceptReply
    {
        PaxosInstanceState PaxosInstanceState;
    };

    struct PrepareReply
    {
        PaxosInstanceState PaxosInstanceState;
        ballot_number_type PreviousBallotNumber;
        std::optional<CommandData> CommandData;
    };

    struct TryPreAccept
    {
        PaxosInstanceState PaxosInstanceState;
        std::optional<CommandData> CommandData;
    };

    enum class Status
    {
        NotSeen,
        PreAccepted,
        Accepted,
        Committed,
        TryPreAcceptOk,
    };

    struct TryPreAcceptReply
    {
        PaxosInstanceState PaxosInstanceState;
        Status Status;
    };

    struct Nak
    {
        PaxosInstanceState PaxosInstanceState;
    };
};

template<
    typename TMessages,
    typename TBallotNumberFactory,
    typename TCommandLog,
    template <typename> typename TFuture
>
class Actions
    :
    public TMessages
{
    typedef typename TMessages::ballot_number_type ballot_number_type;
    typedef typename TMessages::member_type member_type;
    typedef typename TMessages::instance_type instance_type;
    typedef typename TMessages::instance_set_type instance_set_type;
    typedef typename TMessages::command_type command_type;
    typedef typename TMessages::sequence_number_type sequence_number_type;
    
    typedef typename TMessages::PreAccept PreAccept;
    typedef typename TMessages::PreAcceptReply PreAcceptReply;
    typedef typename TMessages::Accept Accept;
    typedef typename TMessages::AcceptReply AcceptReply;
    typedef typename TMessages::Prepare Prepare;
    typedef typename TMessages::PrepareReply PrepareReply;
    typedef typename TMessages::TryPreAccept TryPreAccept;
    typedef typename TMessages::TryPreAcceptReply TryPreAcceptReply;
    typedef typename TMessages::Nak Nak;

    class PreAcceptResult
    {
        std::variant<
            PreAcceptReply,
            Nak
        > Message;
    };

    class AcceptResult
    {
        std::variant<
            AcceptReply,
            Nak
        > Message;
    };

    class PrepareResult
    {
        std::variant<
            PrepareReply,
            Nak
        > Message;
    };

    class IAsyncAcceptor
    {
    public:
        TFuture<PreAcceptResult> PreAccept(
            PreAccept preAccept
        );
    };
};
}