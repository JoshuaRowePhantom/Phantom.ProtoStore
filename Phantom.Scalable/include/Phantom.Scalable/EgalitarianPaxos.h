#pragma once

#include <optional>
#include "Phantom.System/async_utility.h"
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
        std::optional<command_type> Command;
        instance_set_type Dependencies;
        sequence_number_type SequenceNumber;
    };

    struct PreAcceptMessage
    {
        PaxosInstanceState PaxosInstanceState;
        CommandData CommandData;
    };

    struct AcceptMessage
    {
        PaxosInstanceState PaxosInstanceState;
        std::optional<CommandData> CommandData;
    };

    struct CommitMessage
    {
        PaxosInstanceState PaxosInstanceState;
        CommandData CommandData;
    };

    struct PrepareMessage
    {
        PaxosInstanceState PaxosInstanceState;
    };

    struct PreAcceptReplyMessage
    {
        PaxosInstanceState PaxosInstanceState;
        instance_set_type Dependencies;
        sequence_number_type SequenceNumber;
        instance_set_type CommittedDependencies;
    };

    struct AcceptReplyMessage
    {
        PaxosInstanceState PaxosInstanceState;
    };

    struct PrepareReplyMessage
    {
        PaxosInstanceState PaxosInstanceState;
        ballot_number_type PreviousBallotNumber;
        std::optional<CommandData> CommandData;
    };

    struct TryPreAcceptMessage
    {
        PaxosInstanceState PaxosInstanceState;
        std::optional<CommandData> CommandData;
    };

    enum class CommandStatus
    {
        NotSeen,
        PreAccepted,
        Accepted,
        Committed,
        TryPreAcceptOk,
        Executed,
    };

    struct TryPreAcceptReplyMessage
    {
        PaxosInstanceState PaxosInstanceState;
        CommandStatus CommandStatus;
    };

    struct NakMessage
    {
        PaxosInstanceState PaxosInstanceState;
    };
};

template<
    typename TAcceptor,
    typename TActions
> concept Acceptor
= requires (
    TAcceptor acceptor,
    typename TActions::PreAcceptMessage preAcceptMessage,
    typename TActions::AcceptMessage acceptMessage,
    typename TActions::PrepareMessage prepareMessage,
    typename TActions::TryPreAcceptMessage tryPreAcceptMessage,
    typename TActions::CommitMessage commitMessage
    )
{
    { acceptor.OnPreAccept(preAcceptMessage) } -> as_awaitable_convertible_to<typename TActions::PreAcceptResult>;
    { acceptor.OnAccept(acceptMessage) } -> as_awaitable_convertible_to<typename TActions::AcceptResult>;
    { acceptor.OnPrepare(prepareMessage) } -> as_awaitable_convertible_to<typename TActions::PrepareResult>;
    { acceptor.OnTryPreAccept(tryPreAcceptMessage) } -> as_awaitable_convertible_to<typename TActions::TryPreAcceptResult>;
    { acceptor.OnCommit(commitMessage) } -> as_awaitable_convertible_to<void>;
};

template<
    typename TCommandLeader,
    typename TActions
> concept CommandLeader
= requires(
    TCommandLeader commandLeader,
    typename TActions::PreAcceptResult preAcceptResult,
    typename TActions::AcceptResult acceptResult,
    typename TActions::PrepareResult prepareResult,
    typename TActions::TryPreAcceptResult tryPreAcceptResult
    )
{
    { commandLeader.OnPreAcceptReply(preAcceptResult) } -> as_awaitable_convertible_to<typename TActions::OnPreAcceptReplyResult>;
    { commandLeader.OnAcceptReply(acceptResult) } -> as_awaitable_convertible_to<typename TActions::OnAcceptReplyResult>;
    { commandLeader.OnPrepareReply(prepareResult) } -> as_awaitable_convertible_to<typename TActions::OnPrepareReplyResult>;
    { commandLeader.OnTryPreAcceptReply(tryPreAcceptResult) } -> as_awaitable_convertible_to<typename TActions::OnTryPreAcceptReplyResult>;
};

template<
    typename TCommandSequencer,
    typename TCommandExecutor,
    typename TActions
> concept CommandSequencer
= requires(
    TCommandSequencer commandSequencer,
    TCommandExecutor commandExecutor,
    typename TActions::instance_type instance,
    typename TActions::CommitMessage commitMessage)
{
    { commandSequencer.GetCommandExecutionSequence() } -> as_awaitable_async_enumerable_of<typename TActions::SequencedCommand>;
    { commandSequencer.OnCommit(commitMessage) };
};

template<
    typename TCommandLog,
    typename TActions
> concept CommandLog
= requires(
    TCommandLog commandLog,
    typename TActions::instance_type instance,
    typename TActions::CommandData commandData,
    typename TActions::CommandLogEntry commandLogEntry
    )
{
    { commandLog.Get(instance) } -> as_awaitable_convertible_to<typename TActions::CommandLogEntry>;
    { commandLog.Put(instance, commandLogEntry) } -> as_awaitable_convertible_to<void>;
    { commandLog.GetDependencies(instance, commandData) } -> as_awaitable_convertible_to<typename TActions::CommandDependencies>;
};

template<
    typename TMessages,
    typename TBallotNumberFactory,
    typename TCommandLog,
    template <typename> typename TFuture,
    template <typename> typename TGenerator,
    Event TEvent
>
class Actions
    :
    public TMessages
{
    using typename TMessages::ballot_number_type;
    using typename TMessages::member_type;
    using typename TMessages::instance_type;
    using typename TMessages::instance_set_type;
    using typename TMessages::command_type;
    using typename TMessages::sequence_number_type;
    
    using typename TMessages::CommandData;
    using typename TMessages::CommandStatus;
    using typename TMessages::PreAcceptMessage;
    using typename TMessages::PreAcceptReplyMessage;
    using typename TMessages::AcceptMessage;
    using typename TMessages::AcceptReplyMessage;
    using typename TMessages::PrepareMessage;
    using typename TMessages::PrepareReplyMessage;
    using typename TMessages::TryPreAcceptMessage;
    using typename TMessages::TryPreAcceptReplyMessage;
    using typename TMessages::NakMessage;
    using typename TMessages::CommitMessage;

    template<
        typename TMessage
    > class ReplicaResult
    {
        member_type m_replica;
        std::variant<
            TMessage,
            NakMessage
        > m_messageOrNak;

    public:
        template<
            typename TReplica,
            typename TMessageOrNak
        >
        ReplicaResult(
            TReplica&& replica,
            TMessageOrNak&& messageOrNak
        ) : 
            m_replica(
                std::forward<TReplica>(replica)),
            m_messageOrNak(
                std::forward<TMessageOrNak>(messageOrNak)
            )
        {}

        bool HasMessage() const
        {
            return hold_alternative<TMessage>(
                m_messageOrNak);
        }

        TMessage& Message()
        {
            return get<TMessage>(
                m_messageOrNak);
        }

        const TMessage& Message() const
        {
            return get<TMessage>(
                m_messageOrNak);
        }

        bool HasNak() const
        {
            return hold_alternative<Nak>(
                m_messageOrNak);
        }

        NakMessage& Nak()
        {
            return get<NakMessage>(
                m_messageOrNak);
        }

        const NakMessage& Nak() const
        {
            return get<NakMessage>(
                m_messageOrNak);
        }

        member_type& Replica()
        {
            return Replica;
        }

        const member_type& Replica() const
        {
            return m_replica;
        }
    };

    typedef ReplicaResult<PreAcceptReplyMessage> PreAcceptResult;
    typedef ReplicaResult<AcceptReplyMessage> AcceptResult;
    typedef ReplicaResult<PrepareReplyMessage> PrepareResult;
    typedef ReplicaResult<TryPreAcceptReplyMessage> TryPreAcceptResult;

    class IAsyncAcceptor
    {
    public:
        virtual TFuture<PreAcceptResult> OnPreAccept(
            PreAcceptMessage preAcceptMessage
        ) = 0;

        virtual TFuture<AcceptResult> OnAccept(
            AcceptMessage acceptMessage
        ) = 0;

        virtual TFuture<PrepareResult> OnPrepare(
            PrepareMessage prepareMessage
        ) = 0;

        virtual TFuture<TryPreAcceptResult> OnTryPreAccept(
            TryPreAcceptMessage tryPreAcceptMessage
        ) = 0;

        virtual TFuture<void> OnCommit(
            CommitMessage commitMessage
        ) = 0;
    };

    static_assert(Acceptor<
        IAsyncAcceptor,
        Actions
    >);

    enum CommandLeaderMessageDestination
    {
        Nowhere,
        AllReplicas,
        SlowQuorum,
        FastQuorum,
    };

    template<
        typename ... TMessages
    >
    class CommandLeaderResult
    {
    public:
        typedef std::variant<
            TMessages...
        > message_variant_type;

    private:
        CommandLeaderMessageDestination m_destination;
        message_variant_type m_message;
    
    public:
        template<
            typename TMessage
        >
            CommandLeaderResult(
            CommandLeaderMessageDestination destination,
            TMessage&& message
        ) : 
            m_destination(destination),
            m_message(
                std::forward<TMessage>(message))
        {}

        template<
            typename TMessage
        > bool has() const
        {
            return holds_alternative<TMessage>(
                m_message);
        }

        template<
            typename TMessage
        > const TMessage& get() const
        {
            return get<TMessage>(
                m_message);
        }
    };

    typedef CommandLeaderResult<PreAcceptMessage> StartResult;
    typedef CommandLeaderResult<std::monostate, CommitMessage, AcceptMessage> OnPreAcceptReplyResult;
    typedef CommandLeaderResult<std::monostate, CommitMessage, AcceptMessage> OnAcceptReplyResult;
    typedef CommandLeaderResult<std::monostate, CommitMessage> OnPrepareReplyResult;
    typedef CommandLeaderResult<std::monostate, CommitMessage> OnTryPreAcceptReplyResult;

    class IAsyncCommandLeader
    {
    public:
        virtual TFuture<StartResult> Start(
            instance_type,
            std::optional<command_type>
        ) = 0;

        virtual TFuture<OnPreAcceptReplyResult> OnPreAcceptReply(
            PreAcceptResult preAcceptResult
        ) = 0;

        virtual TFuture<OnAcceptReplyResult> OnAcceptReply(
            AcceptResult acceptResult
        ) = 0;

        virtual TFuture<OnPrepareReplyResult> OnPrepareReply(
            PrepareResult prepareResult
        ) = 0;

        virtual TFuture<OnTryPreAcceptReplyResult> OnTryPreAcceptReply(
            TryPreAcceptResult tryPreAcceptResult
        ) = 0;
    };

    static_assert(CommandLeader<
        IAsyncCommandLeader,
        Actions
    >);

    class IAsyncProposer
    {
    public:
        virtual TFuture<CommandData> Propose(
            instance_type instance,
            std::optional<command_type> command
        ) = 0;
    };

    struct SequencedCommand
    {
        instance_type Instance;
        CommandData CommandData;
        TEvent Event;
    };

    class IAsyncCommandSequencer
    {
    public:
        virtual TGenerator<SequencedCommand> GetCommandExecutionSequence(
        ) = 0;

        virtual void OnCommit(
            CommitMessage commitMessage
        ) = 0;
    };

    static_assert(CommandSequencer<
        IAsyncCommandSequencer,
        Actions
    >);

    struct CommandLogEntry
    {
        CommandData CommandData;
        ballot_number_type BallotNumber;
        CommandStatus CommandStatus;
    };

    struct CommandDependencies
    {
        instance_set_type Dependencies;
        sequence_number_type SequenceNumber;
    };

    class IAsyncCommandLog
    {
    public:
        virtual TFuture<CommandLogEntry> Get(
            const instance_type& instance
        ) = 0;

        virtual TFuture<void> Put(
            const instance_type& instance,
            const CommandLogEntry& commandLogEntry
        ) = 0;

        virtual TFuture<CommandDependencies> GetDependencies(
            const instance_type& instance,
            const CommandData& command
        ) = 0;
    };

    static_assert(CommandLog<
        IAsyncCommandLog,
        Actions
    >);
};


}