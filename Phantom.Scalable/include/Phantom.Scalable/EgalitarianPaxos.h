#pragma once

#include <optional>
#include "Phantom.System/async_utility.h"
#include "Consensus.h"

namespace Phantom::Scalable::EgalitarianPaxos
{

enum CommandLeaderMessageDestination
{
    Nowhere,
    AllReplicas,
    SlowQuorum,
    FastQuorum,
};

template<
    typename TQuorumCheckerFactory,
    typename TBallotNumber,
    typename TQuorumChecker,
    typename TMember
>
concept EgalitarianPaxosQuorumCheckerFactory
=
QuorumChecker<TQuorumChecker, TMember>
&&
BallotNumber<TBallotNumber>
&&
requires(
    TQuorumCheckerFactory quorumCheckerFactory,
    TBallotNumber ballotNumber,
    CommandLeaderMessageDestination destination
    )
{
    { quorumCheckerFactory(ballotNumber, destination) } -> as_awaitable_convertible_to<TQuorumChecker>;
};

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

    enum class CommandStatus
    {
        NotSeen,
        PreAccepted,
        Accepted,
        Committed,
        TryPreAcceptOk,
        Executed,
    };

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
        std::optional<ballot_number_type> PreviousBallotNumber;
        std::optional<CommandData> CommandData;
        CommandStatus CommandStatus;
    };

    struct TryPreAcceptMessage
    {
        PaxosInstanceState PaxosInstanceState;
        std::optional<CommandData> CommandData;
    };


    struct TryPreAcceptReplyMessage
    {
        PaxosInstanceState PaxosInstanceState;
        CommandStatus CommandStatus;
    };

    struct NakMessage
    {
        PaxosInstanceState PaxosInstanceState;
        ballot_number_type MaxBallotNumber;
    };

    // This isn't really used as a message, but as the result of an acceptor's OnCommit method.
    // If the PreviousCommandStatus was not Committed or Executed,
    // then the command has been newly committed to this instance.
    struct CommittedMessage
    {
        PaxosInstanceState PaxosInstanceState;
        CommandStatus PreviousCommandStatus;
        std::optional<CommandData> CommandData;
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
    { acceptor.OnCommit(commitMessage) } -> as_awaitable_convertible_to<typename TActions::CommitResult>;
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
    typename TMessages,
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
    using typename TMessages::CommittedMessage;

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
    typedef ReplicaResult<TryPreAcceptReplyMessage> TryPreAcceptResult;
    typedef ReplicaResult<CommittedMessage> CommitResult;

    class IAsyncAcceptor
    {
    public:
        virtual TFuture<PreAcceptResult> OnPreAccept(
            const PreAcceptMessage& preAcceptMessage
        ) = 0;

        virtual TFuture<AcceptResult> OnAccept(
            const AcceptMessage& acceptMessage
        ) = 0;

        virtual TFuture<PrepareResult> OnPrepare(
            const PrepareMessage& prepareMessage
        ) = 0;

        virtual TFuture<TryPreAcceptResult> OnTryPreAccept(
            const TryPreAcceptMessage& tryPreAcceptMessage
        ) = 0;

        virtual TFuture<CommitResult> OnCommit(
            const CommitMessage& commitMessage
        ) = 0;
    };

    static_assert(Acceptor<
        IAsyncAcceptor,
        Actions
    >);

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
        > CommandLeaderResult(
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
            const instance_type&,
            const std::optional<command_type>&
        ) = 0;

        virtual TFuture<OnPreAcceptReplyResult> OnPreAcceptReply(
            const PreAcceptResult& preAcceptResult
        ) = 0;

        virtual TFuture<OnAcceptReplyResult> OnAcceptReply(
            const AcceptResult& acceptResult
        ) = 0;

        virtual TFuture<OnPrepareReplyResult> OnPrepareReply(
            const PrepareResult& prepareResult
        ) = 0;

        virtual TFuture<OnTryPreAcceptReplyResult> OnTryPreAcceptReply(
            const TryPreAcceptResult& tryPreAcceptResult
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
            const instance_type& instance,
            const std::optional<command_type>& command
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
            const CommitMessage& commitMessage
        ) = 0;
    };

    static_assert(CommandSequencer<
        IAsyncCommandSequencer,
        Actions
    >);
};

template<
    typename TCommandLog,
    typename TServices
> concept CommandLog
= requires(
    TCommandLog commandLog,
    typename TServices::PaxosInstanceState paxosState,
    typename TServices::instance_type instance,
    typename TServices::CommandData commandData,
    std::optional<typename TServices::CommandLogEntry> originalCommandLogEntry,
    std::optional<typename TServices::CommandLogEntry> newCommandLogEntry,
    typename TServices::CommandStatus commandStatus
    )
{
    { commandLog.Get(instance) } -> as_awaitable_convertible_to<std::optional<typename TServices::CommandLogEntry>>;

    // Put the command log entry, given the old command log entry.
    // This allows the implementation to diff the two values to determine what to write.
    { commandLog.Put(
        instance, 
        originalCommandLogEntry,
        newCommandLogEntry)
    } -> as_awaitable_convertible_to<void>;

    // The command log implementation should update the dependencies IFF the paxosState
    // represents a ballot >= the existing command log entry OR the paxosState represents a nonexistent command log entry.
    // We do this in the CommandLog implementation for efficiency; it is expected that
    // the majority of commands should commit without conflict.
    // The implementation should properly handle updating the global SequenceNumber for this replica
    // and return it in the UpdateDependenciesResult's log entry.
    { commandLog.UpdateDependencies(paxosState, commandData) } -> as_awaitable_convertible_to<typename TServices::UpdateDependenciesResult>;
};

template<
    typename TMessageSender,
    typename TServices
> concept MessageSender
= requires(
    TMessageSender messageSender,
    typename TServices::CommandLeaderMessageDestination destination,
    typename TServices::PreAcceptMessage preAcceptMessage,
    typename TServices::AcceptMessage acceptMessage,
    typename TServices::PrepareMessage prepareMessage,
    typename TServices::TryPreAcceptMessage tryPreAcceptMessage,
    typename TServices::CommitMessage commitMessage
    )
{
    { messageSender.Send(destination, preAcceptMessage) } -> as_awaitable_async_enumerable_of<typename TServices::PreAcceptResult>;
    { messageSender.Send(destination, acceptMessage) } -> as_awaitable_async_enumerable_of<typename TServices::AcceptResult>;
    { messageSender.Send(destination, prepareMessage) } -> as_awaitable_async_enumerable_of<typename TServices::PrepareResult>;
    { messageSender.Send(destination, tryPreAcceptMessage) } -> as_awaitable_async_enumerable_of<typename TServices::TryPreAcceptResult>;
    { messageSender.Send(commitMessage) } -> as_awaitable_convertible_to<void>;
};

template<
    typename TActions,
    typename TMessageSender,
    typename TCommandLog,
    template <typename> typename TFuture,
    template <typename> typename TGenerator
> class Services
    :
    public TActions
{
public:
    using typename TActions::ballot_number_type;
    using typename TActions::instance_type;
    using typename TActions::instance_set_type;
    using typename TActions::sequence_number_type;
    
    using typename TActions::PaxosInstanceState;
    using typename TActions::CommandData;
    using typename TActions::CommandStatus;
    using typename TActions::CommandLeaderMessageDestination;

    using typename TActions::PreAcceptMessage;
    using typename TActions::AcceptMessage;
    using typename TActions::PrepareMessage;
    using typename TActions::TryPreAcceptMessage;
    using typename TActions::CommitMessage;

    using typename TActions::PreAcceptReplyMessage;
    using typename TActions::AcceptReplyMessage;
    using typename TActions::PrepareReplyMessage;
    using typename TActions::TryPreAcceptReplyMessage;

    using typename TActions::PreAcceptResult;
    using typename TActions::AcceptResult;
    using typename TActions::PrepareResult;
    using typename TActions::TryPreAcceptResult;

    typedef TCommandLog command_log_type;
    typedef TMessageSender message_sender_type;

    struct CommandLogEntry
    {
        CommandData CommandData;
        ballot_number_type BallotNumber;
        CommandStatus CommandStatus;
    };

    struct UpdateDependenciesResult
    {
        // The original command log entry.
        CommandLogEntry OriginalCommandLogEntry;

        // The updated command log entry, if it was successfully updated.
        std::optional<CommandLogEntry> UpdatedCommandLogEntry;
    };

    class IAsyncCommandLog
    {
    public:
        virtual TFuture<CommandLogEntry> Get(
            const instance_type& instance
        ) = 0;

        virtual TFuture<void> Put(
            const instance_type& instance,
            const std::optional<CommandLogEntry>& originalCommandLogEntry,
            const std::optional<CommandLogEntry>& newCommandLogEntry
        ) = 0;

        virtual TFuture<UpdateDependenciesResult> UpdateDependencies(
            const PaxosInstanceState& instance,
            const CommandData& command
        ) = 0;
    };

    static_assert(CommandLog<
        IAsyncCommandLog,
        Services
    >);

    class IAsyncMessageSender
    {
    public:
        virtual TGenerator<PreAcceptResult> Send(
            CommandLeaderMessageDestination destination,
            const PreAcceptMessage& message
        );

        virtual TGenerator<AcceptResult> Send(
            CommandLeaderMessageDestination destination,
            const AcceptMessage& message
        );

        virtual TGenerator<PrepareResult> Send(
            CommandLeaderMessageDestination destination,
            const PrepareMessage& message
        );

        virtual TGenerator<TryPreAcceptResult> Send(
            CommandLeaderMessageDestination destination,
            const TryPreAcceptMessage& message
        );

        virtual TFuture<void> Send(
            const CommitMessage& message
        );
    };

    static_assert(MessageSender<
        IAsyncMessageSender,
        Services
    >);
};

template<
    typename TServices,
    template <typename> typename TFuture
> class StaticAcceptor
    : public TServices
{
public:
    using typename TServices::member_type;
    using typename TServices::command_log_type;
    using typename TServices::message_sender_type;

    using typename TServices::CommandStatus;
    using typename TServices::PaxosInstanceState;

    using typename TServices::PreAcceptMessage;
    using typename TServices::AcceptMessage;
    using typename TServices::PrepareMessage;
    using typename TServices::TryPreAcceptMessage;
    using typename TServices::CommitMessage;

    using typename TServices::PreAcceptResult;
    using typename TServices::AcceptResult;
    using typename TServices::PrepareResult;
    using typename TServices::TryPreAcceptResult;
    using typename TServices::CommitResult;

    using typename TServices::PreAcceptReplyMessage;
    using typename TServices::AcceptReplyMessage;
    using typename TServices::PrepareReplyMessage;
    using typename TServices::TryPreAcceptReplyMessage;
    using typename TServices::NakMessage;
    using typename TServices::CommittedMessage;

    using typename TServices::CommandLogEntry;

    member_type m_thisReplica;
    command_log_type m_commandLog;
    message_sender_type m_messageSender;

    template<
        typename TResult
    > TFuture<std::variant<std::optional<CommandLogEntry>, TResult>> GetCommandLogEntryOrNak(
        const PaxosInstanceState& paxosInstanceState
    )
    {
        auto commandLogEntry = co_await m_commandLog.Get(
            paxosInstanceState.Instance
        );

        if (commandLogEntry
            && commandLogEntry.BallotNumber >= paxosInstanceState.BallotNumber)
        {
            return GenerateNak(
                paxosInstanceState,
                commandLogEntry);
        }

        co_return std::move(
            commandLogEntry);
    }

    template<
        typename TResult
    > TResult GenerateNak(
        const PaxosInstanceState& paxosInstanceState,
        const CommandLogEntry& commandLogEntry
    )
    {
        co_return TResult
        {
            m_thisReplica,
            NakMessage
            {
                paxosInstanceState,
                commandLogEntry.BallotNumber,
            },
        };
    }

    template<
        typename TResult,
        typename TLambda
    > TResult ExecuteCommandLogEntryTransaction(
        const PaxosInstanceState& paxosInstanceState,
        TLambda lambda
    )
    {
        auto originalCommandLogEntry = co_await m_commandLog.Get(
            paxosInstanceState.Instance
        );

        if (originalCommandLogEntry
            && originalCommandLogEntry.BallotNumber >= paxosInstanceState.BallotNumber)
        {
            co_return GenerateNak(
                paxosInstanceState,
                originalCommandLogEntry);
        }

        auto newCommandLogEntry = originalCommandLogEntry;
        if (!newCommandLogEntry)
        {
            newCommandLogEntry = CommandLogEntry
            {
            };
        }

        auto result = co_await lambda(
            originalCommandLogEntry,
            newCommandLogEntry);

        co_await m_commandLog.Put(
            paxosInstanceState.Instance,
            originalCommandLogEntry,
            newCommandLogEntry);

        co_return std::move(
            result);
    }

public:
    template<
        typename TMember,
        typename TCommandLog,
        typename TMessageSender
    >
        StaticAcceptor(
            TMember&& thisReplica,
            TCommandLog&& commandLog,
            TMessageSender&& messageSender
        ) :
        m_thisReplica(
            std::forward<TMember>(thisReplica)),
        m_commandLog(
            std::forward<TCommandLog>(commandLog)),
        m_messageSender(
            std::forward<TMessageSender>(messageSender))
    {}

    TFuture<PreAcceptResult> OnPreAccept(
        const PreAcceptMessage& preAcceptMessage
    )
    {
        auto updateDependenciesResult = co_await m_commandLog.UpdateDependencies(
            preAcceptMessage.PaxosInstanceState,
            preAcceptMessage.CommandData
        );

        if (!updateDependenciesResult.UpdatedCommandLogEntry)
        {
            // The only conditions we accept here are that the ballot number was invalid.
            assert(updateDependenciesResult.OriginalCommandLogEntry);
            assert(preAcceptMessage.PaxosInstanceState.BallotNumber <= updateDependenciesResult.OriginalCommandLogEntry.BallotNumber);

            co_return GenerateNak<PreAcceptResult>(
                preAcceptMessage.PaxosInstanceState,
                updateDependenciesResult.OriginalCommandLogEntry
                );
        }

        co_return PreAcceptResult
        {
            m_thisReplica,
            PreAcceptReplyMessage
            {
                .PaxosInstanceState = preAcceptMessage.PaxosInstanceState,
                .Dependencies = updateDependenciesResult.UpdatedCommandLogEntry.CommandData.Dependencies,
                .SequenceNumber = updateDependenciesResult.UpdatedCommandLogEntry.SequenceNumber,
            },
        };
    }

    TFuture<AcceptResult> OnAccept(
        const AcceptMessage& acceptMessage
    )
    {
        co_return co_await ExecuteCommandLogEntryTransaction(
            acceptMessage.PaxosInstanceState,
            [&](
                auto originalCommandLogEntry,
                auto newCommandLogEntry
                ) -> TFuture<AcceptResult>
        {
            newCommandLogEntry->BallotNumber = acceptMessage.PaxosInstanceState.BallotNumber;
            newCommandLogEntry->CommandData = acceptMessage.CommandData;
            newCommandLogEntry->Status = CommandStatus::Accepted;

            co_return AcceptResult
            {
                m_thisReplica,
                AcceptReplyMessage
                {
                    .PaxosInstanceState = acceptMessage.PaxosInstanceState,
                },
            };
        });
    }

    TFuture<PrepareResult> OnPrepare(
        const PrepareMessage& prepareMessage
    )
    {
        co_return co_await ExecuteCommandLogEntryTransaction(
            prepareMessage.PaxosInstanceState,
            [&](
                auto originalCommandLogEntry,
                auto newCommandLogEntry
                ) -> TFuture<PrepareResult>
        {
            newCommandLogEntry->BallotNumber = prepareMessage.PaxosInstanceState.BallotNumber;

            if (originalCommandLogEntry)
            {
                co_return PrepareResult
                {
                    m_thisReplica,
                    PrepareReplyMessage
                    {
                        .PaxosInstanceState = prepareMessage.PaxosInstanceState,
                        .PreviousBallotNumber = originalCommandLogEntry->BallotNumber,
                        .CommandData = originalCommandLogEntry->CommandData,
                        .CommandStatus = originalCommandLogEntry->CommandStatus,
                    }
                };
            }
            else
            {
                co_return PrepareResult
                {
                    m_thisReplica,
                    PrepareReplyMessage
                    {
                        .PaxosInstanceState = prepareMessage.PaxosInstanceState,
                        .CommandStatus = CommandStatus::NotSeen,
                    }
                };
            }
        });
    }

    TFuture<TryPreAcceptResult> OnTryPreAccept(
        const TryPreAcceptMessage& tryPreAcceptMessage
    )
    {
        co_return co_await ExecuteCommandLogEntryTransaction(
            tryPreAcceptMessage.PaxosInstanceState,
            [&](
                auto originalCommandLogEntry,
                auto newCommandLogEntry
                ) -> TFuture<PrepareResult>
        {
            newCommandLogEntry->BallotNumber = tryPreAcceptMessage.PaxosInstanceState.BallotNumber;

            if (originalCommandLogEntry)
            {
                co_return PrepareResult
                {
                    m_thisReplica,
                    PrepareReplyMessage
                    {
                        .PaxosInstanceState = tryPreAcceptMessage.PaxosInstanceState,
                        .PreviousBallotNumber = originalCommandLogEntry->BallotNumber,
                        .CommandData = originalCommandLogEntry->CommandData,
                        .CommandStatus = originalCommandLogEntry->CommandStatus,
                    }
                };
            }
            else
            {
                co_return PrepareResult
                {
                    m_thisReplica,
                    PrepareReplyMessage
                    {
                        .PaxosInstanceState = tryPreAcceptMessage.PaxosInstanceState,
                        .CommandStatus = CommandStatus::NotSeen,
                    }
                };
            }
        });
    }

    virtual TFuture<CommitResult> OnCommit(
        const CommitMessage& commitMessage
    )
    {
        co_return co_await ExecuteCommandLogEntryTransaction(
            commitMessage.PaxosInstanceState,
            [&](
                auto originalCommandLogEntry,
                auto newCommandLogEntry
                ) -> TFuture<void>
        {
            auto result = CommitResult
            {
                m_thisReplica,
                CommittedMessage
                {
                    .PaxosInstanceState = commitMessage.PaxosInstanceState,
                    .PreviousCommandStatus = originalCommandLogEntry.Status,
                    .CommandData = commitMessage.CommandData,
                }
            };

            if (originalCommandLogEntry.Status != CommandStatus::Executed
                &&
                originalCommandLogEntry.Status != CommandStatus::Committed)
            {
                newCommandLogEntry.Status = CommandStatus::Committed;
            }

            co_return result;
        });
    };
};

template<
    typename TServices,
    typename TQuorumCheckerFactory,
    typename TQuorumChecker,
    typename TMessageSender,
    template <typename> typename TFuture,
    template <typename> typename TGenerator
> 
requires
    EgalitarianPaxosQuorumCheckerFactory<
        TQuorumCheckerFactory,
        typename TServices::ballot_number_type,
        TQuorumChecker,
        typename TServices::member_type
    >
&&
    MessageSender<
        TMessageSender,
        TServices
    >

class StaticCommandLeader
    : public TServices
{
public:
    using typename TServices::member_type;
    using typename TServices::command_log_type;
    using typename TServices::message_sender_type;

    using typename TServices::CommandStatus;
    using typename TServices::PaxosInstanceState;

    using typename TServices::PreAcceptMessage;
    using typename TServices::AcceptMessage;
    using typename TServices::PrepareMessage;
    using typename TServices::TryPreAcceptMessage;
    using typename TServices::CommitMessage;

    using typename TServices::PreAcceptResult;
    using typename TServices::AcceptResult;
    using typename TServices::PrepareResult;
    using typename TServices::TryPreAcceptResult;
    using typename TServices::CommitResult;

    using typename TServices::PreAcceptReplyMessage;
    using typename TServices::AcceptReplyMessage;
    using typename TServices::PrepareReplyMessage;
    using typename TServices::TryPreAcceptReplyMessage;
    using typename TServices::NakMessage;
    using typename TServices::CommittedMessage;
};

}