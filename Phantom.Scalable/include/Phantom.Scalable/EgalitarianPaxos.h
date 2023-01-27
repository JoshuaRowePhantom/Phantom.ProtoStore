#pragma once

#include <iterator>
#include <optional>
#include <variant>
#include "Phantom.System/async_utility.h"
#include "Consensus.h"

namespace Phantom::Consensus::EgalitarianPaxos
{

enum CommandLeaderMessageDestination
{
    Nowhere,
    AllReplicas,
    SlowQuorum,
    FastQuorum,
    FastQuorumRemnant,
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
BallotNumberWithDefault<TBallotNumber>
&&
requires(
    TQuorumCheckerFactory quorumCheckerFactory,
    TBallotNumber ballotNumber,
    CommandLeaderMessageDestination destination
    )
{
    { quorumCheckerFactory(ballotNumber, destination) } -> as_awaitable_convertible_to<TQuorumChecker>;
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

template<
    typename TInstanceSet,
    typename TInstance
> concept InstanceSetType = requires(
    const TInstanceSet sourceInstanceSet,
    TInstanceSet destinationInstanceSet
    )
{
    { 
        std::copy(
            sourceInstanceSet.cbegin(),
            sourceInstanceSet.cend(),
            std::inserter(
                destinationInstanceSet, 
                destinationInstanceSet.end()))
    };
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

    struct PaxosInstanceState
    {
        instance_type Instance;
        ballot_number_type BallotNumber;

        bool operator==(const PaxosInstanceState&) const = default;
    };

    struct CommandData
    {
        std::optional<command_type> Command;
        instance_set_type Dependencies;
        sequence_number_type SequenceNumber;
    };

    struct Vote
    {
        CommandData CommandData;
        ballot_number_type VotedBallotNumber;
    };

    struct PreAcceptMessage
    {
        PaxosInstanceState PaxosInstanceState;
        CommandData CommandData;
    };

    struct AcceptMessage
    {
        PaxosInstanceState PaxosInstanceState;
        CommandData CommandData;
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
        std::optional<Vote> Vote;
        CommandStatus CommandStatus;
    };

    struct TryPreAcceptMessage
    {
        PaxosInstanceState PaxosInstanceState;
        CommandData CommandData;
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
    typename TActions
> concept CommandSequencer
= requires(
    TCommandSequencer commandSequencer,
    typename TActions::instance_type instance,
    typename TActions::CommitMessage commitMessage)
{
    { commandSequencer.GetCommandExecutionSequence() } -> as_awaitable_async_enumerable_of<typename TActions::SequencedCommand>;
    { commandSequencer.OnCommit(commitMessage) };
};

template<
    typename TMessages,
    template <typename> typename TFuture,
    template <typename> typename TGenerator
>
class Actions
    :
    public TMessages
{
public:
    using typename TMessages::ballot_number_type;
    using typename TMessages::member_type;
    using typename TMessages::instance_type;
    using typename TMessages::instance_set_type;
    using typename TMessages::command_type;
    using typename TMessages::sequence_number_type;
    
    using typename TMessages::PaxosInstanceState;
    using typename TMessages::CommandData;
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
    public:
        typedef TMessage message_type;

    private:
        member_type m_replica;
        std::variant<
            message_type,
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
            return holds_alternative<message_type>(
                m_messageOrNak);
        }

        message_type& Message()
        {
            return get<message_type>(
                m_messageOrNak);
        }

        const message_type& Message() const
        {
            return get<message_type>(
                m_messageOrNak);
        }

        bool HasNak() const
        {
            return holds_alternative<NakMessage>(
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

        template<
            typename TLambda
        > auto visit(
            TLambda lambda) const
        {
            return std::visit(
                lambda,
                m_message);
        }
    };

    typedef CommandLeaderResult<PreAcceptMessage> StartResult;
    typedef CommandLeaderResult<std::monostate, CommitMessage, AcceptMessage> OnPreAcceptReplyResult;
    typedef CommandLeaderResult<std::monostate, CommitMessage, AcceptMessage> OnAcceptReplyResult;
    typedef CommandLeaderResult<PrepareMessage> RecoverResult;
    typedef CommandLeaderResult<std::monostate, CommitMessage, AcceptMessage> OnPrepareReplyResult;
    typedef CommandLeaderResult<std::monostate, CommitMessage> OnTryPreAcceptReplyResult;

    class IAsyncCommandLeader
    {
    public:
        virtual TFuture<StartResult> Start(
            const instance_type& instance,
            const command_type& command
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

        virtual TFuture<RecoverResult> Recover(
            const PaxosInstanceState& paxosInstanceState
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
    CommandStatus commandStatus
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
    template <typename> typename TFuture,
    template <typename> typename TGenerator
> 
class Services
    :
    public TActions
{
public:
    using typename TActions::ballot_number_type;
    using typename TActions::instance_type;
    using typename TActions::instance_set_type;
    using typename TActions::sequence_number_type;

    using typename TActions::PaxosInstanceState;
    using typename TActions::Vote;
    using typename TActions::CommandData;

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

    struct CommandLogEntry
    {
        std::optional<Vote> Vote;
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
        virtual TFuture<std::optional<CommandLogEntry>> Get(
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

    //static_assert(MessageSender<
    //    IAsyncMessageSender,
    //    Services
    //>);
};

template<
    typename TServices,
    CommandLog<TServices> TCommandLog,
    template <typename> typename TFuture
> 
class StaticAcceptor
    : public TServices
{
public:
    typedef TCommandLog command_log_type;
    using typename TServices::member_type;

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
        typename TCommandLog
    >
        StaticAcceptor(
            TMember&& thisReplica,
            TCommandLog&& commandLog
        ) :
        m_thisReplica(
            std::forward<TMember>(thisReplica)),
        m_commandLog(
            std::forward<TCommandLog>(commandLog))
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

    TFuture<CommitResult> OnCommit(
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
    typename TBallotNumberFactory,
    template <typename> typename TFuture
> 
requires
    EgalitarianPaxosQuorumCheckerFactory<
        TQuorumCheckerFactory,
        typename TServices::ballot_number_type,
        TQuorumChecker,
        typename TServices::member_type
    >
&&
    AsyncBallotNumberFactory<
        TBallotNumberFactory,
        typename TServices::ballot_number_type
    >
class StaticCommandLeader
    : public TServices
{
public:
    using typename TServices::member_type;
    using typename TServices::instance_type;
    using typename TServices::instance_set_type;
    using typename TServices::command_type;
    using typename TServices::sequence_number_type;

    using typename TServices::Vote;
    using typename TServices::PaxosInstanceState;
    using typename TServices::CommandData;

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

    using typename TServices::StartResult;
    using typename TServices::OnPreAcceptReplyResult;
    using typename TServices::OnAcceptReplyResult;
    using typename TServices::OnPrepareReplyResult;
    using typename TServices::OnTryPreAcceptReplyResult;
    using typename TServices::RecoverResult;

    struct FastPreAcceptingState
    {
        TQuorumChecker FastQuorum;
        std::optional<instance_set_type> InitialDependencies;
        std::optional<sequence_number_type> InitialSequenceNumber;
    };

    struct PreAcceptingState
    {
        TQuorumChecker SlowQuorum;
        std::optional<FastPreAcceptingState> FastState;
    };

    struct AcceptingState
    {
        TQuorumChecker SlowQuorum;
    };

    struct PreparingState
    {
        TQuorumChecker SlowQuorum;
        std::optional<Vote> HighestVote;
        bool NotSeen = true;
    };

    struct TryPreAcceptingState
    {
        TQuorumChecker SlowQuorum;
    };

    struct LeaderState
    {
        std::optional<PaxosInstanceState> PaxosInstanceState;
        std::variant<
            std::monostate,
            PreAcceptingState,
            AcceptingState,
            PreparingState,
            TryPreAcceptingState
        > State;
        std::optional<CommandData> CommandData;
    };

private:
    TQuorumCheckerFactory m_quorumCheckerFactory;
    TBallotNumberFactory m_ballotNumberFactory;
    LeaderState m_leaderState;

    template<
        typename TResult
    > TResult ChangeToCommittedStateAndGetCommittedResult()
    {
        auto result = TResult
        {
            CommandLeaderMessageDestination::AllReplicas,
            CommitMessage
            {
                .PaxosInstanceState = std::move(*m_leaderState.PaxosInstanceState),
                .CommandData = std::move(*m_leaderState.CommandData),
            }
        };
        m_leaderState = LeaderState{};
        return result;
    }

    template<
        typename TResult
    > TResult GetDoNothingResult()
    {
        return TResult
        {
            CommandLeaderMessageDestination::Nowhere,
            std::monostate()
        };
    }

    template<
        typename TExpectedState,
        typename TResult,
        typename TReplicaResult
    > TFuture<TResult> WithExpectedState(
        const TReplicaResult& acceptorResult,
        std::function<TFuture<TResult>(
            TExpectedState&, 
            const typename TReplicaResult::message_type&)
        > lambda
    )
    {
        // Ignore messages for the wrong state or wrong ballot number.
        if (!std::holds_alternative<TExpectedState>(m_leaderState.State)
            ||
            acceptorResult.HasMessage()
            &&
            acceptorResult.Message().PaxosInstanceState != m_leaderState.PaxosInstanceState)
        {
            co_return GetDoNothingResult<TResult>();
        }

        // If the incoming acceptor result is a Nak,
        // then the current leader state should go to recovery.
        if (acceptorResult.HasNak())
        {
            throw 0;
        }

        auto& state = get<TExpectedState>(
            m_leaderState.State);

        co_return co_await lambda(
            state,
            acceptorResult.Message());
    }

public:
    template<
        typename TQuorumCheckerFactory,
        typename TBallotNumberFactory,
        typename TLeaderState = LeaderState
    > StaticCommandLeader(
        TQuorumCheckerFactory&& quorumCheckerFactory,
        TBallotNumberFactory&& ballotNumberFactory,
        TLeaderState&& leaderState = TLeaderState{}
    ) :
        m_quorumCheckerFactory(
            std::forward<TQuorumCheckerFactory>(quorumCheckerFactory)),
        m_ballotNumberFactory(
            std::forward<TBallotNumberFactory>(ballotNumberFactory)),
        m_leaderState(
            std::forward<TLeaderState>(leaderState))
    {}

    template<
        typename TInstance,
        typename TCommand
    >
    TFuture<StartResult> Start(
        TInstance&& instance,
        TCommand&& command,
        std::optional<instance_set_type> dependencies = {},
        std::optional<sequence_number_type> sequenceNumber = {}
    )
    {
        auto ballotNumber = co_await as_awaitable(m_ballotNumberFactory());
        auto slowQuorum = co_await as_awaitable(m_quorumCheckerFactory(
            ballotNumber,
            CommandLeaderMessageDestination::SlowQuorum));
        auto preAcceptMessageDestination = CommandLeaderMessageDestination::SlowQuorum;

        std::optional<FastPreAcceptingState> fastState;

        if (!ballotNumber)
        {
            fastState = FastPreAcceptingState
            {
                .FastQuorum = co_await as_awaitable(m_quorumCheckerFactory(
                    ballotNumber,
                    CommandLeaderMessageDestination::FastQuorum)),
                .InitialDependencies = dependencies,
                .InitialSequenceNumber = sequenceNumber,
            };
        }

        m_leaderState = LeaderState
        {
            .PaxosInstanceState =
            {
                .Instance = std::forward<TInstance>(instance),
                .BallotNumber = std::move(ballotNumber),
            },
            .State = PreAcceptingState
            {
                .SlowQuorum = std::move(slowQuorum),
                .FastState = std::move(fastState),
            },
            .CommandData = CommandData
            {
                .Command = std::forward<TCommand>(command),
            },
        };

        return StartResult
        {
            preAcceptMessageDestination,
            PreAcceptMessage
            {
                .PaxosInstanceState = m_leaderState.PaxosInstanceState,
                .CommandData = *m_leaderState.CommandData,
            },
        };
    }

    TFuture<OnPreAcceptReplyResult> OnPreAcceptReply(
        const PreAcceptResult& preAcceptResult
    )
    {
        throw 0;
        // Current ICE
        //return WithExpectedState<PreAcceptingState, OnPreAcceptReplyResult>(
        //    preAcceptResult,
        //    [&](
        //        auto& preAcceptingState,
        //        auto& message) 
        //    -> TFuture<OnPreAcceptReplyResult>
        //{
        //    std::copy(
        //        message.Dependencies.cbegin(),
        //        message.Dependencies.cend(),
        //        std::inserter(
        //            m_leaderState.CommandData->Dependencies,
        //            m_leaderState.CommandData->Dependencies.begin()));

        //    m_leaderState.CommandData->SequenceNumber = std::max(
        //        m_leaderState.CommandData->SequenceNumber,
        //        message.SequenceNumber);

        //    preAcceptingState.SlowQuorum += preAcceptResult.Replica();

        //    if (preAcceptingState.FastState)
        //    {
        //        auto& fastState = *preAcceptingState.FastState;

        //        // If we haven't already accepted a message on the fast path,
        //        // accept this one.
        //        if (!fastState.InitialSequenceNumber)
        //        {
        //            fastState.InitialSequenceNumber = message.SequenceNumber;
        //            fastState.InitialDependencies = message.Dependencies;
        //            fastState.FastQuorum += preAcceptResult.Replica();
        //        }
        //        // If we've already accepted a message on the fast path,
        //        // check to make sure it has identical attributes.
        //        else if (
        //            fastState.InitialSequenceNumber == message.SequenceNumber
        //            &&
        //            fastState.InitialDependencies == message.Dependencies)
        //        {
        //            fastState.FastQuorum += preAcceptResult.Replica();
        //        }
        //        // We've already accepted a message on the fast path, but have
        //        // a conflicting result.  Use the slow path.
        //        else
        //        {
        //            preAcceptingState.FastState.reset();
        //        }
        //    }

        //    // If we're still on the fast path and the fast path quorum is complete,
        //    // commit the command.
        //    if (preAcceptingState.FastState
        //        &&
        //        preAcceptingState.FastState->FastQuorum)
        //    {
        //        co_return ChangeToCommittedStateAndGetCommittedResult<OnPreAcceptReplyResult>();
        //    }

        //    if (!preAcceptingState.SlowQuorum)
        //    {
        //        co_return GetDoNothingResult<OnPreAcceptReplyResult>();
        //    }

        //    // The slow quorum is valid,
        //    // so return that we can do Accept stuff.
        //    co_return OnPreAcceptReplyResult
        //    {
        //        CommandLeaderMessageDestination::SlowQuorum,
        //        AcceptMessage
        //        {
        //            .PaxosInstanceState = *m_leaderState.PaxosInstanceState,
        //            .CommandData = *m_leaderState.CommandData,
        //        }
        //    };
        //});
    }

    TFuture<OnAcceptReplyResult> OnAcceptReply(
        const AcceptResult& acceptResult
    )
    {
        // If we were in PreAcceptingState on the same ballot number,
        // transition to AcceptingState.  Ignore the result
        // of this operation, since we'll recheck the ballot
        // number in the second call to WithExpectedState below.
        co_await WithExpectedState<PreAcceptingState, OnAcceptReplyResult>(
            acceptResult,
            [&](
                auto& preAcceptingState,
                auto& message)
            -> TFuture<OnAcceptReplyResult>
        {
            m_leaderState.State = AcceptingState
            {
                .SlowQuorum = co_await as_awaitable(m_quorumCheckerFactory(
                    m_leaderState.PaxosInstanceState->BallotNumber,
                    CommandLeaderMessageDestination::SlowQuorum)),
            };

            co_return GetDoNothingResult<OnAcceptReplyResult>();
        });

        co_return co_await WithExpectedState<AcceptingState, OnAcceptReplyResult>(
            acceptResult,
            [&](
                auto& acceptingState,
                auto& message)
            -> TFuture<OnAcceptReplyResult>
        {
            if (acceptingState.SlowQuorum += acceptResult.Replica())
            {
                // The slow quorum has completed,
                // so we can commit!
                co_return ChangeToCommittedStateAndGetCommittedResult<OnAcceptReplyResult>();
            }

            co_return GetDoNothingResult<OnAcceptReplyResult>();
        });
    }

    TFuture<RecoverResult> Recover(
        const PaxosInstanceState& paxosInstanceState
    )
    {
        typename TServices::ballot_number_type ballotNumber = co_await as_awaitable(m_ballotNumberFactory(
            paxosInstanceState.BallotNumber));
        auto slowQuorum = co_await as_awaitable(m_quorumCheckerFactory(
            ballotNumber,
            CommandLeaderMessageDestination::SlowQuorum));

        m_leaderState = LeaderState
        {
            .PaxosInstanceState = PaxosInstanceState
            {
                .Instance = paxosInstanceState.Instance,
                .BallotNumber = std::move(ballotNumber),
            },
            .State = PreparingState
            {
                .SlowQuorum = std::move(slowQuorum),
            }
        };

        co_return RecoverResult
        {
            CommandLeaderMessageDestination::SlowQuorum,
            PrepareMessage
            {
                .PaxosInstanceState = *m_leaderState.PaxosInstanceState,
            },
        };
    }

    TFuture<OnPrepareReplyResult> OnPrepareReply(
        const PrepareResult& prepareResult
    )
    {
        throw 0;
        // Current ICE
        //return WithExpectedState<PreparingState, OnPrepareReplyResult>(
        //    prepareResult,
        //    [&](
        //        auto preparingState,
        //        auto prepareReplyMessage
        //        ) -> TFuture<OnPrepareReplyResult>
        //{
        //    // If we find a message saying the command was committed by any replica,
        //    // then its attributes are fixed by this replica's vote.
        //    // Immediately commit the command.
        //    if (prepareReplyMessage.CommandStatus == CommandStatus::Committed
        //        ||
        //        prepareReplyMessage.CommandStatus == CommandStatus::Executed)
        //    {
        //        m_leaderState.CommandData = prepareReplyMessage.Vote->CommandData;
        //        co_return ChangeToCommittedStateAndGetCommittedResult<OnPrepareReplyResult>();
        //    }

        //    // Keep the highest vote among returned Accepted status values.
        //    if (prepareReplyMessage.CommandStatus == CommandStatus::Accepted
        //        &&
        //        (!preparingState.HighestVote
        //            || preparingState.HighestVote->VotedBallotNumber < prepareReplyMessage.Vote->VotedBallotNumber))
        //    {
        //        preparingState.HighestVote = prepareReplyMessage.Vote;
        //    }

        //    if (prepareReplyMessage.CommandStatus != CommandStatus::NotSeen)
        //    {
        //        preparingState.NotSeen = false;
        //    }

        //    // If we haven't yet reached quorum,
        //    // do nothing.
        //    if (!(preparingState.SlowQuorum += prepareResult.Replica()))
        //    {
        //        co_return GetDoNothingResult<OnPrepareReplyResult>();
        //    }

        //    // If we have no knowledge at all,
        //    // or an accepted vote,
        //    // then we move on to AcceptingState.
        //    if (preparingState.NotSeen
        //        ||
        //        preparingState.HighestVote)
        //    {
        //        if (preparingState.HighestVote)
        //        {
        //            m_leaderState.CommandData = std::move(
        //                preparingState.HighestVote->CommandData);
        //        }
        //        else
        //        {
        //            // Try to commit a noop command.
        //            m_leaderState.CommandData = CommandData{};
        //        }

        //        m_leaderState.State = AcceptingState
        //        {
        //            .SlowQuorum = co_await as_awaitable(m_quorumCheckerFactory(
        //                m_leaderState.PaxosInstanceState->BallotNumber,
        //                CommandLeaderMessageDestination::SlowQuorum)),
        //        };

        //        co_return OnPrepareReplyResult
        //        {
        //            CommandLeaderMessageDestination::SlowQuorum,
        //            AcceptMessage
        //            {
        //                .PaxosInstanceState = *m_leaderState.PaxosInstanceState,
        //                .CommandData = *m_leaderState.CommandData,
        //            },
        //        };
        //    }

        //    throw 0;
        //});
    }

    TFuture<OnTryPreAcceptReplyResult> OnTryPreAcceptReply(
        const TryPreAcceptResult& tryPreAcceptResult
    )
    {
        throw 0;
    }
};

}