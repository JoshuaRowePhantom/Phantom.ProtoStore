#include "MemoryTable.h"
#include "KeyComparer.h"
#include "SkipList.h"
#include <atomic>
#include <cppcoro/async_auto_reset_event.hpp>
#include <cppcoro/async_mutex.hpp>
#include <cppcoro/async_scope.hpp>
#include "AsyncScopeMixin.h"

namespace Phantom::ProtoStore
{

class MemoryTable
    :
    public IMemoryTable,
    public AsyncScopeMixin
{
    static MemoryTableOutcomeAndSequenceNumber ToOutcomeUnknownSubsequentInsertion(
        SequenceNumber sequenceNumber);

    struct MemoryTableValue;

    struct ReplayInsertionKey
    {
        ReplayInsertionKey(
            Row& row);

        Row Row;
    };

    struct InsertionKey
    {
        InsertionKey(
            Row& row,
            shared_ptr<DelayedMemoryTableTransactionOutcome>& delayedTransactionOutcome,
            SequenceNumber readSequenceNumber);

        InsertionKey(
            const InsertionKey&
        ) = delete;

        InsertionKey& operator=(
            const InsertionKey&
        ) = delete;

        // The SkipList might want to move a MemoryTableValue
        // back to the insertion key.
        InsertionKey& operator=(
            MemoryTableValue&& memoryTableValue);

        Row& Row;
        shared_ptr<DelayedMemoryTableTransactionOutcome>& DelayedTransactionOutcome;
        SequenceNumber ReadSequenceNumber;
    };

    struct MemoryTableValue
    {
        // We can be constructed by a movable InsertionKey.
        MemoryTableValue(
            InsertionKey&& other);

        // We can also be constructed by a movable ReplayInsertionKey
        MemoryTableValue(
            ReplayInsertionKey&& other);

        // Memory table is not copyable nor movable.
        MemoryTableValue(
            const MemoryTableValue&
        ) = delete;

        MemoryTableValue& operator=(
            const MemoryTableValue&
        ) = delete;

        MemoryTableValue& operator=(
            MemoryTableValue&&
            ) = delete;

        // This mutex controls reading and writing Row and AsyncTransactionOutcome
        // after the row has been added to the SkipList.
        cppcoro::async_mutex Mutex;

        // This contains owning copies of the row data.
        // .ValueMessage must not be used unless TransactionOutcome indicates Committed
        // or the Mutex is acquired.
        // The Key in it must never be replaced, as it is used in a thread-unsafe way
        // after the skip list node is created.
        // When reading, if ValueMessage is null then use the KeyMessage.
        Row KeyRow;
        Row ValueRow;

        // The sequence number the row was either added
        // or committed at, and the operation outcome.  The protocol to write this
        // value after adding to the skip list is to write
        // the contents of Row, the write this with Release semantics.
        std::atomic<MemoryTableOutcomeAndSequenceNumber> WriteSequenceNumber;

        // The outcome of the owning operation.
        // It must only be checked while the Mutex is held.
        shared_ptr<DelayedMemoryTableTransactionOutcome> DelayedTransactionOutcome;

        AlignedMessage GetKeyMessage() const;
        AlignedMessage GetValueMessage() const;
        AlignedMessage GetTransactionIdMessage() const;
        SequenceNumber GetWriteSequenceNumber() const;

        ResultRow GetResultRow() const;
    };

    struct EnumerationKey
    {
        ProtoValue KeyLow;
        Inclusivity KeyLowInclusivity;
        SequenceNumber ReadSequenceNumber;
        optional<SequenceNumber> SequenceNumberToSkipForKeyLow;
    };

    class MemoryTableRowComparer
    {
        const shared_ptr<const Schema> m_schema;
        const shared_ptr<const KeyComparer> m_keyComparer;

        ProtoValue MakeProtoValueKey(
            const InsertionKey&
        ) const;

        const ProtoValue& MakeProtoValueKey(
            const EnumerationKey&
        ) const;

        const ProtoValue& MakeProtoValueKey(
            const KeyRangeEnd&
        ) const;

        ProtoValue MakeProtoValueKey(
            const ReplayInsertionKey&
        ) const;

    public:
        MemoryTableRowComparer(
            shared_ptr<const Schema> schema,
            shared_ptr<const KeyComparer> keyComparer
        );

        ProtoValue MakeProtoValueKey(
            const MemoryTableValue&
        ) const;

        std::weak_ordering operator()(
            const MemoryTableValue& key1,
            const MemoryTableValue& key2
            ) const;

        std::weak_ordering operator()(
            const MemoryTableValue& key1,
            const InsertionKey& key2
            ) const;

        std::weak_ordering operator()(
            const MemoryTableValue& key1,
            const EnumerationKey& key2
            ) const;

        std::weak_ordering operator()(
            const MemoryTableValue& key1,
            const KeyRangeEnd& high
            ) const;

        std::weak_ordering operator()(
            const MemoryTableValue& key1,
            const ReplayInsertionKey& key2
            ) const;
    };

    // m_comparer must be declared before m_skipList,
    // so that m_skipList can point to it.
    const shared_ptr<const Schema> m_schema;
    const shared_ptr<const KeyComparer> m_keyComparer;
    const MemoryTableRowComparer m_comparer;

    SkipList<MemoryTableValue, void, 32, MemoryTableRowComparer> m_skipList;

    // Resolve a memory table row's outcome when the transaction it is in completes.
    task<> UpdateOutcome(
        MemoryTableValue& memoryTableValue,
        shared_ptr<DelayedMemoryTableTransactionOutcome> delayedTransactionOutcome
    );

    // Resolve a memory table row with acquiring the mutex.
    task<TransactionOutcome> Resolve(
        MemoryTableValue& memoryTableValue);

    void UpdateSequenceNumberRange(
        SequenceNumber writeSequenceNumber
    );

    std::atomic<size_t> m_unresolvedRowCount;
    std::atomic<size_t> m_committedRowCount;
    cppcoro::async_auto_reset_event m_rowResolved;

    std::atomic<SequenceNumber> m_earliestSequenceNumber;
    std::atomic<SequenceNumber> m_latestSequenceNumber;

public:
    MemoryTable(
        shared_ptr<const Schema> schema,
        shared_ptr<const KeyComparer> keyComparer
    );

    ~MemoryTable();

    virtual task<size_t> GetRowCount(
    ) override;

    virtual task<std::optional<SequenceNumber>> AddRow(
        SequenceNumber readSequenceNumber,
        Row row,
        shared_ptr<DelayedMemoryTableTransactionOutcome> delayedTransactionOutcome
    ) override;

    virtual task<> ReplayRow(
        Row row
    ) override;

    virtual row_generator Enumerate(
        shared_ptr<DelayedMemoryTableTransactionOutcome> delayedTransactionOutcome,
        SequenceNumber readSequenceNumber,
        KeyRangeEnd low, 
        KeyRangeEnd high
    ) override;

    virtual SequenceNumber GetLatestSequenceNumber(
    ) override;

    virtual row_generator Checkpoint(
    ) override;
};

}