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
    enum class MemoryTableOutcomeAndSequenceNumber;

    static MemoryTableOutcomeAndSequenceNumber ToMemoryTableOutcomeAndSequenceNumber(
        SequenceNumber sequenceNumber,
        OperationOutcome operationOutcome);

    static MemoryTableOutcomeAndSequenceNumber ToOutcomeUnknownSubsequentInsertion(
        SequenceNumber sequenceNumber);

    static OperationOutcome GetOperationOutcome(
        MemoryTableOutcomeAndSequenceNumber value);

    static SequenceNumber ToSequenceNumber(
        MemoryTableOutcomeAndSequenceNumber value);

    struct MemoryTableValue;

    struct ReplayInsertionKey
    {
        ReplayInsertionKey(
            MemoryTableRow& row);

        MemoryTableRow& Row;
    };

    struct InsertionKey
    {
        InsertionKey(
            MemoryTableRow& row,
            MemoryTableOperationOutcomeTask memoryTableOperationOutcomeTask,
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

        MemoryTableRow& Row;
        MemoryTableOperationOutcomeTask& AsyncOperationOutcome;
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

        // The sequence number the row was either added
        // or committed at, and the operation outcome.  The protocol to write this
        // value after adding to the skip list is to write
        // the contents of Row, the write this with Release semantics.
        std::atomic<MemoryTableOutcomeAndSequenceNumber> WriteSequenceNumber;

        // This mutex controls reading and writing Row and AsyncOperationOutcome
        // after the row has been added to the SkipList.
        cppcoro::async_mutex Mutex;

        // This contains owning copies of the row data.
        // .Value must not be read unless OperationOutcome indicates Committed
        // or the Mutex is acquired.
        // The Key in it must never be replaced, as it is used in a thread-unsafe way
        // after the skip list node is created.
        MemoryTableRow Row;

        // The outcome of the owning operation.
        // It must only be checked while the Mutex is held.
        MemoryTableOperationOutcomeTask AsyncOperationOutcome;
    };

    struct EnumerationKey
    {
        const Message* KeyLow;
        Inclusivity KeyLowInclusivity;
        SequenceNumber ReadSequenceNumber;
        optional<SequenceNumber> SequenceNumberToSkipForKeyLow;
    };

    class MemoryTableRowComparer
    {
        const KeyComparer* const m_keyComparer;
    public:
        MemoryTableRowComparer(
            const KeyComparer* keyComparer
        )
            : m_keyComparer(
                keyComparer)
        {}

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
    const MemoryTableRowComparer m_comparer;
    SkipList<MemoryTableValue, void, 32, MemoryTableRowComparer> m_skipList;

    // Resolve a memory table row's outcome using the passed-in
    // task and original sequence number.  Does not acquire the row's mutex.
    task<OperationOutcome> ResolveMemoryTableRowOutcome(
        MemoryTableOutcomeAndSequenceNumber writeSequenceNumber,
        MemoryTableValue& memoryTableValue,
        MemoryTableOperationOutcomeTask task,
        bool updateRowCounters
    );

    // Resolve a memory table row with acquiring the mutex.
    task<OperationOutcome> ResolveMemoryTableRowOutcome(
        MemoryTableValue& memoryTableValue,
        bool updateRowCounters
    );

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
        const KeyComparer* keyComparer
    );

    ~MemoryTable();

    virtual task<size_t> GetRowCount(
    ) override;

    virtual task<std::optional<SequenceNumber>> AddRow(
        SequenceNumber readSequenceNumber, 
        MemoryTableRow& row,
        MemoryTableOperationOutcomeTask asyncOperationOutcome
    ) override;

    virtual task<> ReplayRow(
        MemoryTableRow& row
    ) override;

    virtual async_generator<ResultRow> Enumerate(
        SequenceNumber readSequenceNumber, 
        KeyRangeEnd low, 
        KeyRangeEnd high
    ) override;

    virtual SequenceNumber GetLatestSequenceNumber(
    ) override;

    task<optional<SequenceNumber>> CheckForWriteConflict(
        SequenceNumber readSequenceNumber,
        SequenceNumber writeSequenceNumber,
        const Message* key
    ) override;

    virtual cppcoro::async_generator<ResultRow> Checkpoint(
    ) override;
};

}