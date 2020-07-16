#include "MemoryTable.h"
#include "KeyComparer.h"
#include "SkipList.h"
#include <atomic>
#include <cppcoro/async_mutex.hpp>
#include <cppcoro/async_scope.hpp>

namespace Phantom::ProtoStore
{

class MemoryTable
    :
    public IMemoryTable
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
        // We can only be constructed by a movable InsertionKey.
        MemoryTableValue(
            InsertionKey&& other);

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
            ) const
        {
            throw 0;
        }

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
    };

    // m_comparer must be declared before m_skipList,
    // so that m_skipList can point to it.
    const MemoryTableRowComparer m_comparer;
    SkipList<MemoryTableValue, void, 32, MemoryTableRowComparer> m_skipList;
    cppcoro::async_scope m_asyncScope;

    // Resolve a memory table row's outcome using the passed-in
    // task and original sequence number.  Does not acquire the row's mutex.
    task<OperationOutcome> ResolveMemoryTableRowOutcome(
        MemoryTableOutcomeAndSequenceNumber writeSequenceNumber,
        MemoryTableValue& memoryTableValue,
        MemoryTableOperationOutcomeTask task
    );

    // Resolve a memory table row with acquiring the mutex.
    task<OperationOutcome> ResolveMemoryTableRowOutcome(
        MemoryTableValue& memoryTableValue
    );

public:
    MemoryTable(
        const KeyComparer* keyComparer
    );

    ~MemoryTable();

    virtual task<> AddRow(
        SequenceNumber readSequenceNumber, 
        MemoryTableRow& row,
        MemoryTableOperationOutcomeTask asyncOperationOutcome
    ) override;

    virtual cppcoro::async_generator<const MemoryTableRow*> Enumerate(
        SequenceNumber readSequenceNumber, 
        KeyRangeEnd low, 
        KeyRangeEnd high
    ) override;

    virtual task<> Join(
    ) override;
};

}