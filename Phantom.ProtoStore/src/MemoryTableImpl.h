#include "MemoryTable.h"
#include "KeyComparer.h"
#include "SkipList.h"

namespace Phantom::ProtoStore
{

class MemoryTable
    :
    public IMemoryTable
{
    struct InsertionKey
    {
        MemoryTableRow* Row;
        SequenceNumber ReadSequenceNumber;

        operator MemoryTableRow* const () const
        {
            return Row;
        }
    };

    struct EnumerationKey
    {
        const Message* KeyLow;
        Inclusivity Inclusivity;
        SequenceNumber ReadSequenceNumber;
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
            const MemoryTableRow* key1,
            const MemoryTableRow* key2
            ) const
        {
            throw 0;
        }

        std::weak_ordering operator()(
            const MemoryTableRow* key1,
            const InsertionKey& key2
            ) const;

        std::weak_ordering operator()(
            const MemoryTableRow* key1,
            const EnumerationKey& key2
            ) const;

        std::weak_ordering operator()(
            const MemoryTableRow* key1,
            const KeyRangeEnd& high
            ) const;
    };

    struct MemoryTableValue
    {
        MemoryTableRow* RawRow;
        std::unique_ptr<MemoryTableRow> OwnedRow;

        MemoryTableRow* Row()
        {
            return RawRow;
        }
    };

    // comparer must be declared before skipList
    const MemoryTableRowComparer m_comparer;
    SkipList<MemoryTableRow*, MemoryTableValue, 32, MemoryTableRowComparer> m_skipList;

public:
    MemoryTable(
        const KeyComparer* keyComparer
    );

    ~MemoryTable();

    virtual task<> AddRow(
        SequenceNumber readSequenceNumber, 
        MemoryTableRow& row
    ) override;

    virtual cppcoro::async_generator<const MemoryTableRow*> Enumerate(
        SequenceNumber readSequenceNumber, 
        KeyRangeEnd low, 
        KeyRangeEnd high
    ) override;
};

}