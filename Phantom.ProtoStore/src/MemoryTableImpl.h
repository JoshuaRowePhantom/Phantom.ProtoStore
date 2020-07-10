#include "MemoryTable.h"
#include "KeyComparer.h"
#include "SkipList.h"

namespace Phantom::ProtoStore
{

class MemoryTable
    :
    public IMemoryTable
{
    struct MemoryTableValue
    {
        // The raw pointers to the key.  This value is 
        // the same as MemoryTableRow.Key after an insertion
        // operation is complete.  This value
        // is never changed once a node is inserted
        // into the skip list.
        const Message* Key;

        // This contains owning copies of the row data.
        // It is not updated until after the initial insertion completes.
        // Important notes for accessing its members:
        //      1.  Neither Key nor Value should be read
        //          unless the owning operation has completed.
        //      2.  WriteSequenceNumber can be changed at any time
        //          if the owning operation has not completed.
        MemoryTableRow Row;
    };

    struct InsertionKey
    {
        const Message* Key;
        SequenceNumber WriteSequenceNumber;
        const Message* Value;

        SequenceNumber ReadSequenceNumber;

        operator MemoryTableValue() const;
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