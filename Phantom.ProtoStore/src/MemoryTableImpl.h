#include "MemoryTable.h"
#include "KeyComparer.h"
#include "SkipList.h"

namespace Phantom::ProtoStore
{

class MemoryTable
    :
    public IMemoryTable
{
    const KeyComparer * const m_keyComparer;

    class MemoryTableRowComparer;
    //SkipList<MemoryTableRow, MemoryTableRowComparer, 32> m_skipList;

public:
    MemoryTable(
        const KeyComparer* keyComparer
    );

    ~MemoryTable();

    virtual task<> AddRow(
        SequenceNumber readSequenceNumber, 
        MemoryTableRow row
    ) override;

    virtual cppcoro::async_generator<const MemoryTableRow*> Enumerate(
        SequenceNumber transactionReadSequenceNumber, 
        SequenceNumber requestedReadSequenceNumber, 
        KeyRangeEnd low, 
        KeyRangeEnd high
    ) override;
};

}