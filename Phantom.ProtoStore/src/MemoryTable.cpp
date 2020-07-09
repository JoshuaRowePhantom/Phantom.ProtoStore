#include "MemoryTableImpl.h"

namespace Phantom::ProtoStore
{

MemoryTable::MemoryTable(
    const KeyComparer* keyComparer
)
    : m_keyComparer(keyComparer)
{}

task<> MemoryTable::AddRow(
    SequenceNumber readSequenceNumber,
    MemoryTableRow row
)
{
    throw 0;
}

cppcoro::async_generator<const MemoryTableRow*> MemoryTable::Enumerate(
    SequenceNumber transactionReadSequenceNumber,
    SequenceNumber requestedReadSequenceNumber,
    KeyRangeEnd low,
    KeyRangeEnd high
)
{
    throw 0;
}

}