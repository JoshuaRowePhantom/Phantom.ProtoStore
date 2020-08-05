#include "IndexMerger.h"
#include "Phantom.System/utility.h"
#include "src/ProtoStoreInternal.pb.h"

namespace Phantom::ProtoStore
{

IndexMerger::IndexMerger(
    IInternalProtoStore* protoStore,
    IndexPartitionMergeGenerator* mergeGenerator
) :
    m_protoStore(protoStore),
    m_mergeGenerator(mergeGenerator)
{
    m_delayedMergeOnePartitionTask = DelayedMergeOnePartition(
        make_completed_shared_task(true));
}

IndexMerger::~IndexMerger()
{
    SyncDestroy();
}

task<> IndexMerger::Merge()
{
    shared_task<bool> mergeTask;

    do
    {
        auto lock = m_delayedMergeOnePartitionTaskLock.scoped_lock_async();
        mergeTask = m_delayedMergeOnePartitionTask;
    } while (co_await mergeTask);
}

shared_task<bool> IndexMerger::DelayedMergeOnePartition(
    shared_task<bool> previousDelayedMergeOnePartition)
{
    co_await previousDelayedMergeOnePartition;
    auto result = co_await MergeOnePartition();

    auto lock = m_delayedMergeOnePartitionTaskLock.scoped_lock_async();
    m_delayedMergeOnePartitionTask = DelayedMergeOnePartition(
        m_delayedMergeOnePartitionTask);

    co_return result;
}

task<> IndexMerger::RestartIncompleteMerge(
    IncompleteMerge incompleteMerge
)
{
    auto newPartitionDataExtent = m_protoStore->AllocateDataExtent();

    throw 0;
}

task<bool> IndexMerger::MergeOnePartition()
{
    auto incompleteMerge = co_await FindIncompleteMerge();

    if (incompleteMerge.has_value())
    {
        co_await RestartIncompleteMerge(
            *incompleteMerge);
        co_return true;
    }

    co_return false;
}

}