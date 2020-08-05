#pragma once

#include "StandardTypes.h"
#include "InternalProtoStore.h"
#include "IndexPartitionMergeGenerator.h"
#include <cppcoro/async_mutex.hpp>
#include "AsyncScopeMixin.h"
#include "src/ProtoStoreInternal.pb.h"

namespace Phantom::ProtoStore
{

class IndexMerger
    : public AsyncScopeMixin
{
    IInternalProtoStore* const m_protoStore;
    IndexPartitionMergeGenerator* const m_mergeGenerator;

    shared_task<bool> m_delayedMergeOnePartitionTask;
    cppcoro::async_mutex m_delayedMergeOnePartitionTaskLock;

    shared_task<bool> DelayedMergeOnePartition(
        shared_task<bool> previousDelayedMergeOnePartition);

    task<bool> MergeOnePartition();

    task<merges_row_list_type> AcquireMergeCandidates(
        IOperation* operation);

    struct IncompleteMerge
    {
        merge_progress_row_type IncompleteProgress;
        merge_progress_row_list_type CompleteProgress;
        merges_row_type Merge;
    };

    task<optional<IncompleteMerge>> FindIncompleteMerge();

    task<> RestartIncompleteMerge(
        IncompleteMerge incompleteMerge
    );

public:
    IndexMerger(
        IInternalProtoStore* protoStore,
        IndexPartitionMergeGenerator* mergeGenerator
    );

    ~IndexMerger();

    task<> Merge();
};
}
