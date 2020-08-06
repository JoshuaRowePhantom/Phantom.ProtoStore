#pragma once

#include "StandardTypes.h"
#include "InternalProtoStore.h"
#include "IndexPartitionMergeGenerator.h"
#include <cppcoro/async_mutex.hpp>
#include "AsyncScopeMixin.h"
#include "src/ProtoStoreInternal.pb.h"
#include "PartitionWriter.h"

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
        merge_progress_row_list_type CompleteProgress;
        merges_row_type Merge;
    };

    task<optional<IncompleteMerge>> FindIncompleteMerge();

    task<> RestartIncompleteMerge(
        IncompleteMerge incompleteMerge
    );

    task<> WriteMergeProgress(
        IInternalOperation* operation,
        ExtentNumber dataExtentNumber,
        const IncompleteMerge& incompleteMerge,
        const WriteRowsResult& writeRowsResult
    );

    task<> WriteMergeCompletion(
        IInternalOperation* operation,
        ExtentNumber dataExtentNumber,
        const IncompleteMerge& incompleteMerge,
        const WriteRowsResult& writeRowsResult
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
