#pragma once

#include "StandardTypes.h"
#include "InternalProtoStore.h"
#include "IndexPartitionMergeGenerator.h"
#include <cppcoro/async_mutex.hpp>
#include "AsyncScopeMixin.h"
#include "ProtoStoreInternal.pb.h"
#include "PartitionWriter.h"
#include <cppcoro/async_generator.hpp>

namespace Phantom::ProtoStore
{

class IndexMerger
    : public AsyncScopeMixin
{
    IInternalProtoStore* const m_protoStore;
    IndexPartitionMergeGenerator* const m_mergeGenerator;

    task<> GenerateMerges(
        const MergeParameters& mergeParameters);

    struct IncompleteMerge
    {
        merge_progress_row_list_type CompleteProgress;
        merges_row_type Merge;
    };

    async_generator<IncompleteMerge> FindIncompleteMerges();

    task<> RestartIncompleteMerge(
        IncompleteMerge incompleteMerge
    );

    task<> WriteMergeProgress(
        IInternalTransaction* operation,
        ExtentName dataExtentName,
        IncompleteMerge& incompleteMerge,
        const WriteRowsResult& writeRowsResult
    );

    task<> WriteMergeCompletion(
        IInternalTransaction* operation,
        ExtentName headerExtentName,
        ExtentName dataExtentName,
        const IncompleteMerge& incompleteMerge,
        const WriteRowsResult& writeRowsResult
    );

    task<> WriteMergedPartitionsTableHeaderExtentNumbers(
        IInternalTransaction* operation,
        ExtentName headerExtentName,
        const IncompleteMerge& incompleteMerge);

public:
    IndexMerger(
        IInternalProtoStore* protoStore,
        IndexPartitionMergeGenerator* mergeGenerator
    );

    ~IndexMerger();

    task<> Merge(
        const MergeParameters& mergeParameters);
};
}
