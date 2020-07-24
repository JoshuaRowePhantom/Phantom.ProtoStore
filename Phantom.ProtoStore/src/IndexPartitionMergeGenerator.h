#pragma once

#include "StandardTypes.h"

namespace Phantom::ProtoStore
{

struct MergeCandidate
{
    partition_row_list_type SourcePartitions;
    LevelNumber DestinationLevel;
};

class IndexPartitionMergeGenerator
{
public:
    vector<MergeCandidate> GetMergeCandidates(
        const MergeParameters& mergeParameters,
        partition_row_list_type partitions,
        merges_row_list_type ongoingMerges);
};
}