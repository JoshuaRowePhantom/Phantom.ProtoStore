#pragma once

#include "StandardTypes.h"

namespace Phantom::ProtoStore
{

class IndexPartitionMergeGenerator
{
public:
    merges_row_list_type GetMergeCandidates(
        const IndexNumber indexNumber,
        const MergeParameters& mergeParameters,
        const partition_row_list_type& partitions,
        const merges_row_list_type& ongoingMerges);
};

}