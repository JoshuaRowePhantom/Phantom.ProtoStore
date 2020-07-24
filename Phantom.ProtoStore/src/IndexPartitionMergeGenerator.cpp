#include "IndexPartitionMergeGenerator.h"
#include <algorithm>
#include "src/ProtoStoreInternal.pb.h"

namespace Phantom::ProtoStore
{

vector<MergeCandidate> IndexPartitionMergeGenerator::GetMergeCandidates(
    const MergeParameters& mergeParameters,
    partition_row_list_type partitions,
    merges_row_list_type ongoingMerges
)
{
    map<LevelNumber, partition_row_list_type> partitionsBySourceLevel;
    std::set<ExtentNumber> mergingPartitions;

    for (auto& ongoingMerge : ongoingMerges)
    {
        for (auto dataExtentNumber : get<1>(ongoingMerge).sourcedataextentnumbers())
        {
            mergingPartitions.insert(
                dataExtentNumber);
        }
    }

    for (auto& partition : partitions)
    {
        // See if the partition has already been acquired as a merge candidate.
        if (mergingPartitions.contains(get<0>(partition).dataextentnumber()))
        {
            continue;
        }

        auto sourceLevel = get<1>(partition).level();
        auto& partitionsAtSourceLevel = partitionsBySourceLevel[sourceLevel];
        partitionsAtSourceLevel.push_back(
            partition);
    }

    vector<MergeCandidate> mergeCandidates;
    for (auto& partitionsAtSourceLevel : partitionsBySourceLevel)
    {
        // Count the distinct merge operations that generated these partitions.
        size_t mergeCount = 0;
        std::set<MergeId> mergeIds;
        for (auto& partition : partitionsAtSourceLevel.second)
        {
            auto mergeUniqueId = get<1>(partition).mergeuniqueid();
            if (mergeUniqueId
                || mergeIds.insert(mergeUniqueId).second)
            {
                ++mergeCount;
            }
        }

        // If that number exceeds the parameter, then it's a merge candidate for the next level.
        if (mergeCount > mergeParameters.mergesperlevel())
        {
            MergeCandidate mergeCandidate =
            {
                .SourcePartitions = partitionsAtSourceLevel.second,
                .DestinationLevel = partitionsAtSourceLevel.first + 1,
            };

            mergeCandidates.push_back(
                mergeCandidate);
        }
    }

    return mergeCandidates;
}

}