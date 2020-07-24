#include "IndexPartitionMergeGenerator.h"
#include <algorithm>
#include "src/ProtoStoreInternal.pb.h"

namespace Phantom::ProtoStore
{

merges_row_list_type IndexPartitionMergeGenerator::GetMergeCandidates(
    const IndexNumber indexNumber,
    const MergeParameters& mergeParameters,
    const partition_row_list_type& partitions,
    const merges_row_list_type& ongoingMerges
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


    merges_row_list_type merges;
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
            MergesKey mergesKey;
            mergesKey.set_indexnumber(indexNumber);
            mergesKey.set_mergesuniqueid(
                get<0>(partitionsAtSourceLevel.second[0]).dataextentnumber());

            MergesValue mergesValue;
            mergesValue.set_sourcelevelnumber(
                partitionsAtSourceLevel.first);
            mergesValue.set_destinationlevelnumber(
                partitionsAtSourceLevel.first + 1);

            for (auto& partition : partitionsAtSourceLevel.second)
            {
                mergesValue.add_sourcedataextentnumbers(
                    get<0>(partition).dataextentnumber());
            }

            merges_row_type merge = std::make_tuple(
                mergesKey,
                mergesValue);

            merges.push_back(
                merge);
        }
    }

    return merges;
}

}