#include "IndexPartitionMergeGenerator.h"
#include <algorithm>
#include "Phantom.ProtoStore/ProtoStore.pb.h"
#include "ProtoStoreInternal.pb.h"
#include "ExtentName.h"
#include <unordered_set>

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
    std::unordered_set<ExtentName> mergingPartitions;

    for (auto& ongoingMerge : ongoingMerges)
    {
        for (auto headerExtentNumber : ongoingMerge.Value.sourceheaderextentnames())
        {
            mergingPartitions.insert(
                headerExtentNumber);
        }
    }

    for (auto& partition : partitions)
    {
        // See if the partition has already been acquired as a merge candidate.
        if (mergingPartitions.contains(partition.Key.headerextentname()))
        {
            continue;
        }

        auto sourceLevel = partition.Value.level();
        // Adjust the source level to be within the merge parameters max level
        sourceLevel = std::min(
            sourceLevel,
            mergeParameters.maxlevel()
        );

        auto& partitionsAtSourceLevel = partitionsBySourceLevel[sourceLevel];
        partitionsAtSourceLevel.push_back(
            partition);
    }

    merges_row_list_type merges;
    for (auto& partitionsAtSourceLevel : partitionsBySourceLevel)
    {
        // Count the distinct merge operations that generated these partitions.
        size_t mergeCount = 0;
        std::unordered_set<MergeId> mergeIds;

        for (auto& partition : partitionsAtSourceLevel.second)
        {
            if (!partition.Value.has_mergeuniqueid()
                || mergeIds.insert(partition.Value.mergeuniqueid()).second)
            {
                ++mergeCount;
            }
        }

        // If that number exceeds the parameter, then it's a merge candidate for the next level.
        if (mergeCount > mergeParameters.mergesperlevel())
        {
            Serialization::MergesKey mergesKey;
            mergesKey.set_indexnumber(indexNumber);
            *mergesKey.mutable_mergesuniqueid() = 
                partitionsAtSourceLevel.second[0].Key.headerextentname();

            Serialization::MergesValue mergesValue;
            mergesValue.set_sourcelevelnumber(
                partitionsAtSourceLevel.first);

            mergesValue.set_destinationlevelnumber(
                std::min(
                    mergeParameters.maxlevel(),
                    partitionsAtSourceLevel.first + 1));

            optional<CheckpointNumber> checkpointNumber;

            for (auto& partition : partitionsAtSourceLevel.second)
            {
                *mergesValue.add_sourceheaderextentnames() = 
                    partition.Key.headerextentname();

                if (!checkpointNumber)
                {
                    checkpointNumber = partition.Value.checkpointnumber();
                }
                else
                {
                    checkpointNumber = std::max(
                        *checkpointNumber,
                        partition.Value.checkpointnumber()
                    );
                }
            }

            mergesValue.set_checkpointnumber(
                *checkpointNumber);

            merges_row_type merge =
            {
                mergesKey,
                mergesValue,
            };

            merges.push_back(
                merge);
        }
    }

    return merges;
}

}