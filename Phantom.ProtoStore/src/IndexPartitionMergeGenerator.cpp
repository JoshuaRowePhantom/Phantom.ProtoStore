#include "IndexPartitionMergeGenerator.h"
#include <algorithm>
#include "Phantom.ProtoStore/ProtoStore.pb.h"
#include "ProtoStoreInternal.pb.h"
#include "ExtentName.h"
#include "Resources.h"
#include <unordered_set>

namespace Phantom::ProtoStore
{

merges_row_list_type IndexPartitionMergeGenerator::GetMergeCandidates(
    const MergeParameters& mergeParameters,
    const partition_row_list_type& partitions,
    const merges_row_list_type& ongoingMerges
)
{
    if (partitions.empty())
    {
        return {};
    }

    IndexNumber indexNumber = partitions[0].Key->index_number();

    map<LevelNumber, partition_row_list_type> partitionsBySourceLevel;
    std::unordered_set<FlatValue<FlatBuffers::IndexHeaderExtentName>, ProtoValueStlHash, ProtoValueStlEqual> mergingPartitions(
        0,
        FlatBuffersSchemas::IndexHeaderExtentName_Comparers.hash,
        FlatBuffersSchemas::IndexHeaderExtentName_Comparers.equal_to
    );

    for (auto& ongoingMerge : ongoingMerges)
    {
        for (auto indexHeaderExtentName : *ongoingMerge.Value->source_header_extent_names())
        {
            mergingPartitions.insert(
                FlatValue{ indexHeaderExtentName });
        }
    }

    for (auto& partition : partitions)
    {
        // See if the partition has already been acquired as a merge candidate.
        if (mergingPartitions.contains(FlatValue{ partition.Key->header_extent_name() }))
        {
            continue;
        }

        auto sourceLevel = partition.Key->header_extent_name()->index_extent_name()->level();
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
        std::unordered_set<
            FlatValue<FlatBuffers::MergesKey>,
            ProtoValueStlHash,
            ProtoValueStlEqual
        > mergeIds(
            0,
            FlatBuffersSchemas::MergesValue_Comparers.hash,
            FlatBuffersSchemas::MergesValue_Comparers.equal_to
        );

        for (auto& partition : partitionsAtSourceLevel.second)
        {
            if (!partition.Value->merge_unique_id()
                || mergeIds.insert(FlatValue{ partition.Value->merge_unique_id() }).second)
            {
                ++mergeCount;
            }
        }

        // If that number exceeds the parameter, then it's a merge candidate for the next level.
        if (mergeCount > mergeParameters.mergesperlevel())
        {
            FlatBuffers::MergesKeyT mergesKey;
            mergesKey.index_number = indexNumber;
            mergesKey.merges_unique_id.reset(partitionsAtSourceLevel.second[0].Key->header_extent_name()->UnPack());

            FlatBuffers::MergesValueT mergesValue;
            mergesValue.source_level_number = partitionsAtSourceLevel.first;
            mergesValue.destination_level_number = 
                std::min(
                    mergeParameters.maxlevel(),
                    partitionsAtSourceLevel.first + 1);

            optional<CheckpointNumber> checkpointNumber;

            for (auto& partition : partitionsAtSourceLevel.second)
            {
                mergesValue.source_header_extent_names.emplace_back(
                    partition.Key->header_extent_name()->UnPack());

                if (!checkpointNumber)
                {
                    checkpointNumber = partition.Value->latest_checkpoint_number();
                }
                else
                {
                    checkpointNumber = std::max(
                        *checkpointNumber,
                        partition.Value->latest_checkpoint_number()
                    );
                }
            }

            mergesValue.latest_checkpoint_number = *checkpointNumber;

            merges_row_type merge =
            {
                ProtoValue { &mergesKey },
                ProtoValue { &mergesValue },
            };

            merges.push_back(
                merge);
        }
    }

    return merges;
}

}