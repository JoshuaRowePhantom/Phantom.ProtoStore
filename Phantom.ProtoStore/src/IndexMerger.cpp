#include "ExtentName.h"
#include "Index.h"
#include "IndexMerger.h"
#include "InternalProtoStore.h"
#include "Partition.h"
#include "PartitionWriterImpl.h"
#include "Phantom.System/utility.h"
#include "ProtoStoreInternal.pb.h"
#include "RowMerger.h"
#include "Schema.h"
#include <algorithm>
#include <unordered_set>

namespace Phantom::ProtoStore
{

IndexMerger::IndexMerger(
    IInternalProtoStore* protoStore,
    IndexPartitionMergeGenerator* mergeGenerator
) :
    m_protoStore(protoStore),
    m_mergeGenerator(mergeGenerator)
{
}

IndexMerger::~IndexMerger()
{
    SyncDestroy();
}

task<> IndexMerger::Merge(
    const MergeParameters& mergeParameters)
{
    co_await GenerateMerges(
        mergeParameters);

    auto incompleteMerges = FindIncompleteMerges();
    vector<task<>> mergeTasks;

    for (auto incompleteMergeIterator = co_await incompleteMerges.begin();
        incompleteMergeIterator != incompleteMerges.end();
        co_await ++incompleteMergeIterator)
    {
        mergeTasks.push_back(
            RestartIncompleteMerge(
                *incompleteMergeIterator));
    }

    for (auto& task : mergeTasks)
    {
        co_await task.when_ready();
    }

    for (auto& task : mergeTasks)
    {
        co_await task;
    }
}

task<> IndexMerger::RestartIncompleteMerge(
    IncompleteMerge incompleteMerge
)
{
    auto index = co_await m_protoStore->GetIndex(
        incompleteMerge.Merge.Key->index_number());

    vector<FlatValue<FlatBuffers::IndexHeaderExtentName>> headerExtents(
        incompleteMerge.Merge.Value->source_header_extent_names()->begin(),
        incompleteMerge.Merge.Value->source_header_extent_names()->end());

    auto partitions = co_await m_protoStore->OpenPartitionsForIndex(
        index,
        headerExtents);

    optional<PartitionCheckpointStartKey> partitionCheckpointStartKey;
    unique_ptr<Message> startKeyMessage;

    auto resumeKey = incompleteMerge.Merge.Value->resume_key();
    if (resumeKey)
    {
        partitionCheckpointStartKey =
        {
            .Key = SchemaDescriptions::MakeProtoValueKey(
                *index->GetSchema(),
                AlignedMessageData
                {
                    nullptr,
                    GetAlignedMessage(resumeKey->key())
                }),
            .WriteSequenceNumber = ToSequenceNumber(
                resumeKey->write_sequence_number()),
        };
    }

    size_t approximateRowCount = 0;
    ExtentOffset inputSize = 0;
    for (auto& partition : partitions)
    {
        approximateRowCount += co_await partition->GetRowCount();
        inputSize += co_await partition->GetApproximateDataSize();
    }

    bool isCompleteMerge;

    do
    {
        auto rowGenerators = [&]() -> row_generators
        {
            for (auto& partition : partitions)
            {
                co_yield partition->Checkpoint(
                    partitionCheckpointStartKey);
            }
        };

        RowMerger rowMerger(
            index->GetKeyComparer());

        auto rowGenerator = rowMerger.Merge(
            rowGenerators());

        ExtentNameT headerExtentName;
        ExtentNameT dataExtentName;
        shared_ptr<IPartitionWriter> partitionWriter;

        co_await m_protoStore->OpenPartitionWriter(
            index->GetIndexNumber(),
            index->GetIndexName(),
            index->GetSchema(),
            index->GetKeyComparer(),
            index->GetValueComparer(),
            incompleteMerge.Merge.Value->destination_level_number(),
            headerExtentName,
            dataExtentName,
            partitionWriter);

        WriteRowsRequest writeRowsRequest =
        {
            .approximateRowCount = approximateRowCount,
            .rows = &rowGenerator,
            .inputSize = inputSize,
            .targetExtentSize = 1024 * 1024 * 1024,
            .targetMessageSize = 1024 * 1024 * 1024,
        };

        auto writeRowsResult = co_await partitionWriter->WriteRows(
            writeRowsRequest);

        isCompleteMerge = writeRowsResult.resumptionRow == rowGenerator.end();

        co_await m_protoStore->InternalExecuteTransaction(
            BeginTransactionRequest{},
            [&](auto operation) -> status_task<>
        {
            operation->BuildCommitPartitionLogEntries(
                headerExtentName,
                dataExtentName);

            if (!isCompleteMerge)
            {
                co_await WriteMergeProgress(
                    operation,
                    headerExtentName,
                    incompleteMerge,
                    writeRowsResult);
            }
            else
            {
                auto updatePartitionsLock = co_await m_protoStore->AcquireUpdatePartitionsLock();

                co_await WriteMergeCompletion(
                    operation,
                    headerExtentName,
                    dataExtentName,
                    incompleteMerge,
                    writeRowsResult);

                // We have to commit the updated partitions rows before
                // we can read them with UpdatePartitionsForIndex.
                co_await operation->Commit();

                co_await m_protoStore->UpdatePartitionsForIndex(
                    incompleteMerge.Merge.Key->index_number(),
                    updatePartitionsLock);
            }

            co_return StatusResult<>{};
        });
    } while (!isCompleteMerge);
}

task<> IndexMerger::WriteMergeProgress(
    IInternalTransaction* operation,
    const ExtentNameT& headerExtentName,
    IncompleteMerge& incompleteMerge,
    const WriteRowsResult& writeRowsResult
)
{
    // We write two rows:
    // 1. Update the Merges row with the current progress.
    // 2. Write a MergeProgress row.
    auto mergesIndex = m_protoStore->GetMergesIndex();
    auto mergeProgressIndex = m_protoStore->GetMergeProgressIndex();

    // Add a new complete merge progress record.
    FlatBuffers::MergeProgressKeyT completeMergeProgressKey;
    completeMergeProgressKey.merges_key.reset(incompleteMerge.Merge.Key->UnPack());
    completeMergeProgressKey.header_extent_name = copy_unique(*headerExtentName.extent_name.AsIndexHeaderExtentName());

    FlatBuffers::MergeProgressValueT completeMergeProgressValue;
    completeMergeProgressValue.data_size = writeRowsResult.writtenDataSize;

    co_await operation->AddRow(
        WriteOperationMetadata{},
        mergeProgressIndex,
        &completeMergeProgressKey,
        &completeMergeProgressValue
    );

    flatbuffers::FlatBufferBuilder mergesValueBuilder;

    // Update the Merges row with the progress.
    FlatBuffers::MergesValueT previousMergesValue;
    incompleteMerge.Merge.Value->UnPackTo(&previousMergesValue);
    
    std::vector<flatbuffers::Offset<FlatBuffers::IndexHeaderExtentName>> sourceHeaderExtentNameOffsets;
    for (auto& sourceHeaderExtentName : previousMergesValue.source_header_extent_names)
    {
        sourceHeaderExtentNameOffsets.push_back(FlatBuffers::CreateIndexHeaderExtentName(
            mergesValueBuilder,
            sourceHeaderExtentName.get()
        ));
    }
    auto sourceHeaderExtentNamesOffset = mergesValueBuilder.CreateVector(
        sourceHeaderExtentNameOffsets);

    auto mergeResumeKeyKeyOffset = CreateDataValue(
        mergesValueBuilder,
        writeRowsResult.resumptionRow->Key.as_aligned_message_if()
    );

    auto mergeResumeKeyOffset = FlatBuffers::CreateMergeResumeKey(
        mergesValueBuilder,
        mergeResumeKeyKeyOffset,
        ToUint64(writeRowsResult.resumptionRow->WriteSequenceNumber)
    );

    auto mergesValueOffset = FlatBuffers::CreateMergesValue(
        mergesValueBuilder,
        sourceHeaderExtentNamesOffset,
        previousMergesValue.source_level_number,
        previousMergesValue.destination_level_number,
        previousMergesValue.latest_partition_number,
        mergeResumeKeyOffset
    );

    mergesValueBuilder.Finish(
        mergesValueOffset);

    auto mergesValue = ProtoValue::FlatBuffer(
        std::move(mergesValueBuilder));

    co_await operation->AddRow(
        WriteOperationMetadata
        {
            .ReadSequenceNumber = incompleteMerge.Merge.WriteSequenceNumber,
        },
        mergesIndex,
        incompleteMerge.Merge.Key,
        mergesValue
        );

    // Also update the IncompleteMerge with these rows.
    incompleteMerge.Merge.Value = std::move(mergesValue);
    incompleteMerge.CompleteProgress.push_back(
        {
            completeMergeProgressKey,
            completeMergeProgressValue,
            SequenceNumber::Latest,
            SequenceNumber::Latest,
        }
    );
}

task<> IndexMerger::WriteMergeCompletion(
    IInternalTransaction* operation,
    const ExtentNameT& headerExtentName,
    const ExtentNameT& dataExtentName,
    const IncompleteMerge& incompleteMerge,
    const WriteRowsResult& writeRowsResult)
{
    // We delete all the MergeProgress rows,
    // delete the Merges row,
    // delete the Partitions rows for the source partitions,
    // add new Partitions rows for the previously completed merged partitions,
    // add a Partitions row for the newly completed merged partition.

    auto indexNumber = incompleteMerge.Merge.Key->index_number();
    auto mergesIndex = m_protoStore->GetMergesIndex();
    auto mergeProgressIndex = m_protoStore->GetMergeProgressIndex();
    auto partitionsIndex = m_protoStore->GetPartitionsIndex();

    // Special case: if the merged index is the Partitions index, we have to write
    // all the partitions for the table.
    if (indexNumber == partitionsIndex->GetIndexNumber())
    {
        co_await WriteMergedPartitionsTableHeaderExtentNumbers(
            operation,
            headerExtentName,
            incompleteMerge);
    }

    // Delete the MergeProgress rows.
    for (auto& progressRow : incompleteMerge.CompleteProgress)
    {
        co_await operation->AddRow(
            WriteOperationMetadata
            {
                .ReadSequenceNumber = progressRow.WriteSequenceNumber,
            },
            mergeProgressIndex,
            progressRow.Key,
            nullptr);
    }

    // Delete the Merges rows.
    co_await operation->AddRow(
        WriteOperationMetadata
        {
            .ReadSequenceNumber = incompleteMerge.Merge.ReadSequenceNumber,
        },
        mergesIndex,
        incompleteMerge.Merge.Key,
        nullptr);

    // Delete the Partitions rows for the source partitions.
    for (auto sourceHeaderExtentName : *incompleteMerge.Merge.Value->source_header_extent_names())
    {
        PartitionsKeyT sourcePartitionsKey;
        sourcePartitionsKey.index_number = indexNumber;
        sourcePartitionsKey.header_extent_name.reset(
            sourceHeaderExtentName->UnPack());

        co_await operation->AddRow(
            WriteOperationMetadata{},
            partitionsIndex,
            &sourcePartitionsKey,
            nullptr);
    }

    // Add Partitions rows for all MergeProgress rows.
    for (auto& completeProgress : incompleteMerge.CompleteProgress)
    {
        PartitionsKeyT completePartitionsKey;
        completePartitionsKey.index_number = indexNumber;
        completePartitionsKey.header_extent_name.reset(
            completeProgress.Key->header_extent_name()->UnPack());

        PartitionsValueT completePartitionsValue;
        completePartitionsValue.merge_unique_id.reset(incompleteMerge.Merge.Key->UnPack());
        completePartitionsValue.size = 0;

        co_await operation->AddRow(
            WriteOperationMetadata{},
            partitionsIndex,
            &completePartitionsKey,
            &completePartitionsValue);
    }

    PartitionNumber partitionsTablePartitionNumber;
    
    // Add a Partitions row for the newly completed Partition.
    {
        PartitionsKeyT completePartitionsKey;
        completePartitionsKey.index_number = indexNumber;
        completePartitionsKey.header_extent_name = copy_unique(*headerExtentName.extent_name.AsIndexHeaderExtentName());

        PartitionsValueT completePartitionsValue;
        completePartitionsValue.merge_unique_id.reset(incompleteMerge.Merge.Key->UnPack());
        completePartitionsValue.size = writeRowsResult.writtenDataSize;
        completePartitionsValue.latest_partition_number = incompleteMerge.Merge.Value->latest_partition_number();

        auto loggedRowWrite = co_await operation->AddRowInternal(
            WriteOperationMetadata{},
            partitionsIndex,
            &completePartitionsKey,
            &completePartitionsValue);
        if (!loggedRowWrite) { std::move(loggedRowWrite).error().throw_exception(); }
    }
    
    // Mark the table as needing reload of its partitions.
    operation->BuildLogRecord(
        LogEntryUnion::LoggedUpdatePartitions,
        [&](auto& builder)
    {
        return FlatBuffers::CreateLoggedUpdatePartitions(
            builder,
            incompleteMerge.Merge.Key->index_number()
        ).Union();
    });

    // Mark all the old partitions' extents as deleted.
    for (auto headerExtentName : *incompleteMerge.Merge.Value->source_header_extent_names())
    {
        operation->BuildLogRecord(
            LogEntryUnion::LoggedDeleteExtent,
            [&](auto& builder)
        {
            auto fullHeaderExtentName = MakeExtentName(headerExtentName);

            return FlatBuffers::CreateLoggedDeleteExtent(
                builder,
                FlatBuffers::CreateExtentName(builder, &fullHeaderExtentName)
            ).Union();
        });

        auto dataExtentName = MakePartitionDataExtentName(
            headerExtentName);

        operation->BuildLogRecord(
            LogEntryUnion::LoggedDeleteExtent,
            [&](auto& builder)
        {
            return FlatBuffers::CreateLoggedDeleteExtent(
                builder,
                FlatBuffers::CreateExtentName(builder, &dataExtentName)
            ).Union();
        });
    }
}

task<> IndexMerger::WriteMergedPartitionsTableHeaderExtentNumbers(
    IInternalTransaction* operation,
    const ExtentNameT& headerExtentName,
    const IncompleteMerge& incompleteMerge)
{

    auto partitions_table_partition_number = std::numeric_limits<PartitionNumber>::max();

    std::unordered_set<
        FlatValue<ExtentName>,
        ProtoValueStlHash,
        ProtoValueStlEqual
    > partitionHeaderExtentNames(
        0,
        FlatBuffersSchemas::ExtentName_Comparers.hash,
        FlatBuffersSchemas::ExtentName_Comparers.equal_to);

    auto existingPartitions = co_await m_protoStore->GetPartitionsForIndex(
        incompleteMerge.Merge.Key->index_number());

    for (auto& existingPartition : existingPartitions)
    {
        partitionHeaderExtentNames.insert(
            FlatValue(
                MakeExtentName(
                    existingPartition.Key->header_extent_name())));

        partitions_table_partition_number =
            std::max(
                partitions_table_partition_number,
                existingPartition.Value->latest_partition_number()
            );
    }

    for (auto sourcePartition : *incompleteMerge.Merge.Value->source_header_extent_names())
    {
        partitionHeaderExtentNames.erase(
            FlatValue{ MakeExtentName(sourcePartition) });
    }

    for (auto& completeMergeProgress : incompleteMerge.CompleteProgress)
    {
        partitionHeaderExtentNames.insert(
            FlatValue{ MakeExtentName(completeMergeProgress.Key->header_extent_name()) });
    }

    partitionHeaderExtentNames.insert(
        headerExtentName);

    operation->BuildLogRecord(
        LogEntryUnion::LoggedPartitionsData,
        [&](auto& builder) -> Offset<void>
    {
        std::vector<Offset<FlatBuffers::IndexHeaderExtentName>> headerExtentNameOffsets;
        for (auto partitionHeaderExtentName : partitionHeaderExtentNames)
        {
            FlatBuffers::IndexHeaderExtentNameT partitionHeaderExtentNameT;
            partitionHeaderExtentName->extent_name_as_IndexHeaderExtentName()->UnPackTo(
                &partitionHeaderExtentNameT
            );

            headerExtentNameOffsets.push_back(
                FlatBuffers::CreateIndexHeaderExtentName(
                    builder,
                    &partitionHeaderExtentNameT));
        }

        return FlatBuffers::CreateLoggedPartitionsDataDirect(
            builder,
            &headerExtentNameOffsets,
            partitions_table_partition_number
        ).Union();
    });
}

async_generator<IndexMerger::IncompleteMerge> IndexMerger::FindIncompleteMerges()
{
    auto mergesIndex = m_protoStore->GetMergesIndex();

    EnumerateRequest enumerateMergesRequest;
    enumerateMergesRequest.KeyLow = ProtoValue::KeyMin();
    enumerateMergesRequest.KeyLowInclusivity = Inclusivity::Inclusive;
    enumerateMergesRequest.KeyHigh = ProtoValue::KeyMax();
    enumerateMergesRequest.KeyHighInclusivity = Inclusivity::Exclusive;
    enumerateMergesRequest.SequenceNumber = SequenceNumber::LatestCommitted;
    enumerateMergesRequest.Index = mergesIndex;

    auto mergesEnumeration = mergesIndex->Enumerate(
        nullptr,
        enumerateMergesRequest);

    for (auto mergesIterator = co_await mergesEnumeration.begin();
        mergesIterator != mergesEnumeration.end();
        co_await ++mergesIterator)
    {
        // We found an incomplete merge.  We're going to return it directly from here.
        IncompleteMerge result =
        {
            .Merge = merges_row_type::FromResultRow(
                std::move(**mergesIterator)),
        };

        // Now get all the MergeProgress rows for this merge.
        auto mergeProgressIndex = m_protoStore->GetMergeProgressIndex();

        FlatBuffers::MergeProgressKeyT mergeProgressKeyHigh;
        mergeProgressKeyHigh.merges_key.reset(result.Merge.Key->UnPack());

        EnumeratePrefixRequest enumerateMergeProgressRequest
        {
            .Index = mergesIndex,
            .SequenceNumber = SequenceNumber::LatestCommitted,
            .Prefix =
            {
                .Key = result.Merge.Key,
                .LastFieldId = 1,
            },
            .ReadValueDisposition = ReadValueDisposition::ReadValue,
        };

        auto mergeProgressEnumeration = mergeProgressIndex->EnumeratePrefix(
            nullptr,
            std::move(enumerateMergeProgressRequest));

        for (auto mergeProgressIterator = co_await mergeProgressEnumeration.begin();
            mergeProgressIterator != mergeProgressEnumeration.end();
            co_await ++mergeProgressIterator)
        {
            result.CompleteProgress.emplace_back(
                merge_progress_row_type::FromResultRow(
                    std::move(**mergeProgressIterator)));
        }

        co_yield result;
    }
}

task<> IndexMerger::GenerateMerges(
    const MergeParameters& mergeParameters)
{
    map<IndexNumber, partition_row_list_type> partitionRowsByIndexNumber;
    map<IndexNumber, merges_row_list_type> mergesRowsByIndexNumber;
    auto mergesIndex = m_protoStore->GetMergesIndex();

    {
        auto partitionsIndex = m_protoStore->GetPartitionsIndex();

        EnumerateRequest enumeratePartitionsRequest;
        enumeratePartitionsRequest.KeyLow = ProtoValue::KeyMin();
        enumeratePartitionsRequest.KeyLowInclusivity = Inclusivity::Inclusive;
        enumeratePartitionsRequest.KeyHigh = ProtoValue::KeyMax();
        enumeratePartitionsRequest.KeyHighInclusivity = Inclusivity::Exclusive;
        enumeratePartitionsRequest.SequenceNumber = SequenceNumber::LatestCommitted;
        enumeratePartitionsRequest.Index = partitionsIndex;

        auto partitionsEnumeration = partitionsIndex->Enumerate(
            nullptr,
            enumeratePartitionsRequest);

        for (auto partitionsIterator = co_await partitionsEnumeration.begin();
            partitionsIterator != partitionsEnumeration.end();
            co_await ++partitionsIterator)
        {
            auto partitionRow = partition_row_type::FromResultRow(std::move(**partitionsIterator));

            auto indexNumber = partitionRow.Key->index_number();

            partitionRowsByIndexNumber[indexNumber].push_back(
                std::move(partitionRow));

        }
    }

    {
        EnumerateRequest enumerateMergesRequest;
        enumerateMergesRequest.KeyLow = ProtoValue::KeyMin();
        enumerateMergesRequest.KeyLowInclusivity = Inclusivity::Inclusive;
        enumerateMergesRequest.KeyHigh = ProtoValue::KeyMax();
        enumerateMergesRequest.KeyHighInclusivity = Inclusivity::Exclusive;
        enumerateMergesRequest.SequenceNumber = SequenceNumber::LatestCommitted;
        enumerateMergesRequest.Index = mergesIndex;

        auto mergesEnumeration = mergesIndex->Enumerate(
            nullptr,
            enumerateMergesRequest);

        for (auto mergesIterator = co_await mergesEnumeration.begin();
            mergesIterator != mergesEnumeration.end();
            co_await ++mergesIterator)
        {
            auto mergesRow = merges_row_type::FromResultRow(
                std::move(**mergesIterator));

            auto indexNumber = mergesRow.Key->index_number();

            mergesRowsByIndexNumber[indexNumber].push_back(
                move(mergesRow));
        }
    }

    bool result = false;

    for (auto indexNumberAndPartitions : partitionRowsByIndexNumber)
    {
        auto indexNumber = indexNumberAndPartitions.first;
        auto newMerges = m_mergeGenerator->GetMergeCandidates(
            mergeParameters,
            indexNumberAndPartitions.second,
            mergesRowsByIndexNumber[indexNumber]);

        co_await m_protoStore->InternalExecuteTransaction(
            BeginTransactionRequest{},
            [&](auto operation) -> status_task<>
        {
            for (auto& newMerge : newMerges)
            {
                co_await operation->AddRow(
                    WriteOperationMetadata{},
                    mergesIndex,
                    newMerge.Key,
                    newMerge.Value
                );
            }

            co_return{};
        });
    }
}
}