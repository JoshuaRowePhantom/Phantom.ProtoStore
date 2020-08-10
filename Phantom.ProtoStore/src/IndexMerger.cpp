#include "IndexMerger.h"
#include "Phantom.System/utility.h"
#include "src/ProtoStoreInternal.pb.h"
#include "Partition.h"
#include "PartitionWriterImpl.h"
#include "Index.h"
#include "Schema.h"
#include "RowMerger.h"
#include "InternalProtoStore.h"
#include <cppcoro/when_all.hpp>

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

task<> IndexMerger::Merge()
{
    co_await GenerateMerges();

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

    co_await cppcoro::when_all(
        move(mergeTasks));
}

task<> IndexMerger::RestartIncompleteMerge(
    IncompleteMerge incompleteMerge
)
{
    auto index = co_await m_protoStore->GetIndex(
        incompleteMerge.Merge.Key.indexnumber());

    vector<ExtentNumber> dataExtentNumbers(
        incompleteMerge.Merge.Value.sourcedataextentnumbers().begin(),
        incompleteMerge.Merge.Value.sourcedataextentnumbers().end());

    auto partitions = co_await m_protoStore->OpenPartitionsForIndex(
        index,
        dataExtentNumbers);

    optional<PartitionCheckpointStartKey> partitionCheckpointStartKey;
    unique_ptr<Message> startKeyMessage;

    if (incompleteMerge.Merge.Value.has_resumekey())
    {
        startKeyMessage.reset(
            index->GetKeyFactory()->GetPrototype()->New());
        startKeyMessage->ParseFromString(
            incompleteMerge.Merge.Value.resumekey().key());
        
        partitionCheckpointStartKey =
        {
            .Key = startKeyMessage.get(),
            .WriteSequenceNumber = ToSequenceNumber(
                incompleteMerge.Merge.Value.resumekey().writesequencenumber()),
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
            index->GetKeyComparer().get());

        auto rowGenerator = rowMerger.Merge(
            rowGenerators());

        ExtentNumber dataExtentNumber;
        shared_ptr<IPartitionWriter> partitionWriter;

        co_await m_protoStore->OpenPartitionWriter(
            dataExtentNumber,
            partitionWriter);

        WriteRowsRequest writeRowsRequest =
        {
            .approximateRowCount = approximateRowCount,
            .rows = &rowGenerator,
            .inputSize = inputSize,
            .targetSize = 1024 * 1024 * 1024,
        };

        auto writeRowsResult = co_await partitionWriter->WriteRows(
            writeRowsRequest);

        isCompleteMerge = writeRowsResult.resumptionRow == rowGenerator.end();

        if (isCompleteMerge)
        {
        }

        co_await m_protoStore->InternalExecuteOperation(
            BeginTransactionRequest{},
            [&](auto operation) -> task<>
        {
            co_await m_protoStore->LogCommitDataExtent(
                operation->LogRecord(),
                dataExtentNumber);

            if (!isCompleteMerge)
            {
                co_await WriteMergeProgress(
                    operation,
                    dataExtentNumber,
                    incompleteMerge,
                    writeRowsResult);
            }
            else
            {
                auto updatePartitionsLock = co_await m_protoStore->AcquireUpdatePartitionsLock();

                co_await WriteMergeCompletion(
                    operation,
                    dataExtentNumber,
                    incompleteMerge,
                    writeRowsResult);

                co_await operation->Commit();

                co_await m_protoStore->UpdatePartitionsForIndex(
                    incompleteMerge.Merge.Key.indexnumber(),
                    updatePartitionsLock);

                co_await operation->Commit();
            }
        });
    } while (!isCompleteMerge);
}

task<> IndexMerger::WriteMergeProgress(
    IInternalOperation* operation,
    ExtentNumber dataExtentNumber,
    IncompleteMerge& incompleteMerge,
    const WriteRowsResult& writeRowsResult
)
{
    // We write two rows:
    // 1. Update the Merges row with the current progress.
    // 2. Write a MergeProgress row.
    auto mergesIndex = co_await m_protoStore->GetMergesIndex();
    auto mergeProgressIndex = co_await m_protoStore->GetMergeProgressIndex();

    // Add a new complete merge progress record.
    MergeProgressKey completeMergeProgressKey;
    completeMergeProgressKey.mutable_mergeskey()->CopyFrom(
        incompleteMerge.Merge.Key
    );
    completeMergeProgressKey.set_dataextentnumber(dataExtentNumber);

    MergeProgressValue completeMergeProgressValue;
    completeMergeProgressValue.set_datasize(
        writeRowsResult.writtenDataSize);

    co_await operation->AddRow(
        WriteOperationMetadata{},
        mergeProgressIndex,
        &completeMergeProgressKey,
        &completeMergeProgressValue
    );

    // Update the Merges row with the progress.
    MergesValue mergesValue;
    mergesValue.CopyFrom(incompleteMerge.Merge.Value);
    
    (*writeRowsResult.resumptionRow).Key->SerializeToString(
        mergesValue.mutable_resumekey()->mutable_key());
    mergesValue.mutable_resumekey()->set_writesequencenumber(
        ToUint64((*writeRowsResult.resumptionRow).WriteSequenceNumber));
    
    co_await operation->AddRow(
        WriteOperationMetadata
        {
            .ReadSequenceNumber = incompleteMerge.Merge.WriteSequenceNumber,
        },
        mergesIndex,
        &incompleteMerge.Merge.Key,
        &mergesValue
        );

    // Also update the IncompleteMerge with these rows.
    incompleteMerge.Merge.Value = mergesValue;
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
    IInternalOperation* operation,
    ExtentNumber dataExtentNumber,
    const IncompleteMerge& incompleteMerge,
    const WriteRowsResult& writeRowsResult)
{
    // We delete all the MergeProgress rows,
    // delete the Merges row,
    // delete the Partitions rows for the source partitions,
    // add new Partitions rows for the previously completed merged partitions,
    // add a Partitions row for the newly completed merged partition.

    auto indexNumber = incompleteMerge.Merge.Key.indexnumber();
    auto mergesIndex = co_await m_protoStore->GetMergesIndex();
    auto mergeProgressIndex = co_await m_protoStore->GetMergeProgressIndex();
    auto partitionsIndex = co_await m_protoStore->GetPartitionsIndex();

    // Special case: if the merged index is the Partitions index, we have to write
    // all the partitions for the table.
    if (indexNumber == partitionsIndex->GetIndexNumber())
    {
        co_await WriteMergedPartitionsTableDataExtentNumbers(
            operation,
            dataExtentNumber,
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
            &progressRow.Key,
            nullptr);
    }

    // Delete the Merges rows.
    co_await operation->AddRow(
        WriteOperationMetadata
        {
            .ReadSequenceNumber = incompleteMerge.Merge.ReadSequenceNumber,
        },
        mergesIndex,
        &incompleteMerge.Merge.Key,
        nullptr);

    // Delete the Partitions rows for the source partitions.
    for (auto sourceDataExtentNumber : incompleteMerge.Merge.Value.sourcedataextentnumbers())
    {
        PartitionsKey sourcePartitionsKey;
        sourcePartitionsKey.set_indexnumber(
            indexNumber);
        sourcePartitionsKey.set_dataextentnumber(
            sourceDataExtentNumber);

        co_await operation->AddRow(
            WriteOperationMetadata{},
            partitionsIndex,
            &sourcePartitionsKey,
            nullptr);
    }

    // Add Partitions rows for all MergeProgress rows.
    for (auto& completeProgress : incompleteMerge.CompleteProgress)
    {
        PartitionsKey completePartitionsKey;
        completePartitionsKey.set_indexnumber(
            indexNumber);
        completePartitionsKey.set_dataextentnumber(
            completeProgress.Key.dataextentnumber());

        PartitionsValue completePartitionsValue;
        completePartitionsValue.set_headerextentnumber(
            completeProgress.Key.dataextentnumber());
        completePartitionsValue.set_level(
            incompleteMerge.Merge.Value.destinationlevelnumber());
        completePartitionsValue.set_mergeuniqueid(
            incompleteMerge.Merge.Key.mergesuniqueid());
        completePartitionsValue.set_size(
            0
        );

        co_await operation->AddRow(
            WriteOperationMetadata{},
            partitionsIndex,
            &completePartitionsKey,
            &completePartitionsValue);
    }

    // Add a Partitions row for the newly completed Partition.
    {
        PartitionsKey completePartitionsKey;
        completePartitionsKey.set_indexnumber(
            indexNumber);
        completePartitionsKey.set_dataextentnumber(
            dataExtentNumber);

        PartitionsValue completePartitionsValue;
        completePartitionsValue.set_headerextentnumber(
            dataExtentNumber);
        completePartitionsValue.set_level(
            incompleteMerge.Merge.Value.destinationlevelnumber());
        completePartitionsValue.set_mergeuniqueid(
            incompleteMerge.Merge.Key.mergesuniqueid());
        completePartitionsValue.set_size(
            writeRowsResult.writtenDataSize
        );

        co_await operation->AddRow(
            WriteOperationMetadata{},
            partitionsIndex,
            &completePartitionsKey,
            &completePartitionsValue);
    }

    // Mark the table as needing reload of its partitions.
    operation->LogRecord().mutable_extras()->add_loggedactions()->mutable_loggedupdatepartitions()->set_indexnumber(
        incompleteMerge.Merge.Key.indexnumber());
}

task<> IndexMerger::WriteMergedPartitionsTableDataExtentNumbers(
    IInternalOperation* operation,
    ExtentNumber dataExtentNumber,
    const IncompleteMerge& incompleteMerge)
{
    auto loggedPartitionsData = operation->LogRecord().mutable_extras()->add_loggedactions()->mutable_loggedpartitionsdata();

    std::set<ExtentNumber> partitions;
    auto existingPartitions = co_await m_protoStore->GetPartitionsForIndex(
        incompleteMerge.Merge.Key.indexnumber());

    for (auto& existingPartition : existingPartitions)
    {
        partitions.insert(
            get<0>(existingPartition).dataextentnumber());
    }

    for (auto sourcePartition : incompleteMerge.Merge.Value.sourcedataextentnumbers())
    {
        partitions.erase(
            sourcePartition);
    }

    for (auto& completeMergeProgress : incompleteMerge.CompleteProgress)
    {
        partitions.insert(
            completeMergeProgress.Key.dataextentnumber());
    }

    partitions.insert(
        dataExtentNumber);

    for (auto partition : partitions)
    {
        loggedPartitionsData->add_dataextentnumbers(
            partition);
    }
}

async_generator<IndexMerger::IncompleteMerge> IndexMerger::FindIncompleteMerges()
{
    auto mergesIndex = co_await m_protoStore->GetMergesIndex();

    EnumerateRequest enumerateMergesRequest;
    enumerateMergesRequest.KeyLow = nullptr;
    enumerateMergesRequest.KeyLowInclusivity = Inclusivity::Inclusive;
    enumerateMergesRequest.KeyHigh = nullptr;
    enumerateMergesRequest.KeyHighInclusivity = Inclusivity::Exclusive;
    enumerateMergesRequest.SequenceNumber = SequenceNumber::LatestCommitted;
    enumerateMergesRequest.Index = mergesIndex;

    auto mergesEnumeration = mergesIndex->Enumerate(
        enumerateMergesRequest);

    for (auto mergesIterator = co_await mergesEnumeration.begin();
        mergesIterator != mergesEnumeration.end();
        co_await ++mergesIterator)
    {
        // We found an incomplete merge.  We're going to return it directly from here.
        IncompleteMerge result;
        (*mergesIterator).Key.unpack(&result.Merge.Key);
        (*mergesIterator).Value.unpack(&result.Merge.Value);
        result.Merge.ReadSequenceNumber = (*mergesIterator).WriteSequenceNumber;
        result.Merge.WriteSequenceNumber = (*mergesIterator).WriteSequenceNumber;

        // Now get all the MergeProgress rows for this merge.
        auto mergeProgressIndex = co_await m_protoStore->GetMergeProgressIndex();

        MergeProgressKey mergeProgressKeyLow;
        mergeProgressKeyLow.mutable_mergeskey()->CopyFrom(
            result.Merge.Key);
        
        MergeProgressKey mergeProgressKeyHigh;
        mergeProgressKeyHigh.mutable_mergeskey()->CopyFrom(
            result.Merge.Key);
        mergeProgressKeyHigh.set_rangediscriminator(1);

        EnumerateRequest enumerateMergeProgressRequest;
        enumerateMergesRequest.KeyLow = &mergeProgressKeyLow;
        enumerateMergesRequest.KeyLowInclusivity = Inclusivity::Inclusive;
        enumerateMergesRequest.KeyHigh = &mergeProgressKeyHigh;
        enumerateMergesRequest.KeyHighInclusivity = Inclusivity::Exclusive;
        enumerateMergesRequest.SequenceNumber = SequenceNumber::LatestCommitted;
        enumerateMergesRequest.Index = mergesIndex;

        auto mergeProgressEnumeration = mergeProgressIndex->Enumerate(
            enumerateMergeProgressRequest);

        for (auto mergeProgressIterator = co_await mergeProgressEnumeration.begin();
            mergeProgressIterator != mergeProgressEnumeration.end();
            co_await ++mergeProgressIterator)
        {
            merge_progress_row_type mergeProgressRow;
            (*mergeProgressIterator).Key.unpack(
                &mergeProgressRow.Key);
            (*mergeProgressIterator).Value.unpack(
                &mergeProgressRow.Value);
            mergeProgressRow.ReadSequenceNumber = (*mergeProgressIterator).WriteSequenceNumber;
            mergeProgressRow.WriteSequenceNumber = (*mergeProgressIterator).WriteSequenceNumber;

            result.CompleteProgress.emplace_back(
                move(mergeProgressRow));
        }

        co_yield result;
    }
}

task<> IndexMerger::GenerateMerges()
{
    map<IndexNumber, partition_row_list_type> partitionRowsByIndexNumber;
    map<IndexNumber, merges_row_list_type> mergesRowsByIndexNumber;
    auto mergesIndex = co_await m_protoStore->GetMergesIndex();

    {
        auto partitionsIndex = co_await m_protoStore->GetPartitionsIndex();

        EnumerateRequest enumeratePartitionsRequest;
        enumeratePartitionsRequest.KeyLow = nullptr;
        enumeratePartitionsRequest.KeyLowInclusivity = Inclusivity::Inclusive;
        enumeratePartitionsRequest.KeyHigh = nullptr;
        enumeratePartitionsRequest.KeyHighInclusivity = Inclusivity::Exclusive;
        enumeratePartitionsRequest.SequenceNumber = SequenceNumber::LatestCommitted;
        enumeratePartitionsRequest.Index = partitionsIndex;

        auto partitionsEnumeration = partitionsIndex->Enumerate(
            enumeratePartitionsRequest);

        for (auto partitionsIterator = co_await partitionsEnumeration.begin();
            partitionsIterator != partitionsEnumeration.end();
            co_await ++partitionsIterator)
        {
            partition_row_type partitionRow;
            (*partitionsIterator).Key.unpack(
                &partitionRow.Key);
            (*partitionsIterator).Value.unpack(
                &partitionRow.Value);
            partitionRow.ReadSequenceNumber = (*partitionsIterator).WriteSequenceNumber;
            partitionRow.WriteSequenceNumber = (*partitionsIterator).WriteSequenceNumber;

            auto indexNumber = partitionRow.Key.indexnumber();

            partitionRowsByIndexNumber[indexNumber].push_back(
                move(partitionRow));
        }
    }

    {
        EnumerateRequest enumerateMergesRequest;
        enumerateMergesRequest.KeyLow = nullptr;
        enumerateMergesRequest.KeyLowInclusivity = Inclusivity::Inclusive;
        enumerateMergesRequest.KeyHigh = nullptr;
        enumerateMergesRequest.KeyHighInclusivity = Inclusivity::Exclusive;
        enumerateMergesRequest.SequenceNumber = SequenceNumber::LatestCommitted;
        enumerateMergesRequest.Index = mergesIndex;

        auto mergesEnumeration = mergesIndex->Enumerate(
            enumerateMergesRequest);

        for (auto mergesIterator = co_await mergesEnumeration.begin();
            mergesIterator != mergesEnumeration.end();
            co_await ++mergesIterator)
        {
            merges_row_type mergesRow;
            (*mergesIterator).Key.unpack(
                &mergesRow.Key);
            (*mergesIterator).Value.unpack(
                &mergesRow.Value);
            mergesRow.ReadSequenceNumber = (*mergesIterator).WriteSequenceNumber;
            mergesRow.WriteSequenceNumber = (*mergesIterator).WriteSequenceNumber;

            auto indexNumber = mergesRow.Key.indexnumber();

            mergesRowsByIndexNumber[indexNumber].push_back(
                move(mergesRow));
        }
    }

    bool result = false;

    for (auto indexNumberAndPartitions : partitionRowsByIndexNumber)
    {
        auto indexNumber = indexNumberAndPartitions.first;
        auto newMerges = m_mergeGenerator->GetMergeCandidates(
            indexNumber,
            MergeParameters{},
            indexNumberAndPartitions.second,
            mergesRowsByIndexNumber[indexNumber]);

        co_await m_protoStore->InternalExecuteOperation(
            BeginTransactionRequest{},
            [&](auto operation) -> task<>
        {
            for (auto& newMerge : newMerges)
            {
                co_await operation->AddRow(
                    WriteOperationMetadata{},
                    mergesIndex,
                    &newMerge.Key,
                    &newMerge.Value
                );
            }
        });
    }
}
}