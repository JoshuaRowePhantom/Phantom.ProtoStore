#include "IndexMerger.h"
#include "Phantom.System/utility.h"
#include "src/ProtoStoreInternal.pb.h"
#include "Partition.h"
#include "PartitionWriterImpl.h"
#include "Index.h"
#include "Schema.h"
#include "RowMerger.h"
#include "InternalProtoStore.h"

namespace Phantom::ProtoStore
{

IndexMerger::IndexMerger(
    IInternalProtoStore* protoStore,
    IndexPartitionMergeGenerator* mergeGenerator
) :
    m_protoStore(protoStore),
    m_mergeGenerator(mergeGenerator)
{
    m_delayedMergeOnePartitionTask = DelayedMergeOnePartition(
        make_completed_shared_task(true));
}

IndexMerger::~IndexMerger()
{
    SyncDestroy();
}

task<> IndexMerger::Merge()
{
    shared_task<bool> mergeTask;

    do
    {
        auto lock = m_delayedMergeOnePartitionTaskLock.scoped_lock_async();
        mergeTask = m_delayedMergeOnePartitionTask;
    } while (co_await mergeTask);
}

shared_task<bool> IndexMerger::DelayedMergeOnePartition(
    shared_task<bool> previousDelayedMergeOnePartition)
{
    co_await previousDelayedMergeOnePartition;
    auto result = co_await MergeOnePartition();

    auto lock = m_delayedMergeOnePartitionTaskLock.scoped_lock_async();
    m_delayedMergeOnePartitionTask = DelayedMergeOnePartition(
        m_delayedMergeOnePartitionTask);

    co_return result;
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

    bool isCompletedMerge = writeRowsResult.resumptionRow == rowGenerator.end();

    optional<cppcoro::async_mutex_lock> updatePartitionsLock;
    if (isCompletedMerge)
    {
        updatePartitionsLock.emplace(
            co_await m_protoStore->AcquireUpdatePartitionsLock());
    }

    co_await m_protoStore->InternalExecuteOperation(
        BeginTransactionRequest{},
        [&](auto operation) -> task<>
    {
        co_await m_protoStore->LogCommitDataExtent(
            operation->LogRecord(),
            dataExtentNumber);

        if (!isCompletedMerge)
        {
            co_await WriteMergeProgress(
                operation,
                dataExtentNumber,
                incompleteMerge,
                writeRowsResult);
        }
        else
        {
            co_await WriteMergeCompletion(
                operation,
                dataExtentNumber,
                incompleteMerge,
                writeRowsResult);
        }
    });

    if (isCompletedMerge)
    {
        co_await m_protoStore->UpdatePartitionsForIndex(
            incompleteMerge.Merge.Key.indexnumber(),
            *updatePartitionsLock);
    }
}

task<> IndexMerger::WriteMergeProgress(
    IInternalOperation* operation,
    ExtentNumber dataExtentNumber,
    const IncompleteMerge& incompleteMerge,
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

    auto mergesIndex = co_await m_protoStore->GetMergesIndex();
    auto mergeProgressIndex = co_await m_protoStore->GetMergeProgressIndex();
    auto partitionsIndex = co_await m_protoStore->GetMergeProgressIndex();

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
            incompleteMerge.Merge.Key.indexnumber());
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
            incompleteMerge.Merge.Key.indexnumber());
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
            incompleteMerge.Merge.Key.indexnumber());
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

    // Special case: if the merged index is the Partitions index, we have to write
    // all the partitions for the table.

    // Mark the table as needing reload of its partitions.
    operation->LogRecord().mutable_extras()->add_loggedactions()->mutable_loggedupdatepartitions()->set_indexnumber(
        incompleteMerge.Merge.Key.indexnumber());

}

task<bool> IndexMerger::MergeOnePartition()
{
    auto incompleteMerge = co_await FindIncompleteMerge();

    if (incompleteMerge.has_value())
    {
        co_await RestartIncompleteMerge(
            *incompleteMerge);
        co_return true;
    }

    co_return false;
}

task<optional<IndexMerger::IncompleteMerge>> IndexMerger::FindIncompleteMerge()
{
    optional<IncompleteMerge> result;

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
        result = IncompleteMerge();
        (*mergesIterator).Key.unpack(&result->Merge.Key);
        (*mergesIterator).Value.unpack(&result->Merge.Value);
        result->Merge.ReadSequenceNumber = (*mergesIterator).WriteSequenceNumber;
        result->Merge.WriteSequenceNumber = (*mergesIterator).WriteSequenceNumber;

        // Now get all the MergeProgress rows for this merge.
        auto mergeProgressIndex = co_await m_protoStore->GetMergeProgressIndex();

        MergeProgressKey mergeProgressKeyLow;
        mergeProgressKeyLow.mutable_mergeskey()->CopyFrom(
            result->Merge.Key);
        
        MergeProgressKey mergeProgressKeyHigh;
        mergeProgressKeyHigh.mutable_mergeskey()->CopyFrom(
            result->Merge.Key);
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

            result->CompleteProgress.emplace_back(
                move(mergeProgressRow));
        }

        break;
    }

    co_return result;
}

}