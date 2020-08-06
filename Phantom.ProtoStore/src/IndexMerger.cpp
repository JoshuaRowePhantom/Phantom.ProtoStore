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

    co_await m_protoStore->InternalExecuteOperation(
        BeginTransactionRequest{},
        [&](auto operation) -> task<>
    {
        co_await m_protoStore->LogCommitDataExtent(
            operation->LogRecord(),
            dataExtentNumber);

        if (writeRowsResult.resumptionRow != rowGenerator.end())
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

}