#include "ExistingPartitions.h"
#include "Index.h"
#include <limits>
#include <unordered_map>
#include <unordered_set>
#include <Phantom.Coroutines/async_reader_writer_lock.h>

namespace Phantom::ProtoStore
{

class ExistingPartitionsImpl :
    public ExistingPartitions
{
    std::shared_ptr<IIndex> m_partitionsIndex;

    using partitions_set = std::set<PartitionNumber>;
    using index_partitions_map = std::map<IndexNumber, partitions_set>;

    
    Phantom::Coroutines::async_reader_writer_lock<> m_lock;
    partitions_set m_allExistingPartitions;
    index_partitions_map m_existingPartitionsByIndex;
    index_partitions_map m_uncommittedPartitionsByIndex;

    task<> ReadPartitionsTableAndInsertIntoMaps(
        IndexNumber lowInclusive,
        IndexNumber highInclusive
    )
    {
        FlatBuffers::PartitionsKeyT low;
        low.index_number = lowInclusive;
        FlatBuffers::PartitionsKeyT high;
        high.index_number = highInclusive;

        EnumerateRequest enumerateRequest
        {
            .Index = m_partitionsIndex.get(),
            .KeyLow = &low,
            .KeyLowInclusivity = Inclusivity::Inclusive,
            .KeyHigh = &high,
            .KeyHighInclusivity = Inclusivity::Inclusive,
            .ReadValueDisposition = ReadValueDisposition::DontReadValue,
        };

        auto partitions = m_partitionsIndex->Enumerate(
            nullptr,
            enumerateRequest);

        for (auto partitionsIterator = co_await partitions.begin();
            partitionsIterator != partitions.end();
            co_await ++partitionsIterator)
        {
            const FlatBuffers::PartitionsKey* partitionsKey = (*partitionsIterator)->Key.cast_if<FlatBuffers::PartitionsKey>();
            auto indexNumber = partitionsKey->index_number();
            auto partitionNumber = partitionsKey->header_extent_name()->index_extent_name()->partition_number();
            
            m_existingPartitionsByIndex[indexNumber].insert(partitionNumber);
            m_allExistingPartitions.insert(partitionNumber);
        }
    }

public:
    ExistingPartitionsImpl(
        std::shared_ptr<IIndex> partitionsIndex
    ) :
        m_partitionsIndex(std::move(partitionsIndex))
    {}

    virtual task<> BeginReplay(
    ) override
    {
        // No locking required since this is called once at startup.

        // Find all the partitions that exist at the start of replay
        // and add them to the existing partitions map.
        co_await ReadPartitionsTableAndInsertIntoMaps(
            0,
            std::numeric_limits<PartitionNumber>::max()
        );
    }

    virtual task<> Replay(
        const FlatMessage<FlatBuffers::LoggedCreatePartition>& loggedCreatePartition
    ) override
    {
        auto indexNumber = loggedCreatePartition->header_extent_name()->index_extent_name()->index_number();
        auto partitionNumber = loggedCreatePartition->header_extent_name()->index_extent_name()->partition_number();

        // Lock required, since this is called whenever the log message is written
        // AND during replay at startup.
        auto lock = co_await m_lock.writer().scoped_lock_async();
        m_allExistingPartitions.insert(partitionNumber);
        m_existingPartitionsByIndex[indexNumber].insert(partitionNumber);
        m_uncommittedPartitionsByIndex[indexNumber].insert(partitionNumber);
    }

    virtual task<> Replay(
        const FlatMessage<FlatBuffers::LoggedUpdatePartitions>& loggedUpdatePartitions
    ) override
    {
        auto indexNumber = loggedUpdatePartitions->index_number();

        // Lock required, since this is called whenever the log message is written
        // AND during replay at startup.
        auto lock = co_await m_lock.writer().scoped_lock_async();

        // The partitions table now represents the correct view of the set of partitions.
        // We have to completely remove our old view of the index.
        for (auto partitionNumber : m_existingPartitionsByIndex[indexNumber])
        {
            m_allExistingPartitions.erase(partitionNumber);
        }
        m_uncommittedPartitionsByIndex.erase(indexNumber);

        co_await ReadPartitionsTableAndInsertIntoMaps(
            indexNumber,
            indexNumber);
    }

    virtual task<> FinishReplay(
    ) override
    {
        // No locking required since this is called once at startup.
        
        // All the items in uncomittedPartitionsbyIndex represent nonexistent partitions,
        // since we never logged the UpdatePartitions.
        for (auto& index : m_uncommittedPartitionsByIndex)
        {
            for (auto partitionNumber : index.second)
            {
                m_allExistingPartitions.erase(partitionNumber);
            }
        }
        m_uncommittedPartitionsByIndex.clear();

        co_return;
    }

    virtual task<bool> DoesPartitionNumberExist(
        PartitionNumber partitionNumber
    ) override
    {
        // Lock required, since this is called during runtime.
        auto lock = co_await m_lock.reader().scoped_lock_async();
        co_return m_allExistingPartitions.contains(partitionNumber);
    }
};

std::shared_ptr<ExistingPartitions> MakeExistingPartitions(
    std::shared_ptr<IIndex> partitionsIndex
)
{
    return std::make_shared<ExistingPartitionsImpl>(
        std::move(partitionsIndex));
}


}