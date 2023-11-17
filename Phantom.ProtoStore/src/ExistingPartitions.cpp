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
    std::shared_ptr<IIndexData> m_partitionsIndex;

    using partitions_set = std::set<PartitionNumber>;
    using index_partitions_map = std::map<IndexNumber, partitions_set>;

    
    Phantom::Coroutines::async_reader_writer_lock<> m_lock;
    partitions_set m_allExistingPartitions;
    partitions_set m_allMemoryTablePartitions;
    index_partitions_map m_diskPartitionsByIndex;
    index_partitions_map m_uncommittedPartitionsByIndex;

public:
    ExistingPartitionsImpl(
        std::shared_ptr<IIndexData> partitionsIndex
    ) :
        m_partitionsIndex(std::move(partitionsIndex))
    {}

    virtual task<> BeginReplay(
    ) override
    {
        // No locking required since this is called once at startup.
        co_return;
    }

    virtual task<> Replay(
        const FlatMessage<FlatBuffers::LoggedCreateExtent>& loggedCreateExtent
    ) override
    {
        auto indexHeaderExtentName = loggedCreateExtent->extent_name()->extent_name_as_IndexHeaderExtentName();
        if (!indexHeaderExtentName)
        {
            co_return;
        }

        auto indexNumber = indexHeaderExtentName->index_extent_name()->index_number();
        auto partitionNumber = indexHeaderExtentName->index_extent_name()->partition_number();

        // Lock required, since this is called whenever the log message is written
        // AND during replay at startup.
        auto lock = co_await m_lock.writer().scoped_lock_async();
        m_allExistingPartitions.insert(partitionNumber);
        m_diskPartitionsByIndex[indexNumber].insert(partitionNumber);
        m_uncommittedPartitionsByIndex[indexNumber].insert(partitionNumber);
    }

    virtual task<> Replay(
        const FlatMessage<FlatBuffers::LoggedCommitExtent>& loggedCommitExtent
    ) override
    {
        auto indexHeaderExtentName = loggedCommitExtent->extent_name()->extent_name_as_IndexHeaderExtentName();
        if (!indexHeaderExtentName)
        {
            co_return;
        }

        auto indexNumber = indexHeaderExtentName->index_extent_name()->index_number();
        auto partitionNumber = indexHeaderExtentName->index_extent_name()->partition_number();

        // Lock required, since this is called whenever the log message is written
        // AND during replay at startup.
        auto lock = co_await m_lock.writer().scoped_lock_async();
        m_uncommittedPartitionsByIndex[indexNumber].erase(partitionNumber);
    }
    
    virtual task<> Replay(
        const FlatMessage<FlatBuffers::LoggedDeleteExtent>& loggedDeleteExtent
    ) override
    {
        auto indexHeaderExtentName = loggedDeleteExtent->extent_name()->extent_name_as_IndexHeaderExtentName();
        if (!indexHeaderExtentName)
        {
            co_return;
        }

        auto indexNumber = indexHeaderExtentName->index_extent_name()->index_number();
        auto partitionNumber = indexHeaderExtentName->index_extent_name()->partition_number();

        // Lock required, since this is called whenever the log message is written
        // AND during replay at startup.
        auto lock = co_await m_lock.writer().scoped_lock_async();
        m_uncommittedPartitionsByIndex[indexNumber].erase(partitionNumber);
        m_diskPartitionsByIndex[indexNumber].erase(partitionNumber);
        m_allExistingPartitions.erase(partitionNumber);
    }

    virtual task<> Replay(
        const FlatMessage<FlatBuffers::LoggedRowWrite>& loggedRowWrite
    ) override
    {
        auto partitionNumber = loggedRowWrite->partition_number();

        // If we check with a read lock (which is cheaper globally,
        // but overall a bit more expensive when serving this request),
        // and the partition number exists, then we know that the
        // data structures have already been updated.
        if (co_await DoesPartitionNumberExist(partitionNumber))
        {
            co_return;
        }

        // Lock required, since this is called whenever the log message is written
        // AND during replay at startup.
        auto lock = co_await m_lock.writer().scoped_lock_async();
        m_allExistingPartitions.insert(partitionNumber);
        m_allMemoryTablePartitions.insert(partitionNumber);
    }

    virtual task<> Replay(
        const FlatMessage<FlatBuffers::LoggedCheckpoint>& loggedCheckpoint
    ) override
    {
        auto lock = co_await m_lock.writer().scoped_lock_async();
        
        for (auto partitionNumber : *loggedCheckpoint->partition_number())
        {
            m_allExistingPartitions.erase(partitionNumber);
            m_allMemoryTablePartitions.erase(partitionNumber);
        }
    }

    virtual task<> Replay(
        const FlatMessage<FlatBuffers::LoggedPartitionsData>& loggedPartitionsData
    ) override
    {
        // Lock required, since this is called whenever the log message is written
        // AND during replay at startup.
        auto lock = co_await m_lock.writer().scoped_lock_async();

        // We read the entire partitions table and add all partitions we find.
        FlatBuffers::PartitionsKeyT prefix;

        EnumeratePrefixRequest enumeratePrefixRequest
        {
            .Index = m_partitionsIndex.get(),
            .Prefix = 
            {
                .Key = { &prefix },
                .LastFieldId = 1,
            },
            .ReadValueDisposition = ReadValueDisposition::DontReadValue,
        };

        auto partitions = m_partitionsIndex->EnumeratePrefix(
            nullptr,
            std::move(enumeratePrefixRequest));

        for (auto partitionsIterator = co_await partitions.begin();
            partitionsIterator != partitions.end();
            co_await ++partitionsIterator)
        {
            const FlatBuffers::PartitionsKey* partitionsKey = (*partitionsIterator)->Key.cast_if<FlatBuffers::PartitionsKey>();
            auto indexNumber = partitionsKey->index_number();
            auto partitionNumber = partitionsKey->header_extent_name()->index_extent_name()->partition_number();

            m_diskPartitionsByIndex[indexNumber].insert(partitionNumber);
            m_allExistingPartitions.insert(partitionNumber);
        }
    }

    virtual task<> FinishReplay(
    ) override
    {
        // No locking required since this is called once at startup.
        
        // All the items in uncommittedPartitionsbyIndex represent nonexistent partitions,
        // since we never logged the CommitExtent message.
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
    std::shared_ptr<IIndexData> partitionsIndex
)
{
    return std::make_shared<ExistingPartitionsImpl>(
        std::move(partitionsIndex));
}


}