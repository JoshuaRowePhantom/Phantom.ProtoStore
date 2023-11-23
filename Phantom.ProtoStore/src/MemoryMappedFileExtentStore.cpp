#include "MemoryMappedFileExtentStore.h"
#include "ExtentName.h"
#include <iomanip>
#include <map>
#include <fstream>
#include <sstream>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <filesystem>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <cppcoro/async_mutex.hpp>
#include <cppcoro/async_scope.hpp>
#include "Phantom.System/async_reader_writer_lock.h"
#include "boost/unordered/concurrent_flat_map.hpp"
#include "Phantom.Coroutines/async_sequence_barrier.h"
#include "Phantom.Coroutines/async_sharded_reader_writer_lock.h"

using boost::interprocess::file_mapping;
using boost::interprocess::mapped_region;

namespace Phantom::ProtoStore
{

class MemoryMappedReadableExtent
    :
    public IReadableExtent,
    public std::enable_shared_from_this<MemoryMappedReadableExtent>
{
    mapped_region m_mappedRegion;
    // This is here for debugging purposes.
    FlatValue<ExtentName> m_extentName;

    friend class MemoryMappedReadBuffer;

public:
    MemoryMappedReadableExtent(
        mapped_region mappedRegion,
        FlatValue<ExtentName> extentName
    );

    virtual task<RawData> Read(
        ExtentOffset,
        size_t
    ) override;
};

struct MemoryMappedExtentStore_flush_region
{
    ExtentOffset beginning;
    ExtentOffset end;

    MemoryMappedExtentStore_flush_region& operator +=(MemoryMappedExtentStore_flush_region other)
    {
        beginning = std::min(beginning, other.beginning);
        end = std::max(end, other.end);
        return *this;
    }
};

class MemoryMappedWritableExtent;

class MemoryMappedWriteBuffer
    :
    public IWriteBuffer
{
    MemoryMappedWritableExtent* m_extent;
    shared_ptr<mapped_region> m_mappedRegion;
    MemoryMappedExtentStore_flush_region m_flushRegion;
    std::optional<google::protobuf::io::ArrayOutputStream> m_stream;

public:
    MemoryMappedWriteBuffer(
        MemoryMappedWritableExtent* extent
    );

    // Begin writing at some location.
    // No other methods should be called until this is called.
    virtual task<WritableRawData> Write(
        ExtentOffset offset,
        size_t count
    ) override;

    // Commit the data to be flushed later.  The task will complete 
    // quickly but it's possible that no IO was done.
    virtual task<> Commit(
    ) override;

    // Ensure all the data written by this instance is persisted.
    virtual task<> Flush(
    ) override;

    virtual void ReturnToPool(
    ) override;
};

class MemoryMappedWritableExtent
    :
    public IWritableExtent
{
    friend class MemoryMappedWriteBuffer;
    string m_filename;
    size_t m_blockSize;
    
    Phantom::Coroutines::async_sequence_barrier<size_t> m_requestedFileSize;
    Phantom::Coroutines::async_sequence_barrier<size_t> m_actualFileSize;
    Phantom::Coroutines::async_scope<> m_fileExtenderScope;

    Phantom::Coroutines::async_sharded_reader_writer_lock<> m_lastMappedFullRegionLock;
    shared_ptr<mapped_region> m_lastFullRegion;
    
    struct atomic_flush_region
    {
        shared_ptr<mapped_region> m_mappedRegion;
        MemoryMappedExtentStore_flush_region m_flushRegion;
    };

    async_reader_writer_lock m_flushLock;
    cppcoro::shared_task<> m_flushTask;

    using flush_map_type = boost::concurrent_flat_map<mapped_region*, atomic_flush_region>;
    flush_map_type m_flushMap;
    std::atomic<size_t> m_nextFlushMapSize = 1;
    Schedulers m_schedulers;

    task<> GetWriteRegion(
        ExtentOffset offset,
        size_t count,
        shared_ptr<mapped_region>& region,
        void*& data,
        MemoryMappedExtentStore_flush_region& flushRegion);

    void Commit(
        const shared_ptr<mapped_region>& mappedRegion,
        MemoryMappedExtentStore_flush_region flushRegion
    );

    virtual task<> Flush() override;

    task<> Flush(
        const flush_map_type& flushMap);

    shared_task<> Flush(
        std::optional<shared_task<>> previousFlushTask);

    task<> Flush(
        const shared_ptr<mapped_region>& mappedRegion,
        MemoryMappedExtentStore_flush_region flushRegion
    );

    task<> FileExtender();

public:
    MemoryMappedWritableExtent(
        Schedulers schedulers,
        string filename,
        size_t m_blockSize
    );

    virtual task<pooled_ptr<IWriteBuffer>> CreateWriteBuffer(
    ) override;
};

MemoryMappedReadableExtent::MemoryMappedReadableExtent(
    mapped_region mappedRegion,
    FlatValue<ExtentName> extentName
) :
    m_mappedRegion(move(mappedRegion)),
    m_extentName(move(extentName))
{
}

task<RawData> MemoryMappedReadableExtent::Read(
    ExtentOffset offset,
    size_t size
)
{
    if (offset > m_mappedRegion.get_size())
    {
        co_return{};
    }
    if (offset + size > m_mappedRegion.get_size())
    {
        co_return{};
    }

    co_return RawData(
        shared_from_this(),
        {
            reinterpret_cast<const byte*>(m_mappedRegion.get_address()) + offset,
            size
        });
}

MemoryMappedWriteBuffer::MemoryMappedWriteBuffer(
    MemoryMappedWritableExtent* extent
) :
    m_extent(extent)
{
}

task<WritableRawData> MemoryMappedWriteBuffer::Write(
    ExtentOffset extentOffset,
    size_t size
)
{
    void* data = nullptr;

    co_await m_extent->GetWriteRegion(
        extentOffset,
        size,
        m_mappedRegion,
        data,
        m_flushRegion
    );
    
    int size32 = static_cast<int>(size);
    assert(size32 == size);

    m_stream.emplace(
        data,
        size32);

    co_return
    {
        m_mappedRegion,
        {
            static_cast<byte*>(data),
            size
        }
    };
}

task<> MemoryMappedWriteBuffer::Commit()
{
    m_extent->Commit(
        m_mappedRegion,
        m_flushRegion);

    co_return;
}

task<> MemoryMappedWriteBuffer::Flush()
{
    return m_extent->Flush(
        m_mappedRegion,
        m_flushRegion
    );
}

void MemoryMappedWriteBuffer::ReturnToPool()
{
    delete this;
}

MemoryMappedWritableExtent::MemoryMappedWritableExtent(
    Schedulers schedulers,
    string filename,
    size_t blockSize
) : 
    m_schedulers(schedulers),
    m_filename(filename),
    m_blockSize(blockSize)
{
    // Start the lazy flush task.
    m_flushTask = Flush(std::optional<shared_task<>>{});
    m_fileExtenderScope.spawn(FileExtender());
}

task<pooled_ptr<IWriteBuffer>> MemoryMappedWritableExtent::CreateWriteBuffer()
{
    co_return pooled_ptr<IWriteBuffer>(
        new MemoryMappedWriteBuffer(this));
}

task<> MemoryMappedWritableExtent::FileExtender()
{
    size_t fileSize = 0;
    while (true)
    {
        m_actualFileSize.publish(fileSize);

        // Wait until we're within 10% of the requested file size.
        size_t fileSizeToLookFor = fileSize - fileSize / 10;
        fileSize = co_await m_requestedFileSize.wait_until_published(
            fileSizeToLookFor);

        co_await m_schedulers.IoScheduler->schedule();

        // Grow an extra 25% over what is requested
        fileSize += fileSize / 4;

        // Always grow by at least 1 block
        fileSize += m_blockSize;

        // And round up to the next block size.
        fileSize = (fileSize + m_blockSize - 1) / m_blockSize * m_blockSize;

        {
            // If we haven't previously mapped the file, we might need to create it.
            if (!m_lastFullRegion)
            {
                std::filebuf filebuf;
                if (!filebuf.open(
                    m_filename,
                    std::ios::binary | std::ios::app
                ))
                {
                    throw std::exception("io error");
                }
            }
        }

        std::filesystem::resize_file(
            m_filename,
            fileSize
        );

        file_mapping fileMapping(
            m_filename.c_str(),
            boost::interprocess::read_write
        );

        auto newMappedRegion = make_shared<mapped_region>(
            fileMapping,
            boost::interprocess::read_write);

        auto mappedRegionLock = co_await m_lastMappedFullRegionLock.writer().scoped_lock_async();
        m_lastFullRegion = newMappedRegion;
    }
}
task<> MemoryMappedWritableExtent::GetWriteRegion(
    ExtentOffset offset,
    size_t count,
    shared_ptr<mapped_region>& region,
    void*& data,
    MemoryMappedExtentStore_flush_region& flushRegion)
{
    m_requestedFileSize.publish(offset + count);
    co_await m_actualFileSize.wait_until_published(offset + count);
    co_await m_schedulers.LockScheduler->schedule();
    auto mappedRegionLock = co_await m_lastMappedFullRegionLock.reader().scoped_lock_async();
    co_await m_schedulers.LockScheduler->schedule();

    region = m_lastFullRegion;
    data = reinterpret_cast<char*>(m_lastFullRegion->get_address()) + offset;
    flushRegion =
    {
        .beginning = offset,
        .end = offset + count,
    };
}

void MemoryMappedWritableExtent::Commit(
    const shared_ptr<mapped_region>& mappedRegion,
    MemoryMappedExtentStore_flush_region flushRegion
)
{
    m_flushMap.try_emplace_or_visit(
        mappedRegion.get(),
        mappedRegion,
        flushRegion,
        [&](flush_map_type::value_type& entry)
        {
            entry.second.m_flushRegion += flushRegion;
        });
}

task<> MemoryMappedWritableExtent::Flush()
{
    shared_task<> flushTask;

    {
        auto flushLock = co_await m_flushLock.reader().scoped_lock_async();
        co_await m_schedulers.ComputeScheduler->schedule();
        flushTask = m_flushTask;
    }

    // This lazily starts the flush task.
    co_await flushTask;
    co_await m_schedulers.ComputeScheduler->schedule();
}

task<> MemoryMappedWritableExtent::Flush(
    const flush_map_type& flushMap)
{
    co_await m_schedulers.ComputeScheduler->schedule();

    cppcoro::async_scope scope;

    auto flushOneMapEntry = [&](const flush_map_type::value_type& flushMapEntry) -> task<>
    {
        co_await m_schedulers.IoScheduler->schedule();
        flushMapEntry.first->flush(
            flushMapEntry.second.m_flushRegion.beginning,
            flushMapEntry.second.m_flushRegion.end,
            false
        );
    };

    flushMap.cvisit_all([&](const flush_map_type::value_type& flushMapEntry)
        {
            scope.spawn(flushOneMapEntry(flushMapEntry));
        });

    co_await scope.join();
}

shared_task<> MemoryMappedWritableExtent::Flush(
    std::optional<shared_task<>> previousFlushTask)
{
    {
        auto flushLock = co_await m_flushLock.writer().scoped_lock_async();
        co_await m_schedulers.ComputeScheduler->schedule();
        auto nextPreviousFlushTask = m_flushTask;

        // Create a new flush task for waiters to await on.
        // We'll flush everything that was queued until now,
        // so new items queued will need to wait on the new flush task.
        m_flushTask = Flush(std::move(nextPreviousFlushTask));
    }

    // And we'll flush at least as much as was queued until now.
    auto newFlushMapSize = m_nextFlushMapSize.load(std::memory_order_relaxed);
    flush_map_type flushMap(
        newFlushMapSize);
    m_flushMap.swap(flushMap);
    
    if (flushMap.size() > newFlushMapSize)
    {
        m_nextFlushMapSize.compare_exchange_strong(
            newFlushMapSize,
            flushMap.size() * 2,
            std::memory_order_relaxed);
    }

    co_await Flush(
        flushMap);

    if (previousFlushTask)
    {
        co_await *previousFlushTask;
        previousFlushTask.reset();
        co_await m_schedulers.ComputeScheduler->schedule();
    }
}

task<> MemoryMappedWritableExtent::Flush(
    const shared_ptr<mapped_region>& mappedRegion,
    MemoryMappedExtentStore_flush_region flushRegion
)
{
    Commit(mappedRegion, flushRegion);
    co_await Flush();
}

MemoryMappedFileExtentStore::MemoryMappedFileExtentStore(
    Schedulers schedulers,
    std::string extentFilenamePrefix,
    std::string extentFilenameSuffix,
    uint64_t writeBlockSize,
    ExtentDeleteAction extentDeleteAction
) :
    m_schedulers(schedulers),
    m_extentFilenamePrefix(extentFilenamePrefix),
    m_extentFilenameSuffix(extentFilenameSuffix),
    m_writeBlockSize(writeBlockSize),
    m_extentDeleteAction(extentDeleteAction)
{
}

std::string MemoryMappedFileExtentStore::GetFilename(
    const ExtentName* extentName)
{
    std::ostringstream result;

    result
        << m_extentFilenamePrefix;

    if (extentName->extent_name_as_DatabaseHeaderExtentName())
    {
        result
            << std::setw(8)
            << std::setfill('0')
            << extentName->extent_name_as_DatabaseHeaderExtentName()->header_copy_number()
            << ".db";
    }
    else if (extentName->extent_name_as_IndexDataExtentName())
    {
        auto indexExtentName = extentName->extent_name_as_IndexDataExtentName()->index_extent_name();
        
        result
            << std::setw(8)
            << std::setfill('0')
            << indexExtentName->index_number()
            << "_"
            << GetSanitizedIndexName(flatbuffers::GetStringView(indexExtentName->index_name()))
            << "_"
            << std::setw(8)
            << indexExtentName->partition_number()
            << "_"
            << std::setw(8)
            << static_cast<uint64_t>(indexExtentName->level())
            << ".dat";
    }
    else if (extentName->extent_name_as_IndexHeaderExtentName())
    {
        auto indexExtentName = extentName->extent_name_as_IndexHeaderExtentName()->index_extent_name();

        result
            << std::setw(8)
            << std::setfill('0')
            << indexExtentName->index_number()
            << "_"
            << GetSanitizedIndexName(flatbuffers::GetStringView(indexExtentName->index_name()))
            << "_"
            << std::setw(8)
            << indexExtentName->partition_number()
            << "_"
            << std::setw(8)
            << static_cast<uint64_t>(indexExtentName->level())
            << ".part";
    }
    else if (extentName->extent_name_as_LogExtentName())
    {
        result
            << std::setw(8)
            << std::setfill('0')
            << extentName->extent_name_as_LogExtentName()->log_extent_sequence_number()
            << ".log";
    }
    else
    {
        assert(false);
    }

    return result.str();
}

std::string MemoryMappedFileExtentStore::GetSanitizedIndexName(
    std::string_view indexName)
{
    string result;
    for (auto character : indexName)
    {
        if (character == ' '
            || character == '_')
        {
            result.push_back('_');
        }
        else if (
            character >= '0' && character <= '9'
            || character >= 'a' && character <= 'z'
            || character >= 'A' && character <= 'Z')
        {
            result.push_back(character);
        }
    }
    return result;
}

task<shared_ptr<IReadableExtent>> MemoryMappedFileExtentStore::OpenExtentForRead(
    std::filesystem::path path)
{
    co_return co_await OpenExtentForRead(
        path,
        nullptr);
}

task<shared_ptr<IReadableExtent>> MemoryMappedFileExtentStore::OpenExtentForRead(
    std::filesystem::path path,
    const ExtentName* extentName)
{
    if (std::filesystem::exists(path))
    {
        file_mapping fileMapping(
            path.string().c_str(),
            boost::interprocess::read_only
        );

        mapped_region mappedRegion(
            fileMapping,
            boost::interprocess::read_only
        );

        co_return std::make_shared<MemoryMappedReadableExtent>(
            move(mappedRegion),
            Clone(extentName)
            );
    }

    co_return std::make_shared<MemoryMappedReadableExtent>(
        mapped_region(),
        Clone(extentName));
}

task<shared_ptr<IReadableExtent>> MemoryMappedFileExtentStore::OpenExtentForRead(
    const ExtentName* extentName)
{
    auto filename = GetFilename(
        extentName);

    return OpenExtentForRead(
        filename,
        extentName);
}

task<shared_ptr<IWritableExtent>> MemoryMappedFileExtentStore::OpenExtentForWrite(
    const ExtentName* extentName)
{
    co_await DeleteExtent(extentName);

    co_return make_shared<MemoryMappedWritableExtent>(
        m_schedulers,
        GetFilename(extentName),
        m_writeBlockSize);
}

task<> MemoryMappedFileExtentStore::DeleteExtent(
    const ExtentName* extentName)
{
    auto filename = GetFilename(
        extentName);

    if (m_extentDeleteAction == ExtentDeleteAction::Rename)
    {
        if (std::filesystem::exists(
            filename
        ))
        {
            auto deletedFilename = filename + ".deleted";

            if (std::filesystem::exists(
                deletedFilename
            ))
            {
                std::filesystem::remove(
                    deletedFilename);
            }

            std::filesystem::rename(
                filename,
                deletedFilename);
        }
    }
    else
    {
        std::filesystem::remove(
            filename);
    }

    co_return;
}

std::function<task<std::shared_ptr<IExtentStore>>()> CreateMemoryMappedFileExtentStore(
    Schedulers schedulers,
    std::string extentFilenamePrefix)
{
    return [=]() -> task<std::shared_ptr<IExtentStore>>
    {
        co_return std::make_shared<MemoryMappedFileExtentStore>(
            schedulers,
            extentFilenamePrefix,
            "",
            512,
            MemoryMappedFileExtentStore::ExtentDeleteAction::Delete
        );
    };
}

}
