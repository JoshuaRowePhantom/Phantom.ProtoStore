#include "MemoryExtentStore.h"
#include <functional>
#include <map>
#include <mutex>
#include <vector>

namespace Phantom::ProtoStore
{
    class MemoryExtentStore::Impl
    {
        class Extent 
            : 
            public IReadableExtent,
            public IWritableExtent
        {
            std::mutex m_mutex;
            std::shared_ptr<vector<uint8_t>> m_bytes;

            class ReadBuffer
                :
                public IReadBuffer
            {
            public:
                ReadBuffer(
                    Extent* m_extent,
                    std::shared_ptr<vector<uint8_t>> m_bytes,
                    ExtentOffset offset,
                    size_t count)
                {
                }

                virtual google::protobuf::io::ZeroCopyInputStream* Stream() override
                {
                    throw 0;
                }
            };

            class WriteBuffer
                :
                public IWriteBuffer
            {
            public:
                WriteBuffer(
                    Extent* m_extent,
                    std::shared_ptr<vector<uint8_t>> m_bytes,
                    ExtentOffset offset,
                    size_t count)
                {
                }

                virtual google::protobuf::io::ZeroCopyOutputStream* Stream() override
                {
                    throw 0;
                }

                virtual task<> Flush() override
                {
                    throw 0;
                }
            };

            virtual task<pooled_ptr<IReadBuffer>> Read(
                ExtentOffset offset,
                size_t count
            ) override
            {
                throw 0;
            }

            virtual task<pooled_ptr<IWriteBuffer>> Write(
                ExtentOffset offset,
                size_t count
            ) override
            {
                throw 0;
            }

        public:
            Extent()
                :
                m_bytes(std::make_shared<vector<uint8_t>>())
            {}
        };

        class OpenedExtent
        {
            bool m_bIsOpen;
            std::function<void()> m_closeFunction;

            OpenedExtent(
                std::function<void()> openFunction,
                std::function<void()> closeFunction)
                :
                m_bIsOpen(false),
                m_closeFunction(closeFunction)
            {
                openFunction();
                m_bIsOpen = true;
            }

            ~OpenedExtent()
            {
                if (m_bIsOpen)
                {
                    m_closeFunction();
                }
            }
        };

        std::map<ExtentNumber, std::shared_ptr<Extent>> m_extents;
        std::mutex m_extentsMutex;

        shared_ptr<Extent> GetExtent(
            ExtentNumber extentNumber)
        {
            std::scoped_lock lock(m_extentsMutex);

            auto existingExtent = m_extents.find(
                extentNumber);

            if (existingExtent != m_extents.end())
            {
                return existingExtent->second;
            }

            auto newExtent = std::make_shared<Extent>();
            m_extents[extentNumber] = newExtent;
            return newExtent;
        }
        
    public:
        task<shared_ptr<IReadableExtent>> OpenExtentForRead(
            ExtentNumber extentNumber)
        {
            co_return GetExtent(
                extentNumber);
        }

        task<shared_ptr<IWritableExtent>> OpenExtentForWrite(
            ExtentNumber extentNumber)
        {
            co_return GetExtent(
                extentNumber);
        }

        task<> DeleteExtent(
            ExtentNumber extentNumber)
        {
            {
                std::scoped_lock lock(m_extentsMutex);
                m_extents.erase(
                    extentNumber);
            }

            co_return;
        }
    };
}
