#include "ExtentStore.h"

namespace Phantom::ProtoStore
{
    class MemoryExtentStore
        : public IExtentStore
    {
        class Impl;
        std::unique_ptr<Impl> m_impl;

    public:
        MemoryExtentStore();
        MemoryExtentStore(
            const MemoryExtentStore& other);
        ~MemoryExtentStore();

        virtual task<shared_ptr<IReadableExtent>> OpenExtentForRead(
            ExtentNumber extentNumber)
            override;

        virtual task<shared_ptr<IWritableExtent>> OpenExtentForWrite(
            ExtentNumber extentNumber)
            override;

        virtual task<> DeleteExtent(
            ExtentNumber extentNumber)
            override;
    };
}