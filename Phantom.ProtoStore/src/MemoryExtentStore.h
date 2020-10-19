#include "ExtentStore.h"

namespace Phantom::ProtoStore
{
    class MemoryExtentStore
        : public IExtentStore
    {
        class Impl;
        unique_ptr<Impl> m_impl;

    public:
        MemoryExtentStore(
            Schedulers schedulers);

        MemoryExtentStore(
            const MemoryExtentStore& other);
        ~MemoryExtentStore();

        virtual task<shared_ptr<IReadableExtent>> OpenExtentForRead(
            ExtentName extentName)
            override;

        virtual task<shared_ptr<IWritableExtent>> OpenExtentForWrite(
            ExtentName extentName)
            override;

        virtual task<> DeleteExtent(
            ExtentName extentName)
            override;

        task<bool> ExtentExists(
            ExtentName extentName);
    };
}