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
            const ExtentName* extentName)
            override;

        virtual task<shared_ptr<IWritableExtent>> OpenExtentForWrite(
            const ExtentName* extentName)
            override;

        virtual task<> DeleteExtent(
            const ExtentName* extentName)
            override;

        task<bool> ExtentExists(
            const ExtentName* extentName);
    };
}