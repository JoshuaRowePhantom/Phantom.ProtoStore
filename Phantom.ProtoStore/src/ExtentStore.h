#pragma once

#include "StandardTypes.h"

namespace Phantom::ProtoStore
{
class IReadableExtent
{
public:
    virtual task<RawData> Read(
        ExtentOffset,
        size_t
    ) = 0;
};

class IWriteBuffer
{
public:
    // Begin writing at some location.
    // No other methods should be called until this is called.
    virtual task<WritableRawData> Write(
        ExtentOffset offset,
        size_t count) = 0;

    // Commit the data to be flushed later.  The task will complete 
    // quickly but it's possible that no IO was done.
    virtual task<> Commit() = 0;
    // Ensure all the data written by this instance is persisted.
    virtual task<> Flush() = 0;
    virtual void ReturnToPool() = 0;
};

class IWritableExtent
{
public:
    virtual task<pooled_ptr<IWriteBuffer>> CreateWriteBuffer() = 0;
};

class IExtentStore
{
public:
    virtual task<shared_ptr<IReadableExtent>> OpenExtentForRead(
        ExtentName extentName)
        = 0;

    virtual task<shared_ptr<IWritableExtent>> OpenExtentForWrite(
        ExtentName extentName) = 0;

    virtual task<> DeleteExtent(
        ExtentName extentName) = 0;
};

class MemoryExtentStore;
#ifdef WIN32 
class WindowsMemoryMappedFileExtentStore;
#endif

}
