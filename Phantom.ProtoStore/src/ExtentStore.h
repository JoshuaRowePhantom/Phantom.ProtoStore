#pragma once

#include "StandardTypes.h"

namespace Phantom::ProtoStore
{
    class IReadBuffer
    {
    public:
        virtual task<> Read(
            ExtentOffset offset,
            size_t count) = 0;

        virtual ZeroCopyInputStream* Stream() = 0;

        virtual void ReturnToPool() = 0;
    };

    class IReadableExtent
    {
        friend class Returner;
    public:
        virtual task<pooled_ptr<IReadBuffer>> CreateReadBuffer() = 0;
    };

    class IWriteBuffer
    {
    public:
        // Begin writing at some location.
        // No other methods should be called until this is called.
        virtual task<> Write(
            ExtentOffset offset,
            size_t count) = 0;

        virtual ZeroCopyOutputStream* Stream() = 0;
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
            ExtentNumber extentNumber)
            = 0;

        virtual task<shared_ptr<IWritableExtent>> OpenExtentForWrite(
            ExtentNumber extentNumber) = 0;

        virtual task<> DeleteExtent(
            ExtentNumber extentNumber) = 0;
    };
}
