#pragma once

#include <Phantom.ProtoStore/include/Phantom.ProtoStore.h>

namespace google::protobuf::io
{
    class ZeroCopyInputStream;
    class ZeroCopyOutputStream;
}

namespace Phantom::ProtoStore
{
    typedef std::uint64_t ExtentNumber;
    typedef std::uint64_t ExtentOffset;

    class IReadBuffer
    {
    public:
        virtual google::protobuf::io::ZeroCopyInputStream* Stream() = 0;
        virtual void ReturnToPool() = 0;
    };

    class IReadableExtent
    {
        friend class Returner;
    public:
        virtual task<pooled_ptr<IReadBuffer>> Read(
            ExtentOffset offset,
            size_t count
        ) = 0;
    };

    class IWriteBuffer
    {
    public:
        virtual google::protobuf::io::ZeroCopyOutputStream* Stream() = 0;
        virtual task<> Flush() = 0;
        virtual void ReturnToPool() = 0;
    };

    class IWritableExtent
    {
    public:
        virtual task<pooled_ptr<IWriteBuffer>> Write(
            ExtentOffset offset,
            size_t count
        ) = 0;
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
