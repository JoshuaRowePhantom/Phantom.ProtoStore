#pragma once

#include <Phantom.ProtoStore/include/Phantom.ProtoStore.h>
#include "MessageStore.h"

namespace Phantom::ProtoStore
{
    class IReadBuffer
    {
        virtual google::protobuf::io::ZeroCopyInputStream* Stream() = 0;
    };

    class IReadableExtent
    {
        virtual task<pooled_ptr<IReadBuffer>> Read(
            ExtentOffset offset,
            size_t count
        ) = 0;
    };

    class IWriteBuffer
    {
        virtual google::protobuf::io::ZeroCopyOutputStream* Stream() = 0;
        virtual task<> Flush() = 0;
    };

    class IWritableExtent
    {
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
