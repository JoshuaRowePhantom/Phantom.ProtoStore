#pragma once

#include <Phantom.ProtoStore/include/Phantom.ProtoStore.h>
#include "ProtoStore.pb.h"

namespace Phantom::ProtoStore
{
    typedef std::uint64_t ExtentNumber;
    typedef std::uint64_t ExtentOffset;

    class IExtentStore;

    struct ReadMessageResult
    {
        ExtentOffset EndOfMessage;
    };

    struct WriteMessageResult
    {
        ExtentOffset EndOfMessage;
    };

    class IRandomMessageReader
    {
    public:
        virtual task<ReadMessageResult> Read(
            ExtentOffset extentOffset,
            Message& message) = 0;
    };

    class IRandomMessageWriter
    {
    public:
        virtual task<WriteMessageResult> Write(
            ExtentOffset extentOffset,
            const Message& message) = 0;
    };

    class ISequentialMessageReader;
    class ISequentialMessageWriter;

    class IMessageStore
    {
    public:
        virtual task<shared_ptr<IRandomMessageReader>> OpenExtentForRandomReadAccess(
            ExtentNumber extentNumber
        ) = 0;

        virtual task<shared_ptr<IRandomMessageWriter>> OpenExtentForRandomWriteAccess(
            ExtentNumber extentNumber
        ) = 0;

        virtual task<shared_ptr<ISequentialMessageWriter>> OpenExtentForSequentialReadAccess(
            ExtentNumber extentNumber
        ) = 0;

        virtual task<shared_ptr<ISequentialMessageWriter>> OpenExtentForSequentialWriteAccess(
            ExtentNumber extentNumber
        ) = 0;
    };

    task<shared_ptr<IMessageStore>> CreateMessageStore(
        shared_ptr<IExtentStore> extentStore);
}
