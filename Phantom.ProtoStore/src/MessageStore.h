#pragma once

#include "StandardTypes.h"
#include "Phantom.ProtoStore/Phantom.ProtoStore.h"
#include "Phantom.ProtoStore/ProtoStore.pb.h"
#include "ExtentStore.h"

namespace Phantom::ProtoStore
{
    struct ReadMessageResult
    {
        ExtentOffset EndOfMessage;
    };

    struct WriteMessageResult
    {
        ExtentOffsetRange DataRange;
        ExtentOffsetRange MessageLengthRange;
        ExtentOffsetRange ChecksumAlgorithmRange;
        ExtentOffsetRange MessageRange;
        ExtentOffsetRange ChecksumRange;
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
            const Message& message,
            FlushBehavior flushBehavior) = 0;
    };

    class ISequentialMessageReader
    {
    public:
        virtual task<ReadMessageResult> Read(
            Message& message
        ) = 0;
    };

    class ISequentialMessageWriter
    {
    public:
        virtual task<WriteMessageResult> Write(
            const Message& message,
            FlushBehavior flushBehavior
        ) = 0;

        virtual task<ExtentOffset> CurrentOffset(
        ) = 0;
    };

    class IMessageStore
    {
    public:
        virtual task<shared_ptr<IRandomMessageReader>> OpenExtentForRandomReadAccess(
            const shared_ptr<IReadableExtent>& extent
        ) = 0;

        virtual task<shared_ptr<IRandomMessageReader>> OpenExtentForRandomReadAccess(
            ExtentNumber extentNumber
        ) = 0;

        virtual task<shared_ptr<IRandomMessageWriter>> OpenExtentForRandomWriteAccess(
            ExtentNumber extentNumber
        ) = 0;

        virtual task<shared_ptr<ISequentialMessageReader>> OpenExtentForSequentialReadAccess(
            ExtentNumber extentNumber
        ) = 0;

        virtual task<shared_ptr<ISequentialMessageWriter>> OpenExtentForSequentialWriteAccess(
            ExtentNumber extentNumber
        ) = 0;
    };

    shared_ptr<IMessageStore> MakeMessageStore(
        shared_ptr<IExtentStore> extentStore);
}
