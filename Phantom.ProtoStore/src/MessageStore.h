#pragma once

#include <Phantom.ProtoStore/include/Phantom.ProtoStore.h>
#include "ProtoStore.pb.h"

namespace Phantom::ProtoStore
{
    typedef std::uint64_t ExtentNumber;
    typedef std::uint64_t ExtentOffset;

    struct ReadMessageResult
    {
        ExtentOffset EndOfMessage;
    };

    class IRandomMessageReader
    {
        template<typename TMessage>
        task<ReadMessageResult> Read(
            ExtentOffset extentOffset,
            Message& message);
    };

    class IRandomMessageWriter;
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
}
