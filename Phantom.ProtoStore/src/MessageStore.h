#pragma once

#include "StandardTypes.h"
#include "Phantom.ProtoStore/Phantom.ProtoStore.h"
#include "Phantom.ProtoStore/ProtoStore.pb.h"
#include "ExtentStore.h"

namespace Phantom::ProtoStore
{
    class IRandomMessageReader
    {
    public:
        // Read a message at the given offset.
        // If there is no message at that offset,
        // return a null DataReference.
        virtual task<DataReference<StoredMessage>> Read(
            ExtentOffset extentOffset
        ) = 0;

        // Read a message at the given offset.
        // If there is no message at that offset,
        // return a null DataReference.
        //[[deprecated]]
        virtual task<DataReference<StoredMessage>> Read(
            ExtentOffset extentOffset,
            Message& message
        ) = 0;

        virtual task<DataReference<StoredMessage>> Read(
            const FlatBuffers::MessageReference_V1* location
        ) = 0;
    };

    class IRandomMessageWriter
    {
    public:
        virtual task<DataReference<StoredMessage>> Write(
            ExtentOffset extentOffset,
            const StoredMessage& message,
            FlushBehavior flushBehavior
        ) = 0;

        virtual task<DataReference<StoredMessage>> Write(
            ExtentOffset extentOffset,
            const Message& message,
            FlushBehavior flushBehavior
        ) = 0;
    };

    class ISequentialMessageReader
    {
    public:
        virtual task<DataReference<StoredMessage>> Read(
        ) = 0;

        virtual task< DataReference<StoredMessage>> Read(
            Message& message
        ) = 0;
    };

    class ISequentialMessageWriter
    {
    public:
        virtual task<DataReference<StoredMessage>> Write(
            const StoredMessage& flatMessage,
            FlushBehavior flushBehavior
        ) = 0;

        virtual task<DataReference<StoredMessage>> Write(
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
            const FlatBuffers::ExtentName* extentName
        ) = 0;

        virtual task<shared_ptr<IRandomMessageWriter>> OpenExtentForRandomWriteAccess(
            const FlatBuffers::ExtentName* extentName
        ) = 0;

        virtual task<shared_ptr<ISequentialMessageReader>> OpenExtentForSequentialReadAccess(
            const shared_ptr<IReadableExtent>& readableExtent
        ) = 0;

        virtual task<shared_ptr<ISequentialMessageReader>> OpenExtentForSequentialReadAccess(
            const FlatBuffers::ExtentName* extentName
        ) = 0;

        virtual task<shared_ptr<ISequentialMessageWriter>> OpenExtentForSequentialWriteAccess(
            const FlatBuffers::ExtentName* extentName
        ) = 0;
    };

    shared_ptr<IMessageStore> MakeMessageStore(
        Schedulers schedulers,
        shared_ptr<IExtentStore> extentStore);
}
