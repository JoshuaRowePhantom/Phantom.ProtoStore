// Phantom.ProtoStore.h : Include file for standard system include files,
// or project specific include files.

#pragma once

#include <any>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <type_traits>
#include <variant>
#include <cppcoro/task.hpp>
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include <Phantom.System/pooled_ptr.h>

namespace Phantom::ProtoStore 
{
    using cppcoro::task;
    using google::protobuf::Message;
    using std::shared_ptr;
    using std::unique_ptr;

    template<typename T>
    concept IsMessage = std::is_convertible_v<T*, Message*>;

    typedef std::string IndexName;

    class ProtoStore;
    class IMessageStore;

    class ProtoIndex
    {
        class Impl;
        Impl* m_pImpl;

    public:
        ProtoStore* ProtoStore() const;
        const IndexName& IndexName() const;
    };

    struct GetIndexRequest
    {
        IndexName IndexName;
    };

    struct CreateIndexRequest
        : GetIndexRequest
    {
        google::protobuf::Descriptor* KeyDescriptor;
        google::protobuf::Descriptor* ValueDescriptor;
        std::vector<std::vector<google::protobuf::FieldDescriptor*>> DescendingFields;
    };

    class ProtoValue
    {
        typedef std::variant<
            std::monostate,
            std::span<const std::byte>
        > message_data_type;

        typedef std::variant<
            std::monostate,
            Message*,
            unique_ptr<Message>
        > message_type;

    public:
        message_data_type message_data;
        message_type message;

        ProtoValue(
            std::span<const std::byte> bytes)
            :
            message_data(bytes)
        {
        }

        ProtoValue(
            Message* message)
            :
            message(message)
        {
        }

        operator Message && ();
    };

    struct WriteOperation
    {
        ProtoIndex Index;
        ProtoValue Key;
        ProtoValue Value;
    };

    struct WriteRequest
    {
        std::vector<WriteOperation> WriteOperations;
    };

    struct ReadRequest
    {};

    struct ReadResult
    {};

    struct OpenRequest
    {
        // These can be optionally provided.
        shared_ptr<IMessageStore> MessageStore;
    };

    struct CreateRequest
        : public OpenRequest
    {};

    class IProtoStore
    {
    public:
        task<ProtoIndex> CreateIndex(
            const CreateIndexRequest& createIndexRequest);

        task<ProtoIndex> GetIndex(
            const GetIndexRequest& getIndexRequest);

        task<void> Write(
            const WriteRequest& writeRequest);

        task<ReadResult> Read(
            const ReadRequest& readRequest);

        static task<ProtoStore> Open(
            OpenRequest openRequest);

        static task<ProtoStore> Create(
            CreateRequest openRequest);
    };
}
