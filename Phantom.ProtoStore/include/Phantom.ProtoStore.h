// Phantom.ProtoStore.h : Include file for standard system include files,
// or project specific include files.

#pragma once

#include <string>
#include <cppcoro/task.hpp>
#include <google/protobuf/descriptor.h>
#include <Phantom.System/pooled_ptr.h>

namespace Phantom::ProtoStore 
{
    using cppcoro::task;
    using std::vector;
    using google::protobuf::Descriptor;
    using google::protobuf::FieldDescriptor;
    using google::protobuf::Message;
    using std::shared_ptr;
    using std::unique_ptr;
    using Phantom::System::pooled_ptr;

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
        Descriptor* KeyDescriptor;
        Descriptor* ValueDescriptor;
        vector<vector<FieldDescriptor*>> DescendingFields;
    };

    struct ProtoValue
    {
        
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

    class ProtoStore
    {
        class Impl;
        shared_ptr<Impl> m_pImpl;
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
