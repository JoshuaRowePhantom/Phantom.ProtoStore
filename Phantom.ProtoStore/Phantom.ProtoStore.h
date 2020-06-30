// Phantom.ProtoStore.h : Include file for standard system include files,
// or project specific include files.

#pragma once

#include <string>
#include <cppcoro/task.hpp>
#include <google/protobuf/descriptor.h>

namespace Phantom::ProtoStore 
{
    using cppcoro::task;

    typedef std::string IndexName;

    class ProtoIndex
    {
        class Impl;
        std::shared_ptr<Impl> m_pImpl;

    public:
        ProtoStore ProtoStore() const;
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

    struct WriteRequest
    {

    };

    struct ReadRequest
    {};

    struct ReadResult
    {};

    class ProtoStore
    {
        class Impl;
        std::shared_ptr<Impl> m_pImpl;
    public:
        task<ProtoIndex> CreateIndex(
            const CreateIndexRequest& createIndexRequest);

        task<ProtoIndex> GetIndex(
            const GetIndexRequest & getIndexRequest);

        task<void> Write(
            const WriteRequest& writeRequest);

        task<ReadResult> Read(
            const ReadRequest& readRequest);
    };
}
