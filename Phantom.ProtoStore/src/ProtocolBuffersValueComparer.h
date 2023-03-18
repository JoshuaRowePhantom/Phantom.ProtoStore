#pragma once

#include "ValueComparer.h"
#include <google/protobuf/descriptor.h>

namespace Phantom::ProtoStore
{

class ProtocolBuffersValueComparer
    :
    public ValueComparer
{
private:
    const google::protobuf::Descriptor* m_messageDescriptor;
    using MessageSortOrderMap = std::unordered_map<const google::protobuf::Descriptor*, SortOrder>;
    using FieldSortOrderMap = std::unordered_map<const google::protobuf::FieldDescriptor*, SortOrder>;

    using Descriptor = google::protobuf::Descriptor;
    using FieldDescriptor = google::protobuf::FieldDescriptor;

    MessageSortOrderMap m_messageSortOrder;
    FieldSortOrderMap m_fieldSortOrder;

    static MessageSortOrderMap GetMessageSortOrders(
        const google::protobuf::Descriptor*,
        MessageSortOrderMap source = {});
    static FieldSortOrderMap GetFieldSortOrders(
        const google::protobuf::Descriptor*,
        FieldSortOrderMap source = {});

    virtual std::weak_ordering CompareImpl(
        const ProtoValue& value1,
        const ProtoValue& value2
    ) const override;

public:
    ProtocolBuffersValueComparer(
        const google::protobuf::Descriptor* messageDescriptor);

    virtual uint64_t Hash(
        const ProtoValue& value
    ) const override;

    virtual flatbuffers::Offset<FlatBuffers::DataValue> BuildDataValue(
        ValueBuilder& valueBuilder,
        const ProtoValue& value
    ) const override;

    virtual int32_t GetEstimatedSize(
        const ProtoValue& value
    ) const override;
};

}