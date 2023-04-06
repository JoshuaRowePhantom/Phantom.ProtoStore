#include "ProtocolBuffersValueComparer.h"

namespace Phantom::ProtoStore
{

ProtocolBuffersValueComparer::ProtocolBuffersValueComparer(
    const Descriptor* messageDescriptor)
    :
    m_messageDescriptor(
        messageDescriptor),
    m_messageSortOrder(
        GetMessageSortOrders(messageDescriptor)),
    m_fieldSortOrder(
        GetFieldSortOrders(messageDescriptor))
{
}

ProtocolBuffersValueComparer::MessageSortOrderMap ProtocolBuffersValueComparer::GetMessageSortOrders(
    const google::protobuf::Descriptor* messageDescriptor,
    MessageSortOrderMap messageSortOrders)
{
    if (messageSortOrders.contains(messageDescriptor))
    {
        return std::move(messageSortOrders);
    }

    auto messageSortOrder =
        messageDescriptor
        ->options()
        .GetExtension(
            ::Phantom::ProtoStore::MessageOptions)
        .sortorder();

    messageSortOrders[messageDescriptor] = messageSortOrder;

    for (int fieldIndex = 0; fieldIndex < messageDescriptor->field_count(); ++fieldIndex)
    {
        auto fieldDescriptor = messageDescriptor->field(fieldIndex);
        if (fieldDescriptor->type() == google::protobuf::FieldDescriptor::TYPE_MESSAGE)
        {
            messageSortOrders = GetMessageSortOrders(
                fieldDescriptor->message_type(),
                std::move(messageSortOrders));
        }
    }

    return std::move(messageSortOrders);
}

ProtocolBuffersValueComparer::FieldSortOrderMap ProtocolBuffersValueComparer::GetFieldSortOrders(
    const google::protobuf::Descriptor* messageDescriptor,
    FieldSortOrderMap fieldSortOrders)
{
    for (int fieldIndex = 0; fieldIndex < messageDescriptor->field_count(); ++fieldIndex)
    {
        auto fieldDescriptor = messageDescriptor->field(fieldIndex);
        if (fieldSortOrders.contains(fieldDescriptor))
        {
            return std::move(fieldSortOrders);
        }

        auto fieldSortOrder =
            fieldDescriptor
            ->options()
            .GetExtension(FieldOptions)
            .sortorder();

        fieldSortOrders[fieldDescriptor] = fieldSortOrder;

        if (fieldDescriptor->type() == FieldDescriptor::TYPE_MESSAGE)
        {
            fieldSortOrders = GetFieldSortOrders(
                fieldDescriptor->message_type(),
                std::move(fieldSortOrders));
        }
    }

    return std::move(fieldSortOrders);
}

std::weak_ordering ProtocolBuffersValueComparer::CompareImpl(
    const ProtoValue& value1,
    const ProtoValue& value2
) const
{
    return CompareImpl(
        value1,
        value2,
        65335
    );
}

std::weak_ordering ProtocolBuffersValueComparer::CompareImpl(
    const ProtoValue& value1,
    const ProtoValue& value2,
    uint16_t lastFieldId
) const
{
    using google::protobuf::internal::WireFormatLite;

    auto span1 = get_uint8_t_span(value1.as_protocol_buffer_bytes_if());
    auto span2 = get_uint8_t_span(value2.as_protocol_buffer_bytes_if());

    google::protobuf::io::CodedInputStream coded1(
        span1.data(),
        span1.size());
    google::protobuf::io::CodedInputStream coded2(
        span2.data(),
        span2.size());

    struct Context
    {
        const Descriptor* MessageDescriptor;
        const FieldDescriptor* FieldDescriptor;
        int FieldIndex;
        SortOrder SortOrder;
        google::protobuf::io::CodedInputStream::Limit Limit1, Limit2;
    };

    static thread_local std::vector<Context> context;
    context.clear();
    context.push_back(
        {
            m_messageDescriptor,
            nullptr,
            0,
            m_messageSortOrder.at(m_messageDescriptor),
        });

    auto applySortOrder = [&](std::weak_ordering order)
    {
        return ApplySortOrder(
            context.back().SortOrder,
            order);
    };

    auto pushField = [&](int fieldIndex)
    {
        auto fieldDescriptor = context.back().MessageDescriptor->field(
            fieldIndex);

        Context newContext
        {
            .FieldDescriptor = fieldDescriptor,
            .FieldIndex = fieldIndex,
        };

        auto fieldSortOrder = m_fieldSortOrder.at(fieldDescriptor);

        if (fieldDescriptor->type() == FieldDescriptor::TYPE_MESSAGE)
        {
            newContext.MessageDescriptor = fieldDescriptor->message_type();
            auto messageSortOrder = m_messageSortOrder.at(newContext.MessageDescriptor);
            fieldSortOrder = CombineSortOrder(
                fieldSortOrder,
                messageSortOrder);
        }

        newContext.SortOrder = CombineSortOrder(
            context.back().SortOrder,
            fieldSortOrder);

        context.push_back(
            newContext);
    };

    auto pop = [&]()
    {
        context.pop_back();
    };

    auto comparePrimitive = [&]<typename CType, WireFormatLite::FieldType FieldType>() -> std::weak_ordering
    {
        CType fieldValue1;
        CType fieldValue2;
        WireFormatLite::ReadPrimitive<CType, FieldType>(
            &coded1,
            &fieldValue1);
        WireFormatLite::ReadPrimitive<CType, FieldType>(
            &coded2,
            &fieldValue2);

        if constexpr (std::same_as<double, CType> || std::same_as<float, CType>)
        {
            auto result = fieldValue1 <=> fieldValue2;
            if (result == std::partial_ordering::unordered)
            {
                if (fieldValue1 <=> 0 == std::partial_ordering::unordered &&
                    fieldValue2 <=> 0 == std::partial_ordering::unordered)
                {
                    return std::weak_ordering::equivalent;
                }
                else if (fieldValue1 <=> 0 == std::partial_ordering::unordered)
                {
                    return std::weak_ordering::greater;
                }
                else
                {
                    return std::weak_ordering::less;
                }
            }

            if (result == std::partial_ordering::less)
            {
                return std::weak_ordering::less;
            }
            else if (result == std::partial_ordering::greater)
            {
                return std::weak_ordering::greater;
            }
            else
            {
                return std::weak_ordering::equivalent;
            }
        }
        else
        {
            return fieldValue1 <=> fieldValue2;
        }
    };

    auto comparePrimitives = [&]<typename CType, WireFormatLite::FieldType FieldType>(
        WireFormatLite::WireType wireType1,
        WireFormatLite::WireType wireType2
        ) -> std::weak_ordering
    {
        auto count1 = 1;
        auto count2 = 1;

        if (wireType1 == WireFormatLite::WIRETYPE_LENGTH_DELIMITED)
        {
            coded1.ReadVarintSizeAsInt(&count1);
        }
        if (wireType2 == WireFormatLite::WIRETYPE_LENGTH_DELIMITED)
        {
            coded2.ReadVarintSizeAsInt(&count2);
        }

        for (auto counter = 0; counter < count1 && counter < count2; counter++)
        {
            auto result = comparePrimitive.operator() < CType, FieldType > ();
            if (result != std::weak_ordering::equivalent)
            {
                return result;
            }
        }

        return count1 <=> count2;
    };

    int fieldIndex = 0;

    while (true)
    {
        auto tag1 = coded1.ReadTag();
        auto tag2 = coded2.ReadTag();

        auto field1 = WireFormatLite::GetTagFieldNumber(tag1);
        auto field2 = WireFormatLite::GetTagFieldNumber(tag2);

        auto type1 = WireFormatLite::GetTagWireType(tag1);
        auto type2 = WireFormatLite::GetTagWireType(tag2);

        // We reached the end of a message.
        if (tag1 == 0 && tag2 == 0)
        {
            // See if we've reached the end of the top-level message.
            // If we have, we're all done.
            if (context.size() == 1)
            {
                return std::weak_ordering::equivalent;
            }
            coded1.PopLimit(context.back().Limit1);
            coded2.PopLimit(context.back().Limit2);
            pop();
            continue;
        }

        auto field = std::min(field1, field2);
        if (field == 0)
        {
            // The actual different field is the present field,
            // not the 0 not-present field.
            field = std::max(field1, field2);
        }

        // If we're at the top level,
        // and the field we're inspecting is past the last field,
        // then the two messages are equal.
        if (context.size() == 1 
            && field > lastFieldId)
        {
            return std::weak_ordering::equivalent;
        }

        // Find the field in the descriptor.
        while (
            fieldIndex < context.back().MessageDescriptor->field_count()
            && context.back().MessageDescriptor->field(fieldIndex)->number() != field
            )
        {
            ++fieldIndex;
        }

        // Canonical keys should always have their fields in the descriptor.
        assert(fieldIndex <= context.back().MessageDescriptor->field_count());

        pushField(
            fieldIndex
        );

        if (field1 < field2)
        {
            return applySortOrder(
                std::weak_ordering::less);
        }

        if (field1 > field2)
        {
            return applySortOrder(
                std::weak_ordering::greater);
        }

        // We are at the same tag.
        // Based on the type, take more action.

        // We require passed in types to satisfy the schema.
        assert(type1 == type2);

        std::weak_ordering comparisonResult;

        switch (context.back().FieldDescriptor->type())
        {
        case FieldDescriptor::TYPE_BOOL:
            comparisonResult = comparePrimitives.operator() < bool, WireFormatLite::TYPE_BOOL > (type1, type2);
            break;

        case FieldDescriptor::TYPE_INT32:
            comparisonResult = comparePrimitives.operator() < int32_t, WireFormatLite::TYPE_INT32 > (type1, type2);
            break;

        case FieldDescriptor::TYPE_INT64:
            comparisonResult = comparePrimitives.operator() < int64_t, WireFormatLite::TYPE_INT64 > (type1, type2);
            break;

        case FieldDescriptor::TYPE_SINT32:
            comparisonResult = comparePrimitives.operator() < int32_t, WireFormatLite::TYPE_SINT32 > (type1, type2);
            break;

        case FieldDescriptor::TYPE_SINT64:
            comparisonResult = comparePrimitives.operator() < int64_t, WireFormatLite::TYPE_SINT64 > (type1, type2);
            break;

        case FieldDescriptor::TYPE_UINT32:
            comparisonResult = comparePrimitives.operator() < uint32_t, WireFormatLite::TYPE_UINT32 > (type1, type2);
            break;

        case FieldDescriptor::TYPE_UINT64:
            comparisonResult = comparePrimitives.operator() < uint64_t, WireFormatLite::TYPE_UINT64 > (type1, type2);
            break;

        case FieldDescriptor::TYPE_FIXED32:
            comparisonResult = comparePrimitives.operator() < uint32_t, WireFormatLite::TYPE_FIXED32 > (type1, type2);
            break;

        case FieldDescriptor::TYPE_FIXED64:
            comparisonResult = comparePrimitives.operator() < uint64_t, WireFormatLite::TYPE_FIXED64 > (type1, type2);
            break;

        case FieldDescriptor::TYPE_SFIXED32:
            comparisonResult = comparePrimitives.operator() < int32_t, WireFormatLite::TYPE_SFIXED32 > (type1, type2);
            break;

        case FieldDescriptor::TYPE_SFIXED64:
            comparisonResult = comparePrimitives.operator() < int64_t, WireFormatLite::TYPE_SFIXED64 > (type1, type2);
            break;

        case FieldDescriptor::TYPE_DOUBLE:
            comparisonResult = comparePrimitives.operator() < double, WireFormatLite::TYPE_DOUBLE > (type1, type2);
            break;

        case FieldDescriptor::TYPE_FLOAT:
            comparisonResult = comparePrimitives.operator() < float, WireFormatLite::TYPE_FLOAT > (type1, type2);
            break;

        case FieldDescriptor::TYPE_ENUM:
            comparisonResult = comparePrimitives.operator() < int, WireFormatLite::TYPE_ENUM > (type1, type2);
            break;

        case FieldDescriptor::TYPE_BYTES:
        case FieldDescriptor::TYPE_STRING:
        {
            assert(type1 == WireFormatLite::WIRETYPE_LENGTH_DELIMITED);
            auto limit1 = coded1.ReadLengthAndPushLimit();
            auto limit2 = coded2.ReadLengthAndPushLimit();
            auto length1 = coded1.BytesUntilLimit();
            auto length2 = coded2.BytesUntilLimit();
            auto length = std::min(length1, length2);

            const void* data1;
            int size1;
            const void* data2;
            int size2;

            coded1.GetDirectBufferPointer(&data1, &size1);
            coded2.GetDirectBufferPointer(&data2, &size2);
            assert(size1 >= length1);
            assert(size2 >= length2);

            comparisonResult = memcmp(data1, data2, length) <=> 0;
            if (comparisonResult == std::weak_ordering::equivalent)
            {
                comparisonResult = length1 <=> length2;
            }
            coded1.Skip(length1);
            coded2.Skip(length2);
            coded1.PopLimit(limit1);
            coded2.PopLimit(limit2);
            break;
        }

        case FieldDescriptor::TYPE_MESSAGE:
            context.back().Limit1 = coded1.ReadLengthAndPushLimit();
            context.back().Limit2 = coded2.ReadLengthAndPushLimit();
            fieldIndex = 0;
            continue;

        default:
            assert(false);
        }

        if (comparisonResult != std::weak_ordering::equivalent)
        {
            return applySortOrder(comparisonResult);
        }

        pop();
    }
}

bool ProtocolBuffersValueComparer::EqualsImpl(
    const ProtoValue& value1,
    const ProtoValue& value2
) const
{
    auto span1 = get_uint8_t_span(value1.as_protocol_buffer_bytes_if());
    auto span2 = get_uint8_t_span(value2.as_protocol_buffer_bytes_if());

    return span1.size() == span2.size()
        && memcmp(span1.data(), span2.data(), span1.size()) == 0;
}

uint64_t ProtocolBuffersValueComparer::Hash(
    const ProtoValue& value
) const
{
    return hash_v1(
        value.as_protocol_buffer_bytes_if()
    );
}

bool ProtocolBuffersValueComparer::IsPrefixOf(
    const Prefix& prefix,
    const ProtoValue& value
) const
{
    return std::weak_ordering::equivalent == CompareImpl(
        prefix.Key,
        value,
        prefix.LastFieldId
    );
}

flatbuffers::Offset<FlatBuffers::DataValue> ProtocolBuffersValueComparer::BuildDataValue(
    ValueBuilder& valueBuilder,
    const ProtoValue& value
) const
{
    if (!value.has_value())
    {
        return 0;
    }

    if (value.as_aligned_message_if())
    {
        return valueBuilder.CreateDataValue(
            value.as_aligned_message_if());
    }

    if (value.as_message_if())
    {
        auto unowningValue = value.pack_unowned();
        auto span = unowningValue.as_protocol_buffer_bytes_if();
        return valueBuilder.CreateDataValue(
            {
                1,
                span
            }
        );
    }

    throw std::range_error("value is not a flatbuffer value");
}

int32_t ProtocolBuffersValueComparer::GetEstimatedSize(
    const ProtoValue& value
) const
{
    if (value.as_aligned_message_if())
    {
        return value.as_aligned_message_if().Payload.size();
    }

    if (value.as_message_if())
    {
        return value.as_message_if()->ByteSize();
    }

    return 0;
}

std::shared_ptr<ValueComparer> MakeProtocolBuffersValueComparer(
    std::shared_ptr<const ProtocolBuffersObjectSchema> protocolBuffersObjectSchema)
{
    return make_shared_with_strong_reference<ProtocolBuffersValueComparer>(
        protocolBuffersObjectSchema,
        protocolBuffersObjectSchema->MessageDescriptor
    );
}
}