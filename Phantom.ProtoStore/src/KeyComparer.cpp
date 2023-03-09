#include "StandardTypes.h"
#include "KeyComparer.h"
#include "Phantom.ProtoStore/ProtoStore.pb.h"
#include "ProtoStoreInternal.pb.h"
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/wire_format_lite.h>
#include <compare>
#include <set>

namespace Phantom::ProtoStore
{

using namespace google::protobuf;

const Serialization::PlaceholderKey KeyMinMessage = [] { Serialization::PlaceholderKey key; key.set_iskeymin(true); return key; }();
const Serialization::PlaceholderKey KeyMaxMessage = [] { Serialization::PlaceholderKey key; key.set_iskeymax(true); return key; }();
const std::span<const byte> KeyMinSpan = as_bytes(KeyMinMessage);
const std::span<const byte> KeyMaxSpan = as_bytes(KeyMaxMessage);

ProtoKeyComparer::ProtoKeyComparer(
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

ProtoKeyComparer::MessageSortOrderMap ProtoKeyComparer::GetMessageSortOrders(
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
        if (fieldDescriptor->type() == FieldDescriptor::TYPE_MESSAGE)
        {
            messageSortOrders = GetMessageSortOrders(
                fieldDescriptor->message_type(),
                std::move(messageSortOrders));
        }
    }

    return std::move(messageSortOrders);
}

ProtoKeyComparer::FieldSortOrderMap ProtoKeyComparer::GetFieldSortOrders(
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

std::weak_ordering BaseKeyComparer::ApplySortOrder(
    SortOrder sortOrder,
    std::weak_ordering value
)
{
    if (sortOrder == SortOrder::Ascending)
    {
        return value;
    }

    return 0 <=> value;
}

SortOrder BaseKeyComparer::CombineSortOrder(
    SortOrder sortOrder1,
    SortOrder sortOrder2
)
{
    if (sortOrder2 == SortOrder::Ascending)
    {
        return sortOrder1;
    }
    if (sortOrder1 == SortOrder::Ascending)
    {
        return SortOrder::Descending;
    }
    return SortOrder::Ascending;
}

std::weak_ordering KeyRangeComparer::operator()(
    const KeyAndSequenceNumberComparerArgument& left,
    const KeyRangeComparerArgument& right
    ) const
{
    auto result = m_keyComparer.Compare(
        left.Key,
        right.Key);

    if (result == std::weak_ordering::equivalent)
    {
        if (right.Inclusivity == Inclusivity::Exclusive)
        {
            result = std::weak_ordering::less;
        }
    }

    // Intentionally backward, so that later sequence numbers compare earlier.
    if (result == std::weak_ordering::equivalent)
    {
        result = right.SequenceNumber <=> left.SequenceNumber;
    }

    return result;
}

std::weak_ordering KeyRangeComparer::operator()(
    const KeyRangeComparerArgument& left,
    const KeyAndSequenceNumberComparerArgument& right
    ) const
{
    auto result = m_keyComparer.Compare(
        left.Key,
        right.Key);

    if (result == std::weak_ordering::equivalent)
    {
        if (left.Inclusivity == Inclusivity::Exclusive)
        {
            result = std::weak_ordering::greater;
        }
    }

    // Intentionally backward, so that later sequence numbers compare earlier.
    if (result == std::weak_ordering::equivalent)
    {
        result = right.SequenceNumber <=> left.SequenceNumber;
    }

    return result;
}


std::weak_ordering ProtoKeyComparer::Compare(
    std::span<const byte> value1,
    std::span<const byte> value2
) const
{
    if (value1.data() == KeyMinSpan.data())
    {
        if (value2.data() == KeyMinSpan.data())
        {
            return std::weak_ordering::equivalent;
        }
        return std::weak_ordering::less;
    }

    if (value2.data() == KeyMinSpan.data())
    {
        return std::weak_ordering::greater;
    }

    if (value1.data() == KeyMaxSpan.data())
    {
        if (value2.data() == KeyMaxSpan.data())
        {
            return std::weak_ordering::equivalent;
        }
        return std::weak_ordering::greater;
    }

    if (value2.data() == KeyMaxSpan.data())
    {
        return std::weak_ordering::less;
    }

    using google::protobuf::internal::WireFormatLite;

    google::protobuf::io::CodedInputStream coded1(
        reinterpret_cast<const uint8_t*>(value1.data()), 
        value1.size());
    google::protobuf::io::CodedInputStream coded2(
        reinterpret_cast<const uint8_t*>(value2.data()),
        value2.size());
    
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
            auto result = comparePrimitive.operator()<CType, FieldType>();
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
            comparisonResult = comparePrimitives.operator()<bool, WireFormatLite::TYPE_BOOL>(type1, type2);
            break;

        case FieldDescriptor::TYPE_INT32:
            comparisonResult = comparePrimitives.operator()<int32_t, WireFormatLite::TYPE_INT32>(type1, type2);
            break;

        case FieldDescriptor::TYPE_INT64:
            comparisonResult = comparePrimitives.operator()<int64_t, WireFormatLite::TYPE_INT64>(type1, type2);
            break;

        case FieldDescriptor::TYPE_SINT32:
            comparisonResult = comparePrimitives.operator()<int32_t, WireFormatLite::TYPE_SINT32>(type1, type2);
            break;

        case FieldDescriptor::TYPE_SINT64:
            comparisonResult = comparePrimitives.operator()<int64_t, WireFormatLite::TYPE_SINT64>(type1, type2);
            break;

        case FieldDescriptor::TYPE_UINT32:
            comparisonResult = comparePrimitives.operator()<uint32_t, WireFormatLite::TYPE_UINT32>(type1, type2);
            break;

        case FieldDescriptor::TYPE_UINT64:
            comparisonResult = comparePrimitives.operator()<uint64_t, WireFormatLite::TYPE_UINT64>(type1, type2);
            break;

        case FieldDescriptor::TYPE_FIXED32:
            comparisonResult = comparePrimitives.operator()<uint32_t, WireFormatLite::TYPE_FIXED32>(type1, type2);
            break;

        case FieldDescriptor::TYPE_FIXED64:
            comparisonResult = comparePrimitives.operator()<uint64_t, WireFormatLite::TYPE_FIXED64>(type1, type2);
            break;

        case FieldDescriptor::TYPE_SFIXED32:
            comparisonResult = comparePrimitives.operator()<int32_t, WireFormatLite::TYPE_SFIXED32>(type1, type2);
            break;

        case FieldDescriptor::TYPE_SFIXED64:
            comparisonResult = comparePrimitives.operator()<int64_t, WireFormatLite::TYPE_SFIXED64>(type1, type2);
            break;

        case FieldDescriptor::TYPE_DOUBLE:
            comparisonResult = comparePrimitives.operator()<double, WireFormatLite::TYPE_DOUBLE>(type1, type2);
            break;

        case FieldDescriptor::TYPE_FLOAT:
            comparisonResult = comparePrimitives.operator()<float, WireFormatLite::TYPE_FLOAT>(type1, type2);
            break;

        case FieldDescriptor::TYPE_ENUM:
            comparisonResult = comparePrimitives.operator()<int, WireFormatLite::TYPE_ENUM>(type1, type2);
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

std::weak_ordering KeyComparer::operator()(
    std::span<const byte> value1,
    std::span<const byte> value2
    ) const
{
    return Compare(value1, value2);
}

FlatBufferPointerKeyComparer::FlatBufferPointerKeyComparer(
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject
)
{
    flatbuffers::FlatBufferBuilder schemaBuilder;

    m_rootComparer = InternalObjectComparer::GetObjectComparer(
        *m_internalComparers,
        flatBuffersReflectionSchema,
        flatBuffersReflectionObject
    );
}

std::weak_ordering FlatBufferPointerKeyComparer::Compare(
    const void* value1,
    const void* value2
) const
{
    return m_rootComparer->Compare(
        value1,
        value2);
}

FlatBufferPointerKeyComparer::InternalObjectComparer::InternalObjectComparer()
{}

FlatBufferPointerKeyComparer::InternalObjectComparer::InternalObjectComparer(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject
)
{
    flatbuffers::ForAllFields(
        flatBuffersReflectionObject,
        false,
        [&](auto flatBuffersReflectionField)
    {
        if (flatBuffersReflectionObject->is_struct())
        {
            m_comparers.push_back(
                GetFieldComparer<flatbuffers::Struct>(
                    internalComparers,
                    flatBuffersReflectionSchema,
                    flatBuffersReflectionObject,
                    flatBuffersReflectionField
                ));
        }
        else
        {
            m_comparers.push_back(
                GetFieldComparer<flatbuffers::Table>(
                    internalComparers,
                    flatBuffersReflectionSchema,
                    flatBuffersReflectionObject,
                    flatBuffersReflectionField
                ));
        }
    });
}

std::weak_ordering FlatBufferPointerKeyComparer::InternalObjectComparer::Compare(
    const void* value1,
    const void* value2
) const
{
    if (value1 == value2)
    {
        return std::weak_ordering::equivalent;
    }

    if (value1 == nullptr)
    {
        return std::weak_ordering::less;
    }

    if (value2 == nullptr)
    {
        return std::weak_ordering::greater;
    }

    for (auto& comparer : m_comparers)
    {
        auto result = comparer.comparerFunction(
            comparer.flatBuffersReflectionField,
            comparer.elementComparer,
            value1,
            value2);

        if (result != std::weak_ordering::equivalent)
        {
            return result;
        }
    }

    return std::weak_ordering::equivalent;
}

FlatBufferPointerKeyComparer::InternalObjectComparer* FlatBufferPointerKeyComparer::InternalObjectComparer::GetObjectComparer(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject
)
{
    if (!internalComparers.contains(flatBuffersReflectionObject))
    {
        internalComparers[flatBuffersReflectionObject] = {};

        internalComparers[flatBuffersReflectionObject] = InternalObjectComparer(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject
        );
    }

    return &internalComparers[flatBuffersReflectionObject];
}

template<
    typename Container
> static FlatBufferPointerKeyComparer::InternalObjectComparer::ComparerFunction FlatBufferPointerKeyComparer::InternalObjectComparer::GetFieldComparer(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject,
    const ::reflection::Field* flatBuffersReflectionField
)
{
    using ::reflection::BaseType;

    switch (flatBuffersReflectionField->type()->base_type())
    {
    case BaseType::Obj:
    {
        auto fieldObject = flatBuffersReflectionSchema->objects()->Get(
            flatBuffersReflectionField->type()->index()
        );

        const InternalObjectComparer* elementObjectComparer = GetObjectComparer(
            internalComparers,
            flatBuffersReflectionSchema,
            fieldObject);

        if (fieldObject->is_struct())
        {
            return ComparerFunction
            {
                flatBuffersReflectionField,
                elementObjectComparer,
                [](
                    const ::reflection::Field* flatBuffersReflectionField,
                    const InternalObjectComparer* elementObjectComparer,
                    const void* value1,
                    const void* value2
                ) -> std::weak_ordering
            {
                auto field1 = flatbuffers::GetFieldStruct(
                    *reinterpret_cast<const Container*>(value1),
                    *flatBuffersReflectionField
                );

                auto field2 = flatbuffers::GetFieldStruct(
                    *reinterpret_cast<const Container*>(value2),
                    *flatBuffersReflectionField
                );

                return elementObjectComparer->Compare(
                    field1,
                    field2);
            }
            }; 
        }
        else if constexpr (std::same_as<Container, flatbuffers::Table>)
        {
            return ComparerFunction
            {
                flatBuffersReflectionField,
                elementObjectComparer,
                [](
                    const ::reflection::Field* flatBuffersReflectionField,
                    const InternalObjectComparer* elementObjectComparer,
                    const void* value1,
                    const void* value2
                ) -> std::weak_ordering
            {
                auto field1 = flatbuffers::GetFieldT(
                    *reinterpret_cast<const Container*>(value1),
                    *flatBuffersReflectionField
                );

                auto field2 = flatbuffers::GetFieldT(
                    *reinterpret_cast<const Container*>(value2),
                    *flatBuffersReflectionField
                );

                return elementObjectComparer->Compare(
                    field1,
                    field2);
            }
            };
        }

        throw std::range_error("flatBuffersReflectionField->type()->base_type()");
    }

    case BaseType::Vector:
        return GetVectorFieldComparer(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField
        );

    case BaseType::Array:
        return GetArrayFieldComparer(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField
        );

    case BaseType::String:
        if constexpr (std::same_as<Container, flatbuffers::Table>)
        {
            return GetPrimitiveFieldComparer<Container, [](auto table, auto field)
            {
                return flatbuffers::GetStringView(flatbuffers::GetFieldS(*table, *field));
            }
            > (
                flatBuffersReflectionField);
        }
        else
        {
            throw std::range_error("flatBuffersReflectionField->type()->base_type()");
        }

    case BaseType::Bool:
        return GetPrimitiveFieldComparer<Container, &GetFieldI<Container, bool>>(
            flatBuffersReflectionField
            );

    case BaseType::Byte:
        return GetPrimitiveFieldComparer<Container, &GetFieldI<Container, int8_t>>(
            flatBuffersReflectionField
            );

    case BaseType::Double:
        return GetPrimitiveFieldComparer<Container, &GetFieldF<Container, double>>(
            flatBuffersReflectionField
            );

    case BaseType::Float:
        return GetPrimitiveFieldComparer<Container, &GetFieldF<Container, float>>(
            flatBuffersReflectionField
            );

    case BaseType::Int:
        return GetPrimitiveFieldComparer<Container, &GetFieldI<Container, int32_t>>(
            flatBuffersReflectionField
            );

    case BaseType::Long:
        return GetPrimitiveFieldComparer<Container, &GetFieldI<Container, int64_t>>(
            flatBuffersReflectionField
            );

    case BaseType::Short:
        return GetPrimitiveFieldComparer<Container, &GetFieldI<Container, int16_t>>(
            flatBuffersReflectionField
            );

    case BaseType::UByte:
        return GetPrimitiveFieldComparer<Container, &GetFieldI<Container, uint8_t>>(
            flatBuffersReflectionField
            );

    case BaseType::UInt:
        return GetPrimitiveFieldComparer<Container, &GetFieldI<Container, uint32_t>>(
            flatBuffersReflectionField
            );

    case BaseType::ULong:
        return GetPrimitiveFieldComparer<Container, &GetFieldI<Container, uint64_t>>(
            flatBuffersReflectionField
            );

    case BaseType::UShort:
        return GetPrimitiveFieldComparer<Container, &GetFieldI<Container, uint16_t>>(
            flatBuffersReflectionField
            );

    case BaseType::UType:
        return GetPrimitiveFieldComparer<Container, &GetFieldI<Container, uint8_t>>(
            flatBuffersReflectionField
            );

    default:
        throw std::range_error("flatBuffersReflectionFieldType");
    }
}

template<
    typename Container,
    auto fieldRetriever
> FlatBufferPointerKeyComparer::InternalObjectComparer::ComparerFunction FlatBufferPointerKeyComparer::InternalObjectComparer::GetPrimitiveFieldComparer(
    const ::reflection::Field* flatBuffersReflectionField
)
{
    return ComparerFunction
    {
        flatBuffersReflectionField,
        nullptr,
        [](
            const ::reflection::Field* flatBuffersReflectionField,
            const InternalObjectComparer* elementObjectComparer,
            const void* value1,
            const void* value2
        ) -> std::weak_ordering
    {
        auto fieldValue1 = fieldRetriever(
            reinterpret_cast<const Container*>(value1),
            flatBuffersReflectionField
        );

        auto fieldValue2 = fieldRetriever(
            reinterpret_cast<const Container*>(value2),
            flatBuffersReflectionField
        );

        return ComparePrimitive(fieldValue1, fieldValue2);
    }
    };
}

template<
    typename Value
> std::weak_ordering FlatBufferPointerKeyComparer::InternalObjectComparer::ComparePrimitive(
    Value value1,
    Value value2
)
{
    auto baseResult = value1 <=> value2;
    if constexpr (std::same_as<decltype(baseResult), std::partial_ordering>)
    {
        if (baseResult == std::partial_ordering::less)
        {
            return std::weak_ordering::less;
        }
        if (baseResult == std::partial_ordering::greater)
        {
            return std::weak_ordering::greater;
        }
        if (baseResult == std::partial_ordering::equivalent)
        {
            return std::weak_ordering::equivalent;
        }

        // The base result was unordered.
        // That could be because either one of value1, value2, or both value1 and value2 are NAN.
        // NAN is sorted to be greater than all non-NAN
        if (std::isnan(value1))
        {
            return std::weak_ordering::greater;
        }
        if (std::isnan(value2))
        {
            return std::weak_ordering::less;
        }

        // Sort NAN by bit pattern.
        if constexpr (std::same_as<double, Value>)
        {
            uint64_t intValue1 = *reinterpret_cast<uint64_t*>(&value1);
            uint64_t intValue2 = *reinterpret_cast<uint64_t*>(&value2);
            return intValue1 <=> intValue2;
        }
        else
        {
            static_assert(std::same_as<float, Value>);
            uint32_t intValue1 = *reinterpret_cast<uint32_t*>(&value1);
            uint32_t intValue2 = *reinterpret_cast<uint32_t*>(&value2);
            return intValue1 <=> intValue2;
        }
    }
    else
    {
        return baseResult;
    }
}

FlatBufferPointerKeyComparer::InternalObjectComparer::ComparerFunction FlatBufferPointerKeyComparer::InternalObjectComparer::GetVectorFieldComparer(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject,
    const ::reflection::Field* flatBuffersReflectionField
)
{
    using ::reflection::BaseType;
    switch (flatBuffersReflectionField->type()->element())
    {
    case BaseType::Bool:
        return GetTypedVectorFieldComparer<bool>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Byte:
        return GetTypedVectorFieldComparer<int8_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Double:
        return GetTypedVectorFieldComparer<double>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Float:
        return GetTypedVectorFieldComparer<float>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Int:
        return GetTypedVectorFieldComparer<int32_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Long:
        return GetTypedVectorFieldComparer<int64_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Obj:
        
        if (flatBuffersReflectionSchema->objects()->Get(
            flatBuffersReflectionField->type()->index())->is_struct())
        {
            return GetTypedVectorFieldComparer<flatbuffers::Struct>(
                internalComparers,
                flatBuffersReflectionSchema,
                flatBuffersReflectionObject,
                flatBuffersReflectionField);
        }
        else
        {
            return GetTypedVectorFieldComparer<flatbuffers::Offset<flatbuffers::Table>>(
                internalComparers,
                flatBuffersReflectionSchema,
                flatBuffersReflectionObject,
                flatBuffersReflectionField);
        }

    case BaseType::Short:
        return GetTypedVectorFieldComparer<int16_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::String:
        return GetTypedVectorFieldComparer<flatbuffers::Offset<flatbuffers::String>>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::UByte:
        return GetTypedVectorFieldComparer<uint8_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::UInt:
        return GetTypedVectorFieldComparer<uint32_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::ULong:
        return GetTypedVectorFieldComparer<uint64_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::UShort:
        return GetTypedVectorFieldComparer<uint16_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    default:
        throw std::range_error("flatBuffersReflectionField->type()->element()");
    }
}

template<
    typename Value
> FlatBufferPointerKeyComparer::InternalObjectComparer::ComparerFunction FlatBufferPointerKeyComparer::InternalObjectComparer::GetTypedVectorFieldComparer(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject,
    const ::reflection::Field* flatBuffersReflectionField
)
{
    using reflection::BaseType;
    InternalObjectComparer* elementObjectComparer = nullptr;
    if (flatBuffersReflectionField->type()->element() == BaseType::Obj)
    {
        elementObjectComparer = GetObjectComparer(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionSchema->objects()->Get(
                flatBuffersReflectionField->type()->index()));
    }

    return ComparerFunction
    {
        flatBuffersReflectionField,
        elementObjectComparer,
        [](
            const ::reflection::Field* flatBuffersReflectionField,
            const InternalObjectComparer* elementObjectComparer,
            const void* value1,
            const void* value2
        ) -> std::weak_ordering
    {
        auto vector1 = flatbuffers::GetFieldV<Value>(
            *reinterpret_cast<const flatbuffers::Table*>(value1),
            *flatBuffersReflectionField);

        auto vector2 = flatbuffers::GetFieldV<Value>(
            *reinterpret_cast<const flatbuffers::Table*>(value2),
            *flatBuffersReflectionField);

        if (vector1 == vector2)
        {
            return std::weak_ordering::equivalent;
        }

        auto size1 = flatbuffers::VectorLength(vector1);
        auto size2 = flatbuffers::VectorLength(vector2);

        auto sizeToCompare = std::min(
            size1,
            size2
        );

        for (int32_t index = 0; index < sizeToCompare; ++index)
        {
            auto value1 = vector1->Get(index);
            auto value2 = vector2->Get(index);

            std::weak_ordering result;

            if constexpr (std::same_as<flatbuffers::Offset<flatbuffers::Table>, Value>
                || std::same_as<flatbuffers::Offset<flatbuffers::Struct>, Value>)
            {
                result = elementObjectComparer->Compare(
                    value1,
                    value2);
            }
            else if constexpr (std::same_as<flatbuffers::Offset<flatbuffers::String>, Value>)
            {
                result = ComparePrimitive(
                    value1->string_view(),
                    value2->string_view());
            }
            else
            {
                result = ComparePrimitive(
                    value1,
                    value2);
            }

            if (result != std::weak_ordering::equivalent)
            {
                return result;
            }
        }

        return size1 <=> size2;
    }
    };
}

template<
> FlatBufferPointerKeyComparer::InternalObjectComparer::ComparerFunction FlatBufferPointerKeyComparer::InternalObjectComparer::GetTypedVectorFieldComparer<
    flatbuffers::Struct
    >(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject,
    const ::reflection::Field* flatBuffersReflectionField
)
{
    using reflection::BaseType;
    InternalObjectComparer* elementObjectComparer = GetObjectComparer(
        internalComparers,
        flatBuffersReflectionSchema,
        flatBuffersReflectionSchema->objects()->Get(
            flatBuffersReflectionField->type()->index()));

    return ComparerFunction
    {
        flatBuffersReflectionField,
        elementObjectComparer,
        [](
            const ::reflection::Field* flatBuffersReflectionField,
            const InternalObjectComparer* elementObjectComparer,
            const void* value1,
            const void* value2
        ) -> std::weak_ordering
    {
        auto vector1 = flatbuffers::GetFieldAnyV(
            *reinterpret_cast<const flatbuffers::Table*>(value1),
            *flatBuffersReflectionField);

        auto vector2 = flatbuffers::GetFieldAnyV(
            *reinterpret_cast<const flatbuffers::Table*>(value2),
            *flatBuffersReflectionField);

        if (vector1 == vector2)
        {
            return std::weak_ordering::equivalent;
        }

        auto size1 = vector1 ? vector1->size() : 0;
        auto size2 = vector2 ? vector2->size() : 0;

        auto sizeToCompare = std::min(
            size1,
            size2
        )
            * flatBuffersReflectionField->type()->element_size();

        for (int32_t index = 0; index < sizeToCompare; index += flatBuffersReflectionField->type()->element_size())
        {
            auto value1 = vector1->Data() + index;
            auto value2 = vector2->Data() + index;

            std::weak_ordering result;

            result = elementObjectComparer->Compare(
                value1,
                value2);

            if (result != std::weak_ordering::equivalent)
            {
                return result;
            }
        }

        return size1 <=> size2;
    }
    };
}

FlatBufferPointerKeyComparer::InternalObjectComparer::ComparerFunction FlatBufferPointerKeyComparer::InternalObjectComparer::GetArrayFieldComparer(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject,
    const ::reflection::Field* flatBuffersReflectionField
)
{
    using ::reflection::BaseType;
    switch (flatBuffersReflectionField->type()->element())
    {
    case BaseType::Bool:
        return GetTypedArrayFieldComparer<bool>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Byte:
        return GetTypedArrayFieldComparer<int8_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Double:
        return GetTypedArrayFieldComparer<double>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Float:
        return GetTypedArrayFieldComparer<float>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Int:
        return GetTypedArrayFieldComparer<int32_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Long:
        return GetTypedArrayFieldComparer<int64_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Obj:

        if (flatBuffersReflectionSchema->objects()->Get(
            flatBuffersReflectionField->type()->index())->is_struct())
        {
            return GetTypedArrayFieldComparer<flatbuffers::Struct>(
                internalComparers,
                flatBuffersReflectionSchema,
                flatBuffersReflectionObject,
                flatBuffersReflectionField);
        }
        else
        {
            throw std::range_error("flatBuffersReflectionField->type()->index())->is_struct()");
        }

    case BaseType::Short:
        return GetTypedArrayFieldComparer<int16_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::UByte:
        return GetTypedArrayFieldComparer<uint8_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::UInt:
        return GetTypedArrayFieldComparer<uint32_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::ULong:
        return GetTypedArrayFieldComparer<uint64_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::UShort:
        return GetTypedArrayFieldComparer<uint16_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    default:
        throw std::range_error("flatBuffersReflectionField->type()->element()");
    }
}

template<
    typename Value
> FlatBufferPointerKeyComparer::InternalObjectComparer::ComparerFunction FlatBufferPointerKeyComparer::InternalObjectComparer::GetTypedArrayFieldComparer(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject,
    const ::reflection::Field* flatBuffersReflectionField
)
{
    return ComparerFunction
    {
        flatBuffersReflectionField,
        nullptr,
        [](
            const ::reflection::Field* flatBuffersReflectionField,
            const InternalObjectComparer* elementObjectComparer,
            const void* value1,
            const void* value2
        ) -> std::weak_ordering
    {
        auto array1 = flatbuffers::GetAnyFieldAddressOf<const Value>(
            *reinterpret_cast<const flatbuffers::Struct*>(value1),
            *flatBuffersReflectionField);

        auto array2 = flatbuffers::GetAnyFieldAddressOf<const Value>(
            *reinterpret_cast<const flatbuffers::Struct*>(value2),
            *flatBuffersReflectionField);

        for (int32_t index = 0; index < flatBuffersReflectionField->type()->fixed_length(); ++index)
        {
            auto value1 = *(array1 + index);
            auto value2 = *(array2 + index);

            return ComparePrimitive(
                value1,
                value2);
        }

        return std::weak_ordering::equivalent;
    }
    };
}

// Specialization for arrays of structures.
template<
> FlatBufferPointerKeyComparer::InternalObjectComparer::ComparerFunction FlatBufferPointerKeyComparer::InternalObjectComparer::GetTypedArrayFieldComparer<
    flatbuffers::Struct
>(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject,
    const ::reflection::Field* flatBuffersReflectionField
)
{
    auto elementObjectType = flatBuffersReflectionSchema->objects()->Get(
        flatBuffersReflectionField->type()->index());

    using reflection::BaseType;
    InternalObjectComparer* elementObjectComparer = GetObjectComparer(
        internalComparers,
        flatBuffersReflectionSchema,
        elementObjectType);

    return
    {
        flatBuffersReflectionField,
        elementObjectComparer,
        [](
                    const ::reflection::Field* flatBuffersReflectionField,
                    const InternalObjectComparer* elementObjectComparer,
                    const void* value1,
                    const void* value2
                ) -> std::weak_ordering
    {
        const uint8_t* array1 = flatbuffers::GetAnyFieldAddressOf<const uint8_t>(
            *reinterpret_cast<const flatbuffers::Struct*>(value1),
            *flatBuffersReflectionField);

        const uint8_t* array2 = flatbuffers::GetAnyFieldAddressOf<const uint8_t>(
            *reinterpret_cast<const flatbuffers::Struct*>(value2),
            *flatBuffersReflectionField);

        for (int32_t index = 0; index < flatBuffersReflectionField->type()->fixed_length(); ++index)
        {
            auto value1 = array1 + index * flatBuffersReflectionField->type()->element_size();
            auto value2 = array2 + index * flatBuffersReflectionField->type()->element_size();

            elementObjectComparer->Compare(
                value1,
                value2);
        }

        return std::weak_ordering::equivalent;
    }
    };
}

template<
    typename Container,
    typename Value
> Value FlatBufferPointerKeyComparer::InternalObjectComparer::GetFieldI(
    const Container* container,
    const ::reflection::Field* flatBuffersReflectionField
)
{
    if constexpr (std::same_as<flatbuffers::Table, Container>)
    {
        return flatbuffers::GetFieldI<Value>(
            *container,
            *flatBuffersReflectionField);
    }
    else
    {
        static_assert(std::same_as<flatbuffers::Struct, Container>);
        return *flatbuffers::GetAnyFieldAddressOf<const Value>(
            *container,
            *flatBuffersReflectionField);
    }
}

template<
    typename Container,
    typename Value
> Value FlatBufferPointerKeyComparer::InternalObjectComparer::GetFieldF(
    const Container* container,
    const ::reflection::Field* flatBuffersReflectionField
)
{
    if constexpr (std::same_as<flatbuffers::Table, Container>)
    {
        return flatbuffers::GetFieldF<Value>(
            *container,
            *flatBuffersReflectionField);
    }
    else
    {
        static_assert(std::same_as<flatbuffers::Struct, Container>);
        return *flatbuffers::GetAnyFieldAddressOf<const Value>(
            *container,
            *flatBuffersReflectionField);
    }
}

}
