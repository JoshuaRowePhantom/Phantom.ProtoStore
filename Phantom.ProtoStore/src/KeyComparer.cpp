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

KeyComparer::KeyComparer(
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

KeyComparer::MessageSortOrderMap KeyComparer::GetMessageSortOrders(
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

KeyComparer::FieldSortOrderMap KeyComparer::GetFieldSortOrders(
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

std::weak_ordering KeyComparer::ApplySortOrder(
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

SortOrder KeyComparer::CombineSortOrder(
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

std::weak_ordering KeyComparer::Compare(
    const Message* left,
    const Message* right
) const
{
    if (left == &KeyMinMessage)
    {
        if (right == &KeyMinMessage)
        {
            return std::weak_ordering::equivalent;
        }
        return std::weak_ordering::less;
    }

    if (right == &KeyMinMessage)
    {
        return std::weak_ordering::greater;
    }

    if (left == &KeyMaxMessage)
    {
        if (right == &KeyMaxMessage)
        {
            return std::weak_ordering::equivalent;
        }
        return std::weak_ordering::greater;
    }

    if (right == &KeyMaxMessage)
    {
        return std::weak_ordering::less;
    }

    auto leftDescriptor = left->GetDescriptor();
    auto rightDescriptor = right->GetDescriptor();

    auto leftReflection = left->GetReflection();
    auto rightReflection = right->GetReflection();

    for (int fieldIndex = 0; fieldIndex < leftDescriptor->field_count(); fieldIndex++)
    {
        auto fieldComparisonResult = CompareFields(
            left,
            right,
            leftReflection,
            rightReflection,
            leftDescriptor->field(fieldIndex),
            rightDescriptor->field(fieldIndex));

        if (fieldComparisonResult != std::weak_ordering::equivalent)
        {
            auto messageSortOrder = m_messageSortOrder.at(leftDescriptor);

            return ApplySortOrder(
                messageSortOrder,
                fieldComparisonResult);
        }
    }

    return std::weak_ordering::equivalent;
}

std::weak_ordering KeyComparer::CompareFields(
    const Message* left,
    const Message* right,
    const Reflection* leftReflection,
    const Reflection* rightReflection,
    const FieldDescriptor* leftFieldDescriptor,
    const FieldDescriptor* rightFieldDescriptor
) const
{
    assert(leftFieldDescriptor->number() == rightFieldDescriptor->number());

    auto compareFields = [=]<typename T>(compare_tag<T>) -> auto
    {
        auto fieldComparisonResult = CompareFields<T>(
            left,
            right,
            leftReflection,
            rightReflection,
            leftFieldDescriptor,
            rightFieldDescriptor);

        if (fieldComparisonResult != std::weak_ordering::equivalent)
        {
            auto fieldSortOrder = m_fieldSortOrder.at(leftFieldDescriptor);

            return ApplySortOrder(
                fieldSortOrder,
                fieldComparisonResult);
        }

        return std::weak_ordering::equivalent;
    };

    switch (leftFieldDescriptor->cpp_type())
    {
    case FieldDescriptor::CppType::CPPTYPE_INT32:
        return compareFields(compare_tag<int32>());
    case FieldDescriptor::CppType::CPPTYPE_INT64:
        return compareFields(compare_tag<int64>());
    case FieldDescriptor::CppType::CPPTYPE_UINT32:
        return compareFields(compare_tag<uint32>());
    case FieldDescriptor::CppType::CPPTYPE_UINT64:
        return compareFields(compare_tag<uint64>());
    case FieldDescriptor::CppType::CPPTYPE_DOUBLE:
        return compareFields(compare_tag<double>());
    case FieldDescriptor::CppType::CPPTYPE_FLOAT:
        return compareFields(compare_tag<float>());
    case FieldDescriptor::CppType::CPPTYPE_BOOL:
        return compareFields(compare_tag<bool>());
    case FieldDescriptor::CppType::CPPTYPE_ENUM:
        return compareFields(compare_tag<int32>());
    case FieldDescriptor::CppType::CPPTYPE_STRING:
        return compareFields(compare_tag<string>());
    case FieldDescriptor::CppType::CPPTYPE_MESSAGE:
        return compareFields(compare_tag<Message>());
    default:
        throw std::exception();
    }
}

template<IsOrderedBy<std::weak_ordering> T>
std::weak_ordering KeyComparer::CompareValues(
    const T& left,
    const T& right
) const
{
    return left <=> right;
}

template<IsOrderedBy<std::strong_ordering> T>
std::weak_ordering KeyComparer::CompareValues(
    const T& left,
    const T& right
) const
{
    return left <=> right;
}

template<IsOrderedBy<std::partial_ordering> T>
std::weak_ordering KeyComparer::CompareValues(
    const T& left,
    const T& right
) const
{
    auto result = left <=> right;
    if (result == std::partial_ordering::unordered
        ||
        result == std::partial_ordering::equivalent)
    {
        return std::weak_ordering::equivalent;
    }
    if (result == std::partial_ordering::greater)
    {
        return std::weak_ordering::greater;
    }
    return std::weak_ordering::less;
}

std::weak_ordering KeyComparer::CompareValues(
    const std::string& left,
    const std::string& right
) const
{
    return left.compare(right) <=> 0;
}

std::weak_ordering KeyComparer::CompareValues(
    const Message& left,
    const Message& right
) const
{
    return Compare(
        &left,
        &right);
}

static auto GetFieldValue(
    const Message* message,
    const Reflection* reflection,
    const FieldDescriptor* fieldDescriptor,
    KeyComparer::compare_tag<int32>
)
{
    return reflection->GetInt32(
        *message,
        fieldDescriptor);
}

static auto GetFieldValue(
    const Message* message,
    const Reflection* reflection,
    const FieldDescriptor* fieldDescriptor,
    KeyComparer::compare_tag<uint32>
)
{
    return reflection->GetUInt32(
        *message,
        fieldDescriptor);
}

static auto GetFieldValue(
    const Message* message,
    const Reflection* reflection,
    const FieldDescriptor* fieldDescriptor,
    KeyComparer::compare_tag<int64>
)
{
    return reflection->GetInt64(
        *message,
        fieldDescriptor);
}

static auto GetFieldValue(
    const Message* message,
    const Reflection* reflection,
    const FieldDescriptor* fieldDescriptor,
    KeyComparer::compare_tag<uint64>
)
{
    return reflection->GetUInt64(
        *message,
        fieldDescriptor);
}

static auto GetFieldValue(
    const Message* message,
    const Reflection* reflection,
    const FieldDescriptor* fieldDescriptor,
    KeyComparer::compare_tag<double>
)
{
    return reflection->GetDouble(
        *message,
        fieldDescriptor);
}

static auto GetFieldValue(
    const Message* message,
    const Reflection* reflection,
    const FieldDescriptor* fieldDescriptor,
    KeyComparer::compare_tag<float>
)
{
    return reflection->GetFloat(
        *message,
        fieldDescriptor);
}

static auto GetFieldValue(
    const Message* message,
    const Reflection* reflection,
    const FieldDescriptor* fieldDescriptor,
    KeyComparer::compare_tag<bool>
)
{
    return reflection->GetBool(
        *message,
        fieldDescriptor);
}

template<typename T>
std::weak_ordering KeyComparer::CompareFields(
    const Message* left,
    const Message* right,
    const Reflection* leftReflection,
    const Reflection* rightReflection,
    const FieldDescriptor* leftFieldDescriptor,
    const FieldDescriptor* rightFieldDescriptor,
    compare_tag<T> tag
) const
{
    if (leftFieldDescriptor->is_repeated())
    {
        return CompareRepeatedFields(
            left,
            right,
            leftReflection,
            rightReflection,
            leftFieldDescriptor,
            rightFieldDescriptor,
            tag);
    }
    
    return CompareNonRepeatedFields(
        left,
        right,
        leftReflection,
        rightReflection,
        leftFieldDescriptor,
        rightFieldDescriptor,
        tag);
}

std::weak_ordering KeyComparer::CompareNonRepeatedFields(
    const Message* left,
    const Message* right,
    const Reflection* leftReflection,
    const Reflection* rightReflection,
    const FieldDescriptor* leftFieldDescriptor,
    const FieldDescriptor* rightFieldDescriptor,
    compare_tag<string> tag
) const
{
    string leftStringCopy;
    auto& leftString = leftReflection->GetStringReference(
        *left,
        leftFieldDescriptor,
        &leftStringCopy);

    string rightStringCopy;
    auto& rightString = rightReflection->GetStringReference(
        *right,
        rightFieldDescriptor,
        &rightStringCopy);

    return CompareValues(
        leftString, 
        rightString);
}

std::weak_ordering KeyComparer::CompareNonRepeatedFields(
    const Message* left,
    const Message* right,
    const Reflection* leftReflection,
    const Reflection* rightReflection,
    const FieldDescriptor* leftFieldDescriptor,
    const FieldDescriptor* rightFieldDescriptor,
    compare_tag<Message> tag
) const
{
    auto& leftMessage = leftReflection->GetMessage(
        *left,
        leftFieldDescriptor,
        nullptr);

    auto& rightMessage = rightReflection->GetMessage(
        *right,
        rightFieldDescriptor,
        nullptr);

    return CompareValues(
        leftMessage, 
        rightMessage);
}

template<typename T>
std::weak_ordering KeyComparer::CompareNonRepeatedFields(
    const Message* left,
    const Message* right,
    const Reflection* leftReflection,
    const Reflection* rightReflection,
    const FieldDescriptor* leftFieldDescriptor,
    const FieldDescriptor* rightFieldDescriptor,
    compare_tag<T> tag
) const
{
    auto leftValue = GetFieldValue(
        left,
        leftReflection,
        leftFieldDescriptor,
        tag);

    auto rightValue = GetFieldValue(
        right,
        rightReflection,
        rightFieldDescriptor,
        tag);

    return CompareValues(
        leftValue, 
        rightValue);
}

template<typename T>
std::weak_ordering KeyComparer::CompareRepeatedFields(
    const Message* left,
    const Message* right,
    const Reflection* leftReflection,
    const Reflection* rightReflection,
    const FieldDescriptor* leftFieldDescriptor,
    const FieldDescriptor* rightFieldDescriptor,
    compare_tag<T> tag
) const
{
    auto leftRepeatedField = leftReflection->GetRepeatedFieldRef<T>(
        *left,
        leftFieldDescriptor);

    auto rightRepeatedField = rightReflection->GetRepeatedFieldRef<T>(
        *right,
        rightFieldDescriptor);

    auto leftIterator = leftRepeatedField.begin();
    auto rightIterator = rightRepeatedField.begin();

    auto leftEnd = leftRepeatedField.end();
    auto rightEnd = rightRepeatedField.end();

    while (leftIterator != leftEnd
        && rightIterator != rightEnd)
    {
        auto comparison = CompareValues(
            *leftIterator++,
            *rightIterator++);

        if (comparison != std::weak_ordering::equivalent)
        {
            return comparison;
        }
    }

    if (leftIterator != leftEnd)
    {
        return std::weak_ordering::greater;
    }
    if (rightIterator != rightEnd)
    {
        return std::weak_ordering::less;
    }
    return std::weak_ordering::equivalent;
}

std::weak_ordering KeyComparer::operator()(
    const google::protobuf::Message* value1,
    const google::protobuf::Message* value2
    ) const
{
    return Compare(
        value1,
        value2);
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


std::weak_ordering KeyComparer::Compare(
    std::span<const byte> value1,
    std::span<const byte> value2
) const
{
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

}