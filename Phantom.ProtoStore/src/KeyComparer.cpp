#include "StandardTypes.h"
#include "KeyComparer.h"
#include "ProtoStore.pb.h"
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <stack>
#include <compare>
#include <set>

namespace Phantom::ProtoStore
{

using namespace google::protobuf;

KeyComparer::KeyComparer(
    const Descriptor* messageDescriptor)
    :
    m_messageDescriptor(
        messageDescriptor)
{
    vector<string> fieldNames;
    for (int fieldIndex = 0; fieldIndex < messageDescriptor->field_count(); fieldIndex++)
    {
        fieldNames.push_back(messageDescriptor->field(fieldIndex)->name());
    }
}

std::weak_ordering KeyComparer::ApplySortOrder(
    SortOrder sortOrder,
    std::weak_ordering value)
{
    if (sortOrder == SortOrder::Ascending)
    {
        return value;
    }

    return 0 <=> value;
}

std::weak_ordering KeyComparer::Compare(
    const Message* left,
    const Message* right)
{
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
            auto messageSortOrder = 
                leftDescriptor
                ->options()
                .GetExtension(
                    ::Phantom::ProtoStore::MessageOptions)
                .sortorder();

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
    const FieldDescriptor* rightFieldDescriptor)
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
            auto fieldSortOrder =
                leftFieldDescriptor
                ->options()
                .GetExtension(FieldOptions)
                .sortorder();

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
    const T& right)
{
    return left <=> right;
}

template<IsOrderedBy<std::strong_ordering> T>
std::weak_ordering KeyComparer::CompareValues(
    const T& left,
    const T& right)
{
    return left <=> right;
}

template<IsOrderedBy<std::partial_ordering> T>
std::weak_ordering KeyComparer::CompareValues(
    const T& left,
    const T& right)
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
    const std::string& right)
{
    return left.compare(right) <=> 0;
}

std::weak_ordering KeyComparer::CompareValues(
    const Message& left,
    const Message& right)
{
    return Compare(
        &left,
        &right);
}

auto GetFieldValue(
    const Message* message,
    const Reflection* reflection,
    const FieldDescriptor* fieldDescriptor,
    KeyComparer::compare_tag<int32>)
{
    return reflection->GetInt32(
        *message,
        fieldDescriptor);
}

auto GetFieldValue(
    const Message* message,
    const Reflection* reflection,
    const FieldDescriptor* fieldDescriptor,
    KeyComparer::compare_tag<uint32>)
{
    return reflection->GetUInt32(
        *message,
        fieldDescriptor);
}

auto GetFieldValue(
    const Message* message,
    const Reflection* reflection,
    const FieldDescriptor* fieldDescriptor,
    KeyComparer::compare_tag<int64>)
{
    return reflection->GetInt64(
        *message,
        fieldDescriptor);
}

auto GetFieldValue(
    const Message* message,
    const Reflection* reflection,
    const FieldDescriptor* fieldDescriptor,
    KeyComparer::compare_tag<uint64>)
{
    return reflection->GetUInt64(
        *message,
        fieldDescriptor);
}

auto GetFieldValue(
    const Message* message,
    const Reflection* reflection,
    const FieldDescriptor* fieldDescriptor,
    KeyComparer::compare_tag<double>)
{
    return reflection->GetDouble(
        *message,
        fieldDescriptor);
}

auto GetFieldValue(
    const Message* message,
    const Reflection* reflection,
    const FieldDescriptor* fieldDescriptor,
    KeyComparer::compare_tag<float>)
{
    return reflection->GetFloat(
        *message,
        fieldDescriptor);
}

auto GetFieldValue(
    const Message* message,
    const Reflection* reflection,
    const FieldDescriptor* fieldDescriptor,
    KeyComparer::compare_tag<bool>)
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
    compare_tag<T> tag)
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
    compare_tag<string> tag)
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
    compare_tag<Message> tag)
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
    compare_tag<T> tag)
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
    compare_tag<T> tag)
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

}