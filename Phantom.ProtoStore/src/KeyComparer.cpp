#include "StandardTypes.h"
#include "KeyComparer.h"
#include "ProtoStore.pb.h"
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <stack>
#include <compare>

namespace Phantom::ProtoStore
{

using namespace google::protobuf;

KeyComparer::KeyComparer(
    const Descriptor* messageDescriptor
)
    :
    m_messageDescriptor(
        messageDescriptor)
{
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
        auto comparisonResult = CompareFields(
            left,
            right,
            leftReflection,
            rightReflection,
            leftDescriptor->field(fieldIndex),
            rightDescriptor->field(fieldIndex));

        if (comparisonResult != std::weak_ordering::equivalent)
        {
            return comparisonResult;
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
    auto compareFields = [=]<typename T>(compare_tag<T>) -> auto
    {
        return CompareFields<T>(
            left,
            right,
            leftReflection,
            rightReflection,
            leftFieldDescriptor,
            rightFieldDescriptor);
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

template<typename T>
requires std::is_same_v<decltype(std::declval<T>() <=> std::declval<T>()), std::weak_ordering>
std::weak_ordering CompareValues(
    const T& left,
    const T& right)
{
    return left <=> right;
}

template<typename T>
requires std::is_same_v<decltype(std::declval<T>() <=> std::declval<T>()), std::strong_ordering>
std::weak_ordering CompareValues(
    const T& left,
    const T& right)
{
    return left <=> right;
}

template<typename T>
requires std::is_same_v<decltype(std::declval<T>() <=> std::declval<T>()), std::partial_ordering>
std::weak_ordering CompareValues(
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

std::weak_ordering CompareValues(
    const std::string& left,
    const std::string& right)
{
    return left.compare(right) <=> 0;
}

std::weak_ordering CompareValues(
    const Message& left,
    const Message& right)
{
    KeyComparer keyComparer(
        left.GetDescriptor());

    return keyComparer.Compare(
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

    return CompareValues(leftString, rightString);
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

    return CompareValues(leftMessage, rightMessage);
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

    return CompareValues(leftValue, rightValue);
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