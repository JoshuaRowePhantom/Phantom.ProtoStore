#pragma once

#include "StandardTypes.h"
#include <compare>
#include <concepts>
#include "Phantom.ProtoStore/ProtoStore.pb.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/dynamic_message.h"

namespace Phantom::ProtoStore
{

class KeySchemaDescription;

template<typename T, typename O>
concept IsOrderedBy = requires (T t)
{
    { t <=> t } -> std::same_as<O>;
};

class KeyComparer
{
public:
    template<typename T>
    struct compare_tag {};

private:
    const google::protobuf::Descriptor* m_messageDescriptor;

    template<IsOrderedBy<std::weak_ordering> T>
    std::weak_ordering CompareValues(
        const T& left,
        const T& right    ) const;

    template<IsOrderedBy<std::strong_ordering> T>
        std::weak_ordering CompareValues(
            const T& left,
            const T& right    ) const;

    template<IsOrderedBy<std::partial_ordering> T>
        std::weak_ordering CompareValues(
            const T& left,
            const T& right    ) const;
    
    std::weak_ordering KeyComparer::CompareValues(
        const std::string& left,
        const std::string& right    ) const;

    std::weak_ordering KeyComparer::CompareValues(
        const google::protobuf::Message& left,
        const google::protobuf::Message& right    ) const;

    std::weak_ordering KeyComparer::CompareFields(
        const google::protobuf::Message* left,
        const google::protobuf::Message* right,
        const google::protobuf::Reflection* leftReflection,
        const google::protobuf::Reflection* rightReflection,
        const google::protobuf::FieldDescriptor* leftFieldDescriptor,
        const google::protobuf::FieldDescriptor* rightFieldDescriptor    ) const;

    template<typename T>
    std::weak_ordering CompareFields(
        const google::protobuf::Message* left,
        const google::protobuf::Message* right,
        const google::protobuf::Reflection* leftReflection,
        const google::protobuf::Reflection* rightReflection,
        const google::protobuf::FieldDescriptor* leftFieldDescriptor,
        const google::protobuf::FieldDescriptor* rightFieldDescriptor,
        compare_tag<T> = compare_tag<T>()    ) const;

    std::weak_ordering KeyComparer::CompareNonRepeatedFields(
        const google::protobuf::Message* left,
        const google::protobuf::Message* right,
        const google::protobuf::Reflection* leftReflection,
        const google::protobuf::Reflection* rightReflection,
        const google::protobuf::FieldDescriptor* leftFieldDescriptor,
        const google::protobuf::FieldDescriptor* rightFieldDescriptor,
        compare_tag<std::string> tag    ) const;

    std::weak_ordering KeyComparer::CompareNonRepeatedFields(
        const google::protobuf::Message* left,
        const google::protobuf::Message* right,
        const google::protobuf::Reflection* leftReflection,
        const google::protobuf::Reflection* rightReflection,
        const google::protobuf::FieldDescriptor* leftFieldDescriptor,
        const google::protobuf::FieldDescriptor* rightFieldDescriptor,
        compare_tag<google::protobuf::Message> tag    ) const;

    template<typename T>
    std::weak_ordering CompareNonRepeatedFields(
        const google::protobuf::Message* left,
        const google::protobuf::Message* right,
        const google::protobuf::Reflection* leftReflection,
        const google::protobuf::Reflection* rightReflection,
        const google::protobuf::FieldDescriptor* leftFieldDescriptor,
        const google::protobuf::FieldDescriptor* rightFieldDescriptor,
        compare_tag<T> = compare_tag<T>()    ) const;

    template<typename T>
    std::weak_ordering CompareRepeatedFields(
        const google::protobuf::Message* left,
        const google::protobuf::Message* right,
        const google::protobuf::Reflection* leftReflection,
        const google::protobuf::Reflection* rightReflection,
        const google::protobuf::FieldDescriptor* leftFieldDescriptor,
        const google::protobuf::FieldDescriptor* rightFieldDescriptor,
        compare_tag<T> = compare_tag<T>()    ) const;

    std::weak_ordering ApplySortOrder(
        SortOrder sortOrder,
        std::weak_ordering value    ) const;

public:
    KeyComparer(
        const google::protobuf::Descriptor* messageDescriptor);

    std::weak_ordering Compare(
        const google::protobuf::Message* value1,
        const google::protobuf::Message* value2    ) const;

    std::weak_ordering operator()(
        const google::protobuf::Message* value1,
        const google::protobuf::Message* value2
    ) const;
};

}