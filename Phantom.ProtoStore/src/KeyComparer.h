#pragma once

#include "StandardTypes.h"
#include <compare>
#include "ProtoStore.pb.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/dynamic_message.h"

namespace Phantom::ProtoStore
{

class KeySchemaDescription;

class KeyComparer
{
public:
    template<typename T>
    struct compare_tag {};

private:
    KeySchemaDescription m_keySchemaDescription;
    google::protobuf::DescriptorPool m_descriptorPool;
    google::protobuf::DynamicMessageFactory m_dynamicMessageFactory;
    const google::protobuf::Descriptor* m_messageDescriptor;

public:
    KeyComparer(
        KeySchemaDescription keySchemaDescription);

    std::weak_ordering Compare(
        std::span<const google::protobuf::uint8> value1,
        std::span<const google::protobuf::uint8> value2);


    std::weak_ordering Compare(
        const google::protobuf::Message* value1,
        const google::protobuf::Message* value2);

    std::weak_ordering KeyComparer::CompareFields(
        const google::protobuf::Message* left,
        const google::protobuf::Message* right,
        const google::protobuf::Reflection* leftReflection,
        const google::protobuf::Reflection* rightReflection,
        const google::protobuf::FieldDescriptor* leftFieldDescriptor,
        const google::protobuf::FieldDescriptor* rightFieldDescriptor);

    template<typename T>
    using ReflectionFieldGetter = T(google::protobuf::Reflection::*)(
        const google::protobuf::Message& message,
        const google::protobuf::FieldDescriptor* fieldDescriptor
        ) const;

    template<typename T>
    using ReflectionRepeatedFieldGetter = T(google::protobuf::Reflection::*)(
        const google::protobuf::Message& message,
        const google::protobuf::FieldDescriptor* fieldDescriptor,
        int index
        ) const;

    template<typename T> 
    std::weak_ordering CompareFields(
        const google::protobuf::Message* left,
        const google::protobuf::Message* right,
        const google::protobuf::Reflection* leftReflection,
        const google::protobuf::Reflection* rightReflection,
        const google::protobuf::FieldDescriptor* leftFieldDescriptor,
        const google::protobuf::FieldDescriptor* rightFieldDescriptor,
        compare_tag<T> = compare_tag<T>());

    std::weak_ordering KeyComparer::CompareNonRepeatedFields(
        const google::protobuf::Message* left,
        const google::protobuf::Message* right,
        const google::protobuf::Reflection* leftReflection,
        const google::protobuf::Reflection* rightReflection,
        const google::protobuf::FieldDescriptor* leftFieldDescriptor,
        const google::protobuf::FieldDescriptor* rightFieldDescriptor,
        compare_tag<std::string> tag);

    template<typename T>
    std::weak_ordering CompareNonRepeatedFields(
        const google::protobuf::Message* left,
        const google::protobuf::Message* right,
        const google::protobuf::Reflection* leftReflection,
        const google::protobuf::Reflection* rightReflection,
        const google::protobuf::FieldDescriptor* leftFieldDescriptor,
        const google::protobuf::FieldDescriptor* rightFieldDescriptor,
        compare_tag<T> = compare_tag<T>());

    template<typename T>
    std::weak_ordering CompareRepeatedFields(
        const google::protobuf::Message* left,
        const google::protobuf::Message* right,
        const google::protobuf::Reflection* leftReflection,
        const google::protobuf::Reflection* rightReflection,
        const google::protobuf::FieldDescriptor* leftFieldDescriptor,
        const google::protobuf::FieldDescriptor* rightFieldDescriptor,
        compare_tag<T> = compare_tag<T>());
};

}