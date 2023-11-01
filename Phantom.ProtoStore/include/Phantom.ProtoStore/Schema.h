#pragma once

#include <functional>
#include <variant>
#include <google/protobuf/descriptor.h>
#include <flatbuffers/reflection.h>
#include "Phantom.ProtoStore/ProtoStore_generated.h"

namespace Phantom::ProtoStore
{

struct ProtocolBuffersObjectSchema
{
    const google::protobuf::Descriptor* MessageDescriptor;

    friend bool operator==(
        const ProtocolBuffersObjectSchema&,
        const ProtocolBuffersObjectSchema&
        ) = default;

    ProtoValueComparers MakeComparers() const;
};

struct FlatBuffersObjectSchema
{
    const reflection::Schema* Schema;
    const reflection::Object* Object;

    friend bool operator==(
        const FlatBuffersObjectSchema&,
        const FlatBuffersObjectSchema&
        ) = default;

    ProtoValueComparers MakeComparers() const;

    using FlatBuffersGraphEncodingOptions = FlatBuffers::FlatBuffersGraphEncodingOptions;
    using FlatBuffersStringEncodingOptions = FlatBuffers::FlatBuffersStringEncodingOptions;
    using FlatBuffersMessageEncodingOptions = FlatBuffers::FlatBuffersMessageEncodingOptions;

    FlatBuffersGraphEncodingOptions GraphEncodingOptions = FlatBuffersGraphEncodingOptions::NoDuplicateDetection;
    FlatBuffersStringEncodingOptions StringEncodingOptions = FlatBuffersStringEncodingOptions::ShareStrings;
    FlatBuffersMessageEncodingOptions MessageEncodingOptions = FlatBuffersMessageEncodingOptions::SerializedByteMessage;
};

struct ProtocolBuffersKeySchema
{
    ProtocolBuffersObjectSchema ObjectSchema;

    friend bool operator==(
        const ProtocolBuffersKeySchema&,
        const ProtocolBuffersKeySchema&
        ) = default;
};

struct ProtocolBuffersValueSchema
{
    ProtocolBuffersObjectSchema ObjectSchema;

    friend bool operator==(
        const ProtocolBuffersValueSchema&,
        const ProtocolBuffersValueSchema&
        ) = default;
};

struct FlatBuffersKeySchema
{
    FlatBuffersObjectSchema ObjectSchema;

    friend bool operator==(
        const FlatBuffersKeySchema&,
        const FlatBuffersKeySchema&
        ) = default;
};

struct FlatBuffersValueSchema
{
    FlatBuffersObjectSchema ObjectSchema;

    friend bool operator==(
        const FlatBuffersValueSchema&,
        const FlatBuffersValueSchema&
        ) = default;
};

struct KeySchema
{
    typedef std::variant<
        std::monostate,
        ProtocolBuffersKeySchema,
        FlatBuffersKeySchema
    > format_schema_type;

    format_schema_type FormatSchema;

    auto AsProtocolBuffersKeySchema(this auto& self) noexcept
    {
        return std::get_if<ProtocolBuffersKeySchema>(&self.FormatSchema);
    }

    auto AsFlatBuffersKeySchema(this auto& self) noexcept
    {
        return std::get_if<FlatBuffersKeySchema>(&self.FormatSchema);
    }

    bool IsProtocolBuffersSchema() const noexcept
    {
        return holds_alternative<ProtocolBuffersKeySchema>(FormatSchema);
    }

    bool IsFlatBuffersSchema() const noexcept
    {
        return holds_alternative<FlatBuffersKeySchema>(FormatSchema);
    }

    friend bool operator==(
        const KeySchema&,
        const KeySchema&
        ) = default;

    KeySchema() {}

    KeySchema(
        format_schema_type schema
    ) : FormatSchema(schema)
    {}

    KeySchema(
        const google::protobuf::Descriptor* messageDescriptor
    ) : FormatSchema
    {
        ProtocolBuffersKeySchema { ProtocolBuffersObjectSchema { messageDescriptor } }
    }
    {}

    KeySchema(
        const reflection::Schema* schema,
        const reflection::Object* object
    ) : FormatSchema
    {
        FlatBuffersKeySchema { FlatBuffersObjectSchema { schema, object }}
    }
    {}
};

struct ValueSchema
{
    typedef std::variant<
        std::monostate,
        ProtocolBuffersValueSchema,
        FlatBuffersValueSchema
    > format_schema_type;

    format_schema_type FormatSchema;

    auto AsProtocolBuffersValueSchema(this auto& self) noexcept
    {
        return get_if<ProtocolBuffersValueSchema>(&self.FormatSchema);
    }

    auto AsFlatBuffersValueSchema(this auto& self) noexcept
    {
        return get_if<FlatBuffersValueSchema>(&self.FormatSchema);
    }

    bool IsProtocolBuffersSchema() const noexcept
    {
        return holds_alternative<ProtocolBuffersValueSchema>(FormatSchema);
    }

    bool IsFlatBuffersSchema() const noexcept
    {
        return holds_alternative<FlatBuffersValueSchema>(FormatSchema);
    }

    friend bool operator==(
        const ValueSchema&,
        const ValueSchema&
        ) = default;

    ValueSchema() {}

    ValueSchema(
        format_schema_type schema
    ) : FormatSchema(schema)
    {}

    ValueSchema(
        const google::protobuf::Descriptor* messageDescriptor
    ) : FormatSchema
    {
        ProtocolBuffersValueSchema { ProtocolBuffersObjectSchema { messageDescriptor } }
    }
    {}

    ValueSchema(
        const reflection::Schema* schema,
        const reflection::Object* object
    ) : FormatSchema
    {
        FlatBuffersValueSchema { FlatBuffersObjectSchema { schema, object }}
    }
    {}
};

enum class KeyToValueMultiplicity
{
    // The key is unique and maps to a single value.
    SingleValue,
    // The key is not unique, and maps to multiple values.
    // Writing a value to a key that already exists will append the value to the set of
    // values for the key. If the value already exists, no change is made.
    // Removing a value eliminates all copies of that value.
    MultiValue,
    // The key is unique and maps to no value.
    NoValue
};

struct Schema
{
    KeySchema KeySchema;
    ValueSchema ValueSchema;

    KeyToValueMultiplicity KeyToValueMultiplicity = KeyToValueMultiplicity::SingleValue;

    static Schema Make(
        Phantom::ProtoStore::KeySchema keySchema,
        Phantom::ProtoStore::ValueSchema valueSchema
    )
    {
        return Schema
        {
            keySchema,
            valueSchema
        };
    }

    friend bool operator==(
        const Schema&,
        const Schema&
        ) = default;

    ProtoValueComparers MakeKeyComparers() const;
    ProtoValueComparers MakeValueComparers() const;
};

}
