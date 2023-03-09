#include <variant>
#include <google/protobuf/descriptor.h>
#include <flatbuffers/reflection.h>

namespace Phantom::ProtoStore
{

struct ProtocolBuffersObjectSchema
{
    const google::protobuf::Descriptor* MessageDescriptor;

    friend bool operator==(
        const ProtocolBuffersObjectSchema&,
        const ProtocolBuffersObjectSchema&
        ) = default;
};

struct FlatBuffersObjectSchema
{
    const reflection::Schema* Schema;
    const reflection::Object* Object;

    friend bool operator==(
        const FlatBuffersObjectSchema&,
        const FlatBuffersObjectSchema&
        ) = default;
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

struct Schema
{
    KeySchema KeySchema;
    ValueSchema ValueSchema;

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
};

}
