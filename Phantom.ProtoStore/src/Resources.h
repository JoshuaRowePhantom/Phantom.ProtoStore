#pragma once

#include <flatbuffers/reflection.h>

namespace Phantom::ProtoStore
{

namespace FlatBuffersSchemas
{
    extern const reflection::Schema* const ReflectionSchema;
    extern const reflection::Object* const ReflectionSchema_Schema;
    extern const reflection::Object* const ReflectionSchema_Object;
    extern const reflection::Object* const ReflectionSchema_Field;
    extern const reflection::Object* const ReflectionSchema_Type;

    extern const ProtoValueComparers ReflectionSchema_SchemaComparers;
    extern const ProtoValueComparers ReflectionSchema_ObjectComparers;
    extern const ProtoValueComparers ReflectionSchema_FieldComparers;
    extern const ProtoValueComparers ReflectionSchema_TypeComparers;

    extern const reflection::Schema* const ProtoStoreInternalSchema;

    extern const reflection::Object* const IndexesByNumberKey_Object;
    extern const reflection::Object* const IndexesByNumberValue_Object;

    extern const reflection::Object* const IndexesByNameKey_Object;
    extern const reflection::Object* const IndexesByNameValue_Object;
    
    extern const reflection::Object* const PartitionsKey_Object;
    extern const reflection::Object* const PartitionsValue_Object;
    
    extern const reflection::Object* const MergesKey_Object;
    extern const reflection::Object* const MergesValue_Object;
    
    extern const reflection::Object* const MergeProgressKey_Object;
    extern const reflection::Object* const MergeProgressValue_Object;

    extern const ProtoValueComparers PartitionsKeyComparers;
    extern const ProtoValueComparers IndexHeaderExtentNameComparers;
    extern const ProtoValueComparers MergesKeyComparers;
    
};

}