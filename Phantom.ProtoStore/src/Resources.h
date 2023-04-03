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

    extern const ProtoValueComparers ReflectionSchema_Schema_Comparers;
    extern const ProtoValueComparers ReflectionSchema_Object_Comparers;
    extern const ProtoValueComparers ReflectionSchema_Field_Comparers;
    extern const ProtoValueComparers ReflectionSchema_Type_Comparers;

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
    
    extern const reflection::Object* const PartitionMessage_Object;

    extern const ProtoValueComparers PartitionsKey_Comparers;
    extern const ProtoValueComparers PartitionsValue_Comparers;
    extern const ProtoValueComparers IndexHeaderExtentName_Comparers;
    extern const ProtoValueComparers MergesKey_Comparers;
    extern const ProtoValueComparers MergesValue_Comparers;
    
};

}