#pragma once

#include <flatbuffers/reflection.h>

namespace Phantom::ProtoStore
{

struct FlatBuffersSchemas_SchemasImpl
{
    const reflection::Schema* ReflectionSchema;
    const reflection::Schema* ProtoStoreSchema;
    const reflection::Schema* ProtoStoreInternalSchema;
};

struct FlatBuffersSchemas_ObjectsImpl
{
    const reflection::Object* ReflectionSchema_Schema;
    const reflection::Object* ReflectionSchema_Object;
    const reflection::Object* ReflectionSchema_Field;
    const reflection::Object* ReflectionSchema_Type;
    
    const reflection::Object* ExtentName_Object;
    const reflection::Object* IndexHeaderExtentName_Object;
    const reflection::Object* Metadata_Object;

    const reflection::Object* IndexesByNumberKey_Object;
    const reflection::Object* IndexesByNumberValue_Object;

    const reflection::Object* IndexesByNameKey_Object;
    const reflection::Object* IndexesByNameValue_Object;

    const reflection::Object* PartitionsKey_Object;
    const reflection::Object* PartitionsValue_Object;

    const reflection::Object* MergesKey_Object;
    const reflection::Object* MergesValue_Object;

    const reflection::Object* MergeProgressKey_Object;
    const reflection::Object* MergeProgressValue_Object;

    const reflection::Object* PartitionMessage_Object;

    const reflection::Object* DistributedTransactionsKey_Object;
    const reflection::Object* DistributedTransactionsValue_Object;

    const reflection::Object* DistributedTransactionReferencesKey_Object;
    const reflection::Object* DistributedTransactionReferencesValue_Object;
};

struct FlatBuffersSchemas_ComparersImpl
{
    ProtoValueComparers ReflectionSchema_Schema_Comparers;
    ProtoValueComparers ReflectionSchema_Object_Comparers;
    ProtoValueComparers ReflectionSchema_Field_Comparers;
    ProtoValueComparers ReflectionSchema_Type_Comparers;

    ProtoValueComparers PartitionsKey_Comparers;
    ProtoValueComparers PartitionsValue_Comparers;

    ProtoValueComparers ExtentName_Comparers;
    ProtoValueComparers IndexHeaderExtentName_Comparers;
    ProtoValueComparers MergesKey_Comparers;
    ProtoValueComparers MergesValue_Comparers;
};

struct FlatBuffersSchemasImpl
    :
    FlatBuffersSchemas_SchemasImpl,
    FlatBuffersSchemas_ObjectsImpl
{
};

const FlatBuffersSchemasImpl& FlatBuffersSchemas();
const FlatBuffersSchemas_ComparersImpl& FlatBuffersSchemas_Comparers();

}