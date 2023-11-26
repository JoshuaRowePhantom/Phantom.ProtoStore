#include "Resources.h"

#include<cmrc/cmrc.hpp>

CMRC_DECLARE(Phantom::ProtoStore::Resources);

namespace Phantom::ProtoStore
{
const FlatBuffersSchemasImpl& FlatBuffersSchemas()
{
    static FlatBuffersSchemas_SchemasImpl schemas =
    {
.ReflectionSchema
= flatbuffers::GetRoot<reflection::Schema>(
    cmrc::Phantom::ProtoStore::Resources::get_filesystem().open("reflection-phantom-protostore.bfbs").begin()),

.ProtoStoreSchema
= flatbuffers::GetRoot<reflection::Schema>(
    cmrc::Phantom::ProtoStore::Resources::get_filesystem().open("ProtoStore.bfbs").begin()),

.ProtoStoreInternalSchema
= flatbuffers::GetRoot<reflection::Schema>(
    cmrc::Phantom::ProtoStore::Resources::get_filesystem().open("ProtoStoreInternal.bfbs").begin()),
    };

    static FlatBuffersSchemas_ObjectsImpl objects =
    {
.ReflectionSchema_Schema
= schemas.ReflectionSchema->objects()->LookupByKey("reflection.Schema"),

.ReflectionSchema_Object
= schemas.ReflectionSchema->objects()->LookupByKey("reflection.Object"),

.ReflectionSchema_Field
= schemas.ReflectionSchema->objects()->LookupByKey("reflection.Field"),

.ReflectionSchema_Type
= schemas.ReflectionSchema->objects()->LookupByKey("reflection.Type"),


.ExtentName_Object
= schemas.ProtoStoreSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.ExtentName"),

.IndexHeaderExtentName_Object
= schemas.ProtoStoreSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.IndexHeaderExtentName"),

.Metadata_Object
= schemas.ProtoStoreSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.Metadata"),

.IndexesByNumberKey_Object
= schemas.ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.IndexesByNumberKey"),
.IndexesByNumberValue_Object
= schemas.ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.IndexesByNumberValue"),

.IndexesByNameKey_Object
= schemas.ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.IndexesByNameKey"),

.IndexesByNameValue_Object
= schemas.ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.IndexesByNameValue"),

.PartitionsKey_Object
= schemas.ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.PartitionsKey"),

.PartitionsValue_Object
= schemas.ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.PartitionsValue"),

.MergesKey_Object
= schemas.ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.MergesKey"),

.MergesValue_Object
= schemas.ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.MergesValue"),

.MergeProgressKey_Object
= schemas.ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.MergeProgressKey"),

.MergeProgressValue_Object
= schemas.ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.MergeProgressValue"),

.PartitionMessage_Object
= schemas.ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.PartitionMessage"),

.DistributedTransactionsKey_Object
= schemas.ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.DistributedTransactionsKey"),

.DistributedTransactionsValue_Object
= schemas.ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.DistributedTransactionsValue"),

.DistributedTransactionReferencesKey_Object
= schemas.ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.DistributedTransactionReferencesKey"),

.DistributedTransactionReferencesValue_Object
= schemas.ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.DistributedTransactionReferencesValue"),

    };

    static FlatBuffersSchemasImpl flatBuffersSchemas =
    {
        schemas,
        objects,
    };

    return flatBuffersSchemas;
}

const FlatBuffersSchemas_ComparersImpl& FlatBuffersSchemas_Comparers()
{
    static auto& schemas = FlatBuffersSchemas();
    static auto& objects = FlatBuffersSchemas();

    static FlatBuffersSchemas_ComparersImpl comparersOwning =
    {
.ReflectionSchema_Schema_Comparers
= FlatBuffersObjectSchema{ schemas.ReflectionSchema, objects.ReflectionSchema_Schema }.MakeComparers(),


.ReflectionSchema_Object_Comparers
= FlatBuffersObjectSchema{ schemas.ReflectionSchema, objects.ReflectionSchema_Object }.MakeComparers(),

.ReflectionSchema_Field_Comparers
= FlatBuffersObjectSchema{ schemas.ReflectionSchema, objects.ReflectionSchema_Field }.MakeComparers(),

.ReflectionSchema_Type_Comparers
= FlatBuffersObjectSchema{ schemas.ReflectionSchema, objects.ReflectionSchema_Type }.MakeComparers(),


.ExtentName_Comparers
= FlatBuffersObjectSchema{ schemas.ProtoStoreSchema, objects.ExtentName_Object }.MakeComparers(),

.IndexHeaderExtentName_Comparers
= FlatBuffersObjectSchema{ schemas.ProtoStoreSchema, objects.IndexHeaderExtentName_Object }.MakeComparers(),

.MergesKey_Comparers
= FlatBuffersObjectSchema{ schemas.ProtoStoreInternalSchema, objects.MergesKey_Object }.MakeComparers(),

.MergesValue_Comparers
= FlatBuffersObjectSchema{ schemas.ProtoStoreInternalSchema, objects.MergesKey_Object }.MakeComparers(),

    };

    static FlatBuffersSchemas_ComparersImpl comparersUnowning =
    {
.ReflectionSchema_Schema_Comparers = comparersOwning.ReflectionSchema_Schema_Comparers.MakeUnowningCopy(),
.ReflectionSchema_Object_Comparers = comparersOwning.ReflectionSchema_Object_Comparers.MakeUnowningCopy(),
.ReflectionSchema_Field_Comparers = comparersOwning.ReflectionSchema_Field_Comparers.MakeUnowningCopy(),
.ReflectionSchema_Type_Comparers = comparersOwning.ReflectionSchema_Type_Comparers.MakeUnowningCopy(),
.ExtentName_Comparers = comparersOwning.ExtentName_Comparers.MakeUnowningCopy(),
.IndexHeaderExtentName_Comparers = comparersOwning.IndexHeaderExtentName_Comparers.MakeUnowningCopy(),
.MergesKey_Comparers = comparersOwning.MergesKey_Comparers.MakeUnowningCopy(),
.MergesValue_Comparers = comparersOwning.MergesValue_Comparers.MakeUnowningCopy(),
    };

    return comparersUnowning;
}


}