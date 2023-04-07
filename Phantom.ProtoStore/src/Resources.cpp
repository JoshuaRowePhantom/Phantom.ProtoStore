#include "Resources.h"

#include<cmrc/cmrc.hpp>

CMRC_DECLARE(Phantom::ProtoStore::Resources);

namespace Phantom::ProtoStore::FlatBuffersSchemas
{

const reflection::Schema* const ReflectionSchema
= flatbuffers::GetRoot<reflection::Schema>(
    cmrc::Phantom::ProtoStore::Resources::get_filesystem().open("reflection-phantom-protostore.bfbs").begin());

const reflection::Object* const ReflectionSchema_Schema
= ReflectionSchema->objects()->LookupByKey("reflection.Schema");

const reflection::Object* const ReflectionSchema_Object
= ReflectionSchema->objects()->LookupByKey("reflection.Object");

const reflection::Object* const ReflectionSchema_Field
= ReflectionSchema->objects()->LookupByKey("reflection.Field");

const reflection::Object* const ReflectionSchema_Type
= ReflectionSchema->objects()->LookupByKey("reflection.Type");


const ProtoValueComparers ReflectionSchema_Schema_Comparers_Owning
= FlatBuffersObjectSchema{ ReflectionSchema, ReflectionSchema_Schema }.MakeComparers();

const ProtoValueComparers ReflectionSchema_Schema_Comparers
= ReflectionSchema_Schema_Comparers_Owning.MakeUnowningCopy();


const ProtoValueComparers ReflectionSchema_Object_Comparers_Owning
= FlatBuffersObjectSchema{ ReflectionSchema, ReflectionSchema_Object }.MakeComparers();

const ProtoValueComparers ReflectionSchema_Object_Comparers
= ReflectionSchema_Object_Comparers_Owning.MakeUnowningCopy();

const ProtoValueComparers ReflectionSchema_Field_Comparers_Owning
= FlatBuffersObjectSchema{ ReflectionSchema, ReflectionSchema_Field }.MakeComparers();

const ProtoValueComparers ReflectionSchema_Field_Comparers
= ReflectionSchema_Field_Comparers_Owning.MakeUnowningCopy();

const ProtoValueComparers ReflectionSchema_Type_Comparers_Owning
= FlatBuffersObjectSchema{ ReflectionSchema, ReflectionSchema_Type }.MakeComparers();

const ProtoValueComparers ReflectionSchema_Type_Comparers
= ReflectionSchema_Type_Comparers_Owning.MakeUnowningCopy();


const reflection::Schema* const ProtoStoreInternalSchema
= flatbuffers::GetRoot<reflection::Schema>(
    cmrc::Phantom::ProtoStore::Resources::get_filesystem().open("ProtoStoreInternal.bfbs").begin());

const reflection::Object* const IndexesByNumberKey_Object
= ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.IndexesByNumberKey");
const reflection::Object* const IndexesByNumberValue_Object
= ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.IndexesByNumberValue");

const reflection::Object* const IndexesByNameKey_Object
= ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.IndexesByNameKey");

const reflection::Object* const IndexesByNameValue_Object
= ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.IndexesByNameValue");

const reflection::Object* const PartitionsKey_Object
= ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.PartitionsKey");

const reflection::Object* const PartitionsValue_Object
= ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.PartitionsValue");

const reflection::Object* const MergesKey_Object
= ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.MergesKey");

const reflection::Object* const MergesValue_Object
= ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.MergesValue");

const reflection::Object* const MergeProgressKey_Object
= ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.MergeProgressKey");

const reflection::Object* const MergeProgressValue_Object
= ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.MergeProgressValue");

const reflection::Object* const PartitionMessage_Object
= ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.PartitionMessage");

const reflection::Object* const DistributedTransactionsKey_Object
= ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.DistributedTransactionsKey");

const reflection::Object* const DistributedTransactionsValue_Object
= ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.DistributedTransactionsValue");

const reflection::Object* const DistributedTransactionReferencesKey_Object
= ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.DistributedTransactionReferencesKey");

const reflection::Object* const DistributedTransactionReferencesValue_Object
= ProtoStoreInternalSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.DistributedTransactionReferencesValue");

const reflection::Schema* const ProtoStoreSchema
= flatbuffers::GetRoot<reflection::Schema>(
    cmrc::Phantom::ProtoStore::Resources::get_filesystem().open("ProtoStore.bfbs").begin());

const reflection::Object* const ExtentName_Object
= ProtoStoreSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.ExtentName");

const reflection::Object* const IndexHeaderExtentName_Object
= ProtoStoreSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.IndexHeaderExtentName");

const ProtoValueComparers ExtentName_Comparers_Owning
= FlatBuffersObjectSchema{ ProtoStoreSchema, ExtentName_Object }.MakeComparers();

const ProtoValueComparers ExtentName_Comparers
= ExtentName_Comparers_Owning.MakeUnowningCopy();

const ProtoValueComparers IndexHeaderExtentName_Comparers_Owning
= FlatBuffersObjectSchema{ ProtoStoreSchema, IndexHeaderExtentName_Object }.MakeComparers();

const ProtoValueComparers IndexHeaderExtentName_Comparers
= IndexHeaderExtentName_Comparers_Owning.MakeUnowningCopy();

const ProtoValueComparers MergesKey_Comparers_Owning
= FlatBuffersObjectSchema{ ProtoStoreInternalSchema, MergesKey_Object }.MakeComparers();

const ProtoValueComparers MergesKey_Comparers
= MergesKey_Comparers_Owning.MakeUnowningCopy();

const ProtoValueComparers MergesValue_Comparers_Owning
= FlatBuffersObjectSchema{ ProtoStoreInternalSchema, MergesKey_Object }.MakeComparers();

const ProtoValueComparers MergesValue_Comparers
= MergesValue_Comparers_Owning.MakeUnowningCopy();

}