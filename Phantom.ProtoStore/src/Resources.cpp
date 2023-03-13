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

const reflection::Schema* const ProtoStoreSchema
= flatbuffers::GetRoot<reflection::Schema>(
    cmrc::Phantom::ProtoStore::Resources::get_filesystem().open("ProtoStore.bfbs").begin());

const reflection::Object* const ExtentName_Object
= ProtoStoreSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.ExtentName");

const reflection::Object* const IndexHeaderExtentName_Object
= ProtoStoreSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.IndexHeaderExtentName");

const ProtoValueComparers ExtentNameComparers_Owning
= FlatBuffersObjectSchema{ ProtoStoreSchema, ExtentName_Object }.MakeComparers();

const ProtoValueComparers ExtentNameComparers
= ExtentNameComparers_Owning.MakeUnowningCopy();

const ProtoValueComparers IndexHeaderExtentNameComparers_Owning
= FlatBuffersObjectSchema{ ProtoStoreSchema, IndexHeaderExtentName_Object }.MakeComparers();

const ProtoValueComparers IndexHeaderExtentNameComparers
= IndexHeaderExtentNameComparers_Owning.MakeUnowningCopy();

const ProtoValueComparers MergesKeyComparers_Owning
= FlatBuffersObjectSchema{ ProtoStoreInternalSchema, MergesKey_Object }.MakeComparers();

const ProtoValueComparers MergesKeyComparers
= MergesKeyComparers_Owning.MakeUnowningCopy();

}