#include "Resources.h"

#include<cmrc/cmrc.hpp>

CMRC_DECLARE(Phantom::ProtoStore::Test::Resources);

namespace Phantom::ProtoStore::FlatBuffersTestSchemas
{

const reflection::Schema* TestSchema 
= flatbuffers::GetRoot<reflection::Schema>(
    cmrc::Phantom::ProtoStore::Test::Resources::get_filesystem().open("ProtoStoreTest.bfbs").begin());

const reflection::Object* Test_TestKey_Object
= TestSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.TestKey");

const reflection::Object* Test_FlatStringKey_Object
= TestSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.FlatStringKey");

const reflection::Object* Test_FlatStringValue_Object
= TestSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.FlatStringValue");


const ProtoValueComparers Test_FlatStringKey_Comparers_Owning
= FlatBuffersObjectSchema{ TestSchema, Test_FlatStringKey_Object }.MakeComparers();

const ProtoValueComparers Test_FlatStringKey_Comparers
= Test_FlatStringKey_Comparers_Owning.MakeUnowningCopy();


const ProtoValueComparers Test_FlatStringValue_Comparers_Owning
= FlatBuffersObjectSchema{ TestSchema, Test_FlatStringValue_Object }.MakeComparers();

const ProtoValueComparers Test_FlatStringValue_Comparers
= Test_FlatStringValue_Comparers_Owning.MakeUnowningCopy();

}