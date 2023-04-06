#include "Resources.h"

#include<cmrc/cmrc.hpp>

CMRC_DECLARE(Phantom::ProtoStore::Test::Resources);

namespace Phantom::ProtoStore
{

const reflection::Schema* FlatBuffersTestSchemas::TestSchema 
= flatbuffers::GetRoot<reflection::Schema>(
    cmrc::Phantom::ProtoStore::Test::Resources::get_filesystem().open("ProtoStoreTest.bfbs").begin());

const reflection::Object* FlatBuffersTestSchemas::Test_TestKey_Object
= TestSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.TestKey");

const reflection::Object* FlatBuffersTestSchemas::Test_FlatStringKey_Object
= TestSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.FlatStringKey");

const reflection::Object* FlatBuffersTestSchemas::Test_FlatStringValue_Object
= TestSchema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.FlatStringValue");

}