#include "Resources.h"

#include<cmrc/cmrc.hpp>

CMRC_DECLARE(Phantom::ProtoStore::Test::Resources);

namespace Phantom::ProtoStore
{

const reflection::Schema* FlatBuffersTestSchemas::TestSchema =
    flatbuffers::GetRoot<reflection::Schema>(
        cmrc::Phantom::ProtoStore::Test::Resources::get_filesystem().open("ProtoStoreTest.bfbs").begin());
const reflection::Object* FlatBuffersTestSchemas::TestStringKeySchema =
    TestSchema->objects()->LookupByKey("Phantom.ProtoStore.Test.FlatBuffers.TestStringKey");
const reflection::Object* FlatBuffersTestSchemas::TestStringValueSchema =
    TestSchema->objects()->LookupByKey("Phantom.ProtoStore.Test.FlatBuffers.TestStringValue");

}