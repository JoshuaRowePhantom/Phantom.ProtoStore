#include "Resources.h"

#include<cmrc/cmrc.hpp>

CMRC_DECLARE(Phantom::ProtoStore::Resources);

namespace Phantom::ProtoStore
{

const reflection::Schema* FlatBuffersSchemas::ReflectionSchema =
    flatbuffers::GetRoot<reflection::Schema>(
        cmrc::Phantom::ProtoStore::Resources::get_filesystem().open("reflection-phantom-protostore.bfbs").begin());

const reflection::Object* FlatBuffersSchemas::ReflectionObjectSchema =
    ReflectionSchema->objects()->LookupByKey("reflection.Object");

const reflection::Schema* FlatBuffersSchemas::ProtoStoreInternalSchema =
    flatbuffers::GetRoot<reflection::Schema>(
        cmrc::Phantom::ProtoStore::Resources::get_filesystem().open("ProtoStoreInternal.fbs").begin());

}