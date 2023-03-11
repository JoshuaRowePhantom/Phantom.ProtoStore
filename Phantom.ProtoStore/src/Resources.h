#include <flatbuffers/reflection.h>

namespace Phantom::ProtoStore
{

struct FlatBuffersSchemas
{
    static const reflection::Schema* ReflectionSchema;
    static const reflection::Object* ReflectionSchema_Schema;
    static const reflection::Object* ReflectionSchema_Object;
    static const reflection::Schema* ProtoStoreInternalSchema;
};

}