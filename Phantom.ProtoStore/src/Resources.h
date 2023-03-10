#include <flatbuffers/reflection.h>

namespace Phantom::ProtoStore
{

struct FlatBuffersSchemas
{
    static const reflection::Schema* ReflectionSchema;
    static const reflection::Object* ReflectionObjectSchema;
    static const reflection::Schema* ProtoStoreInternalSchema;
};

}