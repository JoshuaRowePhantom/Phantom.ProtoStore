#include <flatbuffers/reflection.h>

namespace Phantom::ProtoStore
{

struct FlatBuffersSchemas
{
    static const reflection::Schema* ReflectionSchema;
    static const reflection::Object* ReflectionSchema_Schema;
    static const reflection::Object* ReflectionSchema_Object;
    
    static const reflection::Schema* ProtoStoreInternalSchema;
    static const reflection::Object* IndexesByNameKey_Object;
    static const reflection::Object* IndexesByNameValue_Object;
};

}