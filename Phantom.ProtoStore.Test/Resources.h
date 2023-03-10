#include <flatbuffers/reflection.h>

namespace Phantom::ProtoStore
{

struct FlatBuffersTestSchemas
{
    static const reflection::Schema* TestSchema;
    static const reflection::Object* TestStringKeySchema;
    static const reflection::Object* TestStringValueSchema;
};

}