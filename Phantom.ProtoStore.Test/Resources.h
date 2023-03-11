#include <flatbuffers/reflection.h>

namespace Phantom::ProtoStore
{

struct FlatBuffersTestSchemas
{
    static const reflection::Schema* TestSchema;
    static const reflection::Object* TestFlatStringKeySchema;
    static const reflection::Object* TestFlatStringValueSchema;
};

}