#include <flatbuffers/reflection.h>

namespace Phantom::ProtoStore
{

namespace FlatBuffersTestSchemas
{
extern const reflection::Schema* TestSchema;
extern const reflection::Object* TestTestKeySchema;
extern const reflection::Object* TestFlatStringKeySchema;
extern const reflection::Object* TestFlatStringValueSchema;
};

}