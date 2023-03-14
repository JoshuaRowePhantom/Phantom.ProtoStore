#include <flatbuffers/reflection.h>

namespace Phantom::ProtoStore
{

namespace FlatBuffersTestSchemas
{
extern const reflection::Schema* TestSchema;
extern const reflection::Object* Test_TestKey_Object;
extern const reflection::Object* TestFlatStringKeySchema;
extern const reflection::Object* TestFlatStringValueSchema;
};

}