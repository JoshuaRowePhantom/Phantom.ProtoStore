#include <flatbuffers/reflection.h>

namespace Phantom::ProtoStore
{

namespace FlatBuffersTestSchemas
{
extern const reflection::Schema* TestSchema;
extern const reflection::Object* Test_TestKey_Object;
extern const reflection::Object* Test_FlatStringKey_Object;
extern const reflection::Object* Test_FlatStringValue_Object;

extern const ProtoValueComparers Test_FlatStringKey_Comparers;
extern const ProtoValueComparers Test_FlatStringValue_Comparers;
};

}