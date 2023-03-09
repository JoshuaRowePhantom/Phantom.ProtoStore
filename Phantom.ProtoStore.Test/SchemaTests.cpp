#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/Schema.h"
#include "ProtoStoreInternal.pb.h"
#include "ProtoStoreTest.pb.h"

namespace Phantom::ProtoStore
{

TEST(SchemaTests, Can_round_trip_to_message_descriptor_with_compiled_class)
{
    Serialization::SchemaDescription messageDescription;

    Schema::MakeSchemaDescription(
        messageDescription,
        TestKey::descriptor());

    EXPECT_EQ(
        "Phantom.ProtoStore.TestKey", 
        messageDescription.protocolbuffersdescription().messagedescription().messagename());

    auto messageFactory = Schema::MakeMessageFactory(
        messageDescription.protocolbuffersdescription().messagedescription());

    EXPECT_EQ(
        TestKey::descriptor(),
        messageFactory->GetPrototype()->GetDescriptor());
}

}