#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/Schema.h"
#include "src/ProtoStoreInternal.pb.h"
#include "ProtoStoreTest.pb.h"

namespace Phantom::ProtoStore
{

TEST(SchemaTests, Can_round_trip_to_message_descriptor_with_compiled_class)
{
    MessageDescription messageDescription;

    Schema::MakeMessageDescription(
        messageDescription,
        TestKey::descriptor());

    EXPECT_EQ("Phantom.ProtoStore.TestKey", messageDescription.messagename());

    auto messageFactory = Schema::MakeMessageFactory(
        messageDescription);

    EXPECT_EQ(
        TestKey::descriptor(),
        messageFactory->GetPrototype()->GetDescriptor());
}

}