#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/Schema.h"
#include "src/ProtoStoreInternal.pb.h"
#include "ProtoStoreTest.pb.h"

namespace Phantom::ProtoStore
{

TEST(SchemaTests, Can_initialize_from_message_descriptor)
{
    MessageDescription messageDescription;

    Schema::MakeMessageDescription(
        messageDescription,
        TestKey::descriptor());

    ASSERT_EQ("Phantom.ProtoStore.TestKey", messageDescription.messagename());
}

}