#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/KeyComparer.h"
#include "Phantom.ProtoStore/src/Schema.h"
#include "ProtoStoreInternal.pb.h"
#include "ProtoStoreTest.pb.h"
#include <span>

namespace Phantom::ProtoStore
{

TEST(SchemaTests, Can_round_trip_to_key_comparer_with_compiled_class)
{
    Serialization::SchemaDescription schemaDescription;

    SchemaDescriptions::MakeSchemaDescription(
        schemaDescription,
        TestKey::descriptor());

    EXPECT_EQ(
        "Phantom.ProtoStore.TestKey", 
        schemaDescription.protocolbuffersdescription().messagedescription().messagename());

    auto keyComparer = SchemaDescriptions::MakeKeyComparer(
        schemaDescription);

    TestKey low;
    TestKey high;
    low.set_int32_value(1);
    high.set_int32_value(2);

    auto lowString = low.SerializeAsString();
    auto highString = high.SerializeAsString();

    auto result = keyComparer->Compare(
        as_bytes(std::span<char>{ lowString.data(), lowString.size() }),
        as_bytes(std::span<char>{ highString.data(), highString.size() })
    );

    EXPECT_EQ(std::weak_ordering::less, result);
}

}