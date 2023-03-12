#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/KeyComparer.h"
#include "Phantom.ProtoStore/src/Schema.h"
#include "ProtoStoreInternal.pb.h"
#include "ProtoStoreTest.pb.h"
#include "Phantom.ProtoStore/ProtoStoreTest_generated.h"
#include "Resources.h"
#include <span>

namespace Phantom::ProtoStore
{

TEST(SchemaTests, Can_round_trip_ProtocolBuffers_schema_to_key_comparer_with_compiled_class)
{
    Serialization::IndexSchemaDescription indexSchemaDescription;

    SchemaDescriptions::MakeSchemaDescription(
        indexSchemaDescription,
        Schema
        {
            { TestKey::descriptor() },
            { TestKey::descriptor() }
        });

    EXPECT_EQ(
        "Phantom.ProtoStore.TestKey", 
        indexSchemaDescription.key().description().protocolbuffersdescription().messagedescription().messagename());

    auto schema = SchemaDescriptions::MakeSchema(
        indexSchemaDescription);

    EXPECT_FALSE(schema->KeySchema.IsFlatBuffersSchema());
    EXPECT_TRUE(schema->KeySchema.IsProtocolBuffersSchema());

    EXPECT_FALSE(schema->ValueSchema.IsFlatBuffersSchema());
    EXPECT_TRUE(schema->ValueSchema.IsProtocolBuffersSchema());

    auto keyComparer = SchemaDescriptions::MakeKeyComparer(
        schema);

    TestKey low;
    TestKey high;
    low.set_int32_value(1);
    high.set_int32_value(2);

    auto lowString = low.SerializeAsString();
    auto highString = high.SerializeAsString();

    auto result = keyComparer->Compare(
        ProtoValue(&low).pack(),
        ProtoValue(&high).pack());

    EXPECT_EQ(std::weak_ordering::less, result);
}

TEST(SchemaTests, Can_round_trip_FlatBuffers_schema_to_key_comparer_with_compiled_class)
{
    Serialization::IndexSchemaDescription indexSchemaDescription;

    SchemaDescriptions::MakeSchemaDescription(
        indexSchemaDescription,
        Schema
        {
            { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::TestFlatStringKeySchema },
            { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::TestFlatStringValueSchema },
        });

    auto schema = SchemaDescriptions::MakeSchema(
        indexSchemaDescription);

    EXPECT_TRUE(schema->KeySchema.IsFlatBuffersSchema());
    EXPECT_FALSE(schema->KeySchema.IsProtocolBuffersSchema());
    
    EXPECT_TRUE(schema->ValueSchema.IsFlatBuffersSchema());
    EXPECT_FALSE(schema->ValueSchema.IsProtocolBuffersSchema());

    auto keyComparer = SchemaDescriptions::MakeKeyComparer(
        schema);

    FlatBuffers::FlatStringKeyT low;
    FlatBuffers::FlatStringKeyT high;
    low.value = "hello";
    high.value = "world";

    auto result = keyComparer->Compare(
        ProtoValue(&low),
        ProtoValue(&high));

    EXPECT_EQ(std::weak_ordering::less, result);
}

}