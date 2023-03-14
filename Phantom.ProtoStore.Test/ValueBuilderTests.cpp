#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/KeyComparer.h"
#include "Phantom.ProtoStore/ProtoStoreTest_generated.h"
#include "Resources.h"

namespace Phantom::ProtoStore
{

class ValueBuilderTests : public testing::Test
{
public:
    using TestKey = FlatBuffers::TestKey;
    
    template<
        typename T
    > using Offset = flatbuffers::Offset<T>;

    flatbuffers::FlatBufferBuilder destinationBuilder;
    ValueBuilder valueBuilder = ValueBuilder(&destinationBuilder);

    template<
        IsNativeTable NativeTable1,
        IsNativeTable NativeTable2
    >
    void Do_intern_test(
        const NativeTable1 & source1T,
        const NativeTable2 & source2T,
        ValueBuilder::SchemaItem schemaItem1,
        ValueBuilder::SchemaItem schemaItem2
    )
    {
        FlatValue source1{ source1T };
        FlatValue source2{ source2T };

        EXPECT_EQ(0, valueBuilder.GetInternedValue(
            schemaItem1,
            source1
        ).o);

        EXPECT_EQ(0, valueBuilder.GetInternedValue(
            schemaItem2,
            source2
        ).o);

        auto offset1 = NativeTable1::TableType::Pack(
            valueBuilder.builder(),
            &source1T);

        auto offset2 = NativeTable2::TableType::Pack(
            valueBuilder.builder(),
            &source2T);

        valueBuilder.InternValue(
            schemaItem1,
            offset1.Union()
        );

        EXPECT_EQ(offset1.o, valueBuilder.GetInternedValue(
            schemaItem1,
            source1
        ).o);

        EXPECT_EQ(0, valueBuilder.GetInternedValue(
            schemaItem2,
            source2
        ).o);

        valueBuilder.InternValue(
            schemaItem2,
            offset2.Union()
        );

        EXPECT_EQ(offset1.o, valueBuilder.GetInternedValue(
            schemaItem1,
            source1
        ).o);

        EXPECT_EQ(offset2.o, valueBuilder.GetInternedValue(
            schemaItem2,
            source2
        ).o);
    }
};

TEST_F(ValueBuilderTests, Intern_object_with_string_value)
{
    FlatBuffers::FlatStringKeyT sourceValueT1;
    sourceValueT1.value = "hello world 1";

    FlatBuffers::FlatStringKeyT sourceValueT2;
    sourceValueT2.value = "hello world 2";

    auto schemaItem = ValueBuilder::SchemaItem
    {
        FlatBuffersTestSchemas::TestSchema,
        FlatBuffersTestSchemas::TestFlatStringKeySchema
    };

    Do_intern_test(
        sourceValueT1,
        sourceValueT2,
        schemaItem,
        schemaItem
    );
}

TEST_F(ValueBuilderTests, Intern_object_with_string_vector)
{
    FlatBuffers::TestKeyT sourceValueT1;
    sourceValueT1.string_vector.push_back("hello world");
    
    FlatBuffers::TestKeyT sourceValueT2;
    sourceValueT1.string_vector.push_back("hello world 2");

    auto schemaItem = ValueBuilder::SchemaItem
    {
        FlatBuffersTestSchemas::TestSchema,
        FlatBuffersTestSchemas::Test_TestKey_Object
    };

    Do_intern_test(
        sourceValueT1,
        sourceValueT2,
        schemaItem,
        schemaItem
    );
}

TEST_F(ValueBuilderTests, Intern_object_with_string_vector_empty)
{
    FlatBuffers::TestKeyT sourceValueT1;
    sourceValueT1.string_vector.push_back("hello world");

    FlatBuffers::TestKeyT sourceValueT2;

    auto schemaItem = ValueBuilder::SchemaItem
    {
        FlatBuffersTestSchemas::TestSchema,
        FlatBuffersTestSchemas::Test_TestKey_Object
    };

    Do_intern_test(
        sourceValueT1,
        sourceValueT2,
        schemaItem,
        schemaItem
    );
}

}