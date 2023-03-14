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
};

TEST_F(ValueBuilderTests, Can_intern_object_with_string_value)
{
    FlatBuffers::FlatStringKeyT sourceValueT;
    sourceValueT.value = "hello world";
    FlatValue sourceValue{ sourceValueT };

    auto nonInternedValue = valueBuilder.GetInternedValue(
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::TestFlatStringKeySchema },
        sourceValue);

    EXPECT_EQ(0, nonInternedValue.o);

    auto offset = FlatBuffers::FlatStringKey::Pack(
        valueBuilder.builder(),
        &sourceValueT);

    valueBuilder.InternValue(
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::TestFlatStringKeySchema },
        offset.Union()
    );

    auto internedValue = valueBuilder.GetInternedValue(
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::TestFlatStringKeySchema },
        sourceValue);

    EXPECT_EQ(offset.o, internedValue.o);
}

TEST_F(ValueBuilderTests, Can_intern_intern_multiple_objects_object_with_string_value)
{
    FlatBuffers::FlatStringKeyT sourceValueT1;
    sourceValueT1.value = "hello world 1";
    FlatValue sourceValue1{ sourceValueT1 };

    FlatBuffers::FlatStringKeyT sourceValueT2;
    sourceValueT2.value = "hello world 2";
    FlatValue sourceValue2{ sourceValueT2 };

    auto offset1 = FlatBuffers::FlatStringKey::Pack(
        valueBuilder.builder(),
        &sourceValueT1);

    auto offset2 = FlatBuffers::FlatStringKey::Pack(
        valueBuilder.builder(),
        &sourceValueT2);

    EXPECT_EQ(0, valueBuilder.GetInternedValue(
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::TestFlatStringKeySchema },
            sourceValue1
        ).o);

    EXPECT_EQ(0, valueBuilder.GetInternedValue(
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::TestFlatStringKeySchema },
        sourceValue2
    ).o);

    valueBuilder.InternValue(
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::TestFlatStringKeySchema },
        offset1.Union()
    );

    EXPECT_EQ(offset1.o, valueBuilder.GetInternedValue(
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::TestFlatStringKeySchema },
        sourceValue1
    ).o);

    EXPECT_EQ(0, valueBuilder.GetInternedValue(
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::TestFlatStringKeySchema },
        sourceValue2
    ).o);

    valueBuilder.InternValue(
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::TestFlatStringKeySchema },
        offset2.Union()
    );

    EXPECT_EQ(offset1.o, valueBuilder.GetInternedValue(
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::TestFlatStringKeySchema },
        sourceValue1
    ).o);

    EXPECT_EQ(offset2.o, valueBuilder.GetInternedValue(
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::TestFlatStringKeySchema },
        sourceValue2
    ).o);
}

}