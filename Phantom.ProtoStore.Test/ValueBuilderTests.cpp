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

}