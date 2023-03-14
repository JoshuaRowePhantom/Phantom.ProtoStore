#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/KeyComparer.h"
#include "Phantom.ProtoStore/ProtoStoreTest_generated.h"
#include "Resources.h"
#include <flatbuffers/flatbuffers.h>

namespace Phantom::ProtoStore
{

class ValueBuilderTests : public testing::Test
{
public:
    using TestKey = FlatBuffers::TestKey;
    
    template<
        typename T
    > using Offset = flatbuffers::Offset<T>;

    ValueBuilder valueBuilder;
};

TEST_F(ValueBuilderTests, can_intern_top_level_objects)
{
    FlatBuffers::FlatStringKeyT key1;
    key1.value = "hello world";

    FlatBuffers::FlatStringKeyT key2;
    key2.value = "hello world 2";

    auto offset1 = valueBuilder.CopyTableDag(
        FlatBuffersTestSchemas::TestSchema,
        FlatBuffersTestSchemas::TestFlatStringKeySchema,
        ProtoValue{ &key1 }.as_table_if()
    );

    auto offset2 = valueBuilder.CopyTableDag(
        FlatBuffersTestSchemas::TestSchema,
        FlatBuffersTestSchemas::TestFlatStringKeySchema,
        ProtoValue{ &key1 }.as_table_if()
    );

    auto offset3 = valueBuilder.CopyTableDag(
        FlatBuffersTestSchemas::TestSchema,
        FlatBuffersTestSchemas::TestFlatStringKeySchema,
        ProtoValue{ &key2 }.as_table_if()
    );

    auto offset4 = valueBuilder.CopyTableDag(
        FlatBuffersTestSchemas::TestSchema,
        FlatBuffersTestSchemas::TestFlatStringKeySchema,
        ProtoValue{ &key2 }.as_table_if()
    );

    EXPECT_EQ(offset1.o, offset2.o);
    EXPECT_NE(offset2.o, offset3.o);
    EXPECT_EQ(offset3.o, offset4.o);
}

TEST_F(ValueBuilderTests, can_intern_subobject_of_vector)
{
    FlatBuffers::TestKeyT key;
    FlatBuffers::TestKeyT subkey1;
    subkey1.byte_value = 1;
    FlatBuffers::TestKeyT subkey2;
    subkey2.byte_value = 2;
    key.subkey_vector.push_back(copy_unique(subkey1));
    key.subkey_vector.push_back(copy_unique(subkey2));
    key.subkey_vector.push_back(copy_unique(subkey1));
    key.subkey_vector.push_back(copy_unique(subkey2));

    auto offset1 = valueBuilder.CopyTableDag(
        FlatBuffersTestSchemas::TestSchema,
        FlatBuffersTestSchemas::Test_TestKey_Object,
        ProtoValue{ &key }.as_table_if()
    );

    auto copy = reinterpret_cast<const TestKey*>(
        valueBuilder.builder().GetCurrentBufferPointer() + valueBuilder.builder().GetSize() - offset1.o);

    EXPECT_EQ(
        copy->subkey_vector()->Get(0),
        copy->subkey_vector()->Get(2)
    );

    EXPECT_EQ(
        copy->subkey_vector()->Get(1),
        copy->subkey_vector()->Get(3)
    );

    EXPECT_NE(
        copy->subkey_vector()->Get(0),
        copy->subkey_vector()->Get(1)
    );

    EXPECT_EQ(
        copy->subkey_vector()->Get(0)->byte_value(),
        1
    );

    EXPECT_EQ(
        copy->subkey_vector()->Get(1)->byte_value(),
        2
    );
}

TEST_F(ValueBuilderTests, can_intern_vector)
{
    FlatBuffers::TestKeyT key;
    key.byte_value = 1;
    FlatBuffers::TestKeyT subkey1;
    subkey1.byte_value = 1;
    FlatBuffers::TestKeyT subkey2;
    subkey2.byte_value = 2;
    key.subkey_vector.push_back(copy_unique(subkey1));
    key.subkey_vector.push_back(copy_unique(subkey2));
    key.subkey_vector.push_back(copy_unique(subkey1));
    key.subkey_vector.push_back(copy_unique(subkey2));

    auto offset1 = valueBuilder.CopyTableDag(
        FlatBuffersTestSchemas::TestSchema,
        FlatBuffersTestSchemas::Test_TestKey_Object,
        ProtoValue{ &key }.as_table_if()
    );

    key.byte_value = 2;

    auto offset2 = valueBuilder.CopyTableDag(
        FlatBuffersTestSchemas::TestSchema,
        FlatBuffersTestSchemas::Test_TestKey_Object,
        ProtoValue{ &key }.as_table_if()
    );

    auto copy1 = reinterpret_cast<const TestKey*>(
        valueBuilder.builder().GetCurrentBufferPointer() + valueBuilder.builder().GetSize() - offset1.o);

    auto copy2 = reinterpret_cast<const TestKey*>(
        valueBuilder.builder().GetCurrentBufferPointer() + valueBuilder.builder().GetSize() - offset2.o);

    EXPECT_EQ(
        copy1->subkey_vector(),
        copy2->subkey_vector()
    );

    EXPECT_NE(
        copy1,
        copy2
    );
}


TEST_F(ValueBuilderTests, can_intern_union)
{
    FlatBuffers::TestKeyT key;
    FlatBuffers::ScalarTableT unionValue;
    unionValue.item = 5;
    key.union_value.Set(unionValue);

    auto offset1 = valueBuilder.CopyTableDag(
        FlatBuffersTestSchemas::TestSchema,
        FlatBuffersTestSchemas::Test_TestKey_Object,
        ProtoValue{ &key }.as_table_if()
    );

    key.byte_value = 2;

    auto offset2 = valueBuilder.CopyTableDag(
        FlatBuffersTestSchemas::TestSchema,
        FlatBuffersTestSchemas::Test_TestKey_Object,
        ProtoValue{ &key }.as_table_if()
    );

    auto copy1 = reinterpret_cast<const TestKey*>(
        valueBuilder.builder().GetCurrentBufferPointer() + valueBuilder.builder().GetSize() - offset1.o);

    auto copy2 = reinterpret_cast<const TestKey*>(
        valueBuilder.builder().GetCurrentBufferPointer() + valueBuilder.builder().GetSize() - offset2.o);

    EXPECT_EQ(
        copy1->union_value_as<FlatBuffers::ScalarTable>()->item(),
        5
    );

    EXPECT_EQ(
        copy1->union_value_as<FlatBuffers::ScalarTable>(),
        copy2->union_value_as<FlatBuffers::ScalarTable>()
    );

    EXPECT_NE(
        copy1,
        copy2
    );
}


}