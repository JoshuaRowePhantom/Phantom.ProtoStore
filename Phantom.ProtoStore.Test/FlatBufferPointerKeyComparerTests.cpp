#include "StandardIncludes.h"
#include "Phantom.ProtoStore/ProtoStoreTest_generated.h"
#include "Phantom.ProtoStore/src/KeyComparer.h"
#include <limits>
#include <fstream>

namespace Phantom::ProtoStore
{

using FlatBuffers::TestKeyT;

template<typename T>
void DoFlatBufferPointerKeyComparerTest(
    const T& lesser,
    const T& greater)
{
    std::ifstream sourceBinarySchemaFile("ProtoStoreTest.bfbs", std::ios::binary);

    std::vector<char> sourceBinarySchema(
        std::istreambuf_iterator<char>(sourceBinarySchemaFile),
        {});

    const reflection::Schema* schema = flatbuffers::GetRoot<reflection::Schema>(
        sourceBinarySchema.data());

    FlatBufferPointerKeyComparer keyComparer(
        schema,
        schema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.TestKey"));

    FlatMessage lesserFlatMessage{ &lesser };
    FlatMessage greaterFlatMessage{ &greater };

    EXPECT_EQ(std::weak_ordering::less, keyComparer.Compare(lesserFlatMessage.get(), greaterFlatMessage.get()));
    EXPECT_EQ(std::weak_ordering::greater, keyComparer.Compare(greaterFlatMessage.get(), lesserFlatMessage.get()));
    EXPECT_EQ(std::weak_ordering::equivalent, keyComparer.Compare(lesserFlatMessage.get(), lesserFlatMessage.get()));
    EXPECT_EQ(std::weak_ordering::equivalent, keyComparer.Compare(greaterFlatMessage.get(), greaterFlatMessage.get()));
}

template<
    auto Member
> void DoFlatBufferPointerKeyComparerNumericTest(
    )
{
    TestKeyT low;
    TestKeyT zero;
    TestKeyT high;

    low.*Member = std::numeric_limits<std::decay_t<decltype(low.*Member)>>::lowest();
    high.*Member = std::numeric_limits<std::decay_t<decltype(high.*Member)>>::max();

    if (low.*Member != 0)
    {
        DoFlatBufferPointerKeyComparerTest(
            low,
            zero);
    }

    DoFlatBufferPointerKeyComparerTest(
        low,
        high);

    DoFlatBufferPointerKeyComparerTest(
        zero,
        high);
}

TEST(FlatBufferPointerKeyComparerTests, primitive_types)
{
    DoFlatBufferPointerKeyComparerNumericTest<&TestKeyT::byte_value>();
    DoFlatBufferPointerKeyComparerNumericTest<&TestKeyT::double_value>();
    DoFlatBufferPointerKeyComparerNumericTest<&TestKeyT::float_value>();
    DoFlatBufferPointerKeyComparerNumericTest<&TestKeyT::int_value>();
    DoFlatBufferPointerKeyComparerNumericTest<&TestKeyT::long_value>();
    DoFlatBufferPointerKeyComparerNumericTest<&TestKeyT::short_value>();
    DoFlatBufferPointerKeyComparerNumericTest<&TestKeyT::ubyte_value>();
    DoFlatBufferPointerKeyComparerNumericTest<&TestKeyT::uint_value>();
    DoFlatBufferPointerKeyComparerNumericTest<&TestKeyT::ulong_value>();
    DoFlatBufferPointerKeyComparerNumericTest<&TestKeyT::ushort_value>();
}

template<
    typename Type
> void DoFlatBufferPointerKeyComparerNumericVectorTest(
    std::vector<Type> TestKeyT::* member
)
{
    TestKeyT key_empty;
    TestKeyT key_l;
    TestKeyT key_l_l;
    TestKeyT key_l_h;
    TestKeyT key_h;
    TestKeyT key_h_l;
    TestKeyT key_h_h;

    auto lowValue = std::numeric_limits<Type>::lowest();
    auto highValue = std::numeric_limits<Type>::max();

    (key_l.*member).push_back(lowValue);
    (key_l_l.*member).push_back(lowValue);
    (key_l_l.*member).push_back(lowValue);
    (key_l_h.*member).push_back(lowValue);
    (key_l_h.*member).push_back(highValue);
    (key_h.*member).push_back(highValue);
    (key_h_l.*member).push_back(highValue);
    (key_h_l.*member).push_back(lowValue);
    (key_h_h.*member).push_back(highValue);
    (key_h_h.*member).push_back(highValue);

    DoFlatBufferPointerKeyComparerTest(
        key_empty,
        key_l);

    DoFlatBufferPointerKeyComparerTest(
        key_empty,
        key_l_l);

    DoFlatBufferPointerKeyComparerTest(
        key_empty,
        key_l_h);

    DoFlatBufferPointerKeyComparerTest(
        key_empty,
        key_h);

    DoFlatBufferPointerKeyComparerTest(
        key_empty,
        key_h_l);

    DoFlatBufferPointerKeyComparerTest(
        key_empty,
        key_h_h);


    DoFlatBufferPointerKeyComparerTest(
        key_l,
        key_l_l);

    DoFlatBufferPointerKeyComparerTest(
        key_l,
        key_l_h);

    DoFlatBufferPointerKeyComparerTest(
        key_l,
        key_h);

    DoFlatBufferPointerKeyComparerTest(
        key_l,
        key_h_l);

    DoFlatBufferPointerKeyComparerTest(
        key_l,
        key_h_h);


    DoFlatBufferPointerKeyComparerTest(
        key_l_l,
        key_l_h);

    DoFlatBufferPointerKeyComparerTest(
        key_l_l,
        key_h);

    DoFlatBufferPointerKeyComparerTest(
        key_l_l,
        key_h_l);

    DoFlatBufferPointerKeyComparerTest(
        key_l_l,
        key_h_h);


    DoFlatBufferPointerKeyComparerTest(
        key_l_h,
        key_h);

    DoFlatBufferPointerKeyComparerTest(
        key_l_h,
        key_h_l);

    DoFlatBufferPointerKeyComparerTest(
        key_l_h,
        key_h_h);


    DoFlatBufferPointerKeyComparerTest(
        key_h,
        key_h_l);

    DoFlatBufferPointerKeyComparerTest(
        key_h,
        key_h_h);


    DoFlatBufferPointerKeyComparerTest(
        key_h_l,
        key_h_h);
}

TEST(FlatBufferPointerKeyComparerTests, primitive_vector_types)
{
    DoFlatBufferPointerKeyComparerNumericVectorTest(&TestKeyT::byte_vector);
    DoFlatBufferPointerKeyComparerNumericVectorTest(&TestKeyT::double_vector);
    DoFlatBufferPointerKeyComparerNumericVectorTest(&TestKeyT::float_vector);
    DoFlatBufferPointerKeyComparerNumericVectorTest(&TestKeyT::int_vector);
    DoFlatBufferPointerKeyComparerNumericVectorTest(&TestKeyT::long_vector);
    DoFlatBufferPointerKeyComparerNumericVectorTest(&TestKeyT::short_vector);
    DoFlatBufferPointerKeyComparerNumericVectorTest(&TestKeyT::ubyte_vector);
    DoFlatBufferPointerKeyComparerNumericVectorTest(&TestKeyT::uint_vector);
    DoFlatBufferPointerKeyComparerNumericVectorTest(&TestKeyT::ulong_vector);
    DoFlatBufferPointerKeyComparerNumericVectorTest(&TestKeyT::ushort_vector);
}

}

