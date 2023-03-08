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
//
//template<
//    auto Member
//> void DoFlatBufferPointerKeyComparerNumericVectorTest(
//)
//{
//    TestKeyT low;
//    TestKeyT zero;
//    TestKeyT high;
//
//    low.*Member = std::numeric_limits<std::decay_t<decltype(low.*Member)>>::lowest();
//    high.*Member = std::numeric_limits<std::decay_t<decltype(high.*Member)>>::max();
//
//    if (low.*Member != zero.*Member)
//    {
//        DoFlatBufferPointerKeyComparerTest(
//            low,
//            zero);
//    }
//
//    DoFlatBufferPointerKeyComparerTest(
//        low,
//        high);
//
//    DoFlatBufferPointerKeyComparerTest(
//        zero,
//        high);
//}

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

//
//TEST(FlatBufferPointerKeyComparerTests, primitive_vector_types)
//{
//    DoFlatBufferPointerKeyComparerNumericVectorTest<&TestKeyT::byte_vector>();
//    DoFlatBufferPointerKeyComparerNumericVectorTest<&TestKeyT::double_vector>();
//    DoFlatBufferPointerKeyComparerNumericVectorTest<&TestKeyT::float_vector>();
//    DoFlatBufferPointerKeyComparerNumericVectorTest<&TestKeyT::int_vector>();
//    DoFlatBufferPointerKeyComparerNumericVectorTest<&TestKeyT::long_vector>();
//    DoFlatBufferPointerKeyComparerNumericVectorTest<&TestKeyT::short_vector>();
//    DoFlatBufferPointerKeyComparerNumericVectorTest<&TestKeyT::ubyte_vector>();
//    DoFlatBufferPointerKeyComparerNumericVectorTest<&TestKeyT::uint_vector>();
//    DoFlatBufferPointerKeyComparerNumericVectorTest<&TestKeyT::ulong_vector>();
//    DoFlatBufferPointerKeyComparerNumericVectorTest<&TestKeyT::ushort_vector>();
//}

}

