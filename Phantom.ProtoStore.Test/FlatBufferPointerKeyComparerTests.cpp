#include "StandardIncludes.h"
#include "Phantom.ProtoStore/ProtoStoreTest_generated.h"
#include "Phantom.ProtoStore/src/KeyComparer.h"
#include <limits>
#include <fstream>
#include <cmrc/cmrc.hpp>

CMRC_DECLARE(Phantom::ProtoStore::Test::Resources);

namespace Phantom::ProtoStore
{

using FlatBuffers::TestKeyT;
using FlatBuffers::TestKeyStruct;

FlatBufferPointerKeyComparer GetTestKeyFlatBufferPointerKeyComparer()
{
    auto fileSystem = cmrc::Phantom::ProtoStore::Test::Resources::get_filesystem();
    auto schemaData = fileSystem.open("ProtoStoreTest.bfbs");

    const reflection::Schema* schema = flatbuffers::GetRoot<reflection::Schema>(
        schemaData.begin());

    FlatBufferPointerKeyComparer keyComparer(
        schema,
        schema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.TestKey"));

    return std::move(keyComparer);
}

void DoFlatBufferPointerKeyComparerTest(
    const TestKeyT& lesser,
    const TestKeyT& greater)
{
    auto keyComparer = GetTestKeyFlatBufferPointerKeyComparer();

    FlatMessage lesserFlatMessage{ &lesser };
    FlatMessage greaterFlatMessage{ &greater };

    EXPECT_EQ(std::weak_ordering::less, keyComparer.Compare(lesserFlatMessage.get(), greaterFlatMessage.get()));
    EXPECT_EQ(std::weak_ordering::greater, keyComparer.Compare(greaterFlatMessage.get(), lesserFlatMessage.get()));
    EXPECT_EQ(std::weak_ordering::equivalent, keyComparer.Compare(lesserFlatMessage.get(), lesserFlatMessage.get()));
    EXPECT_EQ(std::weak_ordering::equivalent, keyComparer.Compare(greaterFlatMessage.get(), greaterFlatMessage.get()));
}

void DoFlatBufferPointerKeyComparerTableFieldTest(
    auto member,
    auto value1,
    auto value2
)
{
    TestKeyT low;
    TestKeyT high;

    low.*member = std::move(value1);
    high.*member = std::move(value2);

    DoFlatBufferPointerKeyComparerTest(
        low,
        high);
}

template<
    typename Value
> void DoFlatBufferPointerKeyComparerTableNumericTest(
    Value TestKeyT::*member)
{
    Value lowest = std::numeric_limits<Value>::lowest();
    Value zero = 0;
    Value highest = std::numeric_limits<Value>::max();

    return DoFlatBufferPointerKeyComparerTableFieldTest(
        member,
        lowest,
        highest
    );

    if (lowest != zero)
    {
        return DoFlatBufferPointerKeyComparerTableFieldTest(
            member,
            lowest,
            zero
        );
    }

    return DoFlatBufferPointerKeyComparerTableFieldTest(
        member,
        zero,
        highest
    );
}

TEST(FlatBufferPointerKeyComparerTests, table_primitive_types)
{
    DoFlatBufferPointerKeyComparerTableNumericTest(&TestKeyT::bool_value);
    DoFlatBufferPointerKeyComparerTableNumericTest(&TestKeyT::byte_value);
    DoFlatBufferPointerKeyComparerTableNumericTest(&TestKeyT::double_value);
    DoFlatBufferPointerKeyComparerTableNumericTest(&TestKeyT::float_value);
    DoFlatBufferPointerKeyComparerTableNumericTest(&TestKeyT::int_value);
    DoFlatBufferPointerKeyComparerTableNumericTest(&TestKeyT::long_value);
    DoFlatBufferPointerKeyComparerTableNumericTest(&TestKeyT::short_value);
    DoFlatBufferPointerKeyComparerTableNumericTest(&TestKeyT::ubyte_value);
    DoFlatBufferPointerKeyComparerTableNumericTest(&TestKeyT::uint_value);
    DoFlatBufferPointerKeyComparerTableNumericTest(&TestKeyT::ulong_value);
    DoFlatBufferPointerKeyComparerTableNumericTest(&TestKeyT::ushort_value);
}

TEST(FlatBufferPointerKeyComparerTests, defaulted_value)
{
    TestKeyT low;
    TestKeyT defaulted;
    TestKeyT high;

    low.defaulted_value = 4;
    high.defaulted_value = 6;

    DoFlatBufferPointerKeyComparerTest(
        low,
        defaulted);

    DoFlatBufferPointerKeyComparerTest(
        low,
        high);

    DoFlatBufferPointerKeyComparerTest(
        defaulted,
        high);
}

TEST(FlatBufferPointerKeyComparerTests, table_string_type)
{
    DoFlatBufferPointerKeyComparerTableFieldTest(
        &TestKeyT::string_value,
        "",
        "a");

    DoFlatBufferPointerKeyComparerTableFieldTest(
        &TestKeyT::string_value,
        "aa",
        "aaa");

    DoFlatBufferPointerKeyComparerTableFieldTest(
        &TestKeyT::string_value,
        "aa",
        "b");
}

TEST(FlatBufferPointerKeyComparerTests, table_subtable)
{
    TestKeyT low;
    TestKeyT high;

    low.byte_value = -1;
    high.byte_value = 1;

    DoFlatBufferPointerKeyComparerTableFieldTest(
        &TestKeyT::subkey_value,
        copy_unique(low),
        copy_unique(high));

    DoFlatBufferPointerKeyComparerTableFieldTest(
        &TestKeyT::subkey_value,
        nullptr,
        copy_unique(low));
}

template<
    typename Type
> void DoFlatBufferPointerKeyComparerVectorTest(
    std::vector<Type> TestKeyT::* member,
    auto lowValueLambda,
    auto highValueLambda
)
{
    TestKeyT key_empty;
    TestKeyT key_l;
    TestKeyT key_l_l;
    TestKeyT key_l_h;
    TestKeyT key_h;
    TestKeyT key_h_l;
    TestKeyT key_h_h;

    (key_l.*member).push_back(lowValueLambda());
    (key_l_l.*member).push_back(lowValueLambda());
    (key_l_l.*member).push_back(lowValueLambda());
    (key_l_h.*member).push_back(lowValueLambda());
    (key_l_h.*member).push_back(highValueLambda());
    (key_h.*member).push_back(highValueLambda());
    (key_h_l.*member).push_back(highValueLambda());
    (key_h_l.*member).push_back(lowValueLambda());
    (key_h_h.*member).push_back(highValueLambda());
    (key_h_h.*member).push_back(highValueLambda());

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

template<
    typename Type
> void DoFlatBufferPointerKeyComparerNumericVectorTest(
    std::vector<Type> TestKeyT::* member
)
{
    auto lowValue = std::numeric_limits<Type>::lowest();
    auto highValue = std::numeric_limits<Type>::max();
    return DoFlatBufferPointerKeyComparerVectorTest(
        member,
        [&]() { return lowValue; },
        [&]() { return highValue; }
    );
}

TEST(FlatBufferPointerKeyComparerTests, primitive_vector_types)
{
    DoFlatBufferPointerKeyComparerNumericVectorTest(&TestKeyT::bool_vector);
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

TEST(FlatBufferPointerKeyComparerTests, string_vector)
{
    DoFlatBufferPointerKeyComparerVectorTest(
        &TestKeyT::string_vector,
        []() { return "aaa"; },
        []() { return "aab"; }
    );

    DoFlatBufferPointerKeyComparerVectorTest(
        &TestKeyT::string_vector,
        []() { return "aa"; },
        []() { return "aaa"; }
    );
}

TEST(FlatBufferPointerKeyComparerTests, subkey_vector)
{
    TestKeyT low;
    TestKeyT high;

    low.byte_value = -1;
    high.byte_value = 1;

    DoFlatBufferPointerKeyComparerVectorTest(
        &TestKeyT::subkey_vector,
        [&]() { return copy_unique(low); },
        [&]() { return copy_unique(high); }
    );
}

template<
    typename Type
>
void DoFlatBufferPointerKeyComparerStructNumericTest(
    void (TestKeyStruct::*mutator)(Type)
)
{
    TestKeyT low;
    TestKeyT zero;
    TestKeyT high;

    low.struct_value = std::make_unique<TestKeyStruct>();
    zero.struct_value = std::make_unique<TestKeyStruct>();
    high.struct_value = std::make_unique<TestKeyStruct>();

    auto lowValue = std::numeric_limits<Type>::lowest();
    auto highValue = std::numeric_limits<Type>::max();

    (low.struct_value.get()->*mutator)(lowValue);
    (high.struct_value.get()->*mutator)(highValue);

    if (lowValue != 0)
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

TEST(FlatBufferPointerKeyComparerTests, struct_primitive_types)
{
    DoFlatBufferPointerKeyComparerStructNumericTest(&TestKeyStruct::mutate_bool_value);
    DoFlatBufferPointerKeyComparerStructNumericTest(&TestKeyStruct::mutate_byte_value);
    DoFlatBufferPointerKeyComparerStructNumericTest(&TestKeyStruct::mutate_double_value);
    DoFlatBufferPointerKeyComparerStructNumericTest(&TestKeyStruct::mutate_float_value);
    DoFlatBufferPointerKeyComparerStructNumericTest(&TestKeyStruct::mutate_int_value);
    DoFlatBufferPointerKeyComparerStructNumericTest(&TestKeyStruct::mutate_long_value);
    DoFlatBufferPointerKeyComparerStructNumericTest(&TestKeyStruct::mutate_short_value);
    DoFlatBufferPointerKeyComparerStructNumericTest(&TestKeyStruct::mutate_ubyte_value);
    DoFlatBufferPointerKeyComparerStructNumericTest(&TestKeyStruct::mutate_uint_value);
    DoFlatBufferPointerKeyComparerStructNumericTest(&TestKeyStruct::mutate_ulong_value);
    DoFlatBufferPointerKeyComparerStructNumericTest(&TestKeyStruct::mutate_ushort_value);
}

}

