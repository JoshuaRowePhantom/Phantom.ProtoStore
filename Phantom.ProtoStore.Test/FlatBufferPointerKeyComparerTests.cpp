#include "StandardIncludes.h"
#include "Phantom.ProtoStore/ProtoStoreTest_generated.h"
#include "Phantom.ProtoStore/src/FlatBuffersValueComparer.h"
#include <limits>
#include <fstream>
#include <cmrc/cmrc.hpp>

CMRC_DECLARE(Phantom::ProtoStore::Test::Resources);

namespace Phantom::ProtoStore
{

using FlatBuffers::TestKeyT;
using FlatBuffers::TestKeyStruct;
using FlatBuffers::TestKeyDescendingTableT;
using FlatBuffers::ScalarTableT;

FlatBufferValueComparer GetTestKeyFlatBufferValueComparer()
{
    auto fileSystem = cmrc::Phantom::ProtoStore::Test::Resources::get_filesystem();
    auto schemaData = fileSystem.open("ProtoStoreTest.bfbs");

    const reflection::Schema* schema = flatbuffers::GetRoot<reflection::Schema>(
        schemaData.begin());

    FlatBufferPointerValueComparer keyComparer(
        std::make_shared<FlatBuffersObjectSchema>(
            schema,
            schema->objects()->LookupByKey("Phantom.ProtoStore.FlatBuffers.TestKey")
        ));

    return FlatBufferValueComparer(
        std::move(keyComparer));
}

void DoFlatBufferPointerValueComparerTest(
    const TestKeyT& lesser,
    const TestKeyT& greater)
{
    auto keyComparer = GetTestKeyFlatBufferValueComparer();

    FlatMessage lesserFlatMessage{ &lesser };
    FlatMessage lesserFlatMessage2{ &lesser };
    FlatMessage greaterFlatMessage{ &greater };

    EXPECT_EQ(std::weak_ordering::less, keyComparer.Compare(lesserFlatMessage.get(), greaterFlatMessage.get()));
    EXPECT_EQ(std::weak_ordering::less, keyComparer.Compare(lesserFlatMessage2.get(), greaterFlatMessage.get()));
    EXPECT_EQ(std::weak_ordering::greater, keyComparer.Compare(greaterFlatMessage.get(), lesserFlatMessage.get()));
    EXPECT_EQ(std::weak_ordering::equivalent, keyComparer.Compare(lesserFlatMessage.get(), lesserFlatMessage.get()));
    EXPECT_EQ(std::weak_ordering::equivalent, keyComparer.Compare(lesserFlatMessage.get(), lesserFlatMessage2.get()));
    EXPECT_EQ(std::weak_ordering::equivalent, keyComparer.Compare(greaterFlatMessage.get(), greaterFlatMessage.get()));

    EXPECT_EQ(true, keyComparer.Equals(lesserFlatMessage.get(), lesserFlatMessage.get()));
    EXPECT_EQ(true, keyComparer.Equals(lesserFlatMessage.get(), lesserFlatMessage2.get()));
    EXPECT_EQ(false, keyComparer.Equals(lesserFlatMessage.get(), greaterFlatMessage.get()));
    EXPECT_EQ(false, keyComparer.Equals(greaterFlatMessage.get(), lesserFlatMessage.get()));

    EXPECT_LE(
        lesserFlatMessage.data().Content.Payload.size(),
        keyComparer.GetEstimatedSize(lesserFlatMessage.get()));

    EXPECT_LE(
        greaterFlatMessage.data().Content.Payload.size(),
        keyComparer.GetEstimatedSize(greaterFlatMessage.get()));

    EXPECT_EQ(
        keyComparer.Hash(lesserFlatMessage.get()),
        keyComparer.Hash(lesserFlatMessage.get()));

    EXPECT_EQ(
        keyComparer.Hash(lesserFlatMessage.get()),
        keyComparer.Hash(lesserFlatMessage2.get()));

    EXPECT_EQ(
        keyComparer.Hash(greaterFlatMessage.get()),
        keyComparer.Hash(greaterFlatMessage.get()));

    // Technically, hashes are allowed to collide,
    // but we don't expect it in this small sample set.
    // If they -do- collide, choose a better hash function.
    EXPECT_NE(
        keyComparer.Hash(lesserFlatMessage.get()),
        keyComparer.Hash(greaterFlatMessage.get()));
}

void DoFlatBufferPointerValueComparerTableFieldTest(
    auto member,
    auto value1,
    auto value2
)
{
    TestKeyT low;
    TestKeyT high;

    low.*member = std::move(value1);
    high.*member = std::move(value2);

    DoFlatBufferPointerValueComparerTest(
        low,
        high);
}

template<
    typename Value
> void DoFlatBufferPointerValueComparerTableNumericTest(
    Value TestKeyT::*member)
{
    Value lowest = std::numeric_limits<Value>::lowest();
    Value zero = 0;
    Value highest = std::numeric_limits<Value>::max();

    return DoFlatBufferPointerValueComparerTableFieldTest(
        member,
        lowest,
        highest
    );

    if (lowest != zero)
    {
        return DoFlatBufferPointerValueComparerTableFieldTest(
            member,
            lowest,
            zero
        );
    }

    return DoFlatBufferPointerValueComparerTableFieldTest(
        member,
        zero,
        highest
    );
}

TEST(FlatBufferPointerValueComparerTests, table_primitive_types)
{
    DoFlatBufferPointerValueComparerTableNumericTest(&TestKeyT::bool_value);
    DoFlatBufferPointerValueComparerTableNumericTest(&TestKeyT::byte_value);
    DoFlatBufferPointerValueComparerTableNumericTest(&TestKeyT::double_value);
    DoFlatBufferPointerValueComparerTableNumericTest(&TestKeyT::float_value);
    DoFlatBufferPointerValueComparerTableNumericTest(&TestKeyT::int_value);
    DoFlatBufferPointerValueComparerTableNumericTest(&TestKeyT::long_value);
    DoFlatBufferPointerValueComparerTableNumericTest(&TestKeyT::short_value);
    DoFlatBufferPointerValueComparerTableNumericTest(&TestKeyT::ubyte_value);
    DoFlatBufferPointerValueComparerTableNumericTest(&TestKeyT::uint_value);
    DoFlatBufferPointerValueComparerTableNumericTest(&TestKeyT::ulong_value);
    DoFlatBufferPointerValueComparerTableNumericTest(&TestKeyT::ushort_value);
}

TEST(FlatBufferPointerValueComparerTests, defaulted_value)
{
    TestKeyT low;
    TestKeyT defaulted;
    TestKeyT high;

    low.defaulted_value = 4;
    high.defaulted_value = 6;

    DoFlatBufferPointerValueComparerTest(
        low,
        defaulted);

    DoFlatBufferPointerValueComparerTest(
        low,
        high);

    DoFlatBufferPointerValueComparerTest(
        defaulted,
        high);
}

TEST(FlatBufferPointerValueComparerTests, table_string_type)
{
    DoFlatBufferPointerValueComparerTableFieldTest(
        &TestKeyT::string_value,
        "",
        "a");

    DoFlatBufferPointerValueComparerTableFieldTest(
        &TestKeyT::string_value,
        "aa",
        "aaa");

    DoFlatBufferPointerValueComparerTableFieldTest(
        &TestKeyT::string_value,
        "aa",
        "b");
}

TEST(FlatBufferPointerValueComparerTests, table_subtable)
{
    TestKeyT low;
    TestKeyT high;

    low.byte_value = -1;
    high.byte_value = 1;

    DoFlatBufferPointerValueComparerTableFieldTest(
        &TestKeyT::subkey_value,
        copy_unique(low),
        copy_unique(high));

    DoFlatBufferPointerValueComparerTableFieldTest(
        &TestKeyT::subkey_value,
        nullptr,
        copy_unique(low));
}

template<
    typename Type
> void DoFlatBufferPointerValueComparerVectorTest(
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

    DoFlatBufferPointerValueComparerTest(
        key_empty,
        key_l);

    DoFlatBufferPointerValueComparerTest(
        key_empty,
        key_l_l);

    DoFlatBufferPointerValueComparerTest(
        key_empty,
        key_l_h);

    DoFlatBufferPointerValueComparerTest(
        key_empty,
        key_h);

    DoFlatBufferPointerValueComparerTest(
        key_empty,
        key_h_l);

    DoFlatBufferPointerValueComparerTest(
        key_empty,
        key_h_h);


    DoFlatBufferPointerValueComparerTest(
        key_l,
        key_l_l);

    DoFlatBufferPointerValueComparerTest(
        key_l,
        key_l_h);

    DoFlatBufferPointerValueComparerTest(
        key_l,
        key_h);

    DoFlatBufferPointerValueComparerTest(
        key_l,
        key_h_l);

    DoFlatBufferPointerValueComparerTest(
        key_l,
        key_h_h);


    DoFlatBufferPointerValueComparerTest(
        key_l_l,
        key_l_h);

    DoFlatBufferPointerValueComparerTest(
        key_l_l,
        key_h);

    DoFlatBufferPointerValueComparerTest(
        key_l_l,
        key_h_l);

    DoFlatBufferPointerValueComparerTest(
        key_l_l,
        key_h_h);


    DoFlatBufferPointerValueComparerTest(
        key_l_h,
        key_h);

    DoFlatBufferPointerValueComparerTest(
        key_l_h,
        key_h_l);

    DoFlatBufferPointerValueComparerTest(
        key_l_h,
        key_h_h);


    DoFlatBufferPointerValueComparerTest(
        key_h,
        key_h_l);

    DoFlatBufferPointerValueComparerTest(
        key_h,
        key_h_h);


    DoFlatBufferPointerValueComparerTest(
        key_h_l,
        key_h_h);
}

template<
    typename Type
> void DoFlatBufferPointerValueComparerNumericVectorTest(
    std::vector<Type> TestKeyT::* member
)
{
    auto lowValue = std::numeric_limits<Type>::lowest();
    auto highValue = std::numeric_limits<Type>::max();
    return DoFlatBufferPointerValueComparerVectorTest(
        member,
        [&]() { return lowValue; },
        [&]() { return highValue; }
    );
}

TEST(FlatBufferPointerValueComparerTests, primitive_vector_types)
{
    DoFlatBufferPointerValueComparerNumericVectorTest(&TestKeyT::bool_vector);
    DoFlatBufferPointerValueComparerNumericVectorTest(&TestKeyT::byte_vector);
    DoFlatBufferPointerValueComparerNumericVectorTest(&TestKeyT::double_vector);
    DoFlatBufferPointerValueComparerNumericVectorTest(&TestKeyT::float_vector);
    DoFlatBufferPointerValueComparerNumericVectorTest(&TestKeyT::int_vector);
    DoFlatBufferPointerValueComparerNumericVectorTest(&TestKeyT::long_vector);
    DoFlatBufferPointerValueComparerNumericVectorTest(&TestKeyT::short_vector);
    DoFlatBufferPointerValueComparerNumericVectorTest(&TestKeyT::ubyte_vector);
    DoFlatBufferPointerValueComparerNumericVectorTest(&TestKeyT::uint_vector);
    DoFlatBufferPointerValueComparerNumericVectorTest(&TestKeyT::ulong_vector);
    DoFlatBufferPointerValueComparerNumericVectorTest(&TestKeyT::ushort_vector);
}

TEST(FlatBufferPointerValueComparerTests, string_vector)
{
    DoFlatBufferPointerValueComparerVectorTest(
        &TestKeyT::string_vector,
        []() { return "aaa"; },
        []() { return "aab"; }
    );

    DoFlatBufferPointerValueComparerVectorTest(
        &TestKeyT::string_vector,
        []() { return "aa"; },
        []() { return "aaa"; }
    );
}

TEST(FlatBufferPointerValueComparerTests, subkey_vector)
{
    TestKeyT low;
    TestKeyT high;

    low.byte_value = -1;
    high.byte_value = 1;

    DoFlatBufferPointerValueComparerVectorTest(
        &TestKeyT::subkey_vector,
        [&]() { return copy_unique(low); },
        [&]() { return copy_unique(high); }
    );
}

TEST(FlatBufferPointerValueComparerTests, struct_vector)
{
    TestKeyStruct low;
    TestKeyStruct high;

    low.mutate_byte_value(-1);
    high.mutate_byte_value(1);

    DoFlatBufferPointerValueComparerVectorTest(
        &TestKeyT::struct_vector,
        [&]() { return low; },
        [&]() { return high; }
    );
}

template<
    typename Type
>
void DoFlatBufferPointerValueComparerStructNumericTest(
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
        DoFlatBufferPointerValueComparerTest(
            low,
            zero);
    }

    DoFlatBufferPointerValueComparerTest(
        low,
        high);

    DoFlatBufferPointerValueComparerTest(
        zero,
        high);
}

TEST(FlatBufferPointerValueComparerTests, struct_primitive_types)
{
    DoFlatBufferPointerValueComparerStructNumericTest(&TestKeyStruct::mutate_bool_value);
    DoFlatBufferPointerValueComparerStructNumericTest(&TestKeyStruct::mutate_byte_value);
    DoFlatBufferPointerValueComparerStructNumericTest(&TestKeyStruct::mutate_double_value);
    DoFlatBufferPointerValueComparerStructNumericTest(&TestKeyStruct::mutate_float_value);
    DoFlatBufferPointerValueComparerStructNumericTest(&TestKeyStruct::mutate_int_value);
    DoFlatBufferPointerValueComparerStructNumericTest(&TestKeyStruct::mutate_long_value);
    DoFlatBufferPointerValueComparerStructNumericTest(&TestKeyStruct::mutate_short_value);
    DoFlatBufferPointerValueComparerStructNumericTest(&TestKeyStruct::mutate_ubyte_value);
    DoFlatBufferPointerValueComparerStructNumericTest(&TestKeyStruct::mutate_uint_value);
    DoFlatBufferPointerValueComparerStructNumericTest(&TestKeyStruct::mutate_ulong_value);
    DoFlatBufferPointerValueComparerStructNumericTest(&TestKeyStruct::mutate_ushort_value);
}

TEST(FlatBufferPointerValueComparerTests, table_primitive_descending)
{
    DoFlatBufferPointerValueComparerTableFieldTest(
        &TestKeyT::descending_value,
        5,
        4
    );
}

TEST(FlatBufferPointerValueComparerTests, struct_primitive_descending)
{
    TestKeyT low;
    TestKeyT high;

    low.struct_value = std::make_unique<TestKeyStruct>();
    high.struct_value = std::make_unique<TestKeyStruct>();

    low.descending_value = 5;
    high.descending_value = 4;

    DoFlatBufferPointerValueComparerTest(
        low,
        high
    );
}

TEST(FlatBufferPointerValueComparerTests, descending_table)
{
    TestKeyT low;
    TestKeyT high;

    low.descending_table = std::make_unique<TestKeyDescendingTableT>();
    high.descending_table = std::make_unique<TestKeyDescendingTableT>();

    low.descending_table->ascending_value = 5;
    high.descending_table->ascending_value = 4;

    DoFlatBufferPointerValueComparerTest(
        low,
        high
    );

    low.descending_table = std::make_unique<TestKeyDescendingTableT>();
    high.descending_table = std::make_unique<TestKeyDescendingTableT>();

    low.descending_table->descending_value = 4;
    high.descending_table->descending_value = 5;

    DoFlatBufferPointerValueComparerTest(
        low,
        high
    );
}

TEST(FlatBufferPointerValueComparerTests, union_value)
{
    TestKeyT no_value;
    TestKeyT low_scalar;
    TestKeyT high_scalar;
    TestKeyT low_testKey;
    TestKeyT high_testKey;

    low_scalar.union_value.Set(ScalarTableT());
    low_scalar.union_value.AsScalarTable()->item = 1;
    high_scalar.union_value.Set(ScalarTableT());
    high_scalar.union_value.AsScalarTable()->item = 2;
    low_testKey.union_value.Set(TestKeyT());
    low_testKey.union_value.AsTestKey()->byte_value = 1;
    high_testKey.union_value.Set(TestKeyT());
    high_testKey.union_value.AsTestKey()->byte_value = 2;

    DoFlatBufferPointerValueComparerTest(
        no_value,
        low_scalar
    );
    
    DoFlatBufferPointerValueComparerTest(
        no_value,
        high_scalar
    );

    DoFlatBufferPointerValueComparerTest(
        no_value,
        low_testKey
    );

    DoFlatBufferPointerValueComparerTest(
        no_value,
        high_testKey
    );


    DoFlatBufferPointerValueComparerTest(
        low_scalar,
        high_scalar
    );

    DoFlatBufferPointerValueComparerTest(
        low_scalar,
        low_testKey
    );

    DoFlatBufferPointerValueComparerTest(
        low_scalar,
        high_testKey
    );


    DoFlatBufferPointerValueComparerTest(
        high_scalar,
        low_testKey
    );

    DoFlatBufferPointerValueComparerTest(
        high_scalar,
        high_testKey
    );


    DoFlatBufferPointerValueComparerTest(
        low_testKey,
        high_testKey
    );
}

}

