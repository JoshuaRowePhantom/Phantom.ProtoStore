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

void DoFlatBufferValueComparerTest(
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

void DoFlatBufferValueComparerTableFieldTest(
    auto member,
    auto value1,
    auto value2
)
{
    TestKeyT low;
    TestKeyT high;

    low.*member = std::move(value1);
    high.*member = std::move(value2);

    DoFlatBufferValueComparerTest(
        low,
        high);
}

template<
    typename Value
> void DoFlatBufferValueComparerTableNumericTest(
    Value TestKeyT::*member)
{
    Value lowest = std::numeric_limits<Value>::lowest();
    Value zero = 0;
    Value highest = std::numeric_limits<Value>::max();

    DoFlatBufferValueComparerTableFieldTest(
        member,
        lowest,
        highest
    );

    if (lowest != zero)
    {
        DoFlatBufferValueComparerTableFieldTest(
            member,
            lowest,
            zero
        );
    }

    DoFlatBufferValueComparerTableFieldTest(
        member,
        zero,
        highest
    );
}

TEST(FlatBufferValueComparerTests, table_primitive_types)
{
    DoFlatBufferValueComparerTableNumericTest(&TestKeyT::bool_value);
    DoFlatBufferValueComparerTableNumericTest(&TestKeyT::byte_value);
    DoFlatBufferValueComparerTableNumericTest(&TestKeyT::double_value);
    DoFlatBufferValueComparerTableNumericTest(&TestKeyT::float_value);
    DoFlatBufferValueComparerTableNumericTest(&TestKeyT::int_value);
    DoFlatBufferValueComparerTableNumericTest(&TestKeyT::long_value);
    DoFlatBufferValueComparerTableNumericTest(&TestKeyT::short_value);
    DoFlatBufferValueComparerTableNumericTest(&TestKeyT::ubyte_value);
    DoFlatBufferValueComparerTableNumericTest(&TestKeyT::uint_value);
    DoFlatBufferValueComparerTableNumericTest(&TestKeyT::ulong_value);
    DoFlatBufferValueComparerTableNumericTest(&TestKeyT::ushort_value);
}

TEST(FlatBufferValueComparerTests, defaulted_value)
{
    TestKeyT low;
    TestKeyT defaulted;
    TestKeyT high;

    low.defaulted_value = 4;
    high.defaulted_value = 6;

    DoFlatBufferValueComparerTest(
        low,
        defaulted);

    DoFlatBufferValueComparerTest(
        low,
        high);

    DoFlatBufferValueComparerTest(
        defaulted,
        high);
}

TEST(FlatBufferValueComparerTests, table_string_type)
{
    DoFlatBufferValueComparerTableFieldTest(
        &TestKeyT::string_value,
        "",
        "a");

    DoFlatBufferValueComparerTableFieldTest(
        &TestKeyT::string_value,
        "aa",
        "aaa");

    DoFlatBufferValueComparerTableFieldTest(
        &TestKeyT::string_value,
        "aa",
        "b");
}

TEST(FlatBufferValueComparerTests, table_subtable)
{
    TestKeyT low;
    TestKeyT high;

    low.byte_value = -1;
    high.byte_value = 1;

    DoFlatBufferValueComparerTableFieldTest(
        &TestKeyT::subkey_value,
        copy_unique(low),
        copy_unique(high));

    DoFlatBufferValueComparerTableFieldTest(
        &TestKeyT::subkey_value,
        nullptr,
        copy_unique(low));
}

template<
    typename Type
> void DoFlatBufferValueComparerVectorTest(
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

    DoFlatBufferValueComparerTest(
        key_empty,
        key_l);

    DoFlatBufferValueComparerTest(
        key_empty,
        key_l_l);

    DoFlatBufferValueComparerTest(
        key_empty,
        key_l_h);

    DoFlatBufferValueComparerTest(
        key_empty,
        key_h);

    DoFlatBufferValueComparerTest(
        key_empty,
        key_h_l);

    DoFlatBufferValueComparerTest(
        key_empty,
        key_h_h);


    DoFlatBufferValueComparerTest(
        key_l,
        key_l_l);

    DoFlatBufferValueComparerTest(
        key_l,
        key_l_h);

    DoFlatBufferValueComparerTest(
        key_l,
        key_h);

    DoFlatBufferValueComparerTest(
        key_l,
        key_h_l);

    DoFlatBufferValueComparerTest(
        key_l,
        key_h_h);


    DoFlatBufferValueComparerTest(
        key_l_l,
        key_l_h);

    DoFlatBufferValueComparerTest(
        key_l_l,
        key_h);

    DoFlatBufferValueComparerTest(
        key_l_l,
        key_h_l);

    DoFlatBufferValueComparerTest(
        key_l_l,
        key_h_h);


    DoFlatBufferValueComparerTest(
        key_l_h,
        key_h);

    DoFlatBufferValueComparerTest(
        key_l_h,
        key_h_l);

    DoFlatBufferValueComparerTest(
        key_l_h,
        key_h_h);


    DoFlatBufferValueComparerTest(
        key_h,
        key_h_l);

    DoFlatBufferValueComparerTest(
        key_h,
        key_h_h);


    DoFlatBufferValueComparerTest(
        key_h_l,
        key_h_h);
}

template<
    typename Type
> void DoFlatBufferValueComparerNumericVectorTest(
    std::vector<Type> TestKeyT::* member
)
{
    auto lowValue = std::numeric_limits<Type>::lowest();
    auto highValue = std::numeric_limits<Type>::max();
    return DoFlatBufferValueComparerVectorTest(
        member,
        [&]() { return lowValue; },
        [&]() { return highValue; }
    );
}

TEST(FlatBufferValueComparerTests, primitive_vector_types)
{
    DoFlatBufferValueComparerNumericVectorTest(&TestKeyT::bool_vector);
    DoFlatBufferValueComparerNumericVectorTest(&TestKeyT::byte_vector);
    DoFlatBufferValueComparerNumericVectorTest(&TestKeyT::double_vector);
    DoFlatBufferValueComparerNumericVectorTest(&TestKeyT::float_vector);
    DoFlatBufferValueComparerNumericVectorTest(&TestKeyT::int_vector);
    DoFlatBufferValueComparerNumericVectorTest(&TestKeyT::long_vector);
    DoFlatBufferValueComparerNumericVectorTest(&TestKeyT::short_vector);
    DoFlatBufferValueComparerNumericVectorTest(&TestKeyT::ubyte_vector);
    DoFlatBufferValueComparerNumericVectorTest(&TestKeyT::uint_vector);
    DoFlatBufferValueComparerNumericVectorTest(&TestKeyT::ulong_vector);
    DoFlatBufferValueComparerNumericVectorTest(&TestKeyT::ushort_vector);
}

TEST(FlatBufferValueComparerTests, string_vector)
{
    DoFlatBufferValueComparerVectorTest(
        &TestKeyT::string_vector,
        []() { return "aaa"; },
        []() { return "aab"; }
    );

    DoFlatBufferValueComparerVectorTest(
        &TestKeyT::string_vector,
        []() { return "aa"; },
        []() { return "aaa"; }
    );
}

TEST(FlatBufferValueComparerTests, subkey_vector)
{
    TestKeyT low;
    TestKeyT high;

    low.byte_value = -1;
    high.byte_value = 1;

    DoFlatBufferValueComparerVectorTest(
        &TestKeyT::subkey_vector,
        [&]() { return copy_unique(low); },
        [&]() { return copy_unique(high); }
    );
}

TEST(FlatBufferValueComparerTests, struct_vector)
{
    TestKeyStruct low;
    TestKeyStruct high;

    low.mutate_byte_value(-1);
    high.mutate_byte_value(1);

    DoFlatBufferValueComparerVectorTest(
        &TestKeyT::struct_vector,
        [&]() { return low; },
        [&]() { return high; }
    );
}

template<
    typename Type
>
void DoFlatBufferValueComparerStructNumericTest(
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
        DoFlatBufferValueComparerTest(
            low,
            zero);
    }

    DoFlatBufferValueComparerTest(
        low,
        high);

    DoFlatBufferValueComparerTest(
        zero,
        high);
}

TEST(FlatBufferValueComparerTests, struct_primitive_types)
{
    DoFlatBufferValueComparerStructNumericTest(&TestKeyStruct::mutate_bool_value);
    DoFlatBufferValueComparerStructNumericTest(&TestKeyStruct::mutate_byte_value);
    DoFlatBufferValueComparerStructNumericTest(&TestKeyStruct::mutate_double_value);
    DoFlatBufferValueComparerStructNumericTest(&TestKeyStruct::mutate_float_value);
    DoFlatBufferValueComparerStructNumericTest(&TestKeyStruct::mutate_int_value);
    DoFlatBufferValueComparerStructNumericTest(&TestKeyStruct::mutate_long_value);
    DoFlatBufferValueComparerStructNumericTest(&TestKeyStruct::mutate_short_value);
    DoFlatBufferValueComparerStructNumericTest(&TestKeyStruct::mutate_ubyte_value);
    DoFlatBufferValueComparerStructNumericTest(&TestKeyStruct::mutate_uint_value);
    DoFlatBufferValueComparerStructNumericTest(&TestKeyStruct::mutate_ulong_value);
    DoFlatBufferValueComparerStructNumericTest(&TestKeyStruct::mutate_ushort_value);
}

template<
    typename Type,
    uint16_t Length
>
void DoFlatBufferValueComparerStructNumericArrayTest(
    ::flatbuffers::Array<Type, Length>* (TestKeyStruct::* mutableArrayAccessor)()
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

    (low.struct_value.get()->*mutableArrayAccessor)()->Mutate(1, lowValue);
    (high.struct_value.get()->*mutableArrayAccessor)()->Mutate(1, highValue);

    if (lowValue != 0)
    {
        DoFlatBufferValueComparerTest(
            low,
            zero);
    }

    DoFlatBufferValueComparerTest(
        low,
        high);

    DoFlatBufferValueComparerTest(
        zero,
        high);
}

TEST(FlatBufferValueComparerTests, struct_array_types)
{
    DoFlatBufferValueComparerStructNumericArrayTest(&TestKeyStruct::mutable_byte_array);
    DoFlatBufferValueComparerStructNumericArrayTest(&TestKeyStruct::mutable_ubyte_array);
    DoFlatBufferValueComparerStructNumericArrayTest(&TestKeyStruct::mutable_bool_array);
    DoFlatBufferValueComparerStructNumericArrayTest(&TestKeyStruct::mutable_short_array);
    DoFlatBufferValueComparerStructNumericArrayTest(&TestKeyStruct::mutable_ushort_array);
    DoFlatBufferValueComparerStructNumericArrayTest(&TestKeyStruct::mutable_int_array);
    DoFlatBufferValueComparerStructNumericArrayTest(&TestKeyStruct::mutable_uint_array);
    DoFlatBufferValueComparerStructNumericArrayTest(&TestKeyStruct::mutable_float_array);
    DoFlatBufferValueComparerStructNumericArrayTest(&TestKeyStruct::mutable_long_array);
    DoFlatBufferValueComparerStructNumericArrayTest(&TestKeyStruct::mutable_ulong_array);
    DoFlatBufferValueComparerStructNumericArrayTest(&TestKeyStruct::mutable_double_array);
}

TEST(FlatBufferValueComparerTests, table_primitive_descending)
{
    DoFlatBufferValueComparerTableFieldTest(
        &TestKeyT::descending_value,
        5,
        4
    );
}

TEST(FlatBufferValueComparerTests, struct_primitive_descending)
{
    TestKeyT low;
    TestKeyT high;

    low.struct_value = std::make_unique<TestKeyStruct>();
    high.struct_value = std::make_unique<TestKeyStruct>();

    low.descending_value = 5;
    high.descending_value = 4;

    DoFlatBufferValueComparerTest(
        low,
        high
    );
}

TEST(FlatBufferValueComparerTests, descending_table)
{
    TestKeyT low;
    TestKeyT high;

    low.descending_table = std::make_unique<TestKeyDescendingTableT>();
    high.descending_table = std::make_unique<TestKeyDescendingTableT>();

    low.descending_table->ascending_value = 5;
    high.descending_table->ascending_value = 4;

    DoFlatBufferValueComparerTest(
        low,
        high
    );

    low.descending_table = std::make_unique<TestKeyDescendingTableT>();
    high.descending_table = std::make_unique<TestKeyDescendingTableT>();

    low.descending_table->descending_value = 4;
    high.descending_table->descending_value = 5;

    DoFlatBufferValueComparerTest(
        low,
        high
    );
}

TEST(FlatBufferValueComparerTests, union_value)
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

    DoFlatBufferValueComparerTest(
        no_value,
        low_scalar
    );
    
    DoFlatBufferValueComparerTest(
        no_value,
        high_scalar
    );

    DoFlatBufferValueComparerTest(
        no_value,
        low_testKey
    );

    DoFlatBufferValueComparerTest(
        no_value,
        high_testKey
    );


    DoFlatBufferValueComparerTest(
        low_scalar,
        high_scalar
    );

    DoFlatBufferValueComparerTest(
        low_scalar,
        low_testKey
    );

    DoFlatBufferValueComparerTest(
        low_scalar,
        high_testKey
    );


    DoFlatBufferValueComparerTest(
        high_scalar,
        low_testKey
    );

    DoFlatBufferValueComparerTest(
        high_scalar,
        high_testKey
    );


    DoFlatBufferValueComparerTest(
        low_testKey,
        high_testKey
    );
}

TEST(FlatBufferValueComparerTests, IsPrefixOf_uses_up_to_that_field_when_no_trailing_fields)
{
    auto keyComparer = GetTestKeyFlatBufferValueComparer();

    FlatBuffers::TestKeyT prefixKey;
    FlatBuffers::TestKeyT isPrefixKey;
    FlatBuffers::TestKeyT isNotPrefixKey;

    prefixKey.short_value = 2;
    isPrefixKey.short_value = 2;
    isNotPrefixKey.short_value = 3;

    Prefix prefix{ ProtoValue { &prefixKey }, 4 };

    EXPECT_EQ(true, keyComparer.IsPrefixOf(prefix, ProtoValue{ &prefixKey }));
    EXPECT_EQ(true, keyComparer.IsPrefixOf(prefix, ProtoValue{ &isPrefixKey }));
    EXPECT_EQ(false, keyComparer.IsPrefixOf(prefix, ProtoValue{ &isNotPrefixKey }));
}

TEST(FlatBufferValueComparerTests, IsPrefixOf_uses_up_to_that_field_when_has_trailing_fields_in_target_key)
{
    auto keyComparer = GetTestKeyFlatBufferValueComparer();

    FlatBuffers::TestKeyT prefixKey;
    FlatBuffers::TestKeyT isPrefixKey;
    FlatBuffers::TestKeyT isNotPrefixKey;

    prefixKey.short_value = 2;
    isPrefixKey.short_value = 2;
    isPrefixKey.ushort_value = 3;
    isNotPrefixKey.short_value = 4;

    Prefix prefix{ ProtoValue { &prefixKey }, 4 };

    EXPECT_EQ(true, keyComparer.IsPrefixOf(prefix, ProtoValue{ &prefixKey }));
    EXPECT_EQ(true, keyComparer.IsPrefixOf(prefix, ProtoValue{ &isPrefixKey }));
    EXPECT_EQ(false, keyComparer.IsPrefixOf(prefix, ProtoValue{ &isNotPrefixKey }));
}

TEST(FlatBufferValueComparerTests, IsPrefixOf_uses_up_to_that_field_when_has_trailing_fields_in_source_key)
{
    auto keyComparer = GetTestKeyFlatBufferValueComparer();

    FlatBuffers::TestKeyT prefixKey;
    FlatBuffers::TestKeyT isPrefixKey;
    FlatBuffers::TestKeyT isNotPrefixKey;

    prefixKey.short_value = 2;
    prefixKey.ushort_value = 3;
    isPrefixKey.short_value = 2;
    isNotPrefixKey.short_value = 4;

    Prefix prefix{ ProtoValue { &prefixKey }, 4 };

    EXPECT_EQ(true, keyComparer.IsPrefixOf(prefix, ProtoValue{ &prefixKey }));
    EXPECT_EQ(true, keyComparer.IsPrefixOf(prefix, ProtoValue{ &isPrefixKey }));
    EXPECT_EQ(false, keyComparer.IsPrefixOf(prefix, ProtoValue{ &isNotPrefixKey }));
}

TEST(FlatBufferValueComparerTests, IsPrefixOf_uses_up_to_that_field_when_has_trailing_fields_in_both_keys)
{
    auto keyComparer = GetTestKeyFlatBufferValueComparer();

    FlatBuffers::TestKeyT prefixKey;
    FlatBuffers::TestKeyT isPrefixKey;
    FlatBuffers::TestKeyT isNotPrefixKey;

    prefixKey.short_value = 2;
    prefixKey.ushort_value = 3;
    isPrefixKey.short_value = 2;
    isPrefixKey.ushort_value = 3;
    isNotPrefixKey.short_value = 4;

    Prefix prefix{ ProtoValue { &prefixKey }, 4 };

    EXPECT_EQ(true, keyComparer.IsPrefixOf(prefix, ProtoValue{ &prefixKey }));
    EXPECT_EQ(true, keyComparer.IsPrefixOf(prefix, ProtoValue{ &isPrefixKey }));
    EXPECT_EQ(false, keyComparer.IsPrefixOf(prefix, ProtoValue{ &isNotPrefixKey }));
}

}

