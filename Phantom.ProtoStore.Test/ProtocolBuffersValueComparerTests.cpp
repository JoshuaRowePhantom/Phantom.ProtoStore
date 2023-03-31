#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/ProtocolBuffersValueComparer.h"
#include "ProtoStoreTest.pb.h"

namespace Phantom::ProtoStore
{

template<typename T>
void DoProtocolBuffersValueComparerTest(
    const T& lesser,
    const T& greater)
{
    ProtocolBuffersValueComparer keyComparer(
        T::GetDescriptor());

    auto lesserProto = ProtoValue(&lesser).pack();
    auto greaterProto = ProtoValue(&greater).pack();

    EXPECT_EQ(std::weak_ordering::less, keyComparer.Compare(ProtoValue::KeyMin(), lesserProto));
    EXPECT_EQ(std::weak_ordering::less, keyComparer.Compare(ProtoValue::KeyMin(), greaterProto));
    EXPECT_EQ(std::weak_ordering::greater, keyComparer.Compare(lesserProto, ProtoValue::KeyMin()));
    EXPECT_EQ(std::weak_ordering::greater, keyComparer.Compare(greaterProto, ProtoValue::KeyMin()));

    EXPECT_EQ(std::weak_ordering::greater, keyComparer.Compare(ProtoValue::KeyMax(), lesserProto));
    EXPECT_EQ(std::weak_ordering::greater, keyComparer.Compare(ProtoValue::KeyMax(), greaterProto));
    EXPECT_EQ(std::weak_ordering::less, keyComparer.Compare(lesserProto, ProtoValue::KeyMax()));
    EXPECT_EQ(std::weak_ordering::less, keyComparer.Compare(greaterProto, ProtoValue::KeyMax()));

    EXPECT_EQ(std::weak_ordering::less, keyComparer.Compare(lesserProto, greaterProto));
    EXPECT_EQ(std::weak_ordering::greater, keyComparer.Compare(greaterProto, lesserProto));
    EXPECT_EQ(std::weak_ordering::equivalent, keyComparer.Compare(lesserProto, lesserProto));
    EXPECT_EQ(std::weak_ordering::equivalent, keyComparer.Compare(greaterProto, greaterProto));

    EXPECT_EQ(true, keyComparer.Equals(lesserProto, lesserProto));
    EXPECT_EQ(false, keyComparer.Equals(lesserProto, greaterProto));
    EXPECT_EQ(false, keyComparer.Equals(greaterProto, lesserProto));

    EXPECT_LE(
        lesserProto.as_aligned_message_if().Payload.size(),
        keyComparer.GetEstimatedSize(lesserProto));

    EXPECT_LE(
        greaterProto.as_aligned_message_if().Payload.size(),
        keyComparer.GetEstimatedSize(greaterProto));
}

TEST(ProtocolBuffersValueComparerTests, TestKey_int32)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_int32_value(0);
    greater.set_int32_value(2);

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);


    lesser.set_int32_value(1);
    greater.set_int32_value(2);

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);
}

TEST(ProtocolBuffersValueComparerTests, TestKey_int64)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_int64_value(0);
    greater.set_int64_value(2);

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);

    lesser.set_int64_value(1);
    greater.set_int64_value(2);

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);
}

TEST(ProtocolBuffersValueComparerTests, TestKey_sint32)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_sint32_value(0);
    greater.set_sint32_value(2);

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);

    lesser.set_sint32_value(1);
    greater.set_sint32_value(2);

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);
}

TEST(ProtocolBuffersValueComparerTests, TestKey_sint64)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_sint64_value(0);
    greater.set_sint64_value(2);

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);

    lesser.set_sint64_value(0);
    greater.set_sint64_value(2);

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);
}

TEST(ProtocolBuffersValueComparerTests, TestKey_fixed32)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_fixed32_value(0);
    greater.set_fixed32_value(2);

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);

    lesser.set_fixed32_value(1);
    greater.set_fixed32_value(2);

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);
}

TEST(ProtocolBuffersValueComparerTests, TestKey_fixed64)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_fixed64_value(0);
    greater.set_fixed64_value(2);

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);

    lesser.set_fixed64_value(1);
    greater.set_fixed64_value(2);

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);
}

TEST(ProtocolBuffersValueComparerTests, TestKey_float)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_float_value(0);
    greater.set_float_value(2);

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);

    lesser.set_float_value(1);
    greater.set_float_value(2);

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);
}

TEST(ProtocolBuffersValueComparerTests, TestKey_double)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_double_value(0);
    greater.set_double_value(2);

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);

    lesser.set_double_value(1);
    greater.set_double_value(2);

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);
}

TEST(ProtocolBuffersValueComparerTests, TestKey_string)
{
    TestKey lesser;
    TestKey greater;

    greater.set_string_value("b");

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);

    lesser.set_string_value("a");

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);
}

TEST(ProtocolBuffersValueComparerTests, TestKey_bytes)
{
    TestKey lesser;
    TestKey greater;

    greater.set_bytes_value("b");

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);

    lesser.set_bytes_value("a");

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);
}

TEST(ProtocolBuffersValueComparerTests, TestKey_subkey)
{
    TestKey lesser;
    TestKey greater;

    greater.mutable_subkey_value()->set_string_value("b");

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);

    lesser.mutable_subkey_value()->set_string_value("a");

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);
}

TEST(ProtocolBuffersValueComparerTests, TestKey_repeated_int32)
{
    TestKey lesser;
    TestKey greater;

    greater.mutable_repeated_int32_value()->Add(1);

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);

    lesser.mutable_repeated_int32_value()->Add(0);

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);
}

TEST(ProtocolBuffersValueComparerTests, TestKey_repeated_string)
{
    TestKey lesser;
    TestKey greater;

    *greater.add_repeated_string_value() = "b";

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);

    *lesser.add_repeated_string_value() = "a";

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);

    lesser = {};
    *lesser.add_repeated_string_value() = "a";
    greater = {};
    *greater.add_repeated_string_value() = "a";
    *greater.add_repeated_string_value() = "b";

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);
}

TEST(ProtocolBuffersValueComparerTests, TestKey_repeated_SubKey)
{
    TestKey lesser;
    TestKey greater;

    greater.add_repeated_subkey_value()->set_string_value("b");

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);

    lesser.add_repeated_subkey_value()->set_string_value("a");

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);
}
//
//TEST(ProtocolBuffersValueComparerTests, Uses_lexical_order_not_tag_order)
//{
//    TestKey_OutOfOrderFields lesser;
//    TestKey_OutOfOrderFields greater;
//
//    lesser.set_lexicallyfirstnumericallysecond(1);
//    lesser.set_lexicallysecondnumericallyfirst(2);
//    greater.set_lexicallyfirstnumericallysecond(2);
//    greater.set_lexicallysecondnumericallyfirst(1);
//
//    DoProtocolBuffersValueComparerTest(
//        lesser,
//        greater);
//}

TEST(ProtocolBuffersValueComparerTests, Uses_message_level_sort_order_on_outer_key)
{
    TestKey_DescendingSortOrder_Field lesser;
    TestKey_DescendingSortOrder_Field greater;

    lesser.set_value(2);
    greater.set_value(1);

    DoProtocolBuffersValueComparerTest(
        lesser,
        greater);
}

TEST(ProtocolBuffersValueComparerTests, Compare_KeyMin_and_KeyMax)
{
    ProtocolBuffersValueComparer keyComparer(
        TestKey::GetDescriptor());

    EXPECT_EQ(std::weak_ordering::equivalent, keyComparer.Compare(ProtoValue::KeyMin(), ProtoValue::KeyMin()));
    EXPECT_EQ(std::weak_ordering::less, keyComparer.Compare(ProtoValue::KeyMin(), ProtoValue::KeyMax()));
    EXPECT_EQ(std::weak_ordering::equivalent, keyComparer.Compare(ProtoValue::KeyMax(), ProtoValue::KeyMax()));
    EXPECT_EQ(std::weak_ordering::greater, keyComparer.Compare(ProtoValue::KeyMax(), ProtoValue::KeyMin()));
}

TEST(ProtocolBuffersValueComparerTests, IsPrefixOf_uses_up_to_that_field_when_no_trailing_fields)
{
    ProtocolBuffersValueComparer keyComparer(
        TestKey::GetDescriptor());

    TestKey prefixKey;
    TestKey isPrefixKey;
    TestKey isNotPrefixKey;

    prefixKey.set_sint32_value(2);
    isPrefixKey.set_sint32_value(2);
    isNotPrefixKey.set_sint32_value(3);

    Prefix prefix{ ProtoValue { &prefixKey }.pack(), 3};

    EXPECT_EQ(true, keyComparer.IsPrefixOf(prefix, ProtoValue{ &prefixKey }.pack()));
    EXPECT_EQ(true, keyComparer.IsPrefixOf(prefix, ProtoValue{ &isPrefixKey }.pack()));
    EXPECT_EQ(false, keyComparer.IsPrefixOf(prefix, ProtoValue{ &isNotPrefixKey }.pack()));
}

TEST(ProtocolBuffersValueComparerTests, IsPrefixOf_uses_up_to_that_field_when_has_trailing_fields_in_target_key)
{
    ProtocolBuffersValueComparer keyComparer(
        TestKey::GetDescriptor());

    TestKey prefixKey;
    TestKey isPrefixKey;
    TestKey isNotPrefixKey;

    prefixKey.set_sint32_value(2);
    isPrefixKey.set_sint32_value(2);
    isPrefixKey.set_sint64_value(3);
    isNotPrefixKey.set_sint32_value(4);

    Prefix prefix{ ProtoValue { &prefixKey }.pack(), 3 };

    EXPECT_EQ(true, keyComparer.IsPrefixOf(prefix, ProtoValue{ &prefixKey }.pack()));
    EXPECT_EQ(true, keyComparer.IsPrefixOf(prefix, ProtoValue{ &isPrefixKey }.pack()));
    EXPECT_EQ(false, keyComparer.IsPrefixOf(prefix, ProtoValue{ &isNotPrefixKey }.pack()));
}

TEST(ProtocolBuffersValueComparerTests, IsPrefixOf_uses_up_to_that_field_when_has_trailing_fields_in_source_key)
{
    ProtocolBuffersValueComparer keyComparer(
        TestKey::GetDescriptor());

    TestKey prefixKey;
    TestKey isPrefixKey;
    TestKey isNotPrefixKey;

    prefixKey.set_sint32_value(2);
    prefixKey.set_sint64_value(3);
    isPrefixKey.set_sint32_value(2);
    isNotPrefixKey.set_sint32_value(4);

    Prefix prefix{ ProtoValue { &prefixKey }.pack(), 3 };

    EXPECT_EQ(true, keyComparer.IsPrefixOf(prefix, ProtoValue{ &prefixKey }.pack()));
    EXPECT_EQ(true, keyComparer.IsPrefixOf(prefix, ProtoValue{ &isPrefixKey }.pack()));
    EXPECT_EQ(false, keyComparer.IsPrefixOf(prefix, ProtoValue{ &isNotPrefixKey }.pack()));
}

TEST(ProtocolBuffersValueComparerTests, IsPrefixOf_uses_up_to_that_field_when_has_trailing_fields_in_both_keys)
{
    ProtocolBuffersValueComparer keyComparer(
        TestKey::GetDescriptor());

    TestKey prefixKey;
    TestKey isPrefixKey;
    TestKey isNotPrefixKey;

    prefixKey.set_sint32_value(2);
    prefixKey.set_sint64_value(3);
    isPrefixKey.set_sint32_value(2);
    isPrefixKey.set_sint64_value(3);
    isNotPrefixKey.set_sint32_value(4);

    Prefix prefix{ ProtoValue { &prefixKey }.pack(), 3 };

    EXPECT_EQ(true, keyComparer.IsPrefixOf(prefix, ProtoValue{ &prefixKey }.pack()));
    EXPECT_EQ(true, keyComparer.IsPrefixOf(prefix, ProtoValue{ &isPrefixKey }.pack()));
    EXPECT_EQ(false, keyComparer.IsPrefixOf(prefix, ProtoValue{ &isNotPrefixKey }.pack()));
}

}