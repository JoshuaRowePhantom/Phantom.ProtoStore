#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/KeyComparer.h"
#include "ProtoStoreTest.pb.h"

namespace Phantom::ProtoStore
{

template<typename T>
void DoProtoKeyComparerTest(
    const T& lesser,
    const T& greater)
{
    ProtoKeyComparer keyComparer(
        T::GetDescriptor());

    ProtoValue lesserProto(&lesser, true);
    ProtoValue greaterProto(&greater, true);

    auto lesserSpan = lesserProto.as_bytes_if();
    auto greaterSpan = greaterProto.as_bytes_if();

    EXPECT_EQ(std::weak_ordering::less, keyComparer.Compare(lesserSpan, greaterSpan));
    EXPECT_EQ(std::weak_ordering::greater, keyComparer.Compare(greaterSpan, lesserSpan));
    EXPECT_EQ(std::weak_ordering::equivalent, keyComparer.Compare(lesserSpan, lesserSpan));
    EXPECT_EQ(std::weak_ordering::equivalent, keyComparer.Compare(greaterSpan, greaterSpan));
}

template<typename TKey, typename TValue>
void DoKeyAndSequenceNumberComparerTest(
    const TValue& lesser,
    const TValue& greater)
{
    ProtoKeyComparer keyComparer(
        TKey::GetDescriptor());
    KeyAndSequenceNumberComparer keyAndSequenceNumberComparer(
        keyComparer);

    EXPECT_EQ(std::weak_ordering::less, keyAndSequenceNumberComparer(lesser, greater));
    EXPECT_EQ(std::weak_ordering::greater, keyAndSequenceNumberComparer(greater, lesser));
    EXPECT_EQ(std::weak_ordering::equivalent, keyAndSequenceNumberComparer(lesser, lesser));
    EXPECT_EQ(std::weak_ordering::equivalent, keyAndSequenceNumberComparer(greater, greater));
}

template<typename TKey, typename TValue1, typename TValue2>
void DoKeyRangeComparerTestNotEquivalent(
    const TValue1& lesser,
    const TValue2& greater)
{
    ProtoKeyComparer keyComparer(
        TKey::GetDescriptor());
    KeyRangeComparer keyAndSequenceNumberComparer(
        keyComparer);

    EXPECT_EQ(std::weak_ordering::less, keyAndSequenceNumberComparer(lesser, greater));
    EXPECT_EQ(std::weak_ordering::greater, keyAndSequenceNumberComparer(greater, lesser));
}

TEST(ProtoKeyComparerTests, TestKey_int32)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_int32_value(0);
    greater.set_int32_value(2);

    DoProtoKeyComparerTest(
        lesser,
        greater);


    lesser.set_int32_value(1);
    greater.set_int32_value(2);

    DoProtoKeyComparerTest(
        lesser,
        greater);
}

TEST(ProtoKeyComparerTests, TestKey_int64)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_int64_value(0);
    greater.set_int64_value(2);

    DoProtoKeyComparerTest(
        lesser,
        greater);

    lesser.set_int64_value(1);
    greater.set_int64_value(2);

    DoProtoKeyComparerTest(
        lesser,
        greater);
}

TEST(ProtoKeyComparerTests, TestKey_sint32)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_sint32_value(0);
    greater.set_sint32_value(2);

    DoProtoKeyComparerTest(
        lesser,
        greater);

    lesser.set_sint32_value(1);
    greater.set_sint32_value(2);

    DoProtoKeyComparerTest(
        lesser,
        greater);
}

TEST(ProtoKeyComparerTests, TestKey_sint64)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_sint64_value(0);
    greater.set_sint64_value(2);

    DoProtoKeyComparerTest(
        lesser,
        greater);

    lesser.set_sint64_value(0);
    greater.set_sint64_value(2);

    DoProtoKeyComparerTest(
        lesser,
        greater);
}

TEST(ProtoKeyComparerTests, TestKey_fixed32)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_fixed32_value(0);
    greater.set_fixed32_value(2);

    DoProtoKeyComparerTest(
        lesser,
        greater);

    lesser.set_fixed32_value(1);
    greater.set_fixed32_value(2);

    DoProtoKeyComparerTest(
        lesser,
        greater);
}

TEST(ProtoKeyComparerTests, TestKey_fixed64)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_fixed64_value(0);
    greater.set_fixed64_value(2);

    DoProtoKeyComparerTest(
        lesser,
        greater);

    lesser.set_fixed64_value(1);
    greater.set_fixed64_value(2);

    DoProtoKeyComparerTest(
        lesser,
        greater);
}

TEST(ProtoKeyComparerTests, TestKey_float)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_float_value(0);
    greater.set_float_value(2);

    DoProtoKeyComparerTest(
        lesser,
        greater);

    lesser.set_float_value(1);
    greater.set_float_value(2);

    DoProtoKeyComparerTest(
        lesser,
        greater);
}

TEST(ProtoKeyComparerTests, TestKey_double)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_double_value(0);
    greater.set_double_value(2);

    DoProtoKeyComparerTest(
        lesser,
        greater);

    lesser.set_double_value(1);
    greater.set_double_value(2);

    DoProtoKeyComparerTest(
        lesser,
        greater);
}

TEST(ProtoKeyComparerTests, TestKey_string)
{
    TestKey lesser;
    TestKey greater;

    greater.set_string_value("b");

    DoProtoKeyComparerTest(
        lesser,
        greater);

    lesser.set_string_value("a");

    DoProtoKeyComparerTest(
        lesser,
        greater);
}

TEST(ProtoKeyComparerTests, TestKey_bytes)
{
    TestKey lesser;
    TestKey greater;

    greater.set_bytes_value("b");

    DoProtoKeyComparerTest(
        lesser,
        greater);

    lesser.set_bytes_value("a");

    DoProtoKeyComparerTest(
        lesser,
        greater);
}

TEST(ProtoKeyComparerTests, TestKey_subkey)
{
    TestKey lesser;
    TestKey greater;

    greater.mutable_subkey_value()->set_string_value("b");

    DoProtoKeyComparerTest(
        lesser,
        greater);

    lesser.mutable_subkey_value()->set_string_value("a");

    DoProtoKeyComparerTest(
        lesser,
        greater);
}

TEST(ProtoKeyComparerTests, TestKey_repeated_int32)
{
    TestKey lesser;
    TestKey greater;

    greater.mutable_repeated_int32_value()->Add(1);

    DoProtoKeyComparerTest(
        lesser,
        greater);

    lesser.mutable_repeated_int32_value()->Add(0);

    DoProtoKeyComparerTest(
        lesser,
        greater);
}

TEST(ProtoKeyComparerTests, TestKey_repeated_string)
{
    TestKey lesser;
    TestKey greater;

    *greater.add_repeated_string_value() = "b";

    DoProtoKeyComparerTest(
        lesser,
        greater);

    *lesser.add_repeated_string_value() = "a";

    DoProtoKeyComparerTest(
        lesser,
        greater);

    lesser = {};
    *lesser.add_repeated_string_value() = "a";
    greater = {};
    *greater.add_repeated_string_value() = "a";
    *greater.add_repeated_string_value() = "b";

    DoProtoKeyComparerTest(
        lesser,
        greater);
}

TEST(ProtoKeyComparerTests, TestKey_repeated_SubKey)
{
    TestKey lesser;
    TestKey greater;

    greater.add_repeated_subkey_value()->set_string_value("b");

    DoProtoKeyComparerTest(
        lesser,
        greater);

    lesser.add_repeated_subkey_value()->set_string_value("a");

    DoProtoKeyComparerTest(
        lesser,
        greater);
}
//
//TEST(ProtoKeyComparerTests, Uses_lexical_order_not_tag_order)
//{
//    TestKey_OutOfOrderFields lesser;
//    TestKey_OutOfOrderFields greater;
//
//    lesser.set_lexicallyfirstnumericallysecond(1);
//    lesser.set_lexicallysecondnumericallyfirst(2);
//    greater.set_lexicallyfirstnumericallysecond(2);
//    greater.set_lexicallysecondnumericallyfirst(1);
//
//    DoProtoKeyComparerTest(
//        lesser,
//        greater);
//}

TEST(ProtoKeyComparerTests, Uses_message_level_sort_order_on_outer_key)
{
    TestKey_DescendingSortOrder_Field lesser;
    TestKey_DescendingSortOrder_Field greater;

    lesser.set_value(2);
    greater.set_value(1);

    DoProtoKeyComparerTest(
        lesser,
        greater);
}

TEST(KeyAndSequenceNumberComparerTests, Uses_key_order_first)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_int32_value(0);
    greater.set_int32_value(1);

    ProtoValue lesserProtoValue(&lesser, true);
    ProtoValue greaterProtoValue(&greater, true);

    DoKeyAndSequenceNumberComparerTest<TestKey>(
        KeyAndSequenceNumberComparerArgument { lesserProtoValue.as_bytes_if(), SequenceNumber::Earliest},
        KeyAndSequenceNumberComparerArgument { greaterProtoValue.as_bytes_if(), SequenceNumber::Latest}
        );
}

TEST(KeyAndSequenceNumberComparerTests, Uses_sequence_number_second)
{
    TestKey lesser;

    lesser.set_int32_value(0);

    ProtoValue lesserProtoValue(&lesser, true);

    DoKeyAndSequenceNumberComparerTest<TestKey>(
        KeyAndSequenceNumberComparerArgument{ lesserProtoValue.as_bytes_if(), SequenceNumber::Latest },
        KeyAndSequenceNumberComparerArgument{ lesserProtoValue.as_bytes_if(), SequenceNumber::Earliest }
        );
}

TEST(KeyRangeComparerTests, Uses_key_order_first)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_int32_value(0);
    greater.set_int32_value(1);

    ProtoValue lesserProtoValue(&lesser, true);
    ProtoValue greaterProtoValue(&greater, true);

    DoKeyRangeComparerTestNotEquivalent<TestKey>(
        KeyRangeComparerArgument(lesserProtoValue.as_bytes_if(), SequenceNumber::Earliest, Inclusivity::Inclusive),
        KeyAndSequenceNumberComparerArgument(greaterProtoValue.as_bytes_if(), SequenceNumber::Latest)
        );
}

TEST(KeyRangeComparerTests, Uses_Inclusivity)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_int32_value(0);
    greater.set_int32_value(0);

    ProtoValue lesserProtoValue(&lesser, true);
    ProtoValue greaterProtoValue(&greater, true);

    DoKeyRangeComparerTestNotEquivalent<TestKey>(
        KeyAndSequenceNumberComparerArgument(lesserProtoValue.as_bytes_if(), SequenceNumber::Earliest),
        KeyRangeComparerArgument(greaterProtoValue.as_bytes_if(), SequenceNumber::Earliest, Inclusivity::Exclusive)
        );
}

TEST(KeyRangeComparerTests, Uses_sequence_number_second)
{
    TestKey lesser;

    lesser.set_int32_value(0);

    ProtoValue lesserProtoValue(&lesser, true);

    DoKeyRangeComparerTestNotEquivalent<TestKey>(
        KeyAndSequenceNumberComparerArgument(lesserProtoValue.as_bytes_if(), SequenceNumber::Latest),
        KeyRangeComparerArgument(lesserProtoValue.as_bytes_if(), SequenceNumber::Earliest, Inclusivity::Inclusive)
        );
}

}