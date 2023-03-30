#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/ProtocolBuffersValueComparer.h"
#include "ProtoStoreTest.pb.h"

namespace Phantom::ProtoStore
{

template<typename T>
void DoProtoValueComparerTest(
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

template<typename TKey, typename TValue>
void DoKeyAndSequenceNumberComparerTest(
    const TValue& lesser,
    const TValue& greater)
{
    ProtocolBuffersValueComparer keyComparer(
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
    ProtocolBuffersValueComparer keyComparer(
        TKey::GetDescriptor());
    KeyRangeComparer keyAndSequenceNumberComparer(
        keyComparer);

    EXPECT_EQ(std::weak_ordering::less, keyAndSequenceNumberComparer(lesser, greater));
    EXPECT_EQ(std::weak_ordering::greater, keyAndSequenceNumberComparer(greater, lesser));
}

TEST(ProtoValueComparerTests, TestKey_int32)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_int32_value(0);
    greater.set_int32_value(2);

    DoProtoValueComparerTest(
        lesser,
        greater);


    lesser.set_int32_value(1);
    greater.set_int32_value(2);

    DoProtoValueComparerTest(
        lesser,
        greater);
}

TEST(ProtoValueComparerTests, TestKey_int64)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_int64_value(0);
    greater.set_int64_value(2);

    DoProtoValueComparerTest(
        lesser,
        greater);

    lesser.set_int64_value(1);
    greater.set_int64_value(2);

    DoProtoValueComparerTest(
        lesser,
        greater);
}

TEST(ProtoValueComparerTests, TestKey_sint32)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_sint32_value(0);
    greater.set_sint32_value(2);

    DoProtoValueComparerTest(
        lesser,
        greater);

    lesser.set_sint32_value(1);
    greater.set_sint32_value(2);

    DoProtoValueComparerTest(
        lesser,
        greater);
}

TEST(ProtoValueComparerTests, TestKey_sint64)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_sint64_value(0);
    greater.set_sint64_value(2);

    DoProtoValueComparerTest(
        lesser,
        greater);

    lesser.set_sint64_value(0);
    greater.set_sint64_value(2);

    DoProtoValueComparerTest(
        lesser,
        greater);
}

TEST(ProtoValueComparerTests, TestKey_fixed32)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_fixed32_value(0);
    greater.set_fixed32_value(2);

    DoProtoValueComparerTest(
        lesser,
        greater);

    lesser.set_fixed32_value(1);
    greater.set_fixed32_value(2);

    DoProtoValueComparerTest(
        lesser,
        greater);
}

TEST(ProtoValueComparerTests, TestKey_fixed64)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_fixed64_value(0);
    greater.set_fixed64_value(2);

    DoProtoValueComparerTest(
        lesser,
        greater);

    lesser.set_fixed64_value(1);
    greater.set_fixed64_value(2);

    DoProtoValueComparerTest(
        lesser,
        greater);
}

TEST(ProtoValueComparerTests, TestKey_float)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_float_value(0);
    greater.set_float_value(2);

    DoProtoValueComparerTest(
        lesser,
        greater);

    lesser.set_float_value(1);
    greater.set_float_value(2);

    DoProtoValueComparerTest(
        lesser,
        greater);
}

TEST(ProtoValueComparerTests, TestKey_double)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_double_value(0);
    greater.set_double_value(2);

    DoProtoValueComparerTest(
        lesser,
        greater);

    lesser.set_double_value(1);
    greater.set_double_value(2);

    DoProtoValueComparerTest(
        lesser,
        greater);
}

TEST(ProtoValueComparerTests, TestKey_string)
{
    TestKey lesser;
    TestKey greater;

    greater.set_string_value("b");

    DoProtoValueComparerTest(
        lesser,
        greater);

    lesser.set_string_value("a");

    DoProtoValueComparerTest(
        lesser,
        greater);
}

TEST(ProtoValueComparerTests, TestKey_bytes)
{
    TestKey lesser;
    TestKey greater;

    greater.set_bytes_value("b");

    DoProtoValueComparerTest(
        lesser,
        greater);

    lesser.set_bytes_value("a");

    DoProtoValueComparerTest(
        lesser,
        greater);
}

TEST(ProtoValueComparerTests, TestKey_subkey)
{
    TestKey lesser;
    TestKey greater;

    greater.mutable_subkey_value()->set_string_value("b");

    DoProtoValueComparerTest(
        lesser,
        greater);

    lesser.mutable_subkey_value()->set_string_value("a");

    DoProtoValueComparerTest(
        lesser,
        greater);
}

TEST(ProtoValueComparerTests, TestKey_repeated_int32)
{
    TestKey lesser;
    TestKey greater;

    greater.mutable_repeated_int32_value()->Add(1);

    DoProtoValueComparerTest(
        lesser,
        greater);

    lesser.mutable_repeated_int32_value()->Add(0);

    DoProtoValueComparerTest(
        lesser,
        greater);
}

TEST(ProtoValueComparerTests, TestKey_repeated_string)
{
    TestKey lesser;
    TestKey greater;

    *greater.add_repeated_string_value() = "b";

    DoProtoValueComparerTest(
        lesser,
        greater);

    *lesser.add_repeated_string_value() = "a";

    DoProtoValueComparerTest(
        lesser,
        greater);

    lesser = {};
    *lesser.add_repeated_string_value() = "a";
    greater = {};
    *greater.add_repeated_string_value() = "a";
    *greater.add_repeated_string_value() = "b";

    DoProtoValueComparerTest(
        lesser,
        greater);
}

TEST(ProtoValueComparerTests, TestKey_repeated_SubKey)
{
    TestKey lesser;
    TestKey greater;

    greater.add_repeated_subkey_value()->set_string_value("b");

    DoProtoValueComparerTest(
        lesser,
        greater);

    lesser.add_repeated_subkey_value()->set_string_value("a");

    DoProtoValueComparerTest(
        lesser,
        greater);
}
//
//TEST(ProtoValueComparerTests, Uses_lexical_order_not_tag_order)
//{
//    TestKey_OutOfOrderFields lesser;
//    TestKey_OutOfOrderFields greater;
//
//    lesser.set_lexicallyfirstnumericallysecond(1);
//    lesser.set_lexicallysecondnumericallyfirst(2);
//    greater.set_lexicallyfirstnumericallysecond(2);
//    greater.set_lexicallysecondnumericallyfirst(1);
//
//    DoProtoValueComparerTest(
//        lesser,
//        greater);
//}

TEST(ProtoValueComparerTests, Uses_message_level_sort_order_on_outer_key)
{
    TestKey_DescendingSortOrder_Field lesser;
    TestKey_DescendingSortOrder_Field greater;

    lesser.set_value(2);
    greater.set_value(1);

    DoProtoValueComparerTest(
        lesser,
        greater);
}

TEST(KeyAndSequenceNumberComparerTests, Uses_key_order_first)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_int32_value(0);
    greater.set_int32_value(1);

    auto lesserProtoValue = ProtoValue(&lesser).pack();
    auto greaterProtoValue = ProtoValue(&greater).pack();

    DoKeyAndSequenceNumberComparerTest<TestKey>(
        KeyAndSequenceNumberComparerArgument { lesserProtoValue, SequenceNumber::Earliest},
        KeyAndSequenceNumberComparerArgument { greaterProtoValue, SequenceNumber::Latest}
        );
}

TEST(KeyAndSequenceNumberComparerTests, Uses_sequence_number_second)
{
    TestKey lesser;

    lesser.set_int32_value(0);

    auto lesserProtoValue = ProtoValue(&lesser).pack();

    DoKeyAndSequenceNumberComparerTest<TestKey>(
        KeyAndSequenceNumberComparerArgument{ lesserProtoValue, SequenceNumber::Latest },
        KeyAndSequenceNumberComparerArgument{ lesserProtoValue, SequenceNumber::Earliest }
        );
}

TEST(KeyRangeComparerTests, Uses_key_order_first)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_int32_value(0);
    greater.set_int32_value(1);

    auto lesserProtoValue = ProtoValue(&lesser).pack();
    auto greaterProtoValue = ProtoValue(&greater).pack();

    DoKeyRangeComparerTestNotEquivalent<TestKey>(
        KeyRangeComparerArgument(lesserProtoValue, SequenceNumber::Earliest, Inclusivity::Inclusive),
        KeyAndSequenceNumberComparerArgument(greaterProtoValue, SequenceNumber::Latest)
        );
}

TEST(KeyRangeComparerTests, Uses_Inclusivity)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_int32_value(0);
    greater.set_int32_value(0);

    auto lesserProtoValue = ProtoValue(&lesser).pack();
    auto greaterProtoValue = ProtoValue(&greater).pack();

    DoKeyRangeComparerTestNotEquivalent<TestKey>(
        KeyAndSequenceNumberComparerArgument(lesserProtoValue, SequenceNumber::Earliest),
        KeyRangeComparerArgument(greaterProtoValue, SequenceNumber::Earliest, Inclusivity::Exclusive)
        );
}

TEST(KeyRangeComparerTests, Uses_sequence_number_second)
{
    TestKey lesser;

    lesser.set_int32_value(0);

    auto lesserProtoValue = ProtoValue(&lesser).pack();

    DoKeyRangeComparerTestNotEquivalent<TestKey>(
        KeyAndSequenceNumberComparerArgument(lesserProtoValue, SequenceNumber::Latest),
        KeyRangeComparerArgument(lesserProtoValue, SequenceNumber::Earliest, Inclusivity::Inclusive)
        );
}

TEST(ProtoValueComparerTests, Compare_KeyMin_and_KeyMax)
{
    ProtocolBuffersValueComparer keyComparer(
        TestKey::GetDescriptor());

    EXPECT_EQ(std::weak_ordering::equivalent, keyComparer.Compare(ProtoValue::KeyMin(), ProtoValue::KeyMin()));
    EXPECT_EQ(std::weak_ordering::less, keyComparer.Compare(ProtoValue::KeyMin(), ProtoValue::KeyMax()));
    EXPECT_EQ(std::weak_ordering::equivalent, keyComparer.Compare(ProtoValue::KeyMax(), ProtoValue::KeyMax()));
    EXPECT_EQ(std::weak_ordering::greater, keyComparer.Compare(ProtoValue::KeyMax(), ProtoValue::KeyMin()));
}

}