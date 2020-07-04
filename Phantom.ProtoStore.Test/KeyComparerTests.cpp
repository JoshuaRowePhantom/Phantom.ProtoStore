#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/KeyComparer.h"
#include "ProtoStoreTest.pb.h"

namespace Phantom::ProtoStore
{

template<typename T>
void DoKeyComparerTest(
    const T& lesser,
    const T& greater)
{
    KeyComparer keyComparer(
        T::GetDescriptor());

    ASSERT_EQ(std::weak_ordering::less, keyComparer.Compare(&lesser, &greater));
    ASSERT_EQ(std::weak_ordering::greater, keyComparer.Compare(&greater, &lesser));
    ASSERT_EQ(std::weak_ordering::equivalent, keyComparer.Compare(&lesser, &lesser));
    ASSERT_EQ(std::weak_ordering::equivalent, keyComparer.Compare(&greater, &greater));
}

TEST(KeyComparerTests, TestKey_int32)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_int32_value(0);
    greater.set_int32_value(1);

    DoKeyComparerTest(
        lesser,
        greater);
}

TEST(KeyComparerTests, TestKey_int64)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_int64_value(0);
    greater.set_int64_value(1);

    DoKeyComparerTest(
        lesser,
        greater);
}

TEST(KeyComparerTests, TestKey_sint32)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_sint32_value(0);
    greater.set_sint32_value(1);

    DoKeyComparerTest(
        lesser,
        greater);
}

TEST(KeyComparerTests, TestKey_sint64)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_sint64_value(0);
    greater.set_sint64_value(1);

    DoKeyComparerTest(
        lesser,
        greater);
}

TEST(KeyComparerTests, TestKey_fixed32)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_fixed32_value(0);
    greater.set_fixed32_value(1);

    DoKeyComparerTest(
        lesser,
        greater);
}

TEST(KeyComparerTests, TestKey_fixed64)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_fixed64_value(0);
    greater.set_fixed64_value(1);

    DoKeyComparerTest(
        lesser,
        greater);
}

TEST(KeyComparerTests, TestKey_float)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_float_value(0);
    greater.set_float_value(1);

    DoKeyComparerTest(
        lesser,
        greater);
}

TEST(KeyComparerTests, TestKey_double)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_double_value(0);
    greater.set_double_value(1);

    DoKeyComparerTest(
        lesser,
        greater);
}

TEST(KeyComparerTests, TestKey_string)
{
    TestKey lesser;
    TestKey greater;

    greater.set_string_value("b");

    DoKeyComparerTest(
        lesser,
        greater);

    lesser.set_string_value("a");

    DoKeyComparerTest(
        lesser,
        greater);
}

TEST(KeyComparerTests, TestKey_bytes)
{
    TestKey lesser;
    TestKey greater;

    greater.set_bytes_value("b");

    DoKeyComparerTest(
        lesser,
        greater);

    lesser.set_bytes_value("a");

    DoKeyComparerTest(
        lesser,
        greater);
}

TEST(KeyComparerTests, TestKey_subkey)
{
    TestKey lesser;
    TestKey greater;

    greater.mutable_subkey_value()->set_string_value("b");

    DoKeyComparerTest(
        lesser,
        greater);

    lesser.mutable_subkey_value()->set_string_value("a");

    DoKeyComparerTest(
        lesser,
        greater);
}

TEST(KeyComparerTests, TestKey_repeated_int32)
{
    TestKey lesser;
    TestKey greater;

    greater.mutable_repeated_int32_value()->Add(1);

    DoKeyComparerTest(
        lesser,
        greater);

    lesser.mutable_repeated_int32_value()->Add(0);

    DoKeyComparerTest(
        lesser,
        greater);
}

TEST(KeyComparerTests, Uses_lexical_order_not_tag_order)
{
    TestKey_OutOfOrderFields lesser;
    TestKey_OutOfOrderFields greater;

    lesser.set_lexicallyfirstnumericallysecond(1);
    greater.set_lexicallyfirstnumericallysecond(2);
    lesser.set_lexicallysecondnumericallyfirst(2);
    greater.set_lexicallysecondnumericallyfirst(1);

    DoKeyComparerTest(
        lesser,
        greater);
}

TEST(KeyComparerTests, Uses_message_level_sort_order_on_outer_key)
{
    TestKey_DescendingSortOrder_Field lesser;
    TestKey_DescendingSortOrder_Field greater;

    lesser.set_value(2);
    greater.set_value(1);

    DoKeyComparerTest(
        lesser,
        greater);
}

}