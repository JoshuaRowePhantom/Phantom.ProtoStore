#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/ProtocolBuffersValueComparer.h"
#include "ProtoStoreTest.pb.h"

namespace Phantom::ProtoStore
{

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

TEST(KeyAndSequenceNumberComparerTests, Uses_key_order_first)
{
    TestKey lesser;
    TestKey greater;

    lesser.set_int32_value(0);
    greater.set_int32_value(1);

    auto lesserProtoValue = ProtoValue(&lesser).pack();
    auto greaterProtoValue = ProtoValue(&greater).pack();

    DoKeyAndSequenceNumberComparerTest<TestKey>(
        KeyAndSequenceNumberComparerArgument{ lesserProtoValue, SequenceNumber::Earliest },
        KeyAndSequenceNumberComparerArgument{ greaterProtoValue, SequenceNumber::Latest }
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

template<typename TKey>
void DoKeyRangeComparerTestNotPrefix(
    const ProtoValue& lesser,
    const KeyRangeComparerArgument& middle,
    const ProtoValue& greater)
{
    ProtocolBuffersValueComparer keyComparer(
        TKey::GetDescriptor());
    KeyRangeComparer keyRangeComparer(
        keyComparer);

    EXPECT_EQ(std::weak_ordering::less, keyRangeComparer.lower_bound_compare(lesser, middle));
    EXPECT_EQ(std::weak_ordering::greater, keyRangeComparer.lower_bound_compare(greater, middle));
    EXPECT_EQ(true, keyRangeComparer.lower_bound_less(lesser, middle));
    EXPECT_EQ(false, keyRangeComparer.lower_bound_less(greater, middle));

    EXPECT_EQ(std::weak_ordering::less, keyRangeComparer.upper_bound_compare(middle, greater));
    EXPECT_EQ(std::weak_ordering::greater, keyRangeComparer.upper_bound_compare(middle, lesser));
    EXPECT_EQ(true, keyRangeComparer.upper_bound_less(middle, greater));
    EXPECT_EQ(false, keyRangeComparer.upper_bound_less(middle, lesser));
}

TEST(KeyRangeComparerTests, Uses_key_order_inclusive)
{
    ProtocolBuffersValueComparer keyComparer(
        TestKey::GetDescriptor());
    KeyRangeComparer keyRangeComparer(
        keyComparer);

    TestKey lesser;
    TestKey greater;

    lesser.set_int32_value(0);
    greater.set_int32_value(2);

    auto lesserProtoValue = ProtoValue(&lesser).pack();
    auto greaterProtoValue = ProtoValue(&greater).pack();

    // The KeyRangeComparer has to look for boundaries between keys, but doesn't pay attention to
    // sequence numbers.
    // Because there might be an equivalent key with a higher sequence number, the KeyRangeComparer
    // never returns equivalent.
    EXPECT_EQ(std::weak_ordering::greater, keyRangeComparer.lower_bound_compare(lesserProtoValue, KeyRangeComparerArgument(lesserProtoValue, Inclusivity::Inclusive, std::nullopt)));
    EXPECT_EQ(false, keyRangeComparer.lower_bound_less(lesserProtoValue, KeyRangeComparerArgument(lesserProtoValue, Inclusivity::Inclusive, std::nullopt)));

    EXPECT_EQ(std::weak_ordering::less, keyRangeComparer.lower_bound_compare(lesserProtoValue, KeyRangeComparerArgument(greaterProtoValue, Inclusivity::Inclusive, std::nullopt)));
    EXPECT_EQ(true, keyRangeComparer.lower_bound_less(lesserProtoValue, KeyRangeComparerArgument(greaterProtoValue, Inclusivity::Inclusive, std::nullopt)));

    EXPECT_EQ(std::weak_ordering::greater, keyRangeComparer.lower_bound_compare(greaterProtoValue, KeyRangeComparerArgument(lesserProtoValue, Inclusivity::Inclusive, std::nullopt)));
    EXPECT_EQ(false, keyRangeComparer.lower_bound_less(greaterProtoValue, KeyRangeComparerArgument(lesserProtoValue, Inclusivity::Inclusive, std::nullopt)));

    // The KeyRangeComparer has to look for boundaries between keys, but doesn't pay attention to
    // sequence numbers.
    // Because there might be an equivalent key with a lower sequence number, the KeyRangeComparer
    // never returns equivalent.
    EXPECT_EQ(std::weak_ordering::greater, keyRangeComparer.upper_bound_compare(KeyRangeComparerArgument(lesserProtoValue, Inclusivity::Inclusive, std::nullopt), lesserProtoValue));
    EXPECT_EQ(false, keyRangeComparer.upper_bound_less(KeyRangeComparerArgument(lesserProtoValue, Inclusivity::Inclusive, std::nullopt), lesserProtoValue));

    EXPECT_EQ(std::weak_ordering::less, keyRangeComparer.upper_bound_compare(KeyRangeComparerArgument(lesserProtoValue, Inclusivity::Inclusive, std::nullopt), greaterProtoValue));
    EXPECT_EQ(true, keyRangeComparer.upper_bound_less(KeyRangeComparerArgument(lesserProtoValue, Inclusivity::Inclusive, std::nullopt), greaterProtoValue));

    EXPECT_EQ(std::weak_ordering::greater, keyRangeComparer.upper_bound_compare(KeyRangeComparerArgument(greaterProtoValue, Inclusivity::Inclusive, std::nullopt), lesserProtoValue));
    EXPECT_EQ(false, keyRangeComparer.upper_bound_less(KeyRangeComparerArgument(greaterProtoValue, Inclusivity::Inclusive, std::nullopt), lesserProtoValue));
}

TEST(KeyRangeComparerTests, Uses_key_order_exclusive)
{
    ProtocolBuffersValueComparer keyComparer(
        TestKey::GetDescriptor());
    KeyRangeComparer keyRangeComparer(
        keyComparer);

    TestKey lesser;
    TestKey greater;

    lesser.set_int32_value(0);
    greater.set_int32_value(2);

    auto lesserProtoValue = ProtoValue(&lesser).pack();
    auto greaterProtoValue = ProtoValue(&greater).pack();

    // Equivalent keys should compare inequal when using Exclusive.
    EXPECT_EQ(std::weak_ordering::less, keyRangeComparer.lower_bound_compare(lesserProtoValue, KeyRangeComparerArgument(lesserProtoValue, Inclusivity::Exclusive, std::nullopt)));
    EXPECT_EQ(true, keyRangeComparer.lower_bound_less(lesserProtoValue, KeyRangeComparerArgument(lesserProtoValue, Inclusivity::Exclusive, std::nullopt)));

    EXPECT_EQ(std::weak_ordering::less, keyRangeComparer.lower_bound_compare(lesserProtoValue, KeyRangeComparerArgument(greaterProtoValue, Inclusivity::Exclusive, std::nullopt)));
    EXPECT_EQ(true, keyRangeComparer.lower_bound_less(lesserProtoValue, KeyRangeComparerArgument(greaterProtoValue, Inclusivity::Exclusive, std::nullopt)));

    EXPECT_EQ(std::weak_ordering::greater, keyRangeComparer.lower_bound_compare(greaterProtoValue, KeyRangeComparerArgument(lesserProtoValue, Inclusivity::Exclusive, std::nullopt)));
    EXPECT_EQ(false, keyRangeComparer.lower_bound_less(greaterProtoValue, KeyRangeComparerArgument(lesserProtoValue, Inclusivity::Exclusive, std::nullopt)));

    // Equivalent keys should compare inequal when using Exclusive.
    EXPECT_EQ(std::weak_ordering::less, keyRangeComparer.upper_bound_compare(KeyRangeComparerArgument(lesserProtoValue, Inclusivity::Exclusive, std::nullopt), lesserProtoValue));
    EXPECT_EQ(true, keyRangeComparer.upper_bound_less(KeyRangeComparerArgument(lesserProtoValue, Inclusivity::Exclusive, std::nullopt), lesserProtoValue));

    EXPECT_EQ(std::weak_ordering::less, keyRangeComparer.upper_bound_compare(KeyRangeComparerArgument(lesserProtoValue, Inclusivity::Exclusive, std::nullopt), greaterProtoValue));
    EXPECT_EQ(true, keyRangeComparer.upper_bound_less(KeyRangeComparerArgument(lesserProtoValue, Inclusivity::Exclusive, std::nullopt), greaterProtoValue));

    EXPECT_EQ(std::weak_ordering::greater, keyRangeComparer.upper_bound_compare(KeyRangeComparerArgument(greaterProtoValue, Inclusivity::Exclusive, std::nullopt), lesserProtoValue));
    EXPECT_EQ(false, keyRangeComparer.upper_bound_less(KeyRangeComparerArgument(greaterProtoValue, Inclusivity::Exclusive, std::nullopt), lesserProtoValue));
}

TEST(KeyRangeComparerTests, Uses_LastFieldId)
{
    ProtocolBuffersValueComparer keyComparer(
        TestKey::GetDescriptor());
    KeyRangeComparer keyRangeComparer(
        keyComparer);

    TestKey lesser;
    TestKey matching1;
    TestKey matching2;
    TestKey matching3;
    TestKey prefix;
    TestKey greater;

    lesser.set_sint32_value(1);
    prefix.set_sint32_value(2);
    prefix.set_sint64_value(3);
    matching1.set_sint32_value(2);
    matching1.set_sint64_value(2);
    matching2.set_sint32_value(2);
    matching2.set_sint64_value(3);
    matching3.set_sint32_value(2);
    matching3.set_sint64_value(4);
    greater.set_sint32_value(3);

    auto lesserProtoValue = ProtoValue(&lesser).pack();
    auto matching1ProtoValue = ProtoValue(&matching1).pack();
    auto matching2ProtoValue = ProtoValue(&matching2).pack();
    auto matching3ProtoValue = ProtoValue(&matching3).pack();
    auto prefixProtoValue = ProtoValue(&prefix).pack();
    auto greaterProtoValue = ProtoValue(&greater).pack();

    EXPECT_EQ(std::weak_ordering::less, keyRangeComparer.lower_bound_compare(lesserProtoValue, KeyRangeComparerArgument(prefixProtoValue, Inclusivity::Inclusive, 3)));
    EXPECT_EQ(true, keyRangeComparer.lower_bound_less(lesserProtoValue, KeyRangeComparerArgument(prefixProtoValue, Inclusivity::Inclusive, 3)));

    EXPECT_EQ(std::weak_ordering::greater, keyRangeComparer.lower_bound_compare(matching1ProtoValue, KeyRangeComparerArgument(prefixProtoValue, Inclusivity::Inclusive, 3)));
    EXPECT_EQ(false, keyRangeComparer.lower_bound_less(matching1ProtoValue, KeyRangeComparerArgument(prefixProtoValue, Inclusivity::Inclusive, 3)));

    EXPECT_EQ(std::weak_ordering::greater, keyRangeComparer.lower_bound_compare(matching2ProtoValue, KeyRangeComparerArgument(prefixProtoValue, Inclusivity::Inclusive, 3)));
    EXPECT_EQ(false, keyRangeComparer.lower_bound_less(matching2ProtoValue, KeyRangeComparerArgument(prefixProtoValue, Inclusivity::Inclusive, 3)));

    EXPECT_EQ(std::weak_ordering::greater, keyRangeComparer.lower_bound_compare(matching3ProtoValue, KeyRangeComparerArgument(prefixProtoValue, Inclusivity::Inclusive, 3)));
    EXPECT_EQ(false, keyRangeComparer.lower_bound_less(matching3ProtoValue, KeyRangeComparerArgument(prefixProtoValue, Inclusivity::Inclusive, 3)));

    EXPECT_EQ(std::weak_ordering::greater, keyRangeComparer.lower_bound_compare(greaterProtoValue, KeyRangeComparerArgument(prefixProtoValue, Inclusivity::Inclusive, 3)));
    EXPECT_EQ(false, keyRangeComparer.lower_bound_less(greaterProtoValue, KeyRangeComparerArgument(prefixProtoValue, Inclusivity::Inclusive, 3)));


    EXPECT_EQ(std::weak_ordering::greater, keyRangeComparer.upper_bound_compare(KeyRangeComparerArgument(prefixProtoValue, Inclusivity::Inclusive, 3), lesserProtoValue));
    EXPECT_EQ(false, keyRangeComparer.upper_bound_less(KeyRangeComparerArgument(prefixProtoValue, Inclusivity::Inclusive, 3), lesserProtoValue));

    EXPECT_EQ(std::weak_ordering::greater, keyRangeComparer.upper_bound_compare(KeyRangeComparerArgument(prefixProtoValue, Inclusivity::Inclusive, 3), matching1ProtoValue));
    EXPECT_EQ(false, keyRangeComparer.upper_bound_less(KeyRangeComparerArgument(prefixProtoValue, Inclusivity::Inclusive, 3), matching1ProtoValue));

    EXPECT_EQ(std::weak_ordering::greater, keyRangeComparer.upper_bound_compare(KeyRangeComparerArgument(prefixProtoValue, Inclusivity::Inclusive, 3), matching2ProtoValue));
    EXPECT_EQ(false, keyRangeComparer.upper_bound_less(KeyRangeComparerArgument(prefixProtoValue, Inclusivity::Inclusive, 3), matching2ProtoValue));

    EXPECT_EQ(std::weak_ordering::greater, keyRangeComparer.upper_bound_compare(KeyRangeComparerArgument(prefixProtoValue, Inclusivity::Inclusive, 3), matching3ProtoValue));
    EXPECT_EQ(false, keyRangeComparer.upper_bound_less(KeyRangeComparerArgument(prefixProtoValue, Inclusivity::Inclusive, 3), matching3ProtoValue));

    EXPECT_EQ(std::weak_ordering::less, keyRangeComparer.upper_bound_compare(KeyRangeComparerArgument(prefixProtoValue, Inclusivity::Inclusive, 3), greaterProtoValue));
    EXPECT_EQ(true, keyRangeComparer.upper_bound_less(KeyRangeComparerArgument(prefixProtoValue, Inclusivity::Inclusive, 3), greaterProtoValue));
}

}