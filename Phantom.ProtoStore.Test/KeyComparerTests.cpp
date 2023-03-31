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

}