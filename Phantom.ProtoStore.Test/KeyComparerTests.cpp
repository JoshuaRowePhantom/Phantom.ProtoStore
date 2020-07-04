#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/KeyComparer.h"
#include "ProtoStoreTest.pb.h"

namespace Phantom::ProtoStore
{

void DoKeyComparerTest(
    const TestKey& lesser,
    const TestKey& greater)
{
    KeyComparer keyComparer(
        TestKey::GetDescriptor());

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
}