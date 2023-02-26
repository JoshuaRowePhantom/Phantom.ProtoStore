#include <array>
#include <memory>
#include "async_test.h"
#include "Phantom.System/lifetime_tracker.h"
#include "Phantom.ProtoStore/Phantom.ProtoStore.h"

namespace Phantom::ProtoStore
{
TEST(DataReferenceTest, Can_create_default_instance)
{
    RawData rawData;
    EXPECT_EQ(0, rawData.data().size());
    EXPECT_EQ(nullptr, rawData.data().data());
}

TEST(DataReferenceTest, Can_create_non_default_instance_with_no_controlling_allocation)
{
    std::array<std::byte, 10> data;
    RawData rawData(nullptr, data);
    EXPECT_EQ(10, rawData.data().size());
    EXPECT_EQ(&data[0], rawData.data().data());

    auto copy = rawData;
    EXPECT_EQ(10, copy.data().size());
    EXPECT_EQ(&data[0], copy.data().data());

    RawData copy2;
    copy2 = rawData;
    EXPECT_EQ(10, copy2.data().size());
    EXPECT_EQ(&data[0], copy2.data().data());

    RawData move1 = std::move(rawData);
    EXPECT_EQ(0, rawData.data().size());
    EXPECT_EQ(nullptr, rawData.data().data());
    EXPECT_EQ(10, move1.data().size());
    EXPECT_EQ(&data[0], move1.data().data());

    RawData move2 = std::move(move1);
    EXPECT_EQ(0, move1.data().size());
    EXPECT_EQ(nullptr, move1.data().data());
    EXPECT_EQ(10, move2.data().size());
    EXPECT_EQ(&data[0], move2.data().data());
}

TEST(DataReferenceTest, Can_create_non_default_instance_with_controlling_allocation)
{
    lifetime_statistics statistics;
    
    struct data
    {
        std::array<std::byte, 10> m_data;
        lifetime_tracker m_tracker;
    };

    auto dataPtr = std::make_shared<data>(std::array<std::byte, 10>(), statistics.tracker());
    std::span<const std::byte> span = dataPtr->m_data;

    RawData rawData(std::move(dataPtr), dataPtr->m_data);
    EXPECT_EQ(span.data(), rawData.data().data());

    auto copy = rawData;
    EXPECT_EQ(span.data(), rawData.data().data());

    RawData copy2;
    copy2 = rawData;
    EXPECT_EQ(span.data(), copy2.data().data());
    EXPECT_EQ(span.data(), rawData.data().data());

    RawData move1 = std::move(rawData);
    EXPECT_EQ(nullptr, rawData.data().data());
    EXPECT_EQ(0, rawData.data().size());
    EXPECT_EQ(span.data(), move1.data().data());
    EXPECT_EQ(1, statistics.instance_count);

    RawData move2 = std::move(move1);
    EXPECT_EQ(nullptr, move1.data().data());
    EXPECT_EQ(0, move1.data().size());
    EXPECT_EQ(span.data(), move2.data().data());

    move2 = nullptr;
    EXPECT_EQ(1, statistics.instance_count);
    copy = nullptr;
    EXPECT_EQ(1, statistics.instance_count);
    copy2 = nullptr;
    EXPECT_EQ(0, statistics.instance_count);
}

TEST(DataReferenceTest, Can_convert_to_bool)
{
    DataReference<bool> dataReference1 = { nullptr, true };
    DataReference<bool> dataReference2 = { nullptr, false };

    EXPECT_EQ(true, !!dataReference1);
    EXPECT_EQ(false, !!dataReference2);
}

}