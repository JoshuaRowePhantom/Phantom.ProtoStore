#include <array>
#include <memory>
#include "async_test.h"
#include "lifetime_tracker.h"
#include "Phantom.ProtoStore/Phantom.ProtoStore.h"

namespace Phantom::ProtoStore
{
TEST(DataReferenceTest, Can_create_default_instance)
{
    DataReference dataReference;
    EXPECT_EQ(true, !dataReference);
    EXPECT_EQ(false, static_cast<bool>(dataReference));
    EXPECT_EQ(0, dataReference.span().size());
    EXPECT_EQ(nullptr, dataReference.span().data());
}

TEST(DataReferenceTest, Can_create_non_default_instance_with_no_controlling_allocation)
{
    std::array<std::byte, 10> data;
    DataReference dataReference(nullptr, data);
    EXPECT_EQ(false, !dataReference);
    EXPECT_EQ(true, static_cast<bool>(dataReference));
    EXPECT_EQ(10, dataReference.span().size());
    EXPECT_EQ(&data[0], dataReference.span().data());

    auto copy = dataReference;
    EXPECT_EQ(false, !copy);
    EXPECT_EQ(true, static_cast<bool>(copy));
    EXPECT_EQ(10, copy.span().size());
    EXPECT_EQ(&data[0], copy.span().data());

    DataReference copy2;
    copy2 = dataReference;
    EXPECT_EQ(false, !copy2);
    EXPECT_EQ(true, static_cast<bool>(copy2));
    EXPECT_EQ(10, copy2.span().size());
    EXPECT_EQ(&data[0], copy2.span().data());

    DataReference move1 = std::move(dataReference);
    EXPECT_EQ(true, !dataReference);
    EXPECT_EQ(false, static_cast<bool>(dataReference));
    EXPECT_EQ(0, dataReference.span().size());
    EXPECT_EQ(nullptr, dataReference.span().data());
    EXPECT_EQ(false, !move1);
    EXPECT_EQ(true, static_cast<bool>(move1));
    EXPECT_EQ(10, move1.span().size());
    EXPECT_EQ(&data[0], move1.span().data());

    DataReference move2 = std::move(move1);
    EXPECT_EQ(true, !move1);
    EXPECT_EQ(false, static_cast<bool>(move1));
    EXPECT_EQ(0, move1.span().size());
    EXPECT_EQ(nullptr, move1.span().data());
    EXPECT_EQ(false, !move2);
    EXPECT_EQ(true, static_cast<bool>(move2));
    EXPECT_EQ(10, move2.span().size());
    EXPECT_EQ(&data[0], move2.span().data());
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

    DataReference dataReference(std::move(dataPtr), dataPtr->m_data);
    EXPECT_EQ(false, !dataReference);
    EXPECT_EQ(true, static_cast<bool>(dataReference));
    EXPECT_EQ(span.data(), dataReference.span().data());

    auto copy = dataReference;
    EXPECT_EQ(false, !copy);
    EXPECT_EQ(true, static_cast<bool>(copy));
    EXPECT_EQ(span.data(), dataReference.span().data());

    DataReference copy2;
    copy2 = dataReference;
    EXPECT_EQ(false, !copy2);
    EXPECT_EQ(true, static_cast<bool>(copy2));
    EXPECT_EQ(span.data(), copy2.span().data());
    EXPECT_EQ(span.data(), dataReference.span().data());

    DataReference move1 = std::move(dataReference);
    EXPECT_EQ(true, !dataReference);
    EXPECT_EQ(false, static_cast<bool>(dataReference));
    EXPECT_EQ(nullptr, dataReference.span().data());
    EXPECT_EQ(0, dataReference.span().size());
    EXPECT_EQ(false, !move1);
    EXPECT_EQ(true, static_cast<bool>(move1));
    EXPECT_EQ(span.data(), move1.span().data());
    EXPECT_EQ(1, statistics.instance_count);

    DataReference move2 = std::move(move1);
    EXPECT_EQ(true, !move1);
    EXPECT_EQ(false, static_cast<bool>(move1));
    EXPECT_EQ(nullptr, move1.span().data());
    EXPECT_EQ(0, move1.span().size());
    EXPECT_EQ(false, !move2);
    EXPECT_EQ(true, static_cast<bool>(move2));
    EXPECT_EQ(span.data(), move2.span().data());

    move2 = nullptr;
    EXPECT_EQ(1, statistics.instance_count);
    copy = nullptr;
    EXPECT_EQ(1, statistics.instance_count);
    copy2 = nullptr;
    EXPECT_EQ(0, statistics.instance_count);
}

}