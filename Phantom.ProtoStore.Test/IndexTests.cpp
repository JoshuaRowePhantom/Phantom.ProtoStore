#include "StandardIncludes.h"
#include "TestFactories.h"

namespace Phantom::ProtoStore
{
class IndexTests :
    public testing::Test,
    public TestFactories
{
public:
};

ASYNC_TEST_F(IndexTests, can_insert_row_when_no_rows_are_present)
{
    auto testInMemoryIndex = co_await CreateTestInMemoryIndex(
        {},
        {}
    );

    auto addRowResult = co_await AddTestRow(
        testInMemoryIndex.Index,
        ToSequenceNumber(1),
        {
            "key",
            "value",
        });

    throw_if_failed(addRowResult);
}
}