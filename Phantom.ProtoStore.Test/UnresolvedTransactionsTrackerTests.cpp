#include "StandardIncludes.h"
#include "async_test.h"
#include "Phantom.ProtoStore/src/UnresolvedTransactionsTracker.h"
#include "TestFactories.h"

namespace Phantom::ProtoStore
{

class UnresolvedTransactionsTrackerTests : 
    public testing::Test,
    public TestFactories
{

};

ASYNC_TEST_F(UnresolvedTransactionsTrackerTests, GetTransactionOutcome_returns_Committed_for_nonexisting_row)
{
    auto store = ToProtoStore(co_await CreateMemoryStore());
    auto unresolvedTransactionsTracker = GetUnresolvedTransactionsTracker(store.get());

    auto transactionOutcome = co_await unresolvedTransactionsTracker->GetTransactionOutcome(
        "hello world");
    EXPECT_EQ(TransactionOutcome::Committed, transactionOutcome);
}

}
