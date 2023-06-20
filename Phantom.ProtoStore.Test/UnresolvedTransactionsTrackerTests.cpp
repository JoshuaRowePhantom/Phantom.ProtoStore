#include "StandardIncludes.h"
#include "async_test.h"
#include "Phantom.ProtoStore/src/ProtoStore.h"
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

ASYNC_TEST_F(UnresolvedTransactionsTrackerTests, GetTransactionOutcome_returns_Committed_for_Committed_transaction)
{
    auto store = ToProtoStore(co_await CreateMemoryStore());
    auto unresolvedTransactionsTracker = GetUnresolvedTransactionsTracker(store.get());

    co_await store->ExecuteTransaction(
        {},
        [&](ICommittableTransaction* transaction) -> status_task<>
        {
            co_await co_await transaction->ResolveTransaction("hello world", TransactionOutcome::Committed);
            co_return{};
        });

    auto transactionOutcome = co_await unresolvedTransactionsTracker->GetTransactionOutcome(
        "hello world");
    EXPECT_EQ(TransactionOutcome::Committed, transactionOutcome);
}

ASYNC_TEST_F(UnresolvedTransactionsTrackerTests, GetTransactionOutcome_returns_Aborted_for_Aborted_transaction)
{
    auto store = ToProtoStore(co_await CreateMemoryStore());
    auto unresolvedTransactionsTracker = GetUnresolvedTransactionsTracker(store.get());

    co_await store->ExecuteTransaction(
        {},
        [&](ICommittableTransaction* transaction) -> status_task<>
        {
            co_await co_await transaction->ResolveTransaction("hello world", TransactionOutcome::Aborted);
            co_return{};
        });

    auto transactionOutcome = co_await unresolvedTransactionsTracker->GetTransactionOutcome(
        "hello world");
    EXPECT_EQ(TransactionOutcome::Aborted, transactionOutcome);
}

}
