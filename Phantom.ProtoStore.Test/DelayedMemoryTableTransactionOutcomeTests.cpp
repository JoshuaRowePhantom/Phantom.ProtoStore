#include "async_test.h"
#include "Phantom.Coroutines/async_scope.h"
#include "Phantom.ProtoStore/src/MemoryTable.h"

namespace Phantom::ProtoStore
{

ASYNC_TEST(DelayedMemoryTableTransactionOutcomeTests, Completer_aborts_transaction_that_isnt_committed)
{
    auto delayedOutcome = std::make_shared<DelayedMemoryTableTransactionOutcome>(0);
    delayedOutcome->Complete();
    auto outcome = co_await delayedOutcome->GetOutcome();
    MemoryTableTransactionOutcome expectedOutcome
    {
        .Outcome = TransactionOutcome::Aborted,
        .WriteSequenceNumber = ToSequenceNumber(0),
    };

    EXPECT_EQ(outcome, expectedOutcome);
}

ASYNC_TEST(DelayedMemoryTableTransactionOutcomeTests, Completer_commits_transaction_that_is_committed)
{
    auto delayedOutcome = std::make_shared<DelayedMemoryTableTransactionOutcome>(0);
    delayedOutcome->BeginCommit(ToSequenceNumber(5));
    delayedOutcome->Complete();
    auto outcome = co_await delayedOutcome->GetOutcome();
    MemoryTableTransactionOutcome expectedOutcome
    {
        .Outcome = TransactionOutcome::Committed,
        .WriteSequenceNumber = ToSequenceNumber(5),
    };

    EXPECT_EQ(outcome, expectedOutcome);
}

ASYNC_TEST(DelayedMemoryTableTransactionOutcomeTests, ResolveTargetTransaction_waits_for_transaction_completion_when_no_loop)
{
    Phantom::Coroutines::async_scope<> asyncScope;
    auto delayedOutcome1 = std::make_shared<DelayedMemoryTableTransactionOutcome>(0);
    auto delayedOutcome2 = std::make_shared<DelayedMemoryTableTransactionOutcome>(1);

    MemoryTableTransactionOutcome expectedOutcome1 =
    {
        .Outcome = TransactionOutcome::Committed,
        .WriteSequenceNumber = ToSequenceNumber(5),
    };

    bool complete = false;
    asyncScope.spawn([&]() -> reusable_task<>
    {
        auto outcome1 = co_await delayedOutcome2->ResolveTargetTransaction(delayedOutcome1);
        EXPECT_EQ(outcome1, expectedOutcome1);
        complete = true;
    });

    EXPECT_EQ(false, complete);
    auto outcome1 = delayedOutcome1->BeginCommit(expectedOutcome1.WriteSequenceNumber);
    EXPECT_EQ(outcome1, expectedOutcome1);
    EXPECT_EQ(false, complete);

    delayedOutcome1->Complete();
    EXPECT_EQ(true, complete);

    co_await asyncScope.join();
}

ASYNC_TEST(DelayedMemoryTableTransactionOutcomeTests, ResolveTargetTransaction_aborts_latest_transaction_when_there_is_loop)
{
    Phantom::Coroutines::async_scope<> asyncScope;
    auto delayedOutcome1 = std::make_shared<DelayedMemoryTableTransactionOutcome>(1);
    auto delayedOutcome2 = std::make_shared<DelayedMemoryTableTransactionOutcome>(2);
    auto delayedOutcome3 = std::make_shared<DelayedMemoryTableTransactionOutcome>(3);
    auto delayedOutcome4 = std::make_shared<DelayedMemoryTableTransactionOutcome>(4);

    MemoryTableTransactionOutcome expectedOutcome1 =
    {
        .Outcome = TransactionOutcome::Committed,
        .WriteSequenceNumber = ToSequenceNumber(1),
    };

    MemoryTableTransactionOutcome expectedOutcome2 =
    {
        .Outcome = TransactionOutcome::Committed,
        .WriteSequenceNumber = ToSequenceNumber(2),
    };

    MemoryTableTransactionOutcome expectedOutcome3 =
    {
        .Outcome = TransactionOutcome::Committed,
        .WriteSequenceNumber = ToSequenceNumber(3),
    };

    MemoryTableTransactionOutcome expectedOutcome4 =
    {
        .Outcome = TransactionOutcome::Aborted,
        .WriteSequenceNumber = ToSequenceNumber(0),
    };

    // Set up loop:
    // 2->1
    // 1->4
    // 4->3
    // 3->2
    bool complete2 = false;
    asyncScope.spawn([&]() -> reusable_task<>
    {
        auto outcome1 = co_await delayedOutcome2->ResolveTargetTransaction(delayedOutcome1);
        EXPECT_EQ(outcome1, expectedOutcome1);
        complete2 = true;
    });

    bool complete1 = false;
    asyncScope.spawn([&]() -> reusable_task<>
    {
        auto outcome4 = co_await delayedOutcome1->ResolveTargetTransaction(delayedOutcome4);
        EXPECT_EQ(outcome4, expectedOutcome4);
        complete1 = true;
    });

    bool complete4 = false;
    asyncScope.spawn([&]() -> reusable_task<>
    {
        auto outcome3 = co_await delayedOutcome4->ResolveTargetTransaction(delayedOutcome3);
        EXPECT_EQ(outcome3, expectedOutcome3);
        complete4 = true;
    });
    
    EXPECT_EQ(false, complete2);
    EXPECT_EQ(false, complete1);
    EXPECT_EQ(false, complete4);

    bool complete3 = false;
    asyncScope.spawn([&]() -> reusable_task<>
    {
        auto outcome2 = co_await delayedOutcome3->ResolveTargetTransaction(delayedOutcome2);
        EXPECT_EQ(outcome2, expectedOutcome2);
        complete3 = true;
    });
    
    EXPECT_EQ(true, complete1);
    EXPECT_EQ(false, complete2);
    EXPECT_EQ(false, complete3);
    EXPECT_EQ(false, complete4);

    auto outcome1 = delayedOutcome1->BeginCommit(expectedOutcome1.WriteSequenceNumber);
    auto outcome2 = delayedOutcome2->BeginCommit(expectedOutcome2.WriteSequenceNumber);
    auto outcome3 = delayedOutcome3->BeginCommit(expectedOutcome3.WriteSequenceNumber);
    auto outcome4 = delayedOutcome4->BeginCommit(expectedOutcome4.WriteSequenceNumber);

    EXPECT_EQ(outcome1, expectedOutcome1);
    EXPECT_EQ(outcome2, expectedOutcome2);
    EXPECT_EQ(outcome3, expectedOutcome3);
    EXPECT_EQ(outcome4, expectedOutcome4);

    EXPECT_EQ(true, complete1);
    EXPECT_EQ(false, complete2);
    EXPECT_EQ(false, complete3);
    EXPECT_EQ(false, complete4);

    delayedOutcome1->Complete();
    EXPECT_EQ(true, complete1);
    EXPECT_EQ(true, complete2);
    EXPECT_EQ(false, complete3);
    EXPECT_EQ(false, complete4);

    delayedOutcome2->Complete();
    EXPECT_EQ(true, complete1);
    EXPECT_EQ(true, complete2);
    EXPECT_EQ(true, complete3);
    EXPECT_EQ(false, complete4);

    delayedOutcome3->Complete();
    EXPECT_EQ(true, complete1);
    EXPECT_EQ(true, complete2);
    EXPECT_EQ(true, complete3);
    EXPECT_EQ(true, complete4);

    delayedOutcome4->Complete();
    EXPECT_EQ(true, complete1);
    EXPECT_EQ(true, complete2);
    EXPECT_EQ(true, complete3);
    EXPECT_EQ(true, complete4);

    co_await asyncScope.join();
}

ASYNC_TEST(DelayedMemoryTableTransactionOutcomeTests, ResolveTargetTransaction_aborts_latest_transaction_when_there_is_loop_and_latest_transaction_is_the_one_forming_the_loop)
{
    Phantom::Coroutines::async_scope<> asyncScope;
    auto delayedOutcome1 = std::make_shared<DelayedMemoryTableTransactionOutcome>(1);
    auto delayedOutcome2 = std::make_shared<DelayedMemoryTableTransactionOutcome>(2);
    auto delayedOutcome3 = std::make_shared<DelayedMemoryTableTransactionOutcome>(3);
    auto delayedOutcome4 = std::make_shared<DelayedMemoryTableTransactionOutcome>(4);

    MemoryTableTransactionOutcome expectedOutcome1 =
    {
        .Outcome = TransactionOutcome::Committed,
        .WriteSequenceNumber = ToSequenceNumber(1),
    };

    MemoryTableTransactionOutcome expectedOutcome2 =
    {
        .Outcome = TransactionOutcome::Committed,
        .WriteSequenceNumber = ToSequenceNumber(2),
    };

    MemoryTableTransactionOutcome expectedOutcome3 =
    {
        .Outcome = TransactionOutcome::Committed,
        .WriteSequenceNumber = ToSequenceNumber(3),
    };

    MemoryTableTransactionOutcome expectedOutcome4 =
    {
        .Outcome = TransactionOutcome::Aborted,
        .WriteSequenceNumber = ToSequenceNumber(0),
    };

    // Set up loop:
    // 2->1
    // 1->4
    // 3->2
    // 4->3
    bool complete2 = false;
    asyncScope.spawn([&]() -> reusable_task<>
    {
        auto outcome1 = co_await delayedOutcome2->ResolveTargetTransaction(delayedOutcome1);
    EXPECT_EQ(outcome1, expectedOutcome1);
    complete2 = true;
    });

    bool complete1 = false;
    asyncScope.spawn([&]() -> reusable_task<>
    {
        auto outcome4 = co_await delayedOutcome1->ResolveTargetTransaction(delayedOutcome4);
    EXPECT_EQ(outcome4, expectedOutcome4);
    complete1 = true;
    });

    bool complete3 = false;
    asyncScope.spawn([&]() -> reusable_task<>
    {
        auto outcome2 = co_await delayedOutcome3->ResolveTargetTransaction(delayedOutcome2);
    EXPECT_EQ(outcome2, expectedOutcome2);
    complete3 = true;
    });

    EXPECT_EQ(false, complete2);
    EXPECT_EQ(false, complete1);
    EXPECT_EQ(false, complete3);

    bool complete4 = false;
    asyncScope.spawn([&]() -> reusable_task<>
    {
        auto outcome3 = co_await delayedOutcome4->ResolveTargetTransaction(delayedOutcome3);
        EXPECT_EQ(outcome3, expectedOutcome3);
        complete4 = true;
    });

    EXPECT_EQ(true, complete1);
    EXPECT_EQ(false, complete2);
    EXPECT_EQ(false, complete3);
    EXPECT_EQ(false, complete4);

    auto outcome1 = delayedOutcome1->BeginCommit(expectedOutcome1.WriteSequenceNumber);
    auto outcome2 = delayedOutcome2->BeginCommit(expectedOutcome2.WriteSequenceNumber);
    auto outcome3 = delayedOutcome3->BeginCommit(expectedOutcome3.WriteSequenceNumber);
    auto outcome4 = delayedOutcome4->BeginCommit(expectedOutcome4.WriteSequenceNumber);

    EXPECT_EQ(outcome1, expectedOutcome1);
    EXPECT_EQ(outcome2, expectedOutcome2);
    EXPECT_EQ(outcome3, expectedOutcome3);
    EXPECT_EQ(outcome4, expectedOutcome4);

    EXPECT_EQ(true, complete1);
    EXPECT_EQ(false, complete2);
    EXPECT_EQ(false, complete3);
    EXPECT_EQ(false, complete4);

    delayedOutcome1->Complete();
    EXPECT_EQ(true, complete1);
    EXPECT_EQ(true, complete2);
    EXPECT_EQ(false, complete3);
    EXPECT_EQ(false, complete4);

    delayedOutcome2->Complete();
    EXPECT_EQ(true, complete1);
    EXPECT_EQ(true, complete2);
    EXPECT_EQ(true, complete3);
    EXPECT_EQ(false, complete4);

    delayedOutcome3->Complete();
    EXPECT_EQ(true, complete1);
    EXPECT_EQ(true, complete2);
    EXPECT_EQ(true, complete3);
    EXPECT_EQ(true, complete4);

    delayedOutcome4->Complete();
    EXPECT_EQ(true, complete1);
    EXPECT_EQ(true, complete2);
    EXPECT_EQ(true, complete3);
    EXPECT_EQ(true, complete4);

    co_await asyncScope.join();
}

}