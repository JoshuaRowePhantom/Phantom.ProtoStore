#include "StandardIncludes.h"
#include "async_test.h"
#include "Phantom.ProtoStore/src/UnresolvedTransactionsTracker.h"

namespace Phantom::ProtoStore
{

class UnresolvedTransactionsTrackerTests : public testing::Test
{

};

ASYNC_TEST_F(UnresolvedTransactionsTrackerTests, GetTransactionOutcome_returns_Committed_for_nonexisting_row)
{
    throw 0;
}

}
