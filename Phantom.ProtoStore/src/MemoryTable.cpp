#include "MemoryTableImpl.h"
#include "Phantom.System/atomic.h"
#include "Schema.h"

namespace Phantom::ProtoStore
{

TransactionOutcome GetTransactionOutcome(
    MemoryTableOutcomeAndSequenceNumber value
)
{
    return static_cast<TransactionOutcome>(
        static_cast<std::uint64_t>(value)
        & static_cast<std::uint64_t>(MemoryTableOutcomeAndSequenceNumber::OutcomeMask));
}

SequenceNumber ToSequenceNumber(
    MemoryTableOutcomeAndSequenceNumber value
)
{
    return static_cast<SequenceNumber>(
        static_cast<std::uint64_t>(value)
        & static_cast<std::uint64_t>(MemoryTableOutcomeAndSequenceNumber::NumberMask));
}

MemoryTableTransactionOutcome ToMemoryTableTransactionOutcome(
    MemoryTableOutcomeAndSequenceNumber value)
{
    return
    {
        .Outcome = GetTransactionOutcome(value),
        .WriteSequenceNumber = ToSequenceNumber(value),
    };
}

MemoryTableOutcomeAndSequenceNumber ToMemoryTableOutcomeAndSequenceNumber(
    SequenceNumber sequenceNumber,
    TransactionOutcome transactionOutcome)
{
    return static_cast<MemoryTableOutcomeAndSequenceNumber>(
        static_cast<std::uint64_t>(sequenceNumber)
        | static_cast<std::uint64_t>(transactionOutcome));
}

MemoryTableOutcomeAndSequenceNumber MemoryTable::ToOutcomeUnknownSubsequentInsertion(
    SequenceNumber sequenceNumber)
{
    return static_cast<MemoryTableOutcomeAndSequenceNumber>(
        static_cast<std::uint64_t>(sequenceNumber)
        | static_cast<std::uint64_t>(MemoryTableOutcomeAndSequenceNumber::OutcomeUnknown));
}

MemoryTable::MemoryTable(
    shared_ptr<const Schema> schema,
    shared_ptr<const ValueComparer> keyComparer
)
    : 
    m_schema(
        std::move(schema)
    ),
    m_keyComparer(
        std::move(keyComparer)
    ),
    m_comparer(
        m_schema,
        m_keyComparer
    ),
    m_skipList(
        m_comparer),
    m_earliestSequenceNumber(
        SequenceNumber::Latest
    ),
    m_latestSequenceNumber(
        SequenceNumber::Earliest
    )
{
}

MemoryTable::~MemoryTable()
{
    SyncDestroy();
}

size_t MemoryTable::GetApproximateRowCount()
{
    return m_approximateRowCount.load(
        std::memory_order_acquire);
}

task<std::optional<SequenceNumber>> MemoryTable::AddRow(
    SequenceNumber readSequenceNumber,
    FlatMessage<FlatBuffers::LoggedRowWrite> row,
    shared_ptr<DelayedMemoryTableTransactionOutcome> delayedTransactionOutcome
)
{
    InsertionKey insertionKey(
        *m_schema,
        row,
        delayedTransactionOutcome,
        readSequenceNumber);

    auto memoryTableWriteSequenceNumber = ToMemoryTableOutcomeAndSequenceNumber(
        SequenceNumber(row->sequence_number()),
        TransactionOutcome::Unknown);

    decltype(m_skipList)::iterator iterator;
    bool succeeded = false;

    while (!succeeded)
    {
        auto insertionResult = m_skipList.insert(
            move(insertionKey));

        iterator = insertionResult.first;
        succeeded = insertionResult.second;
        
        if (succeeded)
        {
            break;
        }

        // Do a cursory check to see what the outcome was.
        // We expect that most transactions commit.  The outcome is likely to be "Committed".
        // The cursory check for this is better than acquiring the mutex.
        auto previousMemoryTableWriteSequenceNumber = iterator->WriteSequenceNumber.load(
            std::memory_order_acquire);

        auto previousTransactionOutcome = GetTransactionOutcome(
            previousMemoryTableWriteSequenceNumber);

        // If this conflicting operation was committed,
        // then we return that information right away.
        if (previousTransactionOutcome == TransactionOutcome::Committed)
        {
            co_return ToSequenceNumber(previousMemoryTableWriteSequenceNumber);
        }

        auto lock = co_await iterator->Mutex.scoped_lock_async();
        auto targetTransactionOutcome = iterator->DelayedTransactionOutcome;
        
        if (!targetTransactionOutcome)
        {
            previousMemoryTableWriteSequenceNumber = iterator->WriteSequenceNumber.load(
                std::memory_order_acquire);

            previousTransactionOutcome = GetTransactionOutcome(
                previousMemoryTableWriteSequenceNumber);
        
            assert(previousTransactionOutcome != TransactionOutcome::Unknown);
        }
        else
        {
            // Don't lock while actually resolving.
            lock.unlock();

            // Now we'll resolve whatever transaction might be there.
            // It might be different from the cursory check.
            auto resolution = co_await delayedTransactionOutcome->ResolveTargetTransaction(
                targetTransactionOutcome);
            previousTransactionOutcome = resolution.Outcome;

            // Now reacquire the lock. 
            lock = co_await iterator->Mutex.scoped_lock_async();
            // There may have been another transaction
            // that resolved and updated the row, so we have to check for that.
            if (iterator->DelayedTransactionOutcome != targetTransactionOutcome)
            {
                // Some other transaction updated the row.
                // Start over.
                continue;
            }
        }

        // Now we might discover there's a committed transaction.
        if (previousTransactionOutcome == TransactionOutcome::Committed)
        {
            co_return ToSequenceNumber(previousMemoryTableWriteSequenceNumber);
        }

        assert(previousTransactionOutcome == TransactionOutcome::Aborted);

        // Jolly Joy! The previous transaction aborted,
        // and was the last transaction to touch this row. We are therefore
        // allowed to update this row.
        iterator->ValueRow = std::move(row);
        iterator->DelayedTransactionOutcome = delayedTransactionOutcome;

        memoryTableWriteSequenceNumber = ToOutcomeUnknownSubsequentInsertion(
            SequenceNumber(iterator->ValueRow->sequence_number()));

        iterator->WriteSequenceNumber.store(
            memoryTableWriteSequenceNumber,
            std::memory_order_release);

        // We now allow other potential resolvers to resolve.
        lock.unlock();
        succeeded = true;
        break;
    }

    m_approximateRowCount.fetch_add(
        1,
        std::memory_order_acq_rel);

    spawn(
        UpdateOutcome(
            *iterator,
            // We pass in the parameter version of delayedTransactionOutcome
            // so that no 
            std::move(delayedTransactionOutcome)));

    co_return{};
}

task<> MemoryTable::UpdateOutcome(
    MemoryTableValue& memoryTableValue,
    shared_ptr<DelayedMemoryTableTransactionOutcome> delayedTransactionOutcome)
{
    auto outcome = co_await delayedTransactionOutcome->GetOutcome();

    auto lock = co_await memoryTableValue.Mutex.scoped_lock_async();
    // It's possible that another UpdateOutcome replaced the DelayedTransactionOutcome
    // because this row aborted and the other resolver updated the row.
    // 
    // Check for that condition before updating the row.
    if (delayedTransactionOutcome != memoryTableValue.DelayedTransactionOutcome)
    {
        lock.unlock();

        co_return;
    }

    // After this point, 
    memoryTableValue.WriteSequenceNumber.store(
        ToMemoryTableOutcomeAndSequenceNumber(
            memoryTableValue.GetWriteSequenceNumber(),
            outcome.Outcome),
        std::memory_order_release
    );

    // We don't need the resolver anymore.
    memoryTableValue.DelayedTransactionOutcome = nullptr;
    
    lock.unlock();
}

task<> MemoryTable::ReplayRow(
    FlatMessage<FlatBuffers::LoggedRowWrite> row
)
{
    ReplayInsertionKey replayKey(
        *m_schema,
        row);

    auto [iterator, succeeded] = m_skipList.insert(
        move(replayKey));

    m_approximateRowCount.fetch_add(
        1,
        std::memory_order_relaxed);

    assert(succeeded);

    co_return;
}

row_generator MemoryTable::Enumerate(
    shared_ptr<DelayedMemoryTableTransactionOutcome> delayedTransactionOutcome,
    SequenceNumber readSequenceNumber,
    KeyRangeEnd low,
    KeyRangeEnd high
)
{
    if (!low.Key)
    {
        low.Key = ProtoValue::KeyMin();
    }
    if (!high.Key)
    {
        high.Key = ProtoValue::KeyMax();
    }
    EnumerationKey enumerationKey
    {
        .KeyLow = low.Key,
        .KeyLowInclusivity = low.Inclusivity,
        .ReadSequenceNumber = readSequenceNumber,
        .LastFieldId = low.LastFieldId,
    };

    auto [findIterator, keyComparisonResult] = m_skipList.find(
        enumerationKey);

    // Only perform prefix matching for the first key search;
    // subsequence key searches will use the last-found key
    // as the key to search for "exclusive".
    enumerationKey.LastFieldId = std::nullopt;

    while (findIterator)
    {
        auto& memoryTableValue = *findIterator;

        auto highComparisonResult = m_comparer(
            memoryTableValue,
            high);

        if (highComparisonResult == std::weak_ordering::greater)
        {
            co_return;
        }

        // No matter what, the next key to enumerate will be at least as large
        // as the current key.
        enumerationKey.KeyLow = memoryTableValue.Key;

        // The rowTransactionOutcome value needs to be acquired
        // before reading the sequence number for return determination.
        // It was good enough for searching, but the transaction outcome
        // might have changed and the sequence number increased
        // after locating this row, in which case we should not return it.
        // Since the write sequence number being too high is expected
        // to be the rare case, i.e. readers are reading recently committed rows,
        // acquire the WriteSequenceNumber now.
        auto memoryTableWriteSequenceNumber = memoryTableValue.WriteSequenceNumber.load(
            std::memory_order_acquire);

        auto writeSequenceNumber = ToSequenceNumber(
            memoryTableWriteSequenceNumber);
        auto transactionOutcome = GetTransactionOutcome(
            memoryTableWriteSequenceNumber);

        if (writeSequenceNumber > readSequenceNumber)
        {
            // We found a version of the row later than requested.
            // The actual outcome doesn't matter, because we wouldn't have returned the row anyway.

            // Change the enumeration key to be inclusive so that we'll
            // locate the highest sequence number for the row that
            // is lower than the requested sequence number.
            // As a hint to the skip list, we can increment the iterator to do one less comparison.
            ++findIterator;
            enumerationKey.KeyLowInclusivity = Inclusivity::Inclusive;
            enumerationKey.SequenceNumberToSkipForKeyLow = writeSequenceNumber;
        }
        // We potentially found a version of the row to return, but we have to check its outcome.
        else
        {
            if (transactionOutcome == TransactionOutcome::Unknown)
            {
                // We need to wait for resolution of this row.
                auto lock = co_await memoryTableValue.Mutex.scoped_lock_async();
                auto targetRowDelayedTransactionOutcome = memoryTableValue.DelayedTransactionOutcome;
                lock.unlock();

                if (delayedTransactionOutcome)
                {
                    transactionOutcome = (co_await delayedTransactionOutcome->ResolveTargetTransaction(
                        targetRowDelayedTransactionOutcome
                    )).Outcome;
                }
                else
                {
                    transactionOutcome = (co_await targetRowDelayedTransactionOutcome->GetOutcome())
                        .Outcome;
                }
                
                assert(transactionOutcome == TransactionOutcome::Aborted
                    || transactionOutcome == TransactionOutcome::Committed);
            }

            if (transactionOutcome == TransactionOutcome::Aborted)
            {
                // We found a version of the row later than requested.
                // Change the enumeration key to be inclusive so that we'll
                // locate the highest sequence number for the row that
                // is lower than the requested sequence number.
                // As a hint to the skip list, we can increment the iterator to do one less comparison.
                ++findIterator;
                enumerationKey.KeyLowInclusivity = Inclusivity::Inclusive;
                enumerationKey.SequenceNumberToSkipForKeyLow = writeSequenceNumber;
            }
            else
            {
                // The row resolved as Committed and the write sequence number is good.
                // Yield it to the caller.
                co_yield memoryTableValue.GetResultRow(*m_schema);

                // Change the enumeration key to be exclusive so that we'll
                // skip all lower sequence numbers for the same row.
                // As a hint to the skip list, we can increment the iterator to do one less comparison.
                ++findIterator;
                enumerationKey.KeyLowInclusivity = Inclusivity::Exclusive;
                enumerationKey.SequenceNumberToSkipForKeyLow.reset();
            }
        }

        keyComparisonResult = m_skipList.find_in_place(
            enumerationKey,
            findIterator
        );
    }
}

SequenceNumber MemoryTable::GetLatestSequenceNumber(
)
{
    return m_latestSequenceNumber.load(
        std::memory_order_acquire);
}

void MemoryTable::UpdateSequenceNumberRange(
    SequenceNumber writeSequenceNumber
)
{
    compare_exchange_weak_transform(
        m_earliestSequenceNumber,
        [=](auto value)
    {
        return writeSequenceNumber < value 
            ? writeSequenceNumber
            : value;
    },
        std::memory_order_relaxed,
        std::memory_order_release,
        std::memory_order_relaxed);

    compare_exchange_weak_transform(
        m_latestSequenceNumber,
        [=](auto value)
    {
        return writeSequenceNumber > value
            ? writeSequenceNumber
            : value;
    },
        std::memory_order_relaxed,
        std::memory_order_release,
        std::memory_order_relaxed
        );
}

row_generator MemoryTable::Checkpoint()
{
    auto end = m_skipList.end();
    for (auto iterator = m_skipList.begin();
        iterator != end;
        ++iterator)
    {
        auto& memoryTableValue = *iterator;
        auto transactionOutcome = GetTransactionOutcome(iterator->WriteSequenceNumber.load(std::memory_order_acquire));

        if (transactionOutcome == TransactionOutcome::Unknown)
        {
            // We need to wait for resolution of this row.
            auto lock = co_await memoryTableValue.Mutex.scoped_lock_async();
            auto targetRowDelayedTransactionOutcome = memoryTableValue.DelayedTransactionOutcome;
            lock.unlock();

            transactionOutcome = (co_await targetRowDelayedTransactionOutcome->GetOutcome())
                .Outcome;

            assert(transactionOutcome == TransactionOutcome::Aborted
                || transactionOutcome == TransactionOutcome::Committed);
        }

        if (transactionOutcome == TransactionOutcome::Committed)
        {
            co_yield iterator->GetResultRow(*m_schema);
        }
    }
}

MemoryTable::MemoryTableValue::MemoryTableValue(
    ReplayInsertionKey&& other
)
    :
    Key{ move(other.Key) },
    KeyRow{ move(other.Row) },
    WriteSequenceNumber
{
    ToMemoryTableOutcomeAndSequenceNumber(
        ToSequenceNumber(KeyRow->sequence_number()),
        TransactionOutcome::Committed)
}
{
    assert(KeyRow.get());
}

MemoryTable::MemoryTableValue::MemoryTableValue(
    InsertionKey&& other
)
    :
    Key{ move(other.Key) },
    KeyRow{ move(other.Row) },
    WriteSequenceNumber 
    {
        ToMemoryTableOutcomeAndSequenceNumber(
            ToSequenceNumber(KeyRow->sequence_number()),
            TransactionOutcome::Unknown)
    },
    DelayedTransactionOutcome{ other.DelayedTransactionOutcome }
{
    assert(KeyRow.get());
}

ResultRow MemoryTable::MemoryTableValue::GetResultRow(
    const Schema& schema
) const
{
    auto* valueRow = &KeyRow;
    if (ValueRow)
    {
        valueRow = &ValueRow;
    }

    auto keyAlignedMessage = GetKeyMessage();
    auto valueAlignedMessage = GetValueMessage();
    auto transactionId = GetTransactionId();

    ResultRow resultRow;
    resultRow.Key = SchemaDescriptions::MakeProtoValueKey(
        schema,
        AlignedMessageData
        {
            DataReference<StoredMessage>(KeyRow),
            keyAlignedMessage
        }
    );

    resultRow.WriteSequenceNumber = GetWriteSequenceNumber();
    resultRow.Value = SchemaDescriptions::MakeProtoValueValue(
        schema,
        AlignedMessageData
        {
            DataReference<StoredMessage>(*valueRow),
            valueAlignedMessage
        });
    resultRow.TransactionId = transactionId;

    return std::move(resultRow);
}


AlignedMessage MemoryTable::MemoryTableValue::GetKeyMessage() const
{
    return GetAlignedMessage(
        KeyRow->key());
}

AlignedMessage MemoryTable::MemoryTableValue::GetValueMessage() const
{
    if (ValueRow)
    {
        return GetAlignedMessage(
            ValueRow->value());
    }
    else
    {
        return GetAlignedMessage(
            KeyRow->value());
    }
}

TransactionIdReference MemoryTable::MemoryTableValue::GetTransactionId() const
{
    if (ValueRow)
    {
        return MakeTransactionIdReference(
            ValueRow,
            ValueRow->distributed_transaction_id());
    }
    else
    {
        return MakeTransactionIdReference(
            KeyRow,
            KeyRow->distributed_transaction_id());
    }
}

SequenceNumber MemoryTable::MemoryTableValue::GetWriteSequenceNumber() const
{
    if (ValueRow)
    {
        return ToSequenceNumber(ValueRow->sequence_number());
    }
    else
    {
        return ToSequenceNumber(KeyRow->sequence_number());
    }
}

MemoryTable::ReplayInsertionKey::ReplayInsertionKey(
    const Schema& schema,
    MemoryTable::Row& row
) :
    Key(SchemaDescriptions::MakeProtoValueKey(
        schema,
        row->key())),
    Row(row)
{}

MemoryTable::InsertionKey::InsertionKey(
    const Schema& schema,
    MemoryTable::Row& row,
    shared_ptr<DelayedMemoryTableTransactionOutcome>& delayedTransactionOutcome,
    SequenceNumber readSequenceNumber)
    :
    Key(SchemaDescriptions::MakeProtoValueKey(
        schema,
        row->key())),
    Row(row),
    DelayedTransactionOutcome(delayedTransactionOutcome),
    ReadSequenceNumber(readSequenceNumber)
{
}

MemoryTable::InsertionKey& MemoryTable::InsertionKey::InsertionKey::operator=(
    MemoryTableValue&& memoryTableValue)
{
    Row = move(memoryTableValue.KeyRow);
    return *this;
}

MemoryTable::MemoryTableRowComparer::MemoryTableRowComparer(
    shared_ptr<const Schema> schema,
    shared_ptr<const ValueComparer> keyComparer
) :
    m_schema(std::move(schema)),
    m_keyComparer(std::move(keyComparer))
{}

const ProtoValue& MemoryTable::MemoryTableRowComparer::MakeProtoValueKey(
    const EnumerationKey& value
) const
{
    return value.KeyLow;
}

const ProtoValue& MemoryTable::MemoryTableRowComparer::MakeProtoValueKey(
    const KeyRangeEnd& value
) const
{
    return value.Key;
}

std::weak_ordering MemoryTable::MemoryTableRowComparer::operator()(
    const MemoryTableValue& key1,
    const MemoryTableValue& key2
    ) const
{
    auto comparisonResult = m_keyComparer->Compare(
        key1.Key,
        key2.Key);

    return comparisonResult;
}

std::weak_ordering MemoryTable::MemoryTableRowComparer::operator()(
    const MemoryTableValue& key1,
    const InsertionKey& key2
    ) const
{
    auto comparisonResult = m_keyComparer->CompareNoMinMax(
        key1.Key,
        key2.Key);

    if (comparisonResult != std::weak_ordering::equivalent)
    {
        return comparisonResult;
    }

    auto sequenceNumber = ToSequenceNumber(key1.WriteSequenceNumber.load(std::memory_order_acquire));

    if (sequenceNumber > key2.ReadSequenceNumber
        ||
        sequenceNumber >= ToSequenceNumber(key2.Row->sequence_number()))
    {
        // By returning equivalent here, the SkipList
        // won't insert the row.
        return std::weak_ordering::equivalent;
    }

    return std::weak_ordering::greater;
}

std::weak_ordering MemoryTable::MemoryTableRowComparer::operator()(
    const MemoryTableValue& key1,
    const EnumerationKey& key2
    ) const
{
    // If the requested key comparison is for a prefix,
    // then we are enumerating a range, and we need to seek to the first
    // key that matches the prefix.
    // If the current key matches the prefix, then we return "greater"
    // regardless of other concerns so that we keep searching for a lower key.
    // This might be the first key to enumerate; that's fine. The enumeration logic
    // will switch to searching for specific keys / sequence numbers after locating
    // the enumeration start point.
    if (key2.LastFieldId
        &&
        m_keyComparer->IsPrefixOf(Prefix{ MakeProtoValueKey(key2), *key2.LastFieldId }, key1.Key))
    {
        return std::weak_ordering::greater;
    }
        
    auto comparisonResult = m_keyComparer->Compare(
        key1.Key,
        MakeProtoValueKey(key2)
    );

    if (comparisonResult == std::weak_ordering::equivalent
        &&
        key2.KeyLowInclusivity == Inclusivity::Exclusive)
    {
        return std::weak_ordering::less;
    }

    if (comparisonResult != std::weak_ordering::equivalent)
    {
        return comparisonResult;
    }

    auto sequenceNumber = key1.GetWriteSequenceNumber();

    if (comparisonResult == std::weak_ordering::equivalent
        &&
        key2.SequenceNumberToSkipForKeyLow.has_value()
        &&
        key2.SequenceNumberToSkipForKeyLow <= sequenceNumber)
    {
        return std::weak_ordering::less;
    }

    if (sequenceNumber > key2.ReadSequenceNumber)
    {
        return std::weak_ordering::less;
    }

    if (sequenceNumber == key2.ReadSequenceNumber)
    {
        return std::weak_ordering::equivalent;
    }

    return std::weak_ordering::greater;
}

std::weak_ordering MemoryTable::MemoryTableRowComparer::operator()(
    const MemoryTableValue& key1,
    const KeyRangeEnd& key2
    ) const
{
    std::weak_ordering comparisonResult;
    
    if (key2.LastFieldId
        &&
        m_keyComparer->IsPrefixOf(Prefix { key2.Key, *key2.LastFieldId }, key1.Key))
    {
        comparisonResult = std::weak_ordering::equivalent;
    }
    else
    {
        comparisonResult = m_keyComparer->Compare(
            key1.Key,
            MakeProtoValueKey(key2)
        );
    }

    if (comparisonResult == std::weak_ordering::equivalent
        &&
        key2.Inclusivity == Inclusivity::Exclusive)
    {
        return std::weak_ordering::greater;
    }

    return comparisonResult;
}

std::weak_ordering MemoryTable::MemoryTableRowComparer::operator()(
    const MemoryTableValue& key1,
    const ReplayInsertionKey& key2
    ) const
{
    auto comparisonResult = m_keyComparer->Compare(
        key1.Key,
        key2.Key);

    if (comparisonResult == std::weak_ordering::equivalent)
    {
        comparisonResult = ToSequenceNumber(key2.Row->sequence_number()) <=> key1.GetWriteSequenceNumber();
    }

    return comparisonResult;
}

DelayedMemoryTableTransactionOutcome::DelayedMemoryTableTransactionOutcome(
    MemoryTableTransactionSequenceNumber originatingTransactionSequenceNumber
) : m_originatingTransactionSequenceNumber(originatingTransactionSequenceNumber)
{
    m_outcomeTask = GetOutcomeImpl();
}

shared_task<MemoryTableTransactionOutcome> DelayedMemoryTableTransactionOutcome::GetOutcomeImpl()
{
    co_await m_resolvedSignal;
    co_return ToMemoryTableTransactionOutcome(
        m_outcomeAndSequenceNumber.load(std::memory_order_relaxed)
    );
}

shared_task<MemoryTableTransactionOutcome> DelayedMemoryTableTransactionOutcome::GetOutcome()
{
    return m_outcomeTask;
}

MemoryTableTransactionOutcome DelayedMemoryTableTransactionOutcome::BeginCommit(
    SequenceNumber writeSequenceNumber)
{
    auto previousResult = MemoryTableOutcomeAndSequenceNumber::Earliest;
    auto committedResult = ToMemoryTableOutcomeAndSequenceNumber(writeSequenceNumber, TransactionOutcome::Committed);

    if (!m_outcomeAndSequenceNumber.compare_exchange_strong(
        previousResult,
        committedResult,
        std::memory_order_acq_rel))
    {
        return ToMemoryTableTransactionOutcome(previousResult);
    }

    return ToMemoryTableTransactionOutcome(committedResult);
}

void DelayedMemoryTableTransactionOutcome::Complete()
{
    auto previousValue = m_outcomeAndSequenceNumber.load(std::memory_order_acquire);
    if (previousValue == MemoryTableOutcomeAndSequenceNumber::Earliest)
    {
        m_outcomeAndSequenceNumber.compare_exchange_strong(
            previousValue,
            MemoryTableOutcomeAndSequenceNumber::OutcomeAborted,
            std::memory_order_release
        );
    }

    m_resolvedSignal.set();
}

task<MemoryTableTransactionOutcome> DelayedMemoryTableTransactionOutcome::ResolveTargetTransaction(
    shared_ptr<DelayedMemoryTableTransactionOutcome> targetTransaction)
{
    auto resolvingTransactionWriteLock = co_await m_deadlockDetectionLock.writer().scoped_lock_async();
    m_currentDeadlockDetectionResolutionTarget = std::move(targetTransaction);
    resolvingTransactionWriteLock.unlock();

    // Now search the list of transactions, looking for the latest transaction we find,
    // and looking for ourself to see if there's a loop.
    shared_ptr<DelayedMemoryTableTransactionOutcome> latestTransaction = m_currentDeadlockDetectionResolutionTarget;
    shared_ptr<DelayedMemoryTableTransactionOutcome> deadlockedTransaction = m_currentDeadlockDetectionResolutionTarget;
    while (
        deadlockedTransaction
        &&
        deadlockedTransaction.get() != this)
    {
        auto targetTransactionReadLock = co_await deadlockedTransaction->m_deadlockDetectionLock.reader().scoped_lock_async();
        deadlockedTransaction = deadlockedTransaction->m_currentDeadlockDetectionResolutionTarget;
        targetTransactionReadLock.unlock();

        if (deadlockedTransaction
            &&
            deadlockedTransaction->m_originatingTransactionSequenceNumber > latestTransaction->m_originatingTransactionSequenceNumber)
        {
            latestTransaction = deadlockedTransaction;
        }
    }

    if (deadlockedTransaction.get() == this)
    {
        // We found ourself in the list of deadlocked transactions, meaning we are involved in a loop.
        // Abort the most recent transaction in the deadlock list,
        // preferring to allow older transactions to continue to do work.
        latestTransaction->Complete();
    }

    auto result = co_await m_currentDeadlockDetectionResolutionTarget->GetOutcome();

    resolvingTransactionWriteLock = co_await m_deadlockDetectionLock.writer().scoped_lock_async();
    m_currentDeadlockDetectionResolutionTarget = nullptr;
    resolvingTransactionWriteLock.unlock();

    co_return result;
}

std::shared_ptr<IMemoryTable> MakeMemoryTable(
    shared_ptr<const Schema> schema,
    shared_ptr<const ValueComparer> keyComparer
)
{
    return std::make_shared<MemoryTable>(
        std::move(schema),
        std::move(keyComparer)
    );
}

}
