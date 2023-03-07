#pragma once

#include <expected>
#include <system_error>
#include <variant>

#include "ProtoIndex.h"

namespace Phantom::ProtoStore
{

struct WriteConflict
{
    ProtoIndex Index;
    SequenceNumber ConflictingSequenceNumber;

    friend bool operator ==(
        const WriteConflict&,
        const WriteConflict&
        ) = default;
};

struct UnresolvedTransaction
{
    TransactionId UnresolvedTransactionId;

    friend bool operator ==(
        const UnresolvedTransaction&,
        const UnresolvedTransaction&
        ) = default;
};

enum class TransactionOutcome {
    Unknown = 0,
    Committed = 1,
    Aborted = 2,
};

struct TransactionFailedResult
{
    // The transaction outcome,
    // which will be either Aborted or Unknown.
    TransactionOutcome TransactionOutcome;

    // The failures that were encountered during
    // execution of the transaction. These failures
    // may have been ignored by the actual transaction
    // processor.
    //std::vector<FailedResult> Failures;

    friend bool operator==(
        const TransactionFailedResult&,
        const TransactionFailedResult&
        ) = default;
};

struct FailedResult
{
    std::error_code ErrorCode;

    using error_details_type = std::variant<
        std::monostate,
        WriteConflict,
        UnresolvedTransaction,
        TransactionFailedResult
    >;

    error_details_type ErrorDetails;

    operator std::error_code() const;
    operator std::unexpected<std::error_code>() const;
    operator std::unexpected<FailedResult>() const&;
    operator std::unexpected<FailedResult>()&&;

    friend bool operator ==(
        const FailedResult&,
        const FailedResult&
        ) = default;

    [[noreturn]]
    void throw_exception(
        this auto&& self);
};

class ProtoStoreException : std::exception
{
public:
    const FailedResult Result;

    ProtoStoreException(
        FailedResult result
    ) : Result(std::move(result))
    {
    }
};

[[noreturn]]
void FailedResult::throw_exception(
    this auto&& self)
{
    throw ProtoStoreException{ std::forward<decltype(self)>(self) };
}

}