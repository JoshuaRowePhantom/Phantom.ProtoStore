#pragma once

#include <expected>
#include <system_error>
#include <variant>

#include "ProtoIndex.h"

namespace Phantom::ProtoStore
{

struct WriteConflict
{
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

enum class TransactionOutcome : uint8_t
{
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

template<
    typename Result = void
>
using OperationResult = std::expected<Result, FailedResult>;

template<
    typename Result
> Result&& throw_if_failed(
    OperationResult<Result>&& operationResult
)
{
    if (!operationResult)
    {
        std::move(operationResult).error().throw_exception();
    }
    return std::move(*operationResult);
}

template<
    typename Result
> Result& throw_if_failed(
    const OperationResult<Result>& operationResult
)
{
    if (!operationResult)
    {
        operationResult.error().throw_exception();
    }
}

inline void throw_if_failed(
    const OperationResult<>& operationResult
)
{
    if (!operationResult)
    {
        operationResult.error().throw_exception();
    }
}

class ProtoStoreException : public std::exception
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

extern const std::error_category& ProtoStoreErrorCategory();

enum class ProtoStoreErrorCode
{
    AbortedTransaction,
    WriteConflict,
    UnresolvedTransaction,
    IndexNotFound,
};

std::error_code make_error_code(
    ProtoStoreErrorCode errorCode
);

std::unexpected<std::error_code> make_unexpected(
    ProtoStoreErrorCode errorCode
);

// Operation processors can return this error code
// to generically abort the operation.
std::unexpected<std::error_code> abort_transaction();

}