#include "Phantom.ProtoStore/Errors.h"

namespace Phantom::ProtoStore
{

class ProtoStoreErrorCategoryImpl : public std::error_category
{
    virtual const char* name() const noexcept override
    {
        return "ProtoStore";
    }

    virtual std::string message(int errorValue) const override
    {
        static std::string AbortedTransaction = "Aborted transaction";
        static std::string WriteConflict = "Write conflict";
        static std::string UnresolvedTransaction = "Unresolved transaction";

        switch (static_cast<ProtoStoreErrorCode>(errorValue))
        {
        case ProtoStoreErrorCode::AbortedTransaction:
            return AbortedTransaction;
        case ProtoStoreErrorCode::WriteConflict:
            return WriteConflict;
        case ProtoStoreErrorCode::UnresolvedTransaction:
            return UnresolvedTransaction;
        default:
            return "Unknown error";
        }
    }
};

const std::error_category& ProtoStoreErrorCategory()
{
    static ProtoStoreErrorCategoryImpl result;
    return result;
}

std::error_code make_error_code(
    ProtoStoreErrorCode errorCode
)
{
    return std::error_code{ static_cast<int>(errorCode), ProtoStoreErrorCategory() };
}

std::unexpected<std::error_code> make_unexpected(
    ProtoStoreErrorCode errorCode
)
{
    return std::unexpected{ make_error_code(errorCode) };
}

std::unexpected<std::error_code> abort_transaction()
{
    return make_unexpected(ProtoStoreErrorCode::AbortedTransaction);
}


FailedResult::operator std::error_code() const
{
    return ErrorCode;
}

FailedResult::operator std::unexpected<std::error_code>() const
{
    return std::unexpected{ ErrorCode };
}

FailedResult::operator std::unexpected<FailedResult>() const&
{
    return std::unexpected{ *this };
}

FailedResult::operator std::unexpected<FailedResult>()&&
{
    return std::unexpected{ std::move(*this) };
}

}