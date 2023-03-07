#include "Phantom.ProtoStore/Errors.h"

namespace Phantom::ProtoStore
{

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