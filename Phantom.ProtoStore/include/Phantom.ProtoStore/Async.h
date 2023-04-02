#pragma once

#include "Errors.h"
#include "Phantom.Coroutines/early_termination_task.h"
#include "Phantom.Coroutines/expected_early_termination.h"
#include "Phantom.Coroutines/reusable_task.h"

namespace Phantom::ProtoStore
{

using cppcoro::async_generator;

template<
    typename Result = void
> using StatusResult = std::expected<Result, std::error_code>;

template<
    typename Result = void
> using status_task =
Phantom::Coroutines::basic_reusable_task
<
    Phantom::Coroutines::derived_promise
    <
        Phantom::Coroutines::reusable_task_promise
        <
            StatusResult<Result>
        >,
        Phantom::Coroutines::expected_early_termination_transformer,
        Phantom::Coroutines::await_all_await_transform
    >
>;

struct FailedResult;

template<
    typename Result = void
>
using OperationResult = std::expected<Result, FailedResult>;

template<
    typename Result = void
> using operation_task =
Phantom::Coroutines::basic_reusable_task
<
    Phantom::Coroutines::derived_promise
    <
        Phantom::Coroutines::reusable_task_promise
        <
            OperationResult<Result>
        >,
        Phantom::Coroutines::expected_early_termination_transformer,
        Phantom::Coroutines::await_all_await_transform
    >
>;

template<
    typename Result = void
> using task =
Phantom::Coroutines::reusable_task<Result>;

template<
    typename Result
> using operation_generator = async_generator<OperationResult<Result>>;

}