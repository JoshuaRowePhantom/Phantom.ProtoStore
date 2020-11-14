#pragma once

#include <cppcoro/shared_task.hpp>
#include <cppcoro/task.hpp>
#include <memory>

namespace Phantom::Scalable
{
using cppcoro::task;
using cppcoro::shared_task;
using std::shared_ptr;
using std::unique_ptr;
}
