#include "Phantom.ProtoStore/Scheduler.h"
#include <cppcoro/inline_scheduler.hpp>
#include <cppcoro/static_thread_pool.hpp>

namespace Phantom::ProtoStore
{

Schedulers Schedulers::Default()
{
    static std::shared_ptr<IScheduler> scheduler = std::make_shared<DefaultScheduler<cppcoro::static_thread_pool>>();

    static Schedulers schedulers =
    {
        .LockScheduler = scheduler,
        .IoScheduler = scheduler,
        .ComputeScheduler = scheduler,
    };

    return schedulers;
}

Schedulers Schedulers::Inline()
{
    static std::shared_ptr<IScheduler> scheduler = std::make_shared<DefaultScheduler<cppcoro::inline_scheduler>>();

    static Schedulers schedulers =
    {
        .LockScheduler = scheduler,
        .IoScheduler = scheduler,
        .ComputeScheduler = scheduler,
    };

    return schedulers;
}

}
