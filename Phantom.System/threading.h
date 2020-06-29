#include <chrono>
#include <experimental/coroutine>

namespace Phantom::System {

    typedef std::chrono::time_point<std::chrono::system_clock> utc_timepoint;
    typedef std::chrono::system_clock::duration delay_duration;

    class ThreadingProvider
    {
    public:
        virtual utc_timepoint UtcNow() = 0;
    };

    class SystemThreadingProvider final
        : public ThreadingProvider
    {
    };

    class SimulatedThreadingProvider final
        : public ThreadingProvider
    {

    };
}
