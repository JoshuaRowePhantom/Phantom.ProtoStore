
#include "Utility.h"

void DumpMessage(
    const string& name,
    const Message& message,
    ExtentOffset offset
)
{
    DumpMessage(
        name,
        message.DebugString(),
        offset);
}

void DumpMessage(
    const string& name,
    const string& message,
    ExtentOffset offset
)
{
    cout << name << " @ [" << offset << "]\n" << message << "\n";
}

void PrintUsage()
{
    cout <<
        "Phantom.ProtoStore.Utility: \n"
        "    DumpLog <log>\n"
        "    DumpPartition <data>\n";
}

int main(
    int argn,
    char** argv)
{
    return Phantom::run_async([=]() -> cppcoro::task<int>
    {
        deque<string> args(
            argv,
            argv + argn);

        args.pop_front();

        if (args.empty())
        {
            PrintUsage();
            co_return 0;
        }

        auto arg = args.front();
        args.pop_front();

        if (arg == "DumpPartition")
        {
            if (args.empty())
            {
                PrintUsage();
                co_return 0;
            }

            auto data = args.front();
            args.pop_front();

            co_await DumpPartition(
                data);

            co_return 0;
        }

        if (arg == "DumpLog")
        {
            if (args.empty())
            {
                PrintUsage();
                co_return 0;
            }

            auto logPath = args.front();
            args.pop_front();

            co_await DumpLog(
                logPath);

            co_return 0;
        }    });
}