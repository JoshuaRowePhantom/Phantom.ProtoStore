
#include <queue>
#include <iostream>
#include <vector>
#include "Phantom.System/utility.h"
#include <cppcoro/task.hpp>

using namespace std;

void PrintUsage()
{
    cout <<
        "Phantom.ProtoStore.Utility: \n"
        "    DumpPartition <header> <data>\n";
}

cppcoro::task<> DumpPartition(
    string headerPath,
    string dataPath);

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

            auto header = args.front();
            args.pop_front();

            if (args.empty())
            {
                PrintUsage();
                co_return 0;
            }

            auto data = args.front();
            args.pop_front();

            co_await DumpPartition(
                header,
                data);

            co_return 0;
        }
    });
}