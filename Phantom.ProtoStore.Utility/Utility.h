#include "Phantom.ProtoStore/Phantom.ProtoStore.h"
#include "Phantom.ProtoStore/src/MemoryMappedFileExtentStore.h"
#include "Phantom.ProtoStore/src/MessageStore.h"
#include "Phantom.System/async_utility.h"
#include "Phantom.System/utility.h"
#include "ProtoStoreInternal.pb.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"
#include <cppcoro/task.hpp>
#include <iostream>
#include <queue>
#include <sstream>
#include <vector>

using namespace std;
using namespace Phantom::ProtoStore;

void DumpMessage(
    const string& name,
    const Message& message,
    ExtentOffset offset
);

void DumpMessage(
    const string& name,
    const string& message,
    ExtentOffset offset
);

cppcoro::task<> DumpPartition(
    string dataPath);

cppcoro::task<> DumpLog(
    string logPath);

cppcoro::task<> DumpHeader(
    string headerPath);
