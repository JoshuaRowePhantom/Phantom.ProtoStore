#include <queue>
#include <iostream>
#include <sstream>
#include <vector>
#include "Phantom.System/utility.h"
#include "src/ProtoStoreInternal.pb.h"
#include "Phantom.ProtoStore/src/MemoryMappedFileExtentStore.h"
#include "Phantom.ProtoStore/src/MessageStore.h"
#include <cppcoro/task.hpp>

using namespace std;
using namespace Phantom::ProtoStore;

void DumpMessage(
    string name,
    Message& message,
    ExtentOffset offset
);

cppcoro::task<> DumpPartition(
    string dataPath);

cppcoro::task<> DumpLog(
    string logPath);
