#include "SkipList.h"

namespace Phantom::ProtoStore
{

thread_local std::minstd_rand tls_SkipListRng
{
    std::random_device{}()
};

}