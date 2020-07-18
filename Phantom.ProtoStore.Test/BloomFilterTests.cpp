#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/BloomFilter.h"

namespace Phantom::ProtoStore
{

struct BloomFilterIdentityHashFunction
{};

TEST(BloomFilterTests, can_construct)
{
    BloomFilter<BloomFilterIdentityHashFunction> bloomFilter(
        1024);

}
}