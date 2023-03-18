#include "Phantom.ProtoStore/src/Index.h"

namespace Phantom::ProtoStore
{

task<std::shared_ptr<IIndexData>> MakeInMemoryIndex(
    IndexName indexName,
    const Schema& schema
);

}
