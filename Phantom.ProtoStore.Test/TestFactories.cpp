#include "TestFactories.h"
#include "Phantom.ProtoStore/src/Schema.h"
#include "Phantom.System/utility.h"

namespace Phantom::ProtoStore
{

task<std::shared_ptr<IIndexData>> MakeInMemoryIndex(
    IndexName indexName,
    const Schema& schema
)
{
    static std::atomic<IndexNumber> nextIndexNumber = 10000;

    auto schemaPtr = copy_shared(schema);
    auto indexNumber = nextIndexNumber.fetch_add(1);

    auto keyComparer = SchemaDescriptions::MakeKeyComparer(
        schemaPtr);
    auto valueComparer = SchemaDescriptions::MakeValueComparer(
        schemaPtr);

    auto index = MakeIndex(
        indexName,
        indexNumber,
        ToSequenceNumber(0),
        keyComparer,
        valueComparer,
        nullptr,
        schemaPtr
    );

    auto memoryTable = MakeMemoryTable(
        schemaPtr,
        keyComparer);

    co_await index->SetDataSources(
        memoryTable,
        0,
        {},
        {}
    );

    co_return index;
}

}

