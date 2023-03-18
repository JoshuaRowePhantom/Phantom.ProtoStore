#include "Phantom.ProtoStore/src/Index.h"
#include "Phantom.ProtoStore/src/ValueComparer.h"

namespace Phantom::ProtoStore
{

extern std::atomic<uint64_t> testLocalTransactionId;
extern std::atomic<uint64_t> testWriteId;

task<std::shared_ptr<IIndexData>> MakeInMemoryIndex(
    IndexName indexName,
    const Schema& schema
);

task<OperationResult<>> AddRow(
    const std::shared_ptr<IIndexData>& index,
    ProtoValue key,
    ProtoValue value,
    SequenceNumber writeSequenceNumber = SequenceNumber::Earliest,
    SequenceNumber readSequenceNumber = SequenceNumber::Latest
);

}
