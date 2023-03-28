#include "Phantom.ProtoStore/src/Index.h"
#include "Phantom.ProtoStore/src/ValueComparer.h"

namespace Phantom::ProtoStore
{

class TestFactories
{
protected:
    std::atomic<uint64_t> m_nextTestLocalTransactionId;
    std::atomic<uint64_t> m_nextTestWriteId;
    std::atomic<uint64_t> m_nextWriteSequenceNumber;

    task<std::shared_ptr<IIndexData>> MakeInMemoryIndex(
        IndexName indexName,
        const Schema& schema
    );

    task<OperationResult<>> AddRow(
        const std::shared_ptr<IIndexData>& index,
        ProtoValue key,
        ProtoValue value,
        std::optional<SequenceNumber> writeSequenceNumber = std::nullopt,
        SequenceNumber readSequenceNumber = SequenceNumber::Latest
    );
};

}
