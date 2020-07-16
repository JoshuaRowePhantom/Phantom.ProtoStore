#include "IndexImpl.h"
#include "src/ProtoStoreInternal.pb.h"

namespace Phantom::ProtoStore
{

Index::Index(
    const string& indexName,
    IndexNumber indexNumber,
    SequenceNumber createSequenceNumber,
    shared_ptr<IMessageFactory> keyFactory,
    shared_ptr<IMessageFactory> valueFactory
)
    :
    m_indexName(indexName),
    m_indexNumber(indexNumber)
{
}

IndexNumber Index::GetIndexNumber() const
{
    return m_indexNumber;
}

const IndexName& Index::GetIndexName() const
{
    return m_indexName;
}

task<ReadResult> Index::Read(
    const ReadRequest& readRequest
)
{
    throw 0;
}

task<> Index::Join()
{
    co_return;
}

}