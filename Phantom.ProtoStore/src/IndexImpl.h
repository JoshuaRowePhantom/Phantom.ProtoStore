#pragma once

#include "Index.h"
#include "Schema.h"

namespace Phantom::ProtoStore
{

class Index
    : public IIndex
{
    IndexName m_indexName;
    IndexNumber m_indexNumber;

public:
    Index(
        const string& indexName,
        IndexNumber indexNumber,
        SequenceNumber createSequenceNumber,
        shared_ptr<IMessageFactory> keyFactory,
        shared_ptr<IMessageFactory> valueFactory
    );

    virtual task<ReadResult> Read(
        const ReadRequest& readRequest
    ) override;

    virtual IndexNumber GetIndexNumber(
    ) const override;

    virtual const IndexName& GetIndexName(
    ) const override;

    virtual task<> Join(
    ) override;
};

}
