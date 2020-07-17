#pragma once

#include "PartitionWriter.h"

namespace Phantom::ProtoStore
{
class PartitionWriter
    :
    public IPartitionWriter
{
    shared_ptr<ISequentialMessageWriter> m_dataWriter;
    shared_ptr<ISequentialMessageWriter> m_headerWriter;

public:
    PartitionWriter(
        shared_ptr<ISequentialMessageWriter> dataWriter,
        shared_ptr<ISequentialMessageWriter> headerWriter
    );

    virtual task<> WriteRows(
        cppcoro::async_generator<const MemoryTableRow*> rows
    ) override;
};

}
