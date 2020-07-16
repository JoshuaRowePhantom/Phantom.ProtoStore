#pragma once

#include "StandardTypes.h"

namespace Phantom::ProtoStore
{

class IIndex
{
public:
    virtual task<ReadResult> Read(
        const ReadRequest& readRequest
    ) = 0;

    virtual IndexNumber GetIndexNumber(
    ) const = 0;

    virtual const IndexName& GetIndexName(
    ) const = 0;

    virtual task<> Join(
    ) = 0;
};

}
