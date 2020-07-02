#pragma once

#include "StandardTypes.h"

namespace Phantom::ProtoStore
{
class ProtoStoreFactory
    :
    public IProtoStoreFactory
{
public:
    virtual task<shared_ptr<IProtoStore>> Open(
        const OpenProtoStoreRequest& openRequest
    ) override;

    virtual task<shared_ptr<IProtoStore>> Create(
        const CreateProtoStoreRequest& openRequest
    ) override;
};
}