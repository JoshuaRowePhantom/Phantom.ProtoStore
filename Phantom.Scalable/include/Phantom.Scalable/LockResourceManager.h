#pragma once

#include "ResourceManager.h"

namespace Phantom::Scalable
{

class LockResourceManager
    :
    public IResourceManager
{
public:
    virtual task<ScalablePerformResult> Perform(
        IResourceManagerPerformContext& context,
        const Grpc::ScalablePerformOperation& operation
    ) override;

};

}
