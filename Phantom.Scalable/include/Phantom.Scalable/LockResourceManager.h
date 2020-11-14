#pragma once

#include "ResourceManager.h"

namespace Phantom::Scalable
{

class LockResourceManager
    :
    public IResourceManager
{
    task<ScalablePerformResult> PerformInternalOperation(
        IResourceManagerPerformContext& context,
        const InternalOperation& internalOperation
    );

    task<ScalablePerformResult> PerformGet(
        IResourceManagerPerformContext& context,
        const LockManagerGetOperation& operation
    );

    task<ScalablePerformResult> PerformSet(
        IResourceManagerPerformContext& context,
        const LockManagerSetOperation& operation
    );

public:
    virtual task<ScalablePerformResult> Perform(
        IResourceManagerPerformContext& context,
        const Grpc::ScalablePerformOperation& operation
    ) override;

};

}
