#include "StandardIncludes.h"
#include "Phantom.Scalable/LockResourceManager.h"

namespace Phantom::Scalable
{

task<ScalablePerformResult> LockResourceManager::Perform(
    IResourceManagerPerformContext& context,
    const ScalablePerformOperation& operation
)
{
    auto lockManagerOperation = FromAnyMessage(
        operation,
        &ScalablePerformOperation::has_payload,
        &ScalablePerformOperation::payload,
        &ScalablePerformOperation::has_lockmanagerpayload,
        &ScalablePerformOperation::lockmanagerpayload);

    if (lockManagerOperation->has_get())
    {
    }
    
    if (lockManagerOperation->has_set())
    {

    }
}

}
