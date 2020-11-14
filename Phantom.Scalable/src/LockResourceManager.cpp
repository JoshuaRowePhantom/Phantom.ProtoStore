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
        return PerformGet(
            context,
            lockManagerOperation->get());
    }
    
    if (lockManagerOperation->has_set())
    {
        return PerformSet(
            context,
            lockManagerOperation->set());
    }

    throw 0;
}

task<ScalablePerformResult> LockResourceManager::PerformGet(
    IResourceManagerPerformContext& context,
    const LockManagerGetOperation& operation
)
{
    InternalOperation internalOperation;

    internalOperation.mutable_lockmanagerpayload()->mutable_lockmanageroperation()->mutable_get()->CopyFrom(
        operation);

    return PerformInternalOperation(
        context,
        internalOperation);
}

task<ScalablePerformResult> LockResourceManager::PerformSet(
    IResourceManagerPerformContext& context,
    const LockManagerSetOperation& operation
)
{
    InternalOperation internalOperation;

    internalOperation.mutable_lockmanagerpayload()->mutable_lockmanageroperation()->mutable_set()->CopyFrom(
        operation);

    return PerformInternalOperation(
        context,
        internalOperation);
}


task<ScalablePerformResult> LockResourceManager::PerformInternalOperation(
    IResourceManagerPerformContext& context,
    const InternalOperation& internalOperation
)
{
    auto internalOperationResult = co_await context.AddOperation(
        InternalOperationIdentifier{},
        internalOperation
    );

    ScalablePerformResult performResult;

    if (internalOperationResult.has_fault())
    {
        *performResult.mutable_fault() = move(*internalOperationResult.mutable_fault());
    }
    else if (internalOperationResult.has_lockmanagerpayload()
        && internalOperationResult.lockmanagerpayload().has_lockmanagerresult())
    {
        *performResult.mutable_lockmanagerpayload() = move(*internalOperationResult.mutable_lockmanagerpayload()->mutable_lockmanagerresult());
    }

    co_return performResult;
}
}
