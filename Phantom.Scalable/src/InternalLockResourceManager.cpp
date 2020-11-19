#include "StandardIncludes.h"
#include "Phantom.Scalable/AnyMessage.h"
#include "Phantom.Scalable/DataComponentString.h"
#include "Phantom.Scalable/InternalLockResourceManager.h"
#include "Phantom.Scalable/InternalLockResourceManagerDatabase.h"

namespace Phantom::Scalable
{

AnyMessage<InternalLockManagerOperation> InternalLockResourceManager::GetLockManagerOperation(
    IInternalResourceManagerOperationContext* context
)
{
    auto& internalOperation = context->GetInternalOperation();
    return FromAnyMessage<InternalLockManagerOperation>(
        internalOperation,
        &InternalOperation::has_payload,
        &InternalOperation::payload,
        &InternalOperation::has_lockmanagerpayload,
        &InternalOperation::lockmanagerpayload);
}

task<> InternalLockResourceManager::Propose(
    IInternalResourceManagerProposeContext* context
)
{
    auto lockManagerOperation = GetLockManagerOperation(
        context);

    // For now, all operations interfere with all others.
    InternalOperationInterferenceRelationship relationship;

    co_await context->AddInterferenceRelationship(
        relationship);

    co_return;
}

task<> InternalLockResourceManager::Prepare(
    IInternalResourceManagerPrepareContext* context
)
{
    auto lockManagerOperation = GetLockManagerOperation(
        context);

    if (lockManagerOperation->has_lockmanageroperation())
    {
        if (lockManagerOperation->lockmanageroperation().has_get())
        {
            return PrepareGet(
                context,
                lockManagerOperation->lockmanageroperation().get()
            );
        }

        if (lockManagerOperation->lockmanageroperation().has_set())
        {
            return PrepareSet(
                context,
                lockManagerOperation->lockmanageroperation().set()
            );
        }
    }

    return context->Fail();
}

task<> InternalLockResourceManager::PrepareGet(
    IInternalResourceManagerPrepareContext* context,
    const Grpc::LockManagerGetOperation& getOperation
)
{
    Grpc::Internal::InternalOperationResult result;
    Grpc::LockManagerGetResult* getResult = result.mutable_lockmanagerpayload()->mutable_lockmanagerresult()->mutable_get();

    LockMetadataValue lockMetadataValue;
    LockContentValue lockContentValue;

    if (co_await m_database->Read(
        context->GetStoreOperation(),
        getOperation.lockname(),
        lockMetadataValue,
        &lockContentValue
    )
        &&
        lockMetadataValue.has_committed())
    {
        getResult->mutable_content()->set_version(
            lockMetadataValue.committed().version());
        getResult->mutable_content()->set_data(
            move(*lockContentValue.mutable_data()));
    }

    co_await context->SetResult(
        move(result));
}

task<> InternalLockResourceManager::PrepareSet(
    IInternalResourceManagerPrepareContext* context,
    const Grpc::LockManagerSetOperation& setOperation
)
{
    Grpc::Internal::InternalOperationResult result;
    Grpc::LockManagerSetResult* setResult = result.mutable_lockmanagerpayload()->mutable_lockmanagerresult()->mutable_set();

    LockMetadataValue lockMetadataValue;
    LockContentValue lockContentValue;
    LockContentValue* lockContentValuePointer = nullptr;

    if (setOperation.readcontentaction() == Grpc::LockManagerSetOperationReadContentAction::Read)
    {
        lockContentValuePointer = &lockContentValue;
    }

    bool lockExists = co_await m_database->Read(
        context->GetStoreOperation(),
        setOperation.lockname(),
        lockMetadataValue,
        lockContentValuePointer);

    if (lockExists
        && lockContentValuePointer)
    {
        setResult->mutable_previouscontent()->set_data(
            move(*lockContentValuePointer->mutable_data()));
        setResult->mutable_previouscontent()->set_version(
            lockMetadataValue.committed().version());
    }
    lockContentValue.Clear();

    auto action = setOperation.action();

    switch (setOperation.action())
    {
    case Grpc::LockManagerSetOperationAction::SetExisting:
        if (!lockExists
            ||
            setOperation.version() != 0
            && setOperation.version() != lockMetadataValue.committed().version())
        {
            co_await context->Fail();
            co_return;
        }
        lockMetadataValue.mutable_prepared()->set_version(
            lockMetadataValue.committed().version() + 1);
        lockContentValue.set_data(
            setOperation.data());
        break;

    case Grpc::LockManagerSetOperationAction::Create:
        if (lockExists)
        {
            co_await context->Fail();
            co_return;
        }
        lockMetadataValue.mutable_prepared()->set_version(1);
        lockContentValue.set_data(
            setOperation.data());
        break;

    case Grpc::LockManagerSetOperationAction::CreateOrGet:
        if (lockExists)
        {
            co_await context->MarkReadOnly();
            co_await context->Succeed();
            co_return;
        }

        lockMetadataValue.mutable_prepared()->set_version(1);
        lockContentValue.set_data(
            setOperation.data());
        break;

    case Grpc::LockManagerSetOperationAction::CreateOrSet:
        throw 0;
    case Grpc::LockManagerSetOperationAction::Delete:
        throw 0;
    case Grpc::LockManagerSetOperationAction::DeleteExisting:
        throw 0;

    default:
        co_await context->Fail();
        co_return;
    }
}

task<> InternalLockResourceManager::Commit(
    IInternalResourceManagerCommitContext* context
)
{
    co_return;
}

task<> InternalLockResourceManager::Abort(
    IInternalResourceManagerAbortContext* context
)
{
    co_return;
}

}
