#pragma once

#include "InternalResourceManager.h"

namespace Phantom::Scalable
{

class IInternalLockResourceManagerDatabase;

class InternalLockResourceManager
    :
    public IInternalResourceManager
{
    const shared_ptr<IInternalLockResourceManagerDatabase> m_database;

    AnyMessage<InternalLockManagerOperation> GetLockManagerOperation(
        IInternalResourceManagerOperationContext* context
    );

    task<> PrepareGet(
        IInternalResourceManagerPrepareContext* context,
        const Grpc::LockManagerGetOperation& getOperation
    );

    task<> PrepareSet(
        IInternalResourceManagerPrepareContext* context,
        const Grpc::LockManagerSetOperation& setOperation
    );

public:
    virtual task<> Propose(
        IInternalResourceManagerProposeContext* context
    ) override;

    virtual task<> Prepare(
        IInternalResourceManagerPrepareContext* context
    ) override;

    virtual task<> Commit(
        IInternalResourceManagerCommitContext* context
    ) override;

    virtual task<> Abort(
        IInternalResourceManagerAbortContext* context
    ) override;
};

}
