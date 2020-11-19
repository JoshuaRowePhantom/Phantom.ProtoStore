#pragma once

#include "StandardIncludes.h"
#include "src/PhantomScalableGrpcInternal.pb.h"

namespace Phantom::Scalable
{
class IInternalLockResourceManagerDatabase
{
public:
    virtual task<> Open(
    ) = 0;

    virtual task<bool> Read(
        IProtoStoreOperation* operation,
        const string& lockName,
        Grpc::Internal::LockMetadataValue& metadata,
        Grpc::Internal::LockContentValue* content = nullptr
    ) = 0;

    virtual task<> Write(
        IProtoStoreOperation* operation,
        const string& lockName,
        const Grpc::Internal::LockMetadataValue& metadata,
        const Grpc::Internal::LockContentValue* content
    ) = 0;
};

class InternalLockResourceManagerDatabase
    :
    public IInternalLockResourceManagerDatabase
{
    const shared_ptr<IProtoStore> m_protoStore;
public:
    InternalLockResourceManagerDatabase(
        shared_ptr<IProtoStore> protoStore
    );

    virtual task<> Open(
    ) override;

    virtual task<bool> Read(
        IProtoStoreOperation* operation,
        const string& lockName,
        Grpc::Internal::LockMetadataValue& metadata,
        Grpc::Internal::LockContentValue* content = nullptr
    ) override;

    virtual task<> Write(
        IProtoStoreOperation* operation,
        const string& lockName,
        const Grpc::Internal::LockMetadataValue& metadata,
        const Grpc::Internal::LockContentValue* content
    ) override;
};

}
