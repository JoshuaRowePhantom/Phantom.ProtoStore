#pragma once

#include "StandardIncludes.h"
#include "PhantomScalableGrpcInternal.pb.h"
#include <cppcoro/async_generator.hpp>

namespace Phantom::Scalable
{

class INodeResolver
{
public:
    virtual shared_task<const Grpc::Node> ResolveNode(
        const Grpc::Internal::EpochNumber epochNumber,
        const Grpc::NodeIdentifier& nodeIdentifier
    ) = 0;
};

class IPeerToPeerClient
{
public:
    virtual cppcoro::async_generator<Grpc::Internal::ProcessOperationResponse> ProcessOperation(
        const Grpc::Internal::ProcessOperationRequest& request
    );
};

class IPeerToPeerClientFactory
{
public:
    virtual task<shared_ptr<IPeerToPeerClient>> Open(
        const Grpc::Address& address
    ) = 0;
};

}