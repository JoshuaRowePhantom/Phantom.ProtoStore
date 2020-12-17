#pragma once

#include "StandardIncludes.h"
#include "src/PhantomScalableGrpcInternal.grpc.pb.h"

namespace Phantom::Scalable
{

class INodeResolver
{
public:
    virtual task<shared_ptr<Grpc::Node>> ResolveNode(
        const Grpc::NodeIdentifier& nodeIdentifier
    ) = 0;
};

class INodeSelector
{
public:
    virtual task<std::vector<Grpc::Internal::ParticipantNode>> GetParticipantNodes(
        Grpc::Internal::EpochNumber epochNumber,
        Grpc::Internal::ParticipantResource participantResource
    ) = 0;
};

class IPeerToPeerClient
{
public:
    virtual task<Grpc::Internal::GetOperationInformationResponseMessage> GetOperationInformation(
        const Grpc::Internal::GetOperationInformationRequestMessage& request
    ) = 0;
};

class IPeerToPeerClientFactory
{
public:
    virtual task<shared_ptr<IPeerToPeerClient>> Open(
        const Grpc::Address& address
    ) = 0;
};

}