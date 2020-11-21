#pragma once

#include "StandardIncludes.h"
#include "src/PhantomScalableGrpcInternal.grpc.pb.h"

namespace Phantom::Scalable
{

class PeerToPeerService
    :
    public Grpc::Internal::ScalablePeerToPeerService::AsyncService
{
public:

};
}