#pragma once

#include "StandardIncludes.h"
#include "PhantomScalableGrpcInternal.pb.h"

namespace Phantom::Scalable
{

class PeerToPeerService
    :
    public Grpc::Internal::ScalablePeerToPeerService::AsyncService
{
public:

};

}