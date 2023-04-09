// Phantom.ProtoStore.cpp : Defines the entry point for the application.
//

#include "Phantom.ProtoStore/Phantom.ProtoStore.h"
#include "ProtoStoreInternal.pb.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"

namespace Phantom::ProtoStore
{

OpenProtoStoreRequest::OpenProtoStoreRequest()
{
    DefaultMergeParameters.set_mergesperlevel(
        10);
    DefaultMergeParameters.set_maxlevel(
        10);
}

}
