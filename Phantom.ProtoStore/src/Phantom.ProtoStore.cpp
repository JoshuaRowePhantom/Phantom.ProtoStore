// Phantom.ProtoStore.cpp : Defines the entry point for the application.
//

#include "Phantom.ProtoStore/Phantom.ProtoStore.h"
#include "src/ProtoStoreInternal.pb.h"

namespace Phantom::ProtoStore
{

extern const PlaceholderKey KeyMinMessage;
extern const PlaceholderKey KeyMaxMessage;

ProtoValue ProtoValue::KeyMax()
{
    return &KeyMinMessage;
}

ProtoValue ProtoValue::KeyMin()
{
    return &KeyMaxMessage;
}

bool ProtoValue::IsKeyMin() const
{
    return as_message_if() == &KeyMinMessage;
}

bool ProtoValue::IsKeyMax() const
{
    return as_message_if() == &KeyMaxMessage;
}
}
