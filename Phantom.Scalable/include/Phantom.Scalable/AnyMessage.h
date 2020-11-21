#pragma once

#include "StandardIncludes.h"

namespace Phantom::Scalable
{
template<
    typename TMessage
>
class AnyMessage
{
    unique_ptr<TMessage> holder;
    const TMessage* value_;
public:
    AnyMessage(
        nullptr_t = nullptr
    ) : value_(nullptr)
    {}

    AnyMessage(
        const TMessage* value
    )
        : value_(value)
    {}

    AnyMessage(
        const Any* any
    ) : holder(new TMessage()),
        value_(holder.get())
    {
        auto unpackResult = any->UnpackTo(
            holder.get());
        assert(unpackResult);
    }

    const TMessage* value(
    ) const
    {
        return value_;
    }

    explicit operator bool() const
    {
        return value_;
    }

    const TMessage* operator->() const
    {
        return value_;
    }

    operator const TMessage* () const
    {
        return value_;
    }
};

template<
    typename TMessage,
    typename TParentMessage
>
AnyMessage<TMessage> FromAnyMessage(
    const TParentMessage& parentMessage,
    bool (TParentMessage::* hasAny)() const,
    const google::protobuf::Any& (TParentMessage::* any)() const
)
{
    if ((parentMessage.*hasAny)())
    {
        return AnyMessage<TMessage>(
            &(parentMessage.*any)());
    }

    return AnyMessage<TMessage>(
        static_cast<const TMessage*>(nullptr)
        );
}

template<
    typename TMessage,
    typename TParentMessage
>
AnyMessage<TMessage> FromAnyMessage(
    const TParentMessage& parentMessage,
    bool (TParentMessage::* hasAny)() const,
    const google::protobuf::Any& (TParentMessage::* any)() const,
    bool (TParentMessage::* hasValue)() const,
    const TMessage& (TParentMessage::* value)() const
)
{
    if ((parentMessage.*hasValue)())
    {
        return AnyMessage<TMessage>(
            &(parentMessage.*value)());
    }

    if ((parentMessage.*hasAny)())
    {
        return AnyMessage<TMessage>(
            &(parentMessage.*any)());
    }

    return AnyMessage<TMessage>();
}
}
