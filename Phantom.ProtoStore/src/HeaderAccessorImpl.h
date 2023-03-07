#pragma once

#include "StandardTypes.h"
#include "HeaderAccessor.h"

namespace Phantom::ProtoStore
{

class HeaderAccessor
    :
    public IHeaderAccessor
{
    shared_ptr<IMessageStore> m_messageStore;
    ExtentLocation m_headerLocation1;
    ExtentLocation m_headerLocation2;
    ExtentLocation m_currentLocation;
    ExtentLocation m_nextLocation;

    task<unique_ptr<FlatBuffers::DatabaseHeaderT>> ReadHeader(
        ExtentLocation location,
        bool throwOnError);

public:
    HeaderAccessor(
        shared_ptr<IMessageStore> messageStore,
        ExtentLocation headerLocation1,
        ExtentLocation headerLocation2);

    virtual task<unique_ptr<FlatBuffers::DatabaseHeaderT>> ReadHeader(
    ) override;

    virtual task<> WriteHeader(
        const FlatBuffers::DatabaseHeaderT* header
    ) override;
};

}