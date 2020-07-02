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

public:
    HeaderAccessor(
        shared_ptr<IMessageStore> messageStore,
        ExtentLocation headerLocation1,
        ExtentLocation headerLocation2);

    virtual task<> ReadHeader(
        Header& header
    ) override;

    virtual task<> WriteHeader(
        const Header& header
    ) override;
};

}