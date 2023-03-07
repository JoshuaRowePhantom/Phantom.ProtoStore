#pragma once

#include <memory>
#include "Primitives.h"

namespace Phantom::ProtoStore
{

class IIndex;
class ProtoStore;

class ProtoIndex
{
    friend class ProtoStore;
    friend class LocalTransaction;
    IIndex* m_index;

public:
    ProtoIndex()
        :
        m_index(nullptr)
    {}

    ProtoIndex(
        IIndex* index)
        :
        m_index(index)
    {
    }

    ProtoIndex(
        std::shared_ptr<IIndex> index)
        :
        m_index(index.get())
    {
    }

    ProtoStore* ProtoStore() const;
    const IndexName& IndexName() const;

    friend bool operator==(
        const ProtoIndex&,
        const ProtoIndex&
        ) = default;
};

}