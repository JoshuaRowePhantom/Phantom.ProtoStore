#pragma once

#include <memory>
#include "Primitives.h"

namespace Phantom::ProtoStore
{

class IIndex;
class IIndexData;
class ProtoStore;

class ProtoIndex
{
    friend class ProtoStore;
    friend class LocalTransaction;
    IIndexData* m_index;

public:
    ProtoIndex();

    ProtoIndex(
        IIndexData* index);

    ProtoIndex(
        const std::shared_ptr<IIndexData>& index);
    
    ProtoIndex(
        const std::shared_ptr<IIndex>& index);

    const IndexName& IndexName() const;

    friend bool operator==(
        const ProtoIndex&,
        const ProtoIndex&
        ) = default;
};

}