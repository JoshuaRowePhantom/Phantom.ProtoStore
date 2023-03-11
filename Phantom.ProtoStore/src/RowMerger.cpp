#include "RowMerger.h"
#include "Phantom.System/merge.h"
#include "KeyComparer.h"
#include <flatbuffers/flatbuffers.h>

namespace Phantom::ProtoStore
{

RowMerger::RowMerger(
    std::shared_ptr<const Schema> schema,
    std::shared_ptr<const KeyComparer> keyComparer
)
    :
    m_schema(std::move(schema)),
    m_keyComparer(std::move(keyComparer))
{}

row_generator RowMerger::Merge(
    row_generators rowSources
)
{
    auto comparator = [this](
        const ResultRow& row1,
        const ResultRow& row2
        )
    {
        ProtoValue key1;
        ProtoValue key2;

        if (holds_alternative<ProtocolBuffersKeySchema>(m_schema->KeySchema.FormatSchema))
        {
            key1 = ProtoValue::ProtocolBuffer(
                row1.Key->Payload
            );
            key2 = ProtoValue::ProtocolBuffer(
                row1.Key->Payload
            );
        }
        else
        {
            assert(holds_alternative<FlatBuffersKeySchema>(m_schema->KeySchema.FormatSchema));
            key1 = ProtoValue::FlatBuffer(
                flatbuffers::GetRoot<flatbuffers::Table>(row1.Key->Payload.data())
            );
            key2 = ProtoValue::FlatBuffer(
                flatbuffers::GetRoot<flatbuffers::Table>(row2.Key->Payload.data())
            );
        }

        auto keyOrdering = m_keyComparer->Compare(
            key1,
            key2
        );

        if (keyOrdering == std::weak_ordering::less)
        {
            return true;
        }

        if (keyOrdering == std::weak_ordering::greater)
        {
            return false;
        }

        return row1.WriteSequenceNumber > row2.WriteSequenceNumber;
    };

    auto result = merge_sorted_generators<ResultRow>(
        rowSources.begin(),
        rowSources.end(),
        comparator);

    for (auto iterator = co_await result.begin();
        iterator != result.end();
        co_await ++iterator)
    {
        co_yield *iterator;
    }
}

row_generator RowMerger::Enumerate(
    row_generators rowSources
)
{
    auto mergeEnumeration = Merge(
        move(rowSources));

    ResultRow previousRow;

    for (auto iterator = co_await mergeEnumeration.begin();
        iterator != mergeEnumeration.end();
        co_await ++iterator)
    {
        auto& row = *iterator;

        if (previousRow.Key
            && std::ranges::equal(previousRow.Key->Payload, row.Key->Payload))
        {
            continue;
        }
        
        previousRow = row;

        if (!row.Value)
        {
            continue;
        }

        co_yield std::move(row);
    }
}
}