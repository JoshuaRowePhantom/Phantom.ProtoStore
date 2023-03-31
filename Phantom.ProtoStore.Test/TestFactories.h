#include "Phantom.ProtoStore/src/Index.h"
#include "Phantom.ProtoStore/src/Resources.h"
#include "Phantom.ProtoStore/src/ValueComparer.h"
#include "Phantom.System/utility.h"
#include <optional>
#include <string>
#include <vector>

namespace Phantom::ProtoStore
{

class TestFactories
{
protected:
    std::atomic<uint64_t> m_nextTestLocalTransactionId;
    std::atomic<uint64_t> m_nextTestWriteId;
    std::atomic<uint64_t> m_nextWriteSequenceNumber;

    task<std::shared_ptr<IIndexData>> MakeInMemoryIndex(
        IndexName indexName,
        const Schema& schema
    );

    task<OperationResult<>> AddRow(
        const std::shared_ptr<IIndexData>& index,
        ProtoValue key,
        ProtoValue value,
        std::optional<SequenceNumber> writeSequenceNumber = std::nullopt,
        SequenceNumber readSequenceNumber = SequenceNumber::Latest
    );

    static ProtoValue JsonToProtoValue(
        const reflection::Schema* schema,
        const reflection::Object* object,
        std::optional<string> json);

    template<
        IsFlatBufferTable Value
    > static const reflection::Schema* GetSchema()
    {
        if constexpr (
            std::same_as<Value, FlatBuffers::MergesKey>
            || std::same_as<Value, FlatBuffers::MergesValue>
            || std::same_as<Value, FlatBuffers::PartitionsKey>
            || std::same_as<Value, FlatBuffers::PartitionsValue>
            )
        {
            return FlatBuffersSchemas::ProtoStoreInternalSchema;
        }
        else
        {
            static_assert(always_false<Value>);
        }
    }

    template<
        IsFlatBufferTable Value
    > static const reflection::Schema* GetObject()
    {
        if constexpr (std::same_as<Value, FlatBuffers::MergesKey>)
        {
            return FlatBuffersSchemas::MergesKey_Object;
        }
        else if constexpr (std::same_as<Value, FlatBuffers::MergesValue>)
        {
            return FlatBuffersSchemas::MergesValue_Object;
        }
        else if constexpr (std::same_as<Value, FlatBuffers::PartitionsKey>)
        {
            return FlatBuffersSchemas::PartitionsKey_Object;
        }
        else if constexpr (std::same_as<Value, FlatBuffers::PartitionsValue>)
        {
            return FlatBuffersSchemas::PartitionsValue_Object;
        }
        else
        {
            static_assert(always_false<Value>);
        }
    }

    template<
        IsFlatBufferTable Value
    > static const ProtoValueComparers& GetComparers()
    {

        if constexpr (std::same_as<Value, FlatBuffers::MergesKey>)
        {
            return FlatBuffersSchemas::MergesKey_Comparers;
        }
        else if constexpr (std::same_as<Value, FlatBuffers::MergesValue>)
        {
            return FlatBuffersSchemas::MergesValue_Comparers;
        }
        else if constexpr (std::same_as<Value, FlatBuffers::PartitionsKey>)
        {
            return FlatBuffersSchemas::PartitionsKey_Comparers;
        }
        else if constexpr (std::same_as<Value, FlatBuffers::PartitionsValue>)
        {
            return FlatBuffersSchemas::PartitionsValue_Comparers;
        }
        else
        {
            static_assert(always_false<Value>);
        }
    }

    template<
        typename Key,
        typename Value
    > static std::vector<row<FlatValue<Key>, FlatValue<Value>>> JsonToFlatRows(
        std::vector<std::pair<std::string, std::optional<string>>> rowStrings)
    {
        std::vector<row<FlatValue<Key>, FlatValue<Value>>> result;

        for (const auto& row : rowStrings)
        {
            ProtoValue key = JsonToProtoValue(
                GetSchema<Key>(),
                GetObject<Key>(),
                row.first);
            
            ProtoValue value = JsonToProtoValue(
                GetSchema<Value>(),
                GetObject<Value>(),
                row.second);

            result.push_back(
                {
                    key,
                    value,
                    SequenceNumber::Earliest,
                    SequenceNumber::Earliest,
                }
            );
        }

        return result;
    }

    template<
        typename Key,
        typename Value
    > static bool RowListsAreEqual(
        const std::vector<row<FlatValue<Key>, FlatValue<Value>>>& expected,
        const std::vector<row<FlatValue<Key>, FlatValue<Value>>>& actual
    )
    {
        if (expected.size() != actual.size())
        {
            return false;
        }

        const ProtoValueComparers& keyComparers = GetComparers<Key>();
        const ProtoValueComparers& valueComparers = GetComparers<Value>();

        for (int index = 0; index < expected.size(); ++index)
        {
            if (!keyComparers.equal_to(expected[index].Key, actual[index].Key)
                ||
                !valueComparers.equal_to(expected[index].Value, actual[index].Value))
            {
                return false;
            }
        }

        return true;
    }
};

}
