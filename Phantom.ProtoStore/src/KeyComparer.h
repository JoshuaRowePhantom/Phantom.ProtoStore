#pragma once

#include "StandardTypes.h"
#include <compare>
#include <concepts>
#include <unordered_map>
#include <flatbuffers/reflection.h>
#include "Phantom.ProtoStore/ProtoStore.pb.h"
#include <google/protobuf/descriptor.h>

namespace Phantom::ProtoStore
{

template<typename T, typename O>
concept IsOrderedBy = requires (T t)
{
    { t <=> t } -> std::same_as<O>;
};

class BaseKeyComparer
{
protected:

    static std::weak_ordering ApplySortOrder(
        SortOrder sortOrder,
        std::weak_ordering value
    );

    static SortOrder CombineSortOrder(
        SortOrder sortOrder1,
        SortOrder sortOrder2
    );
};

class ValueBuilder
{
    struct InternedSchemaItem;

    struct InternedValue
    {
        const void* schemaIdentifier;
        flatbuffers::Offset<void> offset;
    };

    struct UninternedValue
    {
        const void* value;
        const InternedSchemaItem* schemaItem;
    };

    struct InterningValue
    {
        const InternedSchemaItem* schemaItem;
        flatbuffers::Offset<void> offset;

        operator InternedValue() const;
    };

    struct InternedValueKeyComparer
    {
        using is_transparent = void;

        ValueBuilder* m_valueBuilder;

        // Hash computation
        size_t operator()(
            const auto& value
            ) const;

        // Equality comparison
        bool operator()(
            const auto& value1,
            const auto& value2
            ) const;
    };

    struct InternedSchemaItem
    {
        const void* schemaIdentifier;
        std::function<size_t(const void*)> hash;
        std::function<bool(const void*, const void*)> equal_to;
    };

    struct SchemaItem
    {
        const reflection::Schema* schema = nullptr;
        const reflection::Object* object = nullptr;
        const reflection::Type* type = nullptr;

        const void* schemaIdentifier() const;
    };

    struct SchemaItemComparer
    {
        // Hash computation
        size_t operator()(
            const SchemaItem& item
            ) const;

        // Equality computation
        bool operator()(
            const SchemaItem& item1,
            const SchemaItem& item2
            ) const;
    };

    flatbuffers::FlatBufferBuilder* const m_flatBufferBuilder;
    std::list<std::any> m_ownedValues;
    std::unordered_map<SchemaItem, InternedSchemaItem, SchemaItemComparer, SchemaItemComparer> m_internedSchemaItemsByItem;
    std::unordered_map<const void*, InternedSchemaItem*> m_internedSchemaItemsByPointer;

    std::unordered_set<
        InternedValue,
        InternedValueKeyComparer,
        InternedValueKeyComparer
    > m_internedValues;

    size_t Hash(
        const auto& value
    );

    bool Equals(
        const auto& value1,
        const auto& value2
    );

    const InternedSchemaItem& InternSchemaItem(
        const SchemaItem& schemaItem
    );

    InternedSchemaItem MakeInternedSchemaItem(
        const SchemaItem& schemaItem
    );

    InternedSchemaItem MakeInternedVectorSchemaItem(
        const SchemaItem& schemaItem
    );
    
    InternedSchemaItem MakeInternedObjectSchemaItem(
        const SchemaItem& schemaItem
    );

public:
    ValueBuilder(
        flatbuffers::FlatBufferBuilder* flatBufferBuilder
    );

    flatbuffers::Offset<void> GetInternedValue(
        const SchemaItem& schemaItem,
        const void* value
    );

    void InternValue(
        const SchemaItem& schemaItem,
        flatbuffers::Offset<void> offset
    );

    flatbuffers::FlatBufferBuilder& builder() const;

    flatbuffers::Offset<FlatBuffers::DataValue> CreateDataValue(
        const AlignedMessage&
    );
};

class KeyComparer : public BaseKeyComparer
{
    virtual std::weak_ordering CompareImpl(
        const ProtoValue& value1,
        const ProtoValue& value2
    ) const = 0;

public:
    std::weak_ordering Compare(
        const ProtoValue& value1,
        const ProtoValue& value2
    ) const;

    std::weak_ordering operator()(
        const ProtoValue& value1,
        const ProtoValue& value2
        ) const;

    virtual uint64_t Hash(
        const ProtoValue& value
    ) const = 0;

    using BuildValueResult = std::variant<
        flatbuffers::Offset<FlatBuffers::ValuePlaceholder>,
        flatbuffers::Offset<FlatBuffers::DataValue>
    >;

    virtual BuildValueResult BuildValue(
        ValueBuilder& valueBuilder,
        const ProtoValue& value
    ) const = 0;
};

class ProtoKeyComparer
    :
    public KeyComparer
{
private:
    const google::protobuf::Descriptor* m_messageDescriptor;
    using MessageSortOrderMap = std::unordered_map<const google::protobuf::Descriptor*, SortOrder>;
    using FieldSortOrderMap = std::unordered_map<const google::protobuf::FieldDescriptor*, SortOrder>;

    MessageSortOrderMap m_messageSortOrder;
    FieldSortOrderMap m_fieldSortOrder;

    static MessageSortOrderMap GetMessageSortOrders(
        const google::protobuf::Descriptor*,
        MessageSortOrderMap source = {});
    static FieldSortOrderMap GetFieldSortOrders(
        const google::protobuf::Descriptor*,
        FieldSortOrderMap source = {});

    virtual std::weak_ordering CompareImpl(
        const ProtoValue& value1,
        const ProtoValue& value2
    ) const override;

public:
    ProtoKeyComparer(
        const google::protobuf::Descriptor* messageDescriptor);

    virtual uint64_t Hash(
        const ProtoValue& value
    ) const override;

    virtual BuildValueResult BuildValue(
        ValueBuilder& valueBuilder,
        const ProtoValue& value
    ) const override;
};

std::shared_ptr<KeyComparer> MakeFlatBufferKeyComparer(
    std::shared_ptr<const FlatBuffersObjectSchema> flatBuffersObjectSchema);

struct KeyAndSequenceNumberComparerArgument
{
    const ProtoValue& Key;
    SequenceNumber SequenceNumber;

    KeyAndSequenceNumberComparerArgument(
        const ProtoValue& key,
        Phantom::ProtoStore::SequenceNumber sequenceNumber
    ) :
        Key(key),
        SequenceNumber(sequenceNumber)
    {}
};

class KeyAndSequenceNumberComparer
{
    const KeyComparer& m_keyComparer;

public:
    KeyAndSequenceNumberComparer(
        const KeyComparer& keyComparer
    ) : m_keyComparer(keyComparer)
    {}

    std::weak_ordering operator ()(
        const KeyAndSequenceNumberComparerArgument& left,
        const KeyAndSequenceNumberComparerArgument& right
    )
    {
        auto comparisonResult = m_keyComparer.Compare(
            left.Key,
            right.Key);

        if (comparisonResult == std::weak_ordering::equivalent)
        {
            // Intentionally backward, so that later sequence numbers compare earlier.
            comparisonResult = right.SequenceNumber <=> left.SequenceNumber;
        }

        return comparisonResult;
    }
};

class KeyAndSequenceNumberLessThanComparer
{
    KeyAndSequenceNumberComparer m_keyAndSequenceNumberComparer;

public:
    KeyAndSequenceNumberLessThanComparer(
        const KeyComparer& keyComparer
    ) : m_keyAndSequenceNumberComparer(keyComparer)
    {}

    template <
        typename T1,
        typename T2
    > bool operator()(
        const T1& left,
        const T2& right
    )
    {
        auto comparisonResult = m_keyAndSequenceNumberComparer(
            left,
            right);

        return comparisonResult == std::weak_ordering::less;
    }
};

struct KeyRangeComparerArgument
{
    const ProtoValue& Key;
    SequenceNumber SequenceNumber;
    Inclusivity Inclusivity;

    KeyRangeComparerArgument(
        const ProtoValue& key,
        Phantom::ProtoStore::SequenceNumber sequenceNumber,
        Phantom::ProtoStore::Inclusivity inclusivity
    ) :
        Key(key),
        SequenceNumber(sequenceNumber),
        Inclusivity(inclusivity)
    {}
};

class KeyRangeComparer
{
    const KeyComparer& m_keyComparer;
public:
    KeyRangeComparer(
        const KeyComparer& keyComparer
    ) : m_keyComparer(keyComparer)
    {
    }

    std::weak_ordering operator()(
        const KeyAndSequenceNumberComparerArgument& left,
        const KeyRangeComparerArgument& right
        ) const;

    std::weak_ordering operator()(
        const KeyRangeComparerArgument& left,
        const KeyAndSequenceNumberComparerArgument& right
        ) const;
};

class KeyRangeLessThanComparer
{
    KeyRangeComparer m_keyRangeComparer;
public:
    KeyRangeLessThanComparer(
        const KeyRangeComparer& keyRangeComparer
    ) :
        m_keyRangeComparer(keyRangeComparer)
    {}

    bool operator()(
        const KeyAndSequenceNumberComparerArgument& left,
        const KeyRangeComparerArgument& right
        ) const
    {
        return m_keyRangeComparer(left, right) == std::weak_ordering::less;
    }

    bool operator()(
        const KeyRangeComparerArgument& left,
        const KeyAndSequenceNumberComparerArgument& right
        ) const
    {
        return m_keyRangeComparer(left, right) == std::weak_ordering::less;
    }
};

}