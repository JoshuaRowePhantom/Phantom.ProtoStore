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

class KeySchemaDescription;

template<typename T, typename O>
concept IsOrderedBy = requires (T t)
{
    { t <=> t } -> std::same_as<O>;
};

extern const Serialization::PlaceholderKey KeyMinMessage;
extern const Serialization::PlaceholderKey KeyMaxMessage;
extern const std::span<const std::byte> KeyMinSpan;
extern const std::span<const std::byte> KeyMaxSpan;

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

class KeyComparer : public BaseKeyComparer
{
public:
    virtual std::weak_ordering Compare(
        std::span<const byte> value1,
        std::span<const byte> value2
    ) const = 0;

    std::weak_ordering operator()(
        std::span<const byte> value1,
        std::span<const byte> value2
        ) const;
};

class ProtoKeyComparer
    :
    public KeyComparer
{
public:
    template<typename T>
    struct compare_tag {};

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

public:
    ProtoKeyComparer(
        const google::protobuf::Descriptor* messageDescriptor);

    virtual std::weak_ordering Compare(
        std::span<const byte> value1,
        std::span<const byte> value2
    ) const override;
};

class FlatBufferPointerKeyComparer
    :
    public BaseKeyComparer
{
public:
    template<typename T>
    struct compare_tag {};

private:
    class InternalObjectComparer;
    using ComparerMap = std::map<const ::reflection::Object*, InternalObjectComparer>;

    class InternalObjectComparer
        :
        public BaseKeyComparer
    {
        using ComparerFunction = std::function<std::weak_ordering(
            const void* value1,
            const void* value2
        )>;

        std::vector<ComparerFunction> m_comparers;

        template<
            typename Value
        > static std::weak_ordering ComparePrimitive(
            Value value1,
            Value value2
        );

        template<
            typename Container,
            typename Value
        > static Value GetFieldI(
            const Container* container,
            const ::reflection::Field* flatBuffersReflectionField
        );

        template<
            typename Container,
            typename Value
        > static Value GetFieldF(
            const Container* container,
            const ::reflection::Field* flatBuffersReflectionField
        );

        template<
            typename Container,
            auto fieldRetriever
        > static ComparerFunction GetPrimitiveFieldComparer(
            const ::reflection::Field* flatBuffersReflectionField
        );

        template<
            typename Container
        > static ComparerFunction GetFieldComparer(
            ComparerMap& internalComparers,
            const ::reflection::Schema* flatBuffersReflectionSchema,
            const ::reflection::Object* flatBuffersReflectionObject,
            const ::reflection::Field* flatBuffersReflectionField
        );

        static InternalObjectComparer* GetObjectComparer(
            ComparerMap& internalComparers,
            const ::reflection::Schema* flatBuffersReflectionSchema,
            const ::reflection::Object* flatBuffersReflectionObject
        );

        static ComparerFunction GetVectorFieldComparer(
            ComparerMap& internalComparers,
            const ::reflection::Schema* flatBuffersReflectionSchema,
            const ::reflection::Object* flatBuffersReflectionObject,
            const ::reflection::Field* flatBuffersReflectionField
        );

        template<
            typename Value
        > static ComparerFunction GetTypedVectorFieldComparer(
            ComparerMap& internalComparers,
            const ::reflection::Schema* flatBuffersReflectionSchema,
            const ::reflection::Object* flatBuffersReflectionObject,
            const ::reflection::Field* flatBuffersReflectionField
        );

        static ComparerFunction GetArrayFieldComparer(
            ComparerMap& internalComparers,
            const ::reflection::Schema* flatBuffersReflectionSchema,
            const ::reflection::Object* flatBuffersReflectionObject,
            const ::reflection::Field* flatBuffersReflectionField
        );

        template<
            typename Value
        > static ComparerFunction GetTypedArrayFieldComparer(
            ComparerMap& internalComparers,
            const ::reflection::Schema* flatBuffersReflectionSchema,
            const ::reflection::Object* flatBuffersReflectionObject,
            const ::reflection::Field* flatBuffersReflectionField
        );

        template<
        > static ComparerFunction GetTypedArrayFieldComparer<
            flatbuffers::Struct
        >(
            ComparerMap& internalComparers,
            const ::reflection::Schema* flatBuffersReflectionSchema,
            const ::reflection::Object* flatBuffersReflectionObject,
            const ::reflection::Field* flatBuffersReflectionField
        );

    public:
        InternalObjectComparer();

        InternalObjectComparer(
            ComparerMap& internalComparers,
            const ::reflection::Schema* flatBuffersReflectionSchema,
            const ::reflection::Object* flatBuffersReflectionObject
        );

        std::weak_ordering Compare(
            const void* value1,
            const void* value2
        ) const;
    };

    ComparerMap m_internalComparers;
    const InternalObjectComparer* m_rootComparer;

public:
    FlatBufferPointerKeyComparer(
        const ::reflection::Schema* flatBuffersReflectionSchema,
        const ::reflection::Object* flatBuffersReflectionObject);

    std::weak_ordering Compare(
        const void* value1,
        const void* value2
    ) const;
};

struct KeyAndSequenceNumberComparerArgument
{
    std::span<const std::byte> Key;
    SequenceNumber SequenceNumber;

    KeyAndSequenceNumberComparerArgument(
        std::span<const std::byte> key,
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
    std::span<const std::byte> Key;
    SequenceNumber SequenceNumber;
    Inclusivity Inclusivity;

    KeyRangeComparerArgument(
        std::span<const std::byte> key,
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