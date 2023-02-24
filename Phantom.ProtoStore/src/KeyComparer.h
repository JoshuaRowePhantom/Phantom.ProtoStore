#pragma once

#include "StandardTypes.h"
#include <compare>
#include <concepts>
#include <unordered_map>
#include "Phantom.ProtoStore/ProtoStore.pb.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/dynamic_message.h"

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
extern const std::span<const byte> KeyMinSpan;
extern const std::span<const byte> KeyMaxSpan;

class KeyComparer
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

    template<IsOrderedBy<std::weak_ordering> T>
    std::weak_ordering CompareValues(
        const T& left,
        const T& right
    ) const;

    template<IsOrderedBy<std::strong_ordering> T>
        std::weak_ordering CompareValues(
            const T& left,
            const T& right
    ) const;

    template<IsOrderedBy<std::partial_ordering> T>
        std::weak_ordering CompareValues(
            const T& left,
            const T& right
    ) const;
    
    std::weak_ordering CompareValues(
        const std::string& left,
        const std::string& right
    ) const;

    std::weak_ordering CompareValues(
        const google::protobuf::Message& left,
        const google::protobuf::Message& right
    ) const;

    std::weak_ordering CompareFields(
        const google::protobuf::Message* left,
        const google::protobuf::Message* right,
        const google::protobuf::Reflection* leftReflection,
        const google::protobuf::Reflection* rightReflection,
        const google::protobuf::FieldDescriptor* leftFieldDescriptor,
        const google::protobuf::FieldDescriptor* rightFieldDescriptor
    ) const;

    template<typename T>
    std::weak_ordering CompareFields(
        const google::protobuf::Message* left,
        const google::protobuf::Message* right,
        const google::protobuf::Reflection* leftReflection,
        const google::protobuf::Reflection* rightReflection,
        const google::protobuf::FieldDescriptor* leftFieldDescriptor,
        const google::protobuf::FieldDescriptor* rightFieldDescriptor,
        compare_tag<T> = compare_tag<T>()
    ) const;

    std::weak_ordering CompareNonRepeatedFields(
        const google::protobuf::Message* left,
        const google::protobuf::Message* right,
        const google::protobuf::Reflection* leftReflection,
        const google::protobuf::Reflection* rightReflection,
        const google::protobuf::FieldDescriptor* leftFieldDescriptor,
        const google::protobuf::FieldDescriptor* rightFieldDescriptor,
        compare_tag<std::string> tag
    ) const;

    std::weak_ordering CompareNonRepeatedFields(
        const google::protobuf::Message* left,
        const google::protobuf::Message* right,
        const google::protobuf::Reflection* leftReflection,
        const google::protobuf::Reflection* rightReflection,
        const google::protobuf::FieldDescriptor* leftFieldDescriptor,
        const google::protobuf::FieldDescriptor* rightFieldDescriptor,
        compare_tag<google::protobuf::Message> tag
    ) const;

    template<typename T>
    std::weak_ordering CompareNonRepeatedFields(
        const google::protobuf::Message* left,
        const google::protobuf::Message* right,
        const google::protobuf::Reflection* leftReflection,
        const google::protobuf::Reflection* rightReflection,
        const google::protobuf::FieldDescriptor* leftFieldDescriptor,
        const google::protobuf::FieldDescriptor* rightFieldDescriptor,
        compare_tag<T> = compare_tag<T>()
    ) const;

    template<typename T>
    std::weak_ordering CompareRepeatedFields(
        const google::protobuf::Message* left,
        const google::protobuf::Message* right,
        const google::protobuf::Reflection* leftReflection,
        const google::protobuf::Reflection* rightReflection,
        const google::protobuf::FieldDescriptor* leftFieldDescriptor,
        const google::protobuf::FieldDescriptor* rightFieldDescriptor,
        compare_tag<T> = compare_tag<T>()
    ) const;

    static std::weak_ordering ApplySortOrder(
        SortOrder sortOrder,
        std::weak_ordering value
    );

    static SortOrder CombineSortOrder(
        SortOrder sortOrder1,
        SortOrder sortOrder2
    );

public:
    KeyComparer(
        const google::protobuf::Descriptor* messageDescriptor);

    std::weak_ordering Compare(
        const google::protobuf::Message* value1,
        const google::protobuf::Message* value2
    ) const;

    std::weak_ordering operator()(
        const google::protobuf::Message* value1,
        const google::protobuf::Message* value2
    ) const;

    std::weak_ordering Compare(
        std::span<const byte> value1,
        std::span<const byte> value2
    ) const;

    std::weak_ordering operator()(
        std::span<const byte> value1,
        std::span<const byte> value2
        ) const; 
};

struct KeyAndSequenceNumberComparerArgument
{
    const Message* Key;
    SequenceNumber SequenceNumber;

    KeyAndSequenceNumberComparerArgument(
        const Message* key,
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
    const Message* Key;
    SequenceNumber SequenceNumber;
    Inclusivity Inclusivity;

    KeyRangeComparerArgument(
        const Message* key,
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