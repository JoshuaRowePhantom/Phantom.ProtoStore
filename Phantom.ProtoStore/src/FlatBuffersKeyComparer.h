#pragma once
#include "KeyComparer.h"
#include "Checksum.h"
#include <flatbuffers/reflection.h>

namespace Phantom::ProtoStore
{
class FlatBufferPointerKeyComparer
    :
    public BaseKeyComparer
{
    using BaseType = ::reflection::BaseType;

private:
    class InternalObjectComparer;
    using ComparerMap = std::map<const ::reflection::Object*, InternalObjectComparer>;

    template<
        typename Value
    > static void HashPrimitive(
        hash_v1_type& hash,
        Value value
    );

    struct InternalFieldComparer
    {
        const ::reflection::Field* flatBuffersReflectionField = nullptr;
        const InternalObjectComparer* elementObjectComparer = nullptr;

        using ComparerFunction = std::weak_ordering (InternalFieldComparer::*)(
            const void* value1,
            const void* value2
            ) const;

        using HasherFunction = void (InternalFieldComparer::*)(
            hash_v1_type& hash,
            const void* value
            ) const;

        ComparerFunction comparerFunction = nullptr;
        HasherFunction hasherFunction = nullptr;

        uint32_t elementSize = 0;
        uint16_t fixedLength = 0;

        std::unordered_map<uint8_t, InternalFieldComparer> unionComparers;

        SortOrder sortOrder = SortOrder::Ascending;
        
        void HashStructObject(
            hash_v1_type& hash,
            const void* value
        ) const;

        template<
            typename Value
        > static std::weak_ordering ComparePrimitive(
            Value value1,
            Value value2
        );
        
        template<
            typename Container
        >
        std::weak_ordering CompareStructField(
            const void* value1,
            const void* value2
        ) const;

        template<
            typename Container
        >
        void HashStructField(
            hash_v1_type& hash,
            const void* value
        ) const;

        std::weak_ordering CompareTableField(
            const void* value1,
            const void* value2
        ) const;

        void HashTableField(
            hash_v1_type& hash,
            const void* value
        ) const;

        std::weak_ordering CompareUnionField(
            const void* value1,
            const void* value2
        ) const;

        void HashUnionField(
            hash_v1_type& hash,
            const void* value
        ) const;

        std::weak_ordering CompareEmptyUnionField(
            const void* value1,
            const void* value2
        ) const;

        void HashEmptyUnionField(
            hash_v1_type& hash,
            const void* value
        ) const;

        std::weak_ordering CompareUnionFieldValue(
            const void* value1,
            const void* value2
        ) const;

        void HashUnionFieldValue(
            hash_v1_type& hash,
            const void* value
        ) const;

        template<
            typename Container,
            auto fieldRetriever
        >
        std::weak_ordering ComparePrimitiveField(
            const void* value1,
            const void* value2
        ) const;

        std::weak_ordering CompareStringField(
            const void* value1,
            const void* value2
        ) const;

        template<
            typename Container,
            auto fieldRetriever
        >
        void HashPrimitiveField(
            hash_v1_type& hash,
            const void* value
        ) const;

        void HashStringField(
            hash_v1_type& hash,
            const void* value
        ) const;

        template<
            typename Value
        >
        std::weak_ordering CompareVectorField(
            const void* value1,
            const void* value2
        ) const;

        template<
            typename Value
        >
        void HashVectorField(
            hash_v1_type& hash,
            const void* value
        ) const;

        template<
        >
        std::weak_ordering CompareVectorField<flatbuffers::Struct>(
            const void* value1,
            const void* value2
        ) const;

        template<
        >
        void HashVectorField<flatbuffers::Struct>(
            hash_v1_type& hash,
            const void* value
        ) const;

        template<
            typename Value
        >
        std::weak_ordering CompareArrayField(
            const void* value1,
            const void* value2
        ) const;
        
        template<
        >
        std::weak_ordering CompareArrayField<flatbuffers::Struct>(
            const void* value1,
            const void* value2
        ) const;

        InternalFieldComparer ApplySortOrder(
            this InternalFieldComparer result,
            SortOrder objectSortOrder,
            SortOrder fieldSortOrder
        )
        {
            result.sortOrder = BaseKeyComparer::CombineSortOrder(
                objectSortOrder,
                fieldSortOrder);
            return result;
        }
    };

    class InternalObjectComparer
        :
        public BaseKeyComparer
    {
        std::vector<InternalFieldComparer> m_comparers;
        std::vector<InternalFieldComparer> m_hashers;

        static SortOrder GetSortOrder(
            const flatbuffers::Vector<flatbuffers::Offset<reflection::KeyValue>>* attributes
        );

        static SortOrder GetSortOrder(
            const ::reflection::Object* flatBuffersReflectionField
        );

        static SortOrder GetSortOrder(
            const ::reflection::Field* flatBuffersReflectionField
        );

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
        > static InternalFieldComparer GetPrimitiveFieldComparer(
            const ::reflection::Field* flatBuffersReflectionField
        );

        static InternalFieldComparer GetStringFieldComparer(
            const ::reflection::Field* flatBuffersReflectionField
        );

        static InternalFieldComparer GetUnionFieldComparer(
            ComparerMap& internalComparers,
            const ::reflection::Schema* flatBuffersReflectionSchema,
            const ::reflection::Object* flatBuffersReflectionObject,
            const ::reflection::Field* flatBuffersReflectionField
        );

        template<
            typename Container
        > static InternalFieldComparer GetFieldComparer(
            ComparerMap& internalComparers,
            const ::reflection::Schema* flatBuffersReflectionSchema,
            const ::reflection::Object* flatBuffersReflectionObject,
            const ::reflection::Field* flatBuffersReflectionField
        );

        static InternalFieldComparer GetVectorFieldComparer(
            ComparerMap& internalComparers,
            const ::reflection::Schema* flatBuffersReflectionSchema,
            const ::reflection::Object* flatBuffersReflectionObject,
            const ::reflection::Field* flatBuffersReflectionField
        );

        template<
            typename Value
        > static InternalFieldComparer GetTypedVectorFieldComparer(
            ComparerMap& internalComparers,
            const ::reflection::Schema* flatBuffersReflectionSchema,
            const ::reflection::Object* flatBuffersReflectionObject,
            const ::reflection::Field* flatBuffersReflectionField
        );

        static InternalFieldComparer GetArrayFieldComparer(
            ComparerMap& internalComparers,
            const ::reflection::Schema* flatBuffersReflectionSchema,
            const ::reflection::Object* flatBuffersReflectionObject,
            const ::reflection::Field* flatBuffersReflectionField
        );

        template<
            typename Value
        > static InternalFieldComparer GetTypedArrayFieldComparer(
            ComparerMap& internalComparers,
            const ::reflection::Schema* flatBuffersReflectionSchema,
            const ::reflection::Object* flatBuffersReflectionObject,
            const ::reflection::Field* flatBuffersReflectionField
        );

        InternalObjectComparer(
            ComparerMap& internalComparers,
            const ::reflection::Schema* flatBuffersReflectionSchema,
            const ::reflection::Object* flatBuffersReflectionObject
        );

    public:
        InternalObjectComparer();

        std::weak_ordering Compare(
            const void* value1,
            const void* value2
        ) const;

        void Hash(
            hash_v1_type& hash,
            const void* value
        ) const;

        static InternalObjectComparer* GetObjectComparer(
            ComparerMap& internalComparers,
            const ::reflection::Schema* flatBuffersReflectionSchema,
            const ::reflection::Object* flatBuffersReflectionObject
        );
    };

    std::shared_ptr<flatbuffers::DetachedBuffer> m_schemaBuffer;
    std::shared_ptr<ComparerMap> m_internalComparers = std::make_shared<ComparerMap>();
    const InternalObjectComparer* m_rootComparer;

public:
    FlatBufferPointerKeyComparer(
        const ::reflection::Schema* flatBuffersReflectionSchema,
        const ::reflection::Object* flatBuffersReflectionObject);

    std::weak_ordering Compare(
        const void* value1,
        const void* value2
    ) const;

    uint64_t Hash(
        const void* value
    ) const;
};

class FlatBufferKeyComparer :
    public KeyComparer
{
    FlatBufferPointerKeyComparer m_comparer;

    virtual std::weak_ordering CompareImpl(
        const ProtoValue& value1,
        const ProtoValue& value2
    ) const override;

public:
    FlatBufferKeyComparer(
        FlatBufferPointerKeyComparer comparer);

    virtual uint64_t Hash(
        const ProtoValue& value
    ) const override;

    virtual BuildValueResult BuildValue(
        ValueBuilder& valueBuilder,
        const ProtoValue& value
    ) const override;
};
}
