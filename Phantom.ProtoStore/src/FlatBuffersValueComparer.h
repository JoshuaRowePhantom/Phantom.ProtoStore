#pragma once
#include "ValueComparer.h"
#include "Checksum.h"
#include "Schema.h"
#include <flatbuffers/reflection.h>

namespace Phantom::ProtoStore
{
class FlatBufferPointerValueComparer
    :
    public BaseValueComparer
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

        ComparerFunction comparerFunction = nullptr;

        uint32_t elementSize = 0;
        uint16_t fixedLength = 0;

        std::unordered_map<uint8_t, InternalFieldComparer> unionComparers;

        SortOrder sortOrder = SortOrder::Ascending;
        
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

        std::weak_ordering CompareTableField(
            const void* value1,
            const void* value2
        ) const;

        std::weak_ordering CompareUnionField(
            const void* value1,
            const void* value2
        ) const;

        std::weak_ordering CompareEmptyUnionField(
            const void* value1,
            const void* value2
        ) const;

        std::weak_ordering CompareUnionFieldValue(
            const void* value1,
            const void* value2
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
            typename Value
        >
        std::weak_ordering CompareVectorField(
            const void* value1,
            const void* value2
        ) const;

        template<
        >
        std::weak_ordering CompareVectorField<flatbuffers::Struct>(
            const void* value1,
            const void* value2
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
            result.sortOrder = BaseValueComparer::CombineSortOrder(
                objectSortOrder,
                fieldSortOrder);
            return result;
        }
    };

    class InternalObjectComparer
        :
        public BaseValueComparer
    {
        std::vector<InternalFieldComparer> m_comparers;

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

    std::shared_ptr<const FlatBuffersObjectSchema> m_flatBuffersObjectSchema;
    std::shared_ptr<ComparerMap> m_internalComparers = std::make_shared<ComparerMap>();
    const InternalObjectComparer* m_rootComparer;

public:
    explicit FlatBufferPointerValueComparer(
        std::shared_ptr<const FlatBuffersObjectSchema> flatBuffersObjectSchema);

    const std::shared_ptr<const FlatBuffersObjectSchema>& Schema() const;

    std::weak_ordering Compare(
        const void* value1,
        const void* value2
    ) const;

    uint64_t Hash(
        const void* value
    ) const;
};

class FlatBufferValueComparer :
    public ValueComparer
{
    FlatBufferPointerValueComparer m_comparer;
    ValueBuilder m_prototypeValueBuilder;

    virtual std::weak_ordering CompareImpl(
        const ProtoValue& value1,
        const ProtoValue& value2
    ) const override;
public:
    FlatBufferValueComparer(
        FlatBufferPointerValueComparer comparer);

    virtual uint64_t Hash(
        const ProtoValue& value
    ) const override;

    virtual BuildValueResult BuildValue(
        ValueBuilder& valueBuilder,
        const ProtoValue& value
    ) const override;
    
    virtual flatbuffers::Offset<FlatBuffers::DataValue> BuildDataValue(
        ValueBuilder& valueBuilder,
        const ProtoValue& value
    ) const override;

    virtual int32_t GetEstimatedSize(
        const ProtoValue& value
    ) const override;
};
}
