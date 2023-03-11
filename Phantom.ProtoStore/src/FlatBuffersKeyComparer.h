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
private:
    class InternalObjectComparer;
    using ComparerMap = std::map<const ::reflection::Object*, InternalObjectComparer>;

    class InternalObjectComparer
        :
        public BaseKeyComparer
    {
        struct ComparerFunction
        {
            const ::reflection::Field* flatBuffersReflectionField = nullptr;
            const InternalObjectComparer* elementComparer = nullptr;

            std::weak_ordering(*comparerFunction)(
                const ::reflection::Field*,
                const InternalObjectComparer*,
                const void* value1,
                const void* value2);

            const SortOrder sortOrder = SortOrder::Ascending;

            ComparerFunction ApplySortOrder(
                SortOrder objectSortOrder,
                SortOrder fieldSortOrder
            )
            {
                return
                {
                    flatBuffersReflectionField,
                    elementComparer,
                    comparerFunction,
                    BaseKeyComparer::CombineSortOrder(
                        objectSortOrder,
                        fieldSortOrder)
                };
            }
        };

        struct HasherFunction
        {
            const ::reflection::Field* flatBuffersReflectionField = nullptr;
            const InternalObjectComparer* elementComparer = nullptr;

            std::span<std::byte>(*hasherFunction)(
                crc_hash_v1_type& hash,
                const ::reflection::Field*,
                const InternalObjectComparer*,
                const void* value);
        };

        std::vector<ComparerFunction> m_comparers;
        std::vector<HasherFunction> m_hashers;

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

        template<
        > static ComparerFunction GetTypedVectorFieldComparer<
            flatbuffers::Struct
        >(
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
            crc_hash_v1_type& hash,
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

    uint64_t Hash(
        const ProtoValue& value
    ) const override;
};
}
