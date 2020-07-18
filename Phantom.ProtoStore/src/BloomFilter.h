#include <atomic>
#include <limits>
#include <vector>
#include <stdint.h>

namespace Phantom::ProtoStore
{

template <
    typename THashFunction,
    typename TValueType
> struct BloomFilterHashFunctionTraits
{

};

namespace detail
{

template<
    typename THashFunction,
    typename TBase
> class BloomFilterHashFunctionBase
    :
    public TBase
{
    typedef BloomFilterHashFunctionTraits<THashFunction, typename TBase::value_type> hash_traits;
    THashFunction m_hashFunction;

protected:
    BloomFilterHashFunctionBase(
        THashFunction hashFunction = THashFunction())
        :
        m_hashFunction(hashFunction)
    {}

    template<
        typename TKey
    > auto initial_hash(
        const TKey& key,
        size_t sizeInBits
    ) const
    {
        return hash_traits::initial_hash(
            m_hashFunction,
            key,
            sizeInBits);
    }

    template<
        typename TKey,
        typename THash
    > auto next_hash(
        THash& previousHash,
        const TKey& key,
        size_t sizeInBits
    ) const
    {
        return hash_traits::next_hash(
            m_hashFunction,
            previousHash,
            key,
            sizeInBits);
    }
};

template<
    typename TValueType
> class BloomFilterElementTraits
{
protected:
    static_assert(std::numeric_limits<TValueType>::radix == 2, "Radix must be 2!");
    static_assert(std::numeric_limits<TValueType>::is_integer, "Must be integral!");
    static_assert(!std::numeric_limits<TValueType>::is_signed, "Must be unsigned!");

public:
    const int bits_per_element =
        std::numeric_limits<TValueType>::digits;

    typedef TValueType value_type;
};

template<
    typename THashFunction,
    typename TElementType,
    typename TContainer,
    typename TBase
> class BloomFilterContainerBase;

// Container base for a span
template<
    typename THashFunction,
    typename TElementType,
    size_t SpanExtent,
    typename TBase
> class BloomFilterContainerBase <
    THashFunction,
    TElementType,
    std::span<TElementType, SpanExtent>,
    TBase
>
{
    std::span<TElementType, SpanExtent> m_elements;

protected:
    TElementType& element_at(
        size_t index)
    {
        return m_elements[index];
    }

    const TElementType& element_at(
        size_t index
    ) const
    {
        return m_elements[index];
    }

public:
    BloomFilterContainerBase(
        std::span<TElementType, SpanExtent> elements,
        THashFunction&& hashFunction = THashFunction()
    )
        :
        TBase(std::forward<THashFunction>(hashFunction)),
        m_elements(elements)
    {}

    std::span<const TElementType, SpanExtent> to_span() const
    {
        return m_elements;
    }

    size_t size_bits() const
    {
        return m_elements.size() * bits_per_element;
    }
};

// Container base for a vector
template<
    typename THashFunction,
    typename TElementType,
    typename TAllocator,
    typename TBase
> class BloomFilterContainerBase <
    THashFunction,
    TElementType,
    std::vector<TElementType, TAllocator>,
    TBase
>
    :
    public TBase
{
    std::vector<TElementType, TAllocator> m_elements;

protected:
    TElementType& element_at(
        size_t index)
    {
        return m_elements[index];
    }

    const TElementType& element_at(
        size_t index
    ) const
    {
        return m_elements[index];
    }

public:
    BloomFilterContainerBase(
        size_t bitCount,
        THashFunction&& hashFunction = THashFunction(),
        TAllocator allocator = TAllocator()
    )
        :
        TBase(std::forward<THashFunction>(hashFunction)),
        m_elements(
            (bitCount + bits_per_element - 1) / bits_per_element, 
            allocator
        )
    {}

    template<
        typename TContainer
    >
    BloomFilterContainerBase(
        TContainer&& container,
        THashFunction&& hashFunction = THashFunction()
    )
        :
        TBase(std::forward<THashFunction>(hashFunction)),
        m_elements(std::forward<TContainer>(container))
    {}

    template<
        typename THashFunction
    > BloomFilterContainerBase(
        std::vector<TElementType, TAllocator>&& container,
        THashFunction&& hashFunction = THashFunction()
        )
        :
        TBase(std::forward<THashFunction>(hashFunction)),
        m_elements(move(container))
    {}

    std::span<const TElementType> to_span() const
    {
        return std::span(
            m_elements.begin(),
            m_elements.end());
    }

    size_t size_bits() const
    {
        return m_elements.size() * bits_per_element;
    }
};

// Element base for ordinary values.
template<
    typename TElementType
> class BloomFilterElementBase
    : public BloomFilterElementTraits<TElementType>
{
protected:
    typedef TElementType element_type;
    typedef TElementType value_type;

    void fetch_or(
        TElementType& element,
        std::memory_order memoryOrder
    )
    {
        element_at(index) = element_at(index) | value;
    }

    bool test(
        const TElementType& value
    ) const
    {
        return element_at(index) & value;
    }
};

// Element base for atomic values.
template<
    typename TValueType
> class BloomFilterElementBase<
    std::atomic<TValueType>
>
    : public BloomFilterElementTraits<TValueType>
{
protected:
    typedef std::atomic<TValueType> element_type;
    typedef TValueType value_type;

    void fetch_or(
        element_type& element,
        std::memory_order memoryOrder
    )
    {
        element_at(index).fetch_or(
            value,
            memoryOrder);
    }

    bool test(
        const element_type& element,
        value_type value,
        std::memory_order memoryOrder
    ) const
    {
        return element.load(memoryOrder) & value;
    }
};

template<
    typename THashFunction,
    typename TElementType = uintmax_t,
    typename TContainerType = std::vector<TElementType>,
    typename TBase = detail::BloomFilterContainerBase<
        THashFunction,
        TElementType,
        TContainerType,
        detail::BloomFilterHashFunctionBase<
            THashFunction,
            detail::BloomFilterElementBase<TElementType>
        >
    >
> class BloomFilterBase
    :
    public TBase
{
public:
    using TBase::TBase;
};
} // namespace detail

template <
    typename THashFunction,
    typename TElementType = uintmax_t,
    typename TContainerType = std::vector<TElementType>
> class BloomFilter
    :
    public detail::BloomFilterBase<THashFunction, TElementType, TContainerType>
{

public:
    using detail::BloomFilterBase<THashFunction, TElementType, TContainerType>::BloomFilterBase;

    template<
        typename TKey
    > void add(
        const TKey& key
    )
    {
        auto initialHash = initial_hash(
            key,
            size_bits());

        do
        {
            fetch_or(
                element_at(initial_hash / bits_per_element),
                1 << (initial_hash % bits_per_element)
            );
        } while (next_hash(
            initial_hash,
            key,
            size_bits()));
    }

    template<
        typename TKey
    > bool test(
        const TKey& key
    ) const
    {
        auto initialHash = initial_hash(
            key,
            size_bits());

        do
        {
            if (!test(
                element_at(initial_hash / bits_per_element),
                1 << (initial_hash % bits_per_element)
            ))
            {
                return false;
            }

        } while (next_hash(
            initial_hash,
            key,
            size_bits()));

        return true;
    }
};

}