#include <atomic>
#include <cmath>
#include <limits>
#include <random>
#include <vector>
#include <stdint.h>

namespace Phantom::ProtoStore
{

template <
    typename THashFunction,
    typename TValueType
> struct BloomFilterHashFunctionTraits
{
    typedef THashFunction hash_function_type;

    template<
        typename TKey
    > static auto initial_hash(
        THashFunction hashFunction,
        const TKey& key,
        size_t sizeInBits
    ) 
    {
        return hashFunction.initial_hash(
            key,
            sizeInBits);
    }

    template<
        typename THash
    > static auto extract_bit_index(
        THashFunction hashFunction,
        THash& previousHash
    ) 
    {
        return hashFunction.extract_bit_index(
            previousHash);
    }

    template<
        typename THash
    > static auto next_hash(
        THashFunction hashFunction,
        THash& previousHash
    ) 
    {
        return hashFunction.next_hash(
            previousHash);
    }
};

template<
    typename THashFunction,
    typename TDistribution = std::uniform_int_distribution<size_t>,
    typename TRandomNumberGenerator = std::ranlux48_base,
    typename TSeedSeq = std::seed_seq
>
struct SeedingPrngBloomFilterHashFunction
{
    struct hash_value
    {
        TRandomNumberGenerator randomNumberGenerator;
        TDistribution distribution;
        size_t bitsToExtract;
    };

    THashFunction m_hashFunction;
    size_t m_bits_to_extract;

    SeedingPrngBloomFilterHashFunction(
        size_t bits_to_extract,
        THashFunction hashFunction = THashFunction()
    )
        :
        m_hashFunction(hashFunction),
        m_bits_to_extract(bits_to_extract)
    {}

    template<
        typename TKey
    > hash_value initial_hash(
        const TKey& key,
        size_t size_bits
    ) const
    {
        auto seed = m_hashFunction(key);
        
        auto seedSequence = std::seed_seq
        {
            seed,
        };

        return
        {
            .randomNumberGenerator { seedSequence },
            .distribution { 0, size_bits - 1 },
            .bitsToExtract { m_bits_to_extract },
        };
    }

    size_t extract_bit_index(
        hash_value & hash
    ) const
    {
        hash.bitsToExtract--;
        return hash.distribution(
            hash.randomNumberGenerator);
    }

    bool next_hash(
        hash_value& hash
    ) const
    {
        return hash.bitsToExtract > 0;
    }
};

template <
    typename TKey,
    typename TValueType
>
struct BloomFilterHashFunctionTraits<
    std::hash<TKey>,
    TValueType
>
    :
    public BloomFilterHashFunctionTraits<
        SeedingPrngBloomFilterHashFunction<std::hash<TKey>>,
        TValueType
    >
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

protected:
    typedef BloomFilterHashFunctionTraits<THashFunction, typename TBase::value_type> hash_traits;
    typedef typename hash_traits::hash_function_type hash_function_type;
    hash_function_type m_hashFunction;

    BloomFilterHashFunctionBase(
        hash_function_type hashFunction = hash_function_type())
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
        typename THash
    > auto extract_bit_index(
        THash& previousHash
    ) const
    {
        return hash_traits::extract_bit_index(
            m_hashFunction,
            previousHash);
    }

    template<
        typename THash
    > auto next_hash(
        THash& previousHash
    ) const
    {
        return hash_traits::next_hash(
            m_hashFunction,
            previousHash);
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
    const value_type one = 1;
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
    :
    public TBase
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
        typename TBase::hash_function_type hashFunction = hash_function_type()
    )
        :
        TBase(hashFunction),
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
    typedef std::vector<TElementType, TAllocator> container_type;
    container_type m_elements;

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
        typename TBase::hash_function_type hashFunction = hash_function_type(),
        TAllocator allocator = TAllocator()
    )
        :
        TBase(hashFunction),
        m_elements(
            (bitCount + bits_per_element - 1) / bits_per_element, 
            allocator
        )
    {}

    BloomFilterContainerBase(
        container_type&& container,
        typename TBase::hash_function_type hashFunction = hash_function_type()
    )
        :
        TBase(hashFunction),
        m_elements(std::forward<TContainer>(container))
    {}

    template<
        typename THashFunction
    > BloomFilterContainerBase(
        container_type container,
        typename TBase::hash_function_type hashFunction = hash_function_type()
        )
        :
        TBase(hashFunction),
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
        element_type& element,
        value_type value,
        std::memory_order memoryOrder
    )
    {
        element |= value;
    }

    bool test_bit(
        const element_type& element,
        value_type value,
        std::memory_order memoryOrder
    ) const
    {
        return element & value;
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
        value_type value,
        std::memory_order memoryOrder
    )
    {
        element.fetch_or(
            value,
            memoryOrder);
    }

    bool test_bit(
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
        auto hash = initial_hash(
            key,
            size_bits());

        bool nextHash;

        do
        {
            auto bit_index = extract_bit_index(
                hash);

            nextHash = next_hash(
                hash);

            fetch_or(
                element_at(bit_index / bits_per_element),
                one << (bit_index % bits_per_element),
                nextHash ? std::memory_order_relaxed : std::memory_order_release
            );
        } while (nextHash);
    }

    template<
        typename TKey
    > bool test(
        const TKey& key
    ) const
    {
        auto hash = initial_hash(
            key,
            size_bits());

        bool isFirst = true;

        do
        {
            auto bit_index = extract_bit_index(
                hash);

            if (!test_bit(
                element_at(bit_index / bits_per_element),
                one << (bit_index % bits_per_element),
                isFirst ? std::memory_order_acquire : std::memory_order_relaxed
            ))
            {
                return false;
            }

            isFirst = false;

        } while (next_hash(
            hash));

        return true;
    }
};

inline double get_BloomFilter_optimal_bit_count_per_element(
    double desiredFalsePositiveErrorRate)
{
    return -(std::log2(desiredFalsePositiveErrorRate) / std::log(2));
}

inline size_t get_BloomFilter_optimal_bit_count(
    double desiredFalsePositiveErrorRate,
    size_t numberOfElements)
{
    return std::ceil(
        get_BloomFilter_optimal_bit_count_per_element(desiredFalsePositiveErrorRate)
        * numberOfElements);
}

inline size_t get_BloomFilter_optimal_hash_function_count(
    double bloomFilterSize,
    size_t numberOfElements)
{
    return std::ceil(
        bloomFilterSize / numberOfElements * std::log(2)
    );
}

inline size_t get_BloomFilter_optimal_hash_function_count_for_optimal_bit_count(
    double desiredFalsePositiveErrorRate)
{
    return std::ceil(
        -std::log2(desiredFalsePositiveErrorRate)
    );
}

}