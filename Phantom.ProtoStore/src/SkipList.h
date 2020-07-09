#pragma once

#include "StandardTypes.h"
#include <assert.h>
#include <array>
#include <atomic>
#include <compare>
#include <iterator>
#include <cppcoro/generator.hpp>
#include <random>
#include <tuple>
#include <vector>

namespace Phantom::ProtoStore
{

extern thread_local std::mt19937 tls_SkipListRng;

enum class SkipListReplaceAction
{
    DontReplace,
    Replace,
};

enum class SkipListAddResult
{
    Replaced,
    NotReplaced,
    Added,
};

template<
    typename TKey,
    typename TValue,
    size_t MaxLevels,
    typename TComparer
>
class SkipList
{
public:
    class iterator;
    typedef std::pair<const TKey, TValue> value_type;
private:
    struct Node;
    struct NextPointers;
    typedef std::atomic<Node*> AtomicNextPointer;
    typedef std::array<AtomicNextPointer, MaxLevels> FullAtomicNextPointersType;
    typedef AtomicNextPointer* AtomicNextPointersType;

    TComparer m_comparer;

    struct Node
    {
        template <
            typename TConstructedKey,
            typename TConstructedValue
        > static Node* Allocate(
            size_t level,
            TConstructedKey&& key,
            TConstructedValue&& value
        )
        {
            std::unique_ptr<char[]> nodeCharArray = make_unique<char[]>(
                sizeof(Node) + sizeof(AtomicNextPointer) * level);
            auto result = reinterpret_cast<Node*>(nodeCharArray.get());
            new(result)Node(
                std::forward<TConstructedKey>(key),
                std::forward<TConstructedValue>(value));
            nodeCharArray.release();
            return result;
        }

        template <
            typename TConstructedKey,
            typename TConstructedValue
        > Node(
            TConstructedKey&& key,
            TConstructedValue&& value)
            : 
            Item(
                std::forward<TConstructedKey>(key),
                std::forward<TConstructedValue>(value)
            )
        {}

        value_type Item;

        SkipList::NextPointers NextPointers()
        {
            return SkipList::NextPointers(
                reinterpret_cast<AtomicNextPointersType>(this + 1));
        }
    };

    struct NextPointers
    {
    private:
        AtomicNextPointersType Value;

    public:
        NextPointers(){}

        NextPointers(
            AtomicNextPointersType value
        )
            : Value(value)
        {}

        AtomicNextPointer& operator[](
            size_t index)
        {
            return Value[index];
        }

        Node* NodePointer()
        {
            return reinterpret_cast<Node*>(Value) - 1;
        }
    };

    typedef std::array<std::tuple<NextPointers, Node*>, MaxLevels> FullNextPointersType;

    FullAtomicNextPointersType m_head;
    std::geometric_distribution<size_t> m_randomDistribution;

    size_t NewRandomLevel()
    {
        return std::min(
            MaxLevels - 1,
            m_randomDistribution(tls_SkipListRng)
        ) + 1;
    }

public:
    SkipList(
        const SkipList&
    ) = delete;

    SkipList(
        SkipList&& other
    ) = delete;

    SkipList& operator =(
        const SkipList&
    ) = delete;

    SkipList& operator =(
        SkipList&& other
    ) = delete;

    SkipList(
        TComparer comparer = TComparer()
    )
        :
        m_comparer(
            comparer)
    {
        for (auto& headPointer : m_head)
        {
            headPointer.store(
                nullptr,
                std::memory_order_release
            );
        }
    }

    ~SkipList()
    {
        Node* next = m_head[0].load(
            std::memory_order_acquire);
        Node* prev;
        while(next)
        {
            prev = next;
            next = next->NextPointers()[0].load(
                std::memory_order_acquire);
            delete prev;
        }
    }

    template<
        typename TSearchKey,
        typename TAddValue
    > std::pair<iterator, bool> insert(
        TSearchKey&& key,
        TAddValue&& value
    )
    {
        // We follow the algorithm described by the SkipList authors.
        // Collect all the pointers to the sets of next pointers and resulting next node
        // for each level, choosing the set of next pointers that is just before
        // the node with the value we are looking for.
        FullNextPointersType location;
        NextPointers currentNextPointers = m_head.data();
        std::weak_ordering lastComparisonResult = std::weak_ordering::less;
        
        size_t level = MaxLevels;
        do
        {
            --level;

            Node* nextAtLevel;

            do
            {
                nextAtLevel = currentNextPointers[level].load(
                    std::memory_order_acquire);

                if (nextAtLevel == nullptr
                    ||
                    (lastComparisonResult = m_comparer(nextAtLevel->Item.first, key)) != std::weak_ordering::less)
                {
                    break;
                }

                currentNextPointers = nextAtLevel->NextPointers();

            } while (true);

            // Any time we got an equivalent comparison,
            // it means the skip list contains the value and we should return.
            if (lastComparisonResult == std::weak_ordering::equivalent)
            {
                return
                {
                    iterator(nextAtLevel),
                    false
                };
            }

            location[level] = std::make_tuple(
                currentNextPointers,
                nextAtLevel);

        } while (level != 0);

        // Build a new node at a random level.
        auto newLevel = NewRandomLevel();

        std::unique_ptr<Node> newNodeHolder = unique_ptr<Node>(Node::Allocate(
            newLevel,
            std::forward<TSearchKey>(key),
            std::forward<TAddValue>(value)));

        Node* newNode = newNodeHolder.get();
        auto newNodeNextPointers = newNode->NextPointers();
        
        // Now hook this new node into the linked lists at each level
        // up to the randomly chosen level.
        for (level = 0; level < newLevel; level++)
        {
            do
            {
                // We already figured out what the next node should be when
                // we did the original traversal.  Use that set
                // of next pointers and expected next value.
                auto expectedNextNode = get<Node*>(location[level]);

                newNodeNextPointers[level].store(
                    expectedNextNode,
                    std::memory_order_relaxed);

                currentNextPointers = get<NextPointers>(location[level]);

                if (currentNextPointers[level].compare_exchange_weak(
                    expectedNextNode,
                    newNode))
                {
                    // If we succeed at replacing the value,
                    // we've successfully committed the object to the list,
                    // so prevent the node from being deleted.
                    newNodeHolder.release();
                    break;
                }

                // Hm, the old next node changed underneath us.
                // Advance to the new next node and try again.
                lastComparisonResult = std::weak_ordering::less;
                Node* nextAtLevel;

                do
                {
                    nextAtLevel = currentNextPointers[level].load(
                        std::memory_order_acquire);

                    // Make sure to do the comparison using the newNode->Value, 
                    // because "value" might have std::moved to the newNode->Value.
                    if (nextAtLevel == nullptr
                        ||
                        (lastComparisonResult = m_comparer(nextAtLevel->Item.first, newNode->Item.first)) != std::weak_ordering::less)
                    {
                        break;
                    }

                    currentNextPointers = nextAtLevel->NextPointers();

                } while (lastComparisonResult == std::weak_ordering::less);

                location[level] = std::make_tuple(
                    currentNextPointers,
                    nextAtLevel);

                // On level 0, it's possible that a newly inserted node
                // has the same key.  We check for that.
                if (lastComparisonResult == std::weak_ordering::equivalent)
                {
                    assert(level == 0);
                    assert(newNodeHolder);

                    return
                    {
                        iterator(nextAtLevel),
                        false
                    };
                }
            } while (true);
        }

        return
        {
            iterator(newNode),
            true
        };
    }

    template<
        typename TKey,
        typename TKeyGenerator,
        typename TKeyComparer
    > cppcoro::generator<std::tuple<const TKey&, TValue*>> Enumerate(
            const TKeyGenerator& keyGenerator,
            TKeyComparer keyComparer
        )
    {
        for (const auto& key : keyGenerator)
        {

        }
        co_return;
    }

    class iterator
    {
        Node* m_current;
        friend class SkipList;
        iterator(
            Node* current)
            :
            m_current(current)
        {}

    public:

        using difference_type = std::ptrdiff_t;
        using value_type = SkipList::value_type;
        using pointer = value_type*;
        using reference = value_type&;
        using iterator_category = std::forward_iterator_tag;

        reference operator*()
        {
            return m_current->Item;
        }

        iterator& operator++()
        {
            m_current = m_current->NextPointers()[0].load(
                std::memory_order_acquire);
            return *this;
        }

        value_type* operator->()
        {
            return &m_current->Item;
        }

        bool operator ==(
            const iterator& other)
        {
            return other.m_current == m_current;
        }

        bool operator !=(
            const iterator& other)
        {
            return other.m_current != m_current;
        }
    };

    iterator begin()
    {
        return m_head[0].load(
            std::memory_order_acquire);
    }

    iterator end()
    {
        return nullptr;
    }
};
}