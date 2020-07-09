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

        AtomicNextPointersType NextPointers()
        {
            return reinterpret_cast<AtomicNextPointersType>(this + 1);
        }
    };

    struct FingerType
    {
        typedef std::array<std::tuple<AtomicNextPointersType, Node*>, MaxLevels> Container;

        FullAtomicNextPointersType* m_head;
        const TComparer* m_comparer;
        Container m_value;

        // Construct an invalid instance.
        FingerType()
        {}

        FingerType(
            FullAtomicNextPointersType* head,
            const TComparer* comparer
        ) : 
            m_head(head),
            m_comparer(comparer)
        {}

        template<typename TSearchKey>
        std::weak_ordering NavigateTo(
            size_t previousLevel,
            size_t level,
            const TSearchKey& key)
        {
            auto nextPointers = 
                previousLevel >= MaxLevels
                ?
                nullptr
                :
                NextPointers(
                    previousLevel);

            if (!nextPointers)
            {
                nextPointers = m_head->data();
            }

            std::weak_ordering lastComparisonResult = std::weak_ordering::greater;
            Node* nextNode;

            do
            {
                nextNode = nextPointers[level].load(
                    std::memory_order_acquire);

                if (nextNode == nullptr)
                {
                    lastComparisonResult = std::weak_ordering::greater;
                    break;
                }
                if ((lastComparisonResult = (*m_comparer)(nextNode->Item.first, key)) != std::weak_ordering::less)
                {
                    break;
                }

                nextPointers = nextNode->NextPointers();

            } while (true);

            m_value[level] =
            {
                nextPointers,
                nextNode,
            };

            return lastComparisonResult;
        }

        void NavigateTo(
            size_t level,
            AtomicNextPointersType nextPointers,
            Node* nextNode
        )
        {
            m_value[level] =
            {
                nextPointers,
                nextNode
            };
        }

        // Navigate from m_head to just before the Node with
        // the requested key.  This method always navigates
        // at all levels.
        template<
            typename TSearchKey
        > std::weak_ordering NavigateTo(
            const TSearchKey& key)
        {
            std::weak_ordering lastComparisonResult = std::weak_ordering::equivalent;

            size_t level = MaxLevels;
            do
            {
                --level;

                lastComparisonResult = NavigateTo(
                    level + 1,
                    level,
                    key);

            } while (level != 0);

            return lastComparisonResult;
        }

        AtomicNextPointersType NextPointers(
            size_t level
        ) const
        {
            return std::get<AtomicNextPointersType>(m_value[level]);
        }

        Node* NextNode(
            size_t level
        ) const
        {
            return std::get<Node*>(m_value[level]);
        }
    };

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
        FingerType location(
            &m_head,
            &m_comparer);
        
        if (location.NavigateTo(key) == std::weak_ordering::equivalent)
        {
            return
            {
                iterator(location),
                false,
            };
        }

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
        for (size_t level = 0; level < newLevel; level++)
        {
            do
            {
                // We already figured out what the next node should be when
                // we did the original traversal.  Use that set
                // of next pointers and expected next value.
                auto expectedNextNode = location.NextNode(level);
                auto previousNextPointersAtLevel = location.NextPointers(level);

                newNodeNextPointers[level].store(
                    expectedNextNode,
                    std::memory_order_relaxed);

                if (previousNextPointersAtLevel[level].compare_exchange_weak(
                    expectedNextNode,
                    newNode))
                {
                    // If we succeed at replacing the value,
                    // we've successfully committed the object to the list,
                    // so prevent the node from being deleted.
                    newNodeHolder.release();

                    location.NavigateTo(
                        level,
                        previousNextPointersAtLevel,
                        newNode);

                    break;
                }

                // Hm, the old next node changed underneath us.
                // Advance to the new next node and try again.
                // Make sure to do the comparison using the newNode->Value, 
                // because "value" might have std::moved to the newNode->Value.
                auto lastComparisonResult = location.NavigateTo(
                    level,
                    level,
                    newNode->Item.first);

                // On level 0, it's possible that a newly inserted node
                // has the same key.  We check for that.
                if (lastComparisonResult == std::weak_ordering::equivalent)
                {
                    assert(level == 0);
                    assert(newNodeHolder);

                    return
                    {
                        iterator(location),
                        false
                    };
                }
            } while (true);
        }

        return
        {
            iterator(location),
            true
        };
    }

    // find the value at or just before the key.
    // The weak_ordering will indicate either "equivalent" or "greater".
    template<
        typename TSearchKey
    > std::pair<iterator, std::weak_ordering> find(
        const TSearchKey& key
    )
    {
        FingerType finger(
            &m_head,
            &m_comparer);

        auto lastComparisonResult = finger.NavigateTo(
            key);

        return
        {
            iterator(finger),
            lastComparisonResult,
        };
    }

    // find the value at or just before the key,
    // using and modifying the passed in iterator that acts
    // as a hint as to where to start.
    // The weak_ordering will indicate either "equivalent" or "greater".
    template<
        typename TSearchKey
    > std::weak_ordering find_in_place(
        const TSearchKey& key,
        iterator& finger)
    {
        auto findResult = find(
            key);

        finger = findResult.first;

        return findResult.second;
    }

    // find the value at or just before the key,
    // using the passed in iterator that acts
    // as a hint as to where to start.
    // The weak_ordering will indicate either "equivalent" or "greater".
    template<
        typename TSearchKey
    > std::pair<iterator, std::weak_ordering> find(
        const TSearchKey& key,
        const iterator& finger
    )
    {
        iterator result = finger;
        return
        {
            result,
            find_in_place(key, result),
        };
    }

    class iterator
    {
        FingerType m_finger;
        friend class SkipList;
        iterator(
            const FingerType& finger)
            : m_finger(finger)
        {
        }

    public:

        using difference_type = std::ptrdiff_t;
        using value_type = SkipList::value_type;
        using pointer = value_type*;
        using reference = value_type&;
        using iterator_category = std::forward_iterator_tag;

        // Construct an invalid instance
        iterator()
        {}

        reference operator*()
        {
            return m_finger.NextNode(0)->Item;
        }

        iterator& operator++()
        {
            m_finger.NavigateTo(
                0,
                m_finger.NextNode(0)->NextPointers(),
                m_finger.NextNode(0)->NextPointers()[0].load(
                    std::memory_order_acquire)
            );
            return *this;
        }

        value_type* operator->()
        {
            return &m_finger.NextNode(0)->Item;
        }

        bool operator ==(
            const iterator& other
        ) const
        {
            return other.m_finger.NextNode(0) == m_finger.NextNode(0);
        }

        bool operator !=(
            const iterator& other
        ) const
        {
            return other.m_finger.NextNode(0) != m_finger.NextNode(0);
        }

        operator bool() const
        {
            return m_finger.NextNode(0);
        }
    };

    iterator begin()
    {
        auto finger = FingerType(
            &m_head,
            &m_comparer);

        finger.NavigateTo(
            0,
            m_head.data(),
            m_head[0].load(
                std::memory_order_acquire));

        return finger;
    }

    iterator end()
    {
        return FingerType(
            &m_head,
            &m_comparer);
    }
};
}