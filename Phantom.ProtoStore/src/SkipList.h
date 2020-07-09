#pragma once

#include "StandardTypes.h"
#include <assert.h>
#include <array>
#include <atomic>
#include <compare>
#include <iterator>
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
    typename TValue, 
    size_t MaxLevels,
    typename TComparer
>
class SkipList
{
    struct Node;
    struct NextPointers;
    typedef std::atomic<Node*> AtomicNextPointer;
    typedef std::array<AtomicNextPointer, MaxLevels> FullAtomicNextPointersType;
    typedef AtomicNextPointer* AtomicNextPointersType;

    TComparer m_comparer;

    struct Node
    {
        template <
            typename TConstructedValue
        > static Node* Allocate(
            size_t level,
            TConstructedValue&& value
        )
        {
            std::unique_ptr<char[]> nodeCharArray = make_unique<char[]>(
                sizeof(Node) + sizeof(AtomicNextPointer) * level);
            auto result = reinterpret_cast<Node*>(nodeCharArray.get());
            new(result)Node(std::forward<TConstructedValue>(value));
            nodeCharArray.release();
            return result;
        }

        template <typename TConstructedValue>
        Node(
            TConstructedValue&& value)
            : Value(std::forward<TConstructedValue>(value))
        {}

        TValue Value;

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
        typename TSearchValue
    >
    SkipListAddResult Add(
        TSearchValue&& value,
        SkipListReplaceAction replaceAction
    )
    {
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
                    (lastComparisonResult = m_comparer(nextAtLevel->Value, value)) != std::weak_ordering::less)
                {
                    break;
                }

                currentNextPointers = nextAtLevel->NextPointers();

            } while (true);

            if (replaceAction == SkipListReplaceAction::DontReplace
                && lastComparisonResult == std::weak_ordering::equivalent)
            {
                return SkipListAddResult::NotReplaced;
            }

            location[level] = std::make_tuple(
                currentNextPointers,
                nextAtLevel);

        } while (level != 0);

        if (lastComparisonResult == std::weak_ordering::equivalent)
        {
            get<Node*>(location[0])->Value = value;
            return SkipListAddResult::Replaced;
        }

        auto newLevel = NewRandomLevel();

        std::unique_ptr<Node> newNodeHolder = unique_ptr<Node>(Node::Allocate(
            newLevel,
            std::forward<TSearchValue>(value)));

        Node* newNode = newNodeHolder.get();
        auto newNodeNextPointers = newNode->NextPointers();
        
        for (level = 0; level < newLevel; level++)
        {
            do
            {
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
                // Advance to the new next node.
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
                        (lastComparisonResult = m_comparer(nextAtLevel->Value, newNode->Value)) != std::weak_ordering::less)
                    {
                        break;
                    }

                    currentNextPointers = nextAtLevel->NextPointers();

                } while (lastComparisonResult == std::weak_ordering::less);

                location[level] = std::make_tuple(
                    currentNextPointers,
                    nextAtLevel);

                // On level 0, it's possible that a newly inserted node
                // has the same value.  We check for that.
                if (lastComparisonResult == std::weak_ordering::equivalent)
                {
                    assert(level == 0);
                    assert(newNodeHolder);

                    if (replaceAction == SkipListReplaceAction::Replace)
                    {
                        nextAtLevel->Value = std::move(newNodeHolder->Value);
                        return SkipListAddResult::Replaced;
                    }
                    else
                    {
                        return SkipListAddResult::NotReplaced;
                    }
                }
            } while (true);
        }

        return SkipListAddResult::Added;
    }

    class Iterator :
        public std::input_iterator_tag
    {
        Node* m_current;
        friend class SkipList;
        Iterator(
            Node* current)
            :
            m_current(current)
        {}

    public:

        using difference_type = std::ptrdiff_t;
        using value_type = std::ptrdiff_t;
        using pointer = const TValue*;
        using reference = const TValue&;
        using iterator_category = std::forward_iterator_tag;

        reference operator*()
        {
            return m_current->Value;
        }

        Iterator& operator++()
        {
            m_current = m_current->NextPointers()[0].load(
                std::memory_order_acquire);
            return *this;
        }

        bool operator ==(
            const Iterator& other)
        {
            return other.m_current == m_current;
        }

        bool operator !=(
            const Iterator& other)
        {
            return other.m_current != m_current;
        }
    };

    Iterator begin()
    {
        return m_head[0].load(
            std::memory_order_acquire);
    }

    Iterator end()
    {
        return nullptr;
    }
};
}