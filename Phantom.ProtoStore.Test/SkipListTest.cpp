#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/SkipList.h"
#include <algorithm>
#include <functional>
#include <random>

using namespace std;

namespace Phantom::ProtoStore
{

template<typename T>
struct WeakComparer
{
    std::weak_ordering operator ()(
        const T& t1,
        const T& t2
        ) const
    {
        if (t1 < t2)
        {
            return std::weak_ordering::less;
        }
        if (t2 < t1)
        {
            return std::weak_ordering::greater;
        }
        return std::weak_ordering::equivalent;
    }
};

TEST(SkipListTests, starts_empty)
{
    SkipList<std::string, std::string, 4, WeakComparer<std::string>> skipList;

    vector<pair<const string, string>> expectedValues;
    vector<pair<const string, string>> actualValues(
        skipList.begin(),
        skipList.end());

    ASSERT_EQ(expectedValues, actualValues);
    ASSERT_EQ(skipList.end(), skipList.find("foo").first);
}

TEST(SkipListTests, insert_convertible_values_via_converting_constructor)
{
    SkipList<std::string, std::string, 4, WeakComparer<std::string>> skipList;

    auto insert = skipList.insert("a", "b");
    ASSERT_EQ("a", insert.first->first);
    ASSERT_EQ("b", insert.first->second);
}

TEST(SkipListTests, insert_convertible_values_via_conversion_operator)
{
    struct Value
    {
        operator string() const
        {
            return "b";
        }
    };

    SkipList<std::string, std::string, 4, WeakComparer<std::string>> skipList;

    auto insert = skipList.insert("a", Value());
    ASSERT_EQ("a", insert.first->first);
    ASSERT_EQ("b", insert.first->second);
}

TEST(SkipListTests, insert_immovable_object)
{
    SkipList<std::string, std::unique_ptr<string>, 4, WeakComparer<std::string>> skipList;

    auto value = make_unique<string>("b");
    auto insert = skipList.insert("a", move(value));
    ASSERT_EQ("a", insert.first->first);
    ASSERT_EQ("b", *(insert.first->second));
}

TEST(SkipListTests, insert_distinct_strings)
{
    SkipList<std::string, int, 4, WeakComparer<std::string>> skipList;
    
    auto insert1 = skipList.insert(
        "a",
        1
    );

    ASSERT_EQ("a", insert1.first->first);
    ASSERT_EQ(1, insert1.first->second);
    ASSERT_EQ(true, insert1.second);

    auto insert2 = skipList.insert(
        "c",
        3);

    ASSERT_EQ("c", insert2.first->first);
    ASSERT_EQ(3, insert2.first->second);
    ASSERT_EQ(true, insert2.second);

    auto insert3 = skipList.insert(
        "b",
        2);

    ASSERT_EQ("b", insert3.first->first);
    ASSERT_EQ(2, insert3.first->second);
    ASSERT_EQ(true, insert3.second);

    vector<pair<const string, int>> expectedValues
    {
        make_pair("a", 1),
        make_pair("b", 2),
        make_pair("c", 3),
    };

    vector<pair<const string, int>> actualValues(
        skipList.begin(),
        skipList.end());

    ASSERT_EQ(expectedValues, actualValues);
}

TEST(SkipListTests, can_find_without_finger)
{
    SkipList<int, int, 8, WeakComparer<int>> skipList;

    for (int value = 0; value < 100; value += 2)
    {
        skipList.insert(value, value);
    }

    for (int value = 0; value < 100; value += 2)
    {
        auto findResult = skipList.find(value);
        ASSERT_EQ(true, findResult.first);
        ASSERT_EQ(value, findResult.first->first);
        ASSERT_EQ(std::weak_ordering::equivalent, findResult.second);
    }

    for (int value = -1; value < 99; value += 2)
    {
        auto findResult = skipList.find(value);
        ASSERT_EQ(true, findResult.first);
        ASSERT_EQ(value + 1, findResult.first->first);
        ASSERT_EQ(std::weak_ordering::greater, findResult.second);
    }

    {
        auto findResult = skipList.find(99);
        ASSERT_EQ(false, findResult.first);
        ASSERT_EQ(skipList.end(), findResult.first);
        ASSERT_EQ(std::weak_ordering::greater, findResult.second);
    }
}

TEST(SkipListTests, can_find_forward_and_backward_from_insertion_point)
{
    SkipList<int, int, 8, WeakComparer<int>> skipList;

    for (int value = 0; value < 100; value += 2)
    {
        skipList.insert(value, value);
    }

    auto insertionPoint = skipList.begin();
    int insertionValue;

    auto doFindOperations = [&]()
    {
        insertionValue;
        for (int value = 0; value < 100; value += 2)
        {
            auto findResult = skipList.find(value, insertionPoint);
            ASSERT_EQ(true, findResult.first);
            ASSERT_EQ(value, findResult.first->first);
            ASSERT_EQ(std::weak_ordering::equivalent, findResult.second);
        }

        for (int value = -1; value < 99; value += 2)
        {
            auto findResult = skipList.find(value, insertionPoint);
            ASSERT_EQ(true, findResult.first);
            ASSERT_EQ(value + 1, findResult.first->first);
            ASSERT_EQ(std::weak_ordering::greater, findResult.second);
        }

        {
            auto findResult = skipList.find(99, insertionPoint);
            ASSERT_EQ(false, findResult.first);
            ASSERT_EQ(skipList.end(), findResult.first);
            ASSERT_EQ(std::weak_ordering::greater, findResult.second);
        }

        {
            auto findResult = skipList.find(99, insertionPoint);
            ASSERT_EQ(false, findResult.first);
            ASSERT_EQ(skipList.end(), findResult.first);
            ASSERT_EQ(std::weak_ordering::greater, findResult.second);
        }
    };

    doFindOperations();

    for (insertionValue = 0; insertionValue < 100; insertionValue += 2)
    {
        auto insertResult = skipList.insert(insertionValue, insertionValue);
        ASSERT_EQ(true, insertResult.first);
        ASSERT_EQ(false, insertResult.second);
        insertionPoint = insertResult.first;

        doFindOperations();
    }
}

TEST(SkipListTests, insert_duplicate_strings_returns_false)
{
    SkipList<std::string, int, 32, WeakComparer<std::string>> skipList;

    auto insert1 = skipList.insert(
        "a",
        1
    );

    ASSERT_EQ("a", insert1.first->first);
    ASSERT_EQ(1, insert1.first->second);
    ASSERT_EQ(true, insert1.second);

    auto insert2 = skipList.insert(
        "c",
        3);

    ASSERT_EQ("c", insert2.first->first);
    ASSERT_EQ(3, insert2.first->second);
    ASSERT_EQ(true, insert2.second);

    auto insert3 = skipList.insert(
        "b",
        2);

    ASSERT_EQ("b", insert3.first->first);
    ASSERT_EQ(2, insert3.first->second);
    ASSERT_EQ(true, insert3.second);

    auto insert4 = skipList.insert(
        "b",
        4);

    ASSERT_EQ("b", insert4.first->first);
    ASSERT_EQ(2, insert4.first->second);
    ASSERT_EQ(false, insert4.second);

    vector<pair<const string, int>> expectedValues
    {
        make_pair("a", 1),
        make_pair("b", 2),
        make_pair("c", 3),
    };

    vector<pair<const string, int>> actualValues(
        skipList.begin(),
        skipList.end());

    ASSERT_EQ(expectedValues, actualValues);
}

TEST(SkipListTests, insert_duplicate_strings_iterator_can_replace_value)
{
    SkipList<std::string, int, 32, WeakComparer<std::string>> skipList;

    auto insert3 = skipList.insert(
        "b",
        2);

    auto insert4 = skipList.insert(
        "b",
        4);

    insert4.first->second = 6;

    vector<pair<const string, int>> expectedValues
    {
        make_pair("b", 6),
    };

    vector<pair<const string, int>> actualValues(
        skipList.begin(),
        skipList.end());

    ASSERT_EQ(expectedValues, actualValues);
}

struct ReentrantValue
{
    string m_value;
    std::function<void()> m_moveTask;
    int m_targetMovementCount;
    int m_totalMovementCount;

    ReentrantValue(
        const string& value,
        std::function<void()> moveTask = {},
        int targetMovementCount = std::numeric_limits<int>::max()
    )
        : m_value(value),
        m_moveTask(moveTask),
        m_targetMovementCount(targetMovementCount),
        m_totalMovementCount(0)
    {}

    ReentrantValue(
        string&& value)
        :
        m_value(value),
        m_targetMovementCount(std::numeric_limits<int>::max()),
        m_totalMovementCount(0)
    {
    }

    ReentrantValue(
        ReentrantValue&& other
    )
        : 
        m_value(move(other.m_value)),
        m_moveTask(move(other.m_moveTask)),
        m_targetMovementCount(other.m_targetMovementCount - 1),
        m_totalMovementCount(other.m_totalMovementCount + 1)
    {
        if (m_targetMovementCount == 0)
        {
            m_moveTask();
            m_moveTask = {};
        }
    }

    ~ReentrantValue()
    {
        assert(!m_moveTask);
    }

    ReentrantValue(
        const ReentrantValue& other
    ) = delete;

    ReentrantValue& operator=(
        ReentrantValue&& other
        )
    {
        m_value = other.m_value;
        m_moveTask = move(other.m_moveTask);
        m_targetMovementCount = other.m_targetMovementCount - 1;
        m_totalMovementCount = other.m_totalMovementCount + 1;

        if (m_targetMovementCount == 0)
        {
            m_moveTask();
            m_moveTask = {};
        }

        return *this;
    }

    ReentrantValue& operator=(
        const string& other
        )
    {
        m_value = other;
        return *this;
    }

    ReentrantValue& operator=(
        string&& other
        )
    {
        m_value = move(other);
        return *this;
    }

    ReentrantValue& operator=(
        const ReentrantValue& other
        ) = delete;

    bool operator<(
        const ReentrantValue& other
        )
        const
    {
        return m_value < other.m_value;
    }

    operator const string&() const
    {
        return m_value;
    }
};

TEST(SkipListTests, insert_distinct_values_reentrantly_at_insertion_point_does_insertion)
{
    SkipList<ReentrantValue, int, 32, WeakComparer<ReentrantValue>> skipList;

    auto reentrantLambda = [&]
    {
        skipList.insert(string("b"), 2);
        skipList.insert(string("c"), 3);
    };

    skipList.insert(
        string("a"),
        1);

    skipList.insert(
        string("e"),
        5);

    skipList.insert(
        ReentrantValue(
            string("d"),
            reentrantLambda,
            1),
        4);

    vector<pair<string, int>> expectedValues
    {
        {"a", 1},
        {"b", 2},
        {"c", 3},
        {"d", 4},
        {"e", 5},
    };

    vector<pair<string, int>> actualValues(
        skipList.begin(),
        skipList.end());

    ASSERT_EQ(expectedValues, actualValues);
}

TEST(SkipListTests, insert_duplicate_values_reentrantly_at_insertion_point_does_not_replace_and_puts_key_back)
{
    SkipList<ReentrantValue, int, 32, WeakComparer<ReentrantValue>> skipList;

    auto reentrantLambda = [&]
    {
        skipList.insert(string("b"), 2);
        skipList.insert(string("c"), 3);
    };

    skipList.insert(
        string("a"),
        1);

    skipList.insert(
        string("e"),
        5);

    auto reentrantValue = ReentrantValue(
        string("c"),
        reentrantLambda,
        1);

    auto nonReplacingInsert = skipList.insert(
        move(reentrantValue),
        6);

    ASSERT_EQ("c", (const string&)nonReplacingInsert.first->first);
    ASSERT_EQ(3, nonReplacingInsert.first->second);
    ASSERT_EQ(false, nonReplacingInsert.second);
    ASSERT_EQ("c", reentrantValue.m_value);
    ASSERT_EQ(2, reentrantValue.m_totalMovementCount);

    vector<pair<string, int>> expectedValues
    {
        {"a", 1},
        {"b", 2},
        {"c", 3},
        {"e", 5},
    };

    vector<pair<string, int>> actualValues(
        skipList.begin(),
        skipList.end());

    ASSERT_EQ(expectedValues, actualValues);
}

TEST(SkipListPerformanceTests, PerformanceTest(Perf1))
{
    vector<pair<string, int>> allValues;
    int valueCountPerThread = 1000000;
    int threadCount = 4;
    vector<thread> threads;

    mt19937 rng;
    uniform_int_distribution<int> distribution('a', 'z');

    allValues.reserve(valueCountPerThread * threadCount);
    for (int valueCounter = 0; valueCounter < valueCountPerThread * threadCount; valueCounter++)
    {
        string randomString(' ', 20);
        for (int stringIndex = 0; stringIndex < randomString.size(); stringIndex++)
        {
            randomString[stringIndex] = distribution(rng);
        }
        allValues.push_back(
            {
                randomString,
                distribution(rng)
            });
    }

    vector<pair<string, int>> expectedValues = allValues;
    std::sort(
        expectedValues.begin(),
        expectedValues.end());

    SkipList<string, int, 32, WeakComparer<string>> skipList;

    for (int threadCounter = 0; threadCounter < threadCount; threadCounter++)
    {
        auto threadLambda = [&skipList, &allValues, threadCounter, valueCountPerThread]
        {
            auto begin = allValues.begin() + threadCounter * valueCountPerThread;
            auto end = allValues.begin() + threadCounter * valueCountPerThread + valueCountPerThread;
            for (auto value = begin; value != end; value++)
            {
                skipList.insert(
                    move(value->first),
                    value->second);
            }
        };

        threads.emplace_back(
            threadLambda);
    }

    auto startTime = chrono::high_resolution_clock::now();

    for (auto& thread : threads)
    {
        thread.join();
    }

    auto endTime = chrono::high_resolution_clock::now();

    auto runtimeMs = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);

    std::cout << "SkipListPerformanceTests runtime: " << runtimeMs.count() << "\r\n";

    vector<pair<string, int>> actualValues(
        skipList.begin(),
        skipList.end());

    ASSERT_EQ(
        expectedValues,
        actualValues);
}

TEST(SkipListTests, SkipList_using_void_value)
{
    SkipList<string, void, 4, WeakComparer<string>> skipList;

    auto insertion1 = skipList.insert(
        "a");

    auto insertion2 = skipList.insert(
        "b");

    auto insertion3 = skipList.insert(
        "b");

    vector<string> expectedValues
    {
        "a",
        "b",
    };

    vector<string> actualValues(
        skipList.begin(),
        skipList.end());

    ASSERT_EQ(
        expectedValues,
        actualValues);
}
}