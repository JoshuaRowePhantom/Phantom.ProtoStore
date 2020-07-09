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

TEST(SkipListTests, Can_add_distinct_strings)
{
    SkipList<std::string, 32, WeakComparer<std::string>> skipList;
    
    ASSERT_EQ(SkipListAddResult::Added, skipList.Add(
        "a",
        SkipListReplaceAction::DontReplace
    ));

    ASSERT_EQ(SkipListAddResult::Added, skipList.Add(
        "c",
        SkipListReplaceAction::DontReplace
    ));

    ASSERT_EQ(SkipListAddResult::Added, skipList.Add(
        "b",
        SkipListReplaceAction::DontReplace
    ));

    vector<string> expectedValues
    {
        "a",
        "b",
        "c",
    };

    vector<string> actualValues(
        skipList.begin(),
        skipList.end());

    ASSERT_EQ(expectedValues, actualValues);
}

struct ReentrantValue
{
    string m_value;
    std::function<void()> m_moveTask;
    int m_targetMovementCount;

    ReentrantValue(
        const string& value,
        std::function<void()> moveTask = {},
        int targetMovementCount = std::numeric_limits<int>::max()
    )
        : m_value(value),
        m_moveTask(moveTask),
        m_targetMovementCount(targetMovementCount)
    {}

    ReentrantValue(
        string&& value)
        :
        m_value(value),
        m_targetMovementCount(std::numeric_limits<int>::max())
    {
    }

    ReentrantValue(
        ReentrantValue&& other
    )
        : 
        m_value(move(other.m_value)),
        m_moveTask(move(other.m_moveTask)),
        m_targetMovementCount(other.m_targetMovementCount - 1)
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
        m_value = other;
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

TEST(SkipListTests, Can_add_values_reentrantly)
{
    SkipList<ReentrantValue, 32, WeakComparer<ReentrantValue>> skipList;

    auto reentrantLambda = [&]
    {
        skipList.Add(string("b"), SkipListReplaceAction::DontReplace);
        skipList.Add(string("c"), SkipListReplaceAction::DontReplace);
    };

    ASSERT_EQ(SkipListAddResult::Added, skipList.Add(
        string("a"),
        SkipListReplaceAction::DontReplace
    ));

    ASSERT_EQ(SkipListAddResult::Added, skipList.Add(
        string("e"),
        SkipListReplaceAction::DontReplace
    ));

    ASSERT_EQ(SkipListAddResult::Added, skipList.Add(
        ReentrantValue(
            string("d"),
            reentrantLambda,
            1),
        SkipListReplaceAction::DontReplace
    ));

    vector<string> expectedValues
    {
        "a",
        "b",
        "c",
        "d",
        "e",
    };

    vector<string> actualValues(
        skipList.begin(),
        skipList.end());

    ASSERT_EQ(expectedValues, actualValues);
}

TEST(SkipListPerformanceTests, DISABLED_Performance_1)
{
    vector<string> allValues;
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
            randomString);
    }

    vector<string> expectedValues = allValues;
    std::sort(
        expectedValues.begin(),
        expectedValues.end());

    SkipList<string, 32, WeakComparer<string>> skipList;

    for (int threadCounter = 0; threadCounter < threadCount; threadCounter++)
    {
        auto threadLambda = [&skipList, &allValues, threadCounter, valueCountPerThread]
        {
            auto begin = allValues.begin() + threadCounter * valueCountPerThread;
            auto end = allValues.begin() + threadCounter * valueCountPerThread + valueCountPerThread;
            for (auto value = begin; value != end; value++)
            {
                ASSERT_EQ(
                    SkipListAddResult::Added,
                    skipList.Add(
                        move(*value),
                        SkipListReplaceAction::DontReplace));
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

    vector<string> actualValues(
        skipList.begin(),
        skipList.end());

    ASSERT_EQ(
        expectedValues,
        actualValues);
}
}