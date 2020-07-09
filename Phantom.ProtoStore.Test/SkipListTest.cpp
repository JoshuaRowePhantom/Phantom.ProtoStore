#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/SkipList.h"
#include <functional>

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

}