#include "StandardIncludes.h"
#include "Phantom.System/functional.h"
#include <string>

namespace Phantom
{

class ArgumentConverterTests : public testing::Test 
{
public:
    template<typename T>
    struct tagged_value
    {
        std::string value;
    };

    struct tag1 {};
    struct tag2 {};
    struct tag3 {};

    std::string Invocable(
        std::string value) 
    {
        return value;
    }

    template<typename T>
    auto get_tagged_value_converter()
    {
        return [](tagged_value<T> t) { return t.value; };
    }
};

TEST_F(ArgumentConverterTests, Can_convert_one_type)
{
    auto converter1 = get_tagged_value_converter<tag1>();
    auto argumentConverter = ArgumentConverter(converter1);

    std::string resultString = argumentConverter(std::string("foo"));
    EXPECT_EQ(std::string("foo"), resultString);
    std::string resultTag1 = argumentConverter(tagged_value<tag1> { "bar" });
    EXPECT_EQ(std::string("bar"), resultTag1);
    tagged_value<tag2> resultTag2 = argumentConverter(tagged_value<tag2> { "baz" });
    EXPECT_EQ(std::string("baz"), resultTag2.value);
}

TEST_F(ArgumentConverterTests, Can_convert_two_types)
{
    auto converter1 = get_tagged_value_converter<tag1>();
    auto converter2 = get_tagged_value_converter<tag2>();
    auto argumentConverter = ArgumentConverter(
        converter1,
        converter2);

    std::string resultString = argumentConverter(std::string("foo"));
    EXPECT_EQ(std::string("foo"), resultString);
    std::string resultTag1 = argumentConverter(tagged_value<tag1> { "bar" });
    EXPECT_EQ(std::string("bar"), resultTag1);
    std::string resultTag2 = argumentConverter(tagged_value<tag2> { "baz" });
    EXPECT_EQ(std::string("baz"), resultTag2);
}

TEST_F(ArgumentConverterTests, ArgumentConversionAdapter_can_invoke_via_converter)
{
    auto converter1 = get_tagged_value_converter<tag1>();
    auto converter2 = get_tagged_value_converter<tag2>();
    auto argumentConverter = ArgumentConverter(
        converter1,
        converter2);
    auto argumentConversionAdapter = ArgumentConversionAdapter(
        [](std::string s1, const std::string& s2)
    {
        return s1 + s2;
    },
        argumentConverter
        );

    std::string resultString1 = argumentConversionAdapter(
        tagged_value<tag1> { "bar" },
        tagged_value<tag2> { "baz" }
    );
    EXPECT_EQ(std::string("barbaz"), resultString1);

    std::string resultString2 = argumentConversionAdapter(
        std::string { "bar" },
        tagged_value<tag2> { "baz" }
    );
    EXPECT_EQ(std::string("barbaz"), resultString2);
}

}