#include "StandardIncludes.h"

#include "Phantom.ProtoStore/src/HeaderAccessorImpl.h"
#include "Phantom.ProtoStore/src/MemoryExtentStore.h"
#include "Phantom.ProtoStore/src/MessageStore.h"
#include "Phantom.ProtoStore/src/RandomMessageAccessor.h"
#include "src/ProtoStoreInternal.pb.h"

using google::protobuf::util::MessageDifferencer;

namespace Phantom::ProtoStore
{

TEST(HeaderAccessorTests, Throws_exception_when_no_valid_header)
{
    run_async([]() -> task<>
    {
        auto store = make_shared<MemoryExtentStore>();
        auto messageStore = MakeMessageStore(
            store);
        auto randomMessageAccessor = MakeRandomMessageAccessor(
            messageStore);

        auto headerAccessor = MakeHeaderAccessor(
            randomMessageAccessor);

        Header header;
        ASSERT_THROW(
            co_await headerAccessor->ReadHeader(
                header),
            range_error);
    });
}

TEST(HeaderAccessorTests, Can_read_header_from_location1_when_no_valid_location2)
{
    run_async([]() -> task<>
    {
        auto store = make_shared<MemoryExtentStore>();
        auto messageStore = MakeMessageStore(
            store);
        auto randomMessageAccessor = MakeRandomMessageAccessor(
            messageStore);

        Header location1Header;
        location1Header.set_epoch(1);
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation1,
            location1Header
        );

        auto headerAccessor = MakeHeaderAccessor(
            randomMessageAccessor);

        Header actualHeader;
        co_await headerAccessor->ReadHeader(
            actualHeader);

        ASSERT_TRUE(MessageDifferencer::Equals(
            location1Header,
            actualHeader));
    });
}

TEST(HeaderAccessorTests, Can_write_to_location2_when_valid_location1)
{
    run_async([]() -> task<>
    {
        auto store = make_shared<MemoryExtentStore>();
        auto messageStore = MakeMessageStore(
            store);
        auto randomMessageAccessor = MakeRandomMessageAccessor(
            messageStore);

        Header location1Header;
        location1Header.set_epoch(1);
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation1,
            location1Header
        );

        auto headerAccessor = MakeHeaderAccessor(
            randomMessageAccessor);

        Header actualHeader;
        co_await headerAccessor->ReadHeader(
            actualHeader);

        Header location2Header;
        location2Header.set_epoch(2);
        co_await headerAccessor->WriteHeader(
            location2Header);

        Header actualLocation1Header;
        co_await randomMessageAccessor->ReadMessage(
            DefaultHeaderLocation1,
            actualLocation1Header);
        Header actualLocation2Header;
        co_await randomMessageAccessor->ReadMessage(
            DefaultHeaderLocation2,
            actualLocation2Header);

        ASSERT_TRUE(MessageDifferencer::Equals(actualLocation1Header, location1Header));
        ASSERT_TRUE(MessageDifferencer::Equals(actualLocation2Header, location2Header));
    });
}

TEST(HeaderAccessorTests, Can_read_header_from_location2_when_no_valid_location1)
{
    run_async([]() -> task<>
    {
        auto store = make_shared<MemoryExtentStore>();
        auto messageStore = MakeMessageStore(
            store);
        auto randomMessageAccessor = MakeRandomMessageAccessor(
            messageStore);

        Header location2Header;
        location2Header.set_epoch(1);
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation2,
            location2Header
        );

        auto headerAccessor = MakeHeaderAccessor(
            randomMessageAccessor);

        Header actualHeader;
        co_await headerAccessor->ReadHeader(
            actualHeader);

        ASSERT_TRUE(MessageDifferencer::Equals(
            location2Header,
            actualHeader));
    });
}

TEST(HeaderAccessorTests, Can_write_to_location1_when_valid_location2)
{
    run_async([]() -> task<>
    {
        auto store = make_shared<MemoryExtentStore>();
        auto messageStore = MakeMessageStore(
            store);
        auto randomMessageAccessor = MakeRandomMessageAccessor(
            messageStore);

        Header location2Header;
        location2Header.set_epoch(1);
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation2,
            location2Header
        );

        auto headerAccessor = MakeHeaderAccessor(
            randomMessageAccessor);

        Header actualHeader;
        co_await headerAccessor->ReadHeader(
            actualHeader);

        Header location1Header;
        location1Header.set_epoch(2);
        co_await headerAccessor->WriteHeader(
            location1Header);

        Header actualLocation1Header;
        co_await randomMessageAccessor->ReadMessage(
            DefaultHeaderLocation1,
            actualLocation1Header);
        Header actualLocation2Header;
        co_await randomMessageAccessor->ReadMessage(
            DefaultHeaderLocation2,
            actualLocation2Header);

        ASSERT_TRUE(MessageDifferencer::Equals(actualLocation1Header, location1Header));
        ASSERT_TRUE(MessageDifferencer::Equals(actualLocation2Header, location2Header));
    });
}

TEST(HeaderAccessorTests, Can_read_header_from_location2_when_location1_is_older)
{
    run_async([]() -> task<>
    {
        auto store = make_shared<MemoryExtentStore>();
        auto messageStore = MakeMessageStore(
            store);
        auto randomMessageAccessor = MakeRandomMessageAccessor(
            messageStore);

        Header location1Header;
        location1Header.set_epoch(1);
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation1,
            location1Header
        );

        Header location2Header;
        location2Header.set_epoch(2);
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation2,
            location2Header
        );

        auto headerAccessor = MakeHeaderAccessor(
            randomMessageAccessor);

        Header actualHeader;
        co_await headerAccessor->ReadHeader(
            actualHeader);

        ASSERT_TRUE(MessageDifferencer::Equals(
            location2Header,
            actualHeader));
    });
}

TEST(HeaderAccessorTests, Can_write_to_location1_when_valid_location1_is_older)
{
    run_async([]() -> task<>
    {
        auto store = make_shared<MemoryExtentStore>();
        auto messageStore = MakeMessageStore(
            store);
        auto randomMessageAccessor = MakeRandomMessageAccessor(
            messageStore);

        Header location1Header;
        location1Header.set_epoch(1);
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation1,
            location1Header
        );

        Header location2Header;
        location2Header.set_epoch(2);
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation2,
            location2Header
        );

        auto headerAccessor = MakeHeaderAccessor(
            randomMessageAccessor);

        Header actualHeader;
        co_await headerAccessor->ReadHeader(
            actualHeader);

        Header newHeader;
        newHeader.set_epoch(4);
        co_await headerAccessor->WriteHeader(
            newHeader);

        Header actualLocation1Header;
        co_await randomMessageAccessor->ReadMessage(
            DefaultHeaderLocation1,
            actualLocation1Header);
        Header actualLocation2Header;
        co_await randomMessageAccessor->ReadMessage(
            DefaultHeaderLocation2,
            actualLocation2Header);

        ASSERT_TRUE(MessageDifferencer::Equals(actualLocation1Header, newHeader));
        ASSERT_TRUE(MessageDifferencer::Equals(actualLocation2Header, location2Header));
    });
}

TEST(HeaderAccessorTests, Can_read_header_from_location1_when_location2_is_older)
{
    run_async([]() -> task<>
    {
        auto store = make_shared<MemoryExtentStore>();
        auto messageStore = MakeMessageStore(
            store);
        auto randomMessageAccessor = MakeRandomMessageAccessor(
            messageStore);

        Header location1Header;
        location1Header.set_epoch(3);
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation1,
            location1Header
        );

        Header location2Header;
        location2Header.set_epoch(2);
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation2,
            location2Header
        );

        auto headerAccessor = MakeHeaderAccessor(
            randomMessageAccessor);

        Header actualHeader;
        co_await headerAccessor->ReadHeader(
            actualHeader);

        ASSERT_TRUE(MessageDifferencer::Equals(
            location1Header,
            actualHeader));
    });
}

TEST(HeaderAccessorTests, Can_write_to_location2_when_valid_location2_is_older)
{
    run_async([]() -> task<>
    {
        auto store = make_shared<MemoryExtentStore>();
        auto messageStore = MakeMessageStore(
            store);
        auto randomMessageAccessor = MakeRandomMessageAccessor(
            messageStore);

        Header location1Header;
        location1Header.set_epoch(3);
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation1,
            location1Header
        );

        Header location2Header;
        location2Header.set_epoch(2);
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation2,
            location2Header
        );

        auto headerAccessor = MakeHeaderAccessor(
            randomMessageAccessor);

        Header actualHeader;
        co_await headerAccessor->ReadHeader(
            actualHeader);

        Header newHeader;
        newHeader.set_epoch(4);
        co_await headerAccessor->WriteHeader(
            newHeader);

        Header actualLocation1Header;
        co_await randomMessageAccessor->ReadMessage(
            DefaultHeaderLocation1,
            actualLocation1Header);
        Header actualLocation2Header;
        co_await randomMessageAccessor->ReadMessage(
            DefaultHeaderLocation2,
            actualLocation2Header);

        ASSERT_TRUE(MessageDifferencer::Equals(actualLocation1Header, location1Header));
        ASSERT_TRUE(MessageDifferencer::Equals(actualLocation2Header, newHeader));
    });
}

TEST(HeaderAccessorTests, Can_alternate_write_request_locations)
{
    run_async([]() -> task<>
    {
        auto store = make_shared<MemoryExtentStore>();
        auto messageStore = MakeMessageStore(
            store);
        auto randomMessageAccessor = MakeRandomMessageAccessor(
            messageStore);

        Header location1Header;
        location1Header.set_epoch(1);
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation1,
            location1Header
        );

        Header location2Header;
        location2Header.set_epoch(2);
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation2,
            location2Header
        );

        auto headerAccessor = MakeHeaderAccessor(
            randomMessageAccessor);

        Header actualHeader;
        co_await headerAccessor->ReadHeader(
            actualHeader);

        Header newHeader;
        newHeader.set_epoch(3);
        co_await headerAccessor->WriteHeader(
            newHeader);

        Header actualLocation1Header;
        Header actualLocation2Header;

        co_await randomMessageAccessor->ReadMessage(
            DefaultHeaderLocation1,
            actualLocation1Header);
        co_await randomMessageAccessor->ReadMessage(
            DefaultHeaderLocation2,
            actualLocation2Header);

        location1Header = newHeader;
        ASSERT_TRUE(MessageDifferencer::Equals(actualLocation1Header, location1Header));
        ASSERT_TRUE(MessageDifferencer::Equals(actualLocation2Header, location2Header));

        newHeader.set_epoch(4);
        co_await headerAccessor->WriteHeader(
            newHeader);

        co_await randomMessageAccessor->ReadMessage(
            DefaultHeaderLocation1,
            actualLocation1Header);
        co_await randomMessageAccessor->ReadMessage(
            DefaultHeaderLocation2,
            actualLocation2Header);

        location2Header = newHeader;
        ASSERT_TRUE(MessageDifferencer::Equals(actualLocation1Header, location1Header));
        ASSERT_TRUE(MessageDifferencer::Equals(actualLocation2Header, location2Header));

        newHeader.set_epoch(5);
        co_await headerAccessor->WriteHeader(
            newHeader);

        co_await randomMessageAccessor->ReadMessage(
            DefaultHeaderLocation1,
            actualLocation1Header);
        co_await randomMessageAccessor->ReadMessage(
            DefaultHeaderLocation2,
            actualLocation2Header);

        location1Header = newHeader;
        ASSERT_TRUE(MessageDifferencer::Equals(actualLocation1Header, location1Header));
        ASSERT_TRUE(MessageDifferencer::Equals(actualLocation2Header, location2Header));
    });
}

}