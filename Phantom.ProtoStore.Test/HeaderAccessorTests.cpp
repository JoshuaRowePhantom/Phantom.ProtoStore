#include "StandardIncludes.h"

#include "Phantom.ProtoStore/src/HeaderAccessorImpl.h"
#include "Phantom.ProtoStore/src/MemoryExtentStore.h"
#include "Phantom.ProtoStore/src/MessageStore.h"
#include "Phantom.ProtoStore/src/RandomMessageAccessor.h"
#include "src/ProtoStoreInternal_generated.h"

using google::protobuf::util::MessageDifferencer;

namespace Phantom::ProtoStore
{

using namespace FlatBuffers;

TEST(HeaderAccessorTests, Throws_exception_when_no_valid_header)
{
    run_async([]() -> task<>
    {
        auto store = make_shared<MemoryExtentStore>(
            Schedulers::Default());
        auto messageStore = MakeMessageStore(
            Schedulers::Default(),
            store);
        auto randomMessageAccessor = MakeRandomMessageAccessor(
            messageStore);

        auto headerAccessor = MakeHeaderAccessor(
            randomMessageAccessor);

        EXPECT_THROW(
            co_await headerAccessor->ReadHeader(),
            range_error);
    });
}

TEST(HeaderAccessorTests, Can_read_header_from_location1_when_no_valid_location2)
{
    run_async([]() -> task<>
    {
        auto store = make_shared<MemoryExtentStore>(
            Schedulers::Default());
        auto messageStore = MakeMessageStore(
            Schedulers::Default(),
            store);
        auto randomMessageAccessor = MakeRandomMessageAccessor(
            messageStore);

        DatabaseHeaderT location1Header;
        location1Header.epoch = 1;
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation1,
            FlatMessage{ &location1Header }.data(),
            FlushBehavior::Flush
        );

        auto headerAccessor = MakeHeaderAccessor(
            randomMessageAccessor);

        auto actualHeader = co_await headerAccessor->ReadHeader();

        EXPECT_EQ(
            location1Header,
            *actualHeader);
    });
}

TEST(HeaderAccessorTests, Can_write_to_location2_when_valid_location1)
{
    run_async([]() -> task<>
    {
        auto store = make_shared<MemoryExtentStore>(
            Schedulers::Default());
        auto messageStore = MakeMessageStore(
            Schedulers::Default(),
            store);
        auto randomMessageAccessor = MakeRandomMessageAccessor(
            messageStore);

        DatabaseHeaderT location1Header;
        location1Header.epoch = 1;
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation1,
            FlatMessage{ &location1Header }.data()
        );

        auto headerAccessor = MakeHeaderAccessor(
            randomMessageAccessor);

        auto actualHeader = co_await headerAccessor->ReadHeader();

        DatabaseHeaderT location2Header;
        location2Header.epoch = 2;
        co_await headerAccessor->WriteHeader(
            &location2Header);

        auto actualLocation1Header = FlatMessage<DatabaseHeader>
        { 
            co_await randomMessageAccessor->ReadMessage(
                DefaultHeaderLocation1) 
        }->UnPack();

        auto actualLocation2Header = FlatMessage<DatabaseHeader>
        {
            co_await randomMessageAccessor->ReadMessage(
                DefaultHeaderLocation2)
        }->UnPack();

        EXPECT_EQ(*actualLocation1Header, location1Header);
        EXPECT_EQ(*actualLocation2Header, location2Header);
    });
}

TEST(HeaderAccessorTests, Can_read_header_from_location2_when_no_valid_location1)
{
    run_async([]() -> task<>
    {
        auto store = make_shared<MemoryExtentStore>(
            Schedulers::Default());
        auto messageStore = MakeMessageStore(
            Schedulers::Default(),
            store);
        auto randomMessageAccessor = MakeRandomMessageAccessor(
            messageStore);

        DatabaseHeaderT location2Header;
        location2Header.epoch = 1;
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation2,
            FlatMessage{ &location2Header }.data()
        );

        auto headerAccessor = MakeHeaderAccessor(
            randomMessageAccessor);

        auto actualHeader = co_await headerAccessor->ReadHeader();

        EXPECT_EQ(
            location2Header,
            *actualHeader);
    });
}

TEST(HeaderAccessorTests, Can_write_to_location1_when_valid_location2)
{
    run_async([]() -> task<>
    {
        auto store = make_shared<MemoryExtentStore>(
            Schedulers::Default());
        auto messageStore = MakeMessageStore(
            Schedulers::Default(),
            store);
        auto randomMessageAccessor = MakeRandomMessageAccessor(
            messageStore);

        DatabaseHeaderT location2Header;
        location2Header.epoch = 1;
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation2,
            FlatMessage{ &location2Header }.data()
        );

        auto headerAccessor = MakeHeaderAccessor(
            randomMessageAccessor);

        auto actualHeader = co_await headerAccessor->ReadHeader();

        DatabaseHeaderT location1Header;
        location1Header.epoch = 2;
        co_await headerAccessor->WriteHeader(
            &location1Header);

        auto actualLocation1Header = FlatMessage<DatabaseHeader>
        {
            co_await randomMessageAccessor->ReadMessage(
                DefaultHeaderLocation1)
        }->UnPack();

        auto actualLocation2Header = FlatMessage<DatabaseHeader>
        {
            co_await randomMessageAccessor->ReadMessage(
                DefaultHeaderLocation2)
        }->UnPack();

        EXPECT_EQ(*actualLocation1Header, location1Header);
        EXPECT_EQ(*actualLocation2Header, location2Header);
    });
}

TEST(HeaderAccessorTests, Can_read_header_from_location2_when_location1_is_older)
{
    run_async([]() -> task<>
    {
        auto store = make_shared<MemoryExtentStore>(
            Schedulers::Default());
        auto messageStore = MakeMessageStore(
            Schedulers::Default(),
            store);
        auto randomMessageAccessor = MakeRandomMessageAccessor(
            messageStore);

        DatabaseHeaderT location1Header;
        location1Header.epoch = 1;
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation1,
            FlatMessage{ &location1Header }.data()
        );

        DatabaseHeaderT location2Header;
        location2Header.epoch = 2;
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation2,
            FlatMessage{ &location2Header }.data()
        );

        auto headerAccessor = MakeHeaderAccessor(
            randomMessageAccessor);

        auto actualHeader = co_await headerAccessor->ReadHeader();

        EXPECT_EQ(
            location2Header,
            *actualHeader);
    });
}

TEST(HeaderAccessorTests, Can_write_to_location1_when_valid_location1_is_older)
{
    run_async([]() -> task<>
    {
        auto store = make_shared<MemoryExtentStore>(
            Schedulers::Default());
        auto messageStore = MakeMessageStore(
            Schedulers::Default(),
            store);
        auto randomMessageAccessor = MakeRandomMessageAccessor(
            messageStore);

        DatabaseHeaderT location1Header;
        location1Header.epoch = 1;
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation1,
            FlatMessage{ &location1Header }.data()
        );

        DatabaseHeaderT location2Header;
        location2Header.epoch = 2;
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation2,
            FlatMessage{ &location2Header }.data()
        );

        auto headerAccessor = MakeHeaderAccessor(
            randomMessageAccessor);

        auto actualHeader = co_await headerAccessor->ReadHeader();

        DatabaseHeaderT newHeader;
        newHeader.epoch = 4;
        co_await headerAccessor->WriteHeader(
            &newHeader);

        auto actualLocation1Header = FlatMessage<DatabaseHeader>
        {
            co_await randomMessageAccessor->ReadMessage(
                DefaultHeaderLocation1)
        }->UnPack();

        auto actualLocation2Header = FlatMessage<DatabaseHeader>
        {
            co_await randomMessageAccessor->ReadMessage(
                DefaultHeaderLocation2)
        }->UnPack();

        EXPECT_EQ(*actualLocation1Header, newHeader);
        EXPECT_EQ(*actualLocation2Header, location2Header);
    });
}

TEST(HeaderAccessorTests, Can_read_header_from_location1_when_location2_is_older)
{
    run_async([]() -> task<>
    {
        auto store = make_shared<MemoryExtentStore>(
            Schedulers::Default());
        auto messageStore = MakeMessageStore(
            Schedulers::Default(),
            store);
        auto randomMessageAccessor = MakeRandomMessageAccessor(
            messageStore);

        DatabaseHeaderT location1Header;
        location1Header.epoch = 3;
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation1,
            FlatMessage{ &location1Header }.data()
        );

        DatabaseHeaderT location2Header;
        location2Header.epoch = 2;
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation2,
            FlatMessage{ &location2Header }.data()
        );

        auto headerAccessor = MakeHeaderAccessor(
            randomMessageAccessor);

        auto actualHeader = co_await headerAccessor->ReadHeader();

        EXPECT_EQ(
            location1Header,
            *actualHeader);
    });
}

TEST(HeaderAccessorTests, Can_write_to_location2_when_valid_location2_is_older)
{
    run_async([]() -> task<>
    {
        auto store = make_shared<MemoryExtentStore>(
            Schedulers::Default());
        auto messageStore = MakeMessageStore(
            Schedulers::Default(),
            store);
        auto randomMessageAccessor = MakeRandomMessageAccessor(
            messageStore);

        DatabaseHeaderT location1Header;
        location1Header.epoch = 3;
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation1,
            FlatMessage{ &location1Header }.data()
        );

        DatabaseHeaderT location2Header;
        location2Header.epoch = 2;
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation2,
            FlatMessage{ &location2Header }.data()
        );

        auto headerAccessor = MakeHeaderAccessor(
            randomMessageAccessor);

        auto actualHeader = co_await headerAccessor->ReadHeader();

        DatabaseHeaderT newHeader;
        newHeader.epoch = 4;
        co_await headerAccessor->WriteHeader(
            &newHeader);

        auto actualLocation1Header = FlatMessage<DatabaseHeader>
        {
            co_await randomMessageAccessor->ReadMessage(
                DefaultHeaderLocation1)
        }->UnPack();

        auto actualLocation2Header = FlatMessage<DatabaseHeader>
        {
            co_await randomMessageAccessor->ReadMessage(
                DefaultHeaderLocation2)
        }->UnPack();

        EXPECT_EQ(*actualLocation1Header, location1Header);
        EXPECT_EQ(*actualLocation2Header, newHeader);
    });
}

TEST(HeaderAccessorTests, Can_alternate_write_request_locations)
{
    run_async([]() -> task<>
    {
        auto store = make_shared<MemoryExtentStore>(
            Schedulers::Default());
        auto messageStore = MakeMessageStore(
            Schedulers::Default(),
            store);
        auto randomMessageAccessor = MakeRandomMessageAccessor(
            messageStore);

        DatabaseHeaderT location1Header;
        location1Header.epoch = 1;
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation1,
            FlatMessage{ &location1Header }.data()
        );

        DatabaseHeaderT location2Header;
        location2Header.epoch = 2;
        co_await randomMessageAccessor->WriteMessage(
            DefaultHeaderLocation2,
            FlatMessage{ &location2Header }.data()
        );

        auto headerAccessor = MakeHeaderAccessor(
            randomMessageAccessor);

        auto actualHeader = co_await headerAccessor->ReadHeader();

        DatabaseHeaderT newHeader;
        newHeader.epoch = 3;
        co_await headerAccessor->WriteHeader(
            &newHeader);

        DatabaseHeaderT ;
        DatabaseHeaderT ;

        auto actualLocation1Header = FlatMessage<DatabaseHeader>
        {
            co_await randomMessageAccessor->ReadMessage(
                DefaultHeaderLocation1)
        }->UnPack();

        auto actualLocation2Header = FlatMessage<DatabaseHeader>
        {
            co_await randomMessageAccessor->ReadMessage(
                DefaultHeaderLocation2)
        }->UnPack();

        location1Header = newHeader;
        EXPECT_EQ(*actualLocation1Header, location1Header);
        EXPECT_EQ(*actualLocation2Header, location2Header);

        newHeader.epoch = 4;
        co_await headerAccessor->WriteHeader(
            &newHeader);

        actualLocation1Header = FlatMessage<DatabaseHeader>
        {
            co_await randomMessageAccessor->ReadMessage(
                DefaultHeaderLocation1)
        }->UnPack();

        actualLocation2Header = FlatMessage<DatabaseHeader>
        {
            co_await randomMessageAccessor->ReadMessage(
                DefaultHeaderLocation2)
        }->UnPack();

        location2Header = newHeader;
        EXPECT_EQ(*actualLocation1Header, location1Header);
        EXPECT_EQ(*actualLocation2Header, location2Header);

        newHeader.epoch = 5;
        co_await headerAccessor->WriteHeader(
            &newHeader);

        actualLocation1Header = FlatMessage<DatabaseHeader>
        {
            co_await randomMessageAccessor->ReadMessage(
                DefaultHeaderLocation1)
        }->UnPack();

        actualLocation2Header = FlatMessage<DatabaseHeader>
        {
            co_await randomMessageAccessor->ReadMessage(
                DefaultHeaderLocation2)
        }->UnPack();

        location1Header = newHeader;
        EXPECT_EQ(*actualLocation1Header, location1Header);
        EXPECT_EQ(*actualLocation2Header, location2Header);
    });
}

}