#include "StandardIncludes.h"

#include "Phantom.ProtoStore/src/HeaderAccessorImpl.h"
#include "Phantom.ProtoStore/src/MemoryExtentStore.h"
#include "Phantom.ProtoStore/src/MessageStore.h"
#include "src/ProtoStoreInternal_generated.h"

using google::protobuf::util::MessageDifferencer;

namespace Phantom::ProtoStore
{

using namespace FlatBuffers;

class HeaderAccessorTests : public testing::Test
{
protected:
    std::shared_ptr<IExtentStore> extentStore = std::make_shared<MemoryExtentStore>(
        Schedulers::Default());
    std::shared_ptr<IMessageStore> messageStore = MakeMessageStore(
        Schedulers::Default(),
        extentStore);

    auto MakeHeaderAccessor()
    {
        return Phantom::ProtoStore::MakeHeaderAccessor(messageStore);
    }

    task<DataReference<StoredMessage>> WriteHeader(
        const DatabaseHeaderT& header,
        ExtentLocation location)
    {
        auto extent = co_await messageStore->OpenExtentForRandomWriteAccess(
            location.extentName);

        co_return co_await extent->Write(
            location.extentOffset,
            FlatMessage{ &header }.data(),
            FlushBehavior::Flush
        );
    }

    task<std::unique_ptr<FlatBuffers::DatabaseHeaderT>> ReadHeader(
        ExtentLocation location
    )
    {
        auto extent = co_await messageStore->OpenExtentForRandomReadAccess(
            location.extentName);

        co_return FlatMessage<FlatBuffers::DatabaseHeader> 
        {
            co_await extent->Read(
                location.extentOffset
            )
        }->UnPack();
    }
};
ASYNC_TEST_F(HeaderAccessorTests, Throws_exception_when_no_valid_header)
{
    auto headerAccessor = MakeHeaderAccessor();

    EXPECT_EQ(nullptr, co_await headerAccessor->ReadHeader());
}

ASYNC_TEST_F(HeaderAccessorTests, Can_read_header_from_location1_when_no_valid_location2)
{
    DatabaseHeaderT location1Header;
    location1Header.epoch = 1;

    co_await WriteHeader(
        location1Header,
        DefaultHeaderLocation1);

    auto headerAccessor = MakeHeaderAccessor();

    auto actualHeader = co_await headerAccessor->ReadHeader();

    EXPECT_EQ(
        location1Header,
        *actualHeader);
}
ASYNC_TEST_F(HeaderAccessorTests, Can_write_to_location2_when_valid_location1)
{
    DatabaseHeaderT location1Header;
    location1Header.epoch = 1;

    co_await WriteHeader(
        location1Header,
        DefaultHeaderLocation1);

    auto headerAccessor = MakeHeaderAccessor();

    auto actualHeader = co_await headerAccessor->ReadHeader();

    DatabaseHeaderT location2Header;
    location2Header.epoch = 2;
    co_await headerAccessor->WriteHeader(
        &location2Header);

    auto actualLocation1Header = co_await ReadHeader(
        DefaultHeaderLocation1);
    auto actualLocation2Header = co_await ReadHeader(
        DefaultHeaderLocation2);

    EXPECT_EQ(*actualLocation1Header, location1Header);
    EXPECT_EQ(*actualLocation2Header, location2Header);
}
ASYNC_TEST_F(HeaderAccessorTests, Can_read_header_from_location2_when_no_valid_location1)
{
    DatabaseHeaderT location2Header;
    location2Header.epoch = 1;

    co_await WriteHeader(
        location2Header,
        DefaultHeaderLocation2);

    auto headerAccessor = MakeHeaderAccessor();

    auto actualHeader = co_await headerAccessor->ReadHeader();

    EXPECT_EQ(
        location2Header,
        *actualHeader);
}
ASYNC_TEST_F(HeaderAccessorTests, Can_write_to_location1_when_valid_location2)
{
    DatabaseHeaderT location2Header;
    location2Header.epoch = 1;

    co_await WriteHeader(
        location2Header,
        DefaultHeaderLocation2);

    auto headerAccessor = MakeHeaderAccessor();

    auto actualHeader = co_await headerAccessor->ReadHeader();

    DatabaseHeaderT location1Header;
    location1Header.epoch = 2;
    co_await headerAccessor->WriteHeader(
        &location1Header);

    auto actualLocation1Header = co_await ReadHeader(
        DefaultHeaderLocation1);
    auto actualLocation2Header = co_await ReadHeader(
        DefaultHeaderLocation2);

    EXPECT_EQ(*actualLocation1Header, location1Header);
    EXPECT_EQ(*actualLocation2Header, location2Header);
}
ASYNC_TEST_F(HeaderAccessorTests, Can_read_header_from_location2_when_location1_is_older)
{
    DatabaseHeaderT location1Header;
    location1Header.epoch = 1;

    co_await WriteHeader(
        location1Header,
        DefaultHeaderLocation1);

    DatabaseHeaderT location2Header;
    location2Header.epoch = 2;

    co_await WriteHeader(
        location2Header,
        DefaultHeaderLocation2);

    auto headerAccessor = MakeHeaderAccessor();

    auto actualHeader = co_await headerAccessor->ReadHeader();

    EXPECT_EQ(
        location2Header,
        *actualHeader);
}
ASYNC_TEST_F(HeaderAccessorTests, Can_write_to_location1_when_valid_location1_is_older)
{
    DatabaseHeaderT location1Header;
    location1Header.epoch = 1;

    co_await WriteHeader(
        location1Header,
        DefaultHeaderLocation1);

    DatabaseHeaderT location2Header;
    location2Header.epoch = 2;

    co_await WriteHeader(
        location2Header,
        DefaultHeaderLocation2);

    auto headerAccessor = MakeHeaderAccessor();

    auto actualHeader = co_await headerAccessor->ReadHeader();

    DatabaseHeaderT newHeader;
    newHeader.epoch = 4;
    co_await headerAccessor->WriteHeader(
        &newHeader);

    auto actualLocation1Header = co_await ReadHeader(
        DefaultHeaderLocation1);
    auto actualLocation2Header = co_await ReadHeader(
        DefaultHeaderLocation2);

    EXPECT_EQ(*actualLocation1Header, newHeader);
    EXPECT_EQ(*actualLocation2Header, location2Header);
}
ASYNC_TEST_F(HeaderAccessorTests, Can_read_header_from_location1_when_location2_is_older)
{
    DatabaseHeaderT location1Header;
    location1Header.epoch = 3;

    co_await WriteHeader(
        location1Header,
        DefaultHeaderLocation1);

    DatabaseHeaderT location2Header;
    location2Header.epoch = 2;

    co_await WriteHeader(
        location2Header,
        DefaultHeaderLocation2);

    auto headerAccessor = MakeHeaderAccessor();

    auto actualHeader = co_await headerAccessor->ReadHeader();

    EXPECT_EQ(
        location1Header,
        *actualHeader);
}
ASYNC_TEST_F(HeaderAccessorTests, Can_write_to_location2_when_valid_location2_is_older)
{
    DatabaseHeaderT location1Header;
    location1Header.epoch = 3;

    co_await WriteHeader(
        location1Header,
        DefaultHeaderLocation1);

    DatabaseHeaderT location2Header;
    location2Header.epoch = 2;

    co_await WriteHeader(
        location2Header,
        DefaultHeaderLocation2);

    auto headerAccessor = MakeHeaderAccessor();

    auto actualHeader = co_await headerAccessor->ReadHeader();

    DatabaseHeaderT newHeader;
    newHeader.epoch = 4;
    co_await headerAccessor->WriteHeader(
        &newHeader);

    auto actualLocation1Header = co_await ReadHeader(
        DefaultHeaderLocation1);
    auto actualLocation2Header = co_await ReadHeader(
        DefaultHeaderLocation2);

    EXPECT_EQ(*actualLocation1Header, location1Header);
    EXPECT_EQ(*actualLocation2Header, newHeader);
}
ASYNC_TEST_F(HeaderAccessorTests, Can_alternate_write_request_locations)
{
    DatabaseHeaderT location1Header;
    location1Header.epoch = 1;

    co_await WriteHeader(
        location1Header,
        DefaultHeaderLocation1);

    DatabaseHeaderT location2Header;
    location2Header.epoch = 2;

    co_await WriteHeader(
        location2Header,
        DefaultHeaderLocation2);

    auto headerAccessor = MakeHeaderAccessor();

    auto actualHeader = co_await headerAccessor->ReadHeader();

    DatabaseHeaderT newHeader;
    newHeader.epoch = 3;
    co_await headerAccessor->WriteHeader(
        &newHeader);

    auto actualLocation1Header = co_await ReadHeader(
        DefaultHeaderLocation1);
    auto actualLocation2Header = co_await ReadHeader(
        DefaultHeaderLocation2);

    location1Header = newHeader;
    EXPECT_EQ(*actualLocation1Header, location1Header);
    EXPECT_EQ(*actualLocation2Header, location2Header);

    newHeader.epoch = 4;
    co_await headerAccessor->WriteHeader(
        &newHeader);

    actualLocation1Header = co_await ReadHeader(
        DefaultHeaderLocation1);
    actualLocation2Header = co_await ReadHeader(
        DefaultHeaderLocation2);

    location2Header = newHeader;
    EXPECT_EQ(*actualLocation1Header, location1Header);
    EXPECT_EQ(*actualLocation2Header, location2Header);

    newHeader.epoch = 5;
    co_await headerAccessor->WriteHeader(
        &newHeader);

    actualLocation1Header = co_await ReadHeader(
        DefaultHeaderLocation1);
    actualLocation2Header = co_await ReadHeader(
        DefaultHeaderLocation2);

    location1Header = newHeader;
    EXPECT_EQ(*actualLocation1Header, location1Header);
    EXPECT_EQ(*actualLocation2Header, location2Header);
}

}