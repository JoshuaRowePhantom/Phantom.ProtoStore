#include "StandardIncludes.h"
#include "TestFactories.h"

namespace Phantom::ProtoStore
{

class ProtoStoreIndexTests :
    public ::testing::Test,
    public TestFactories
{
public:

    task<> AsyncSetUp()
    {
        store = co_await CreateStore(createStoreRequest);
    }

    task<> AsyncTearDown()
    {
        co_await store->Join();
    }

    task<> ReopenStore()
    {
        co_await store->Join();
        store = co_await OpenStore(createStoreRequest);
    }

    CreateProtoStoreRequest createStoreRequest = GetCreateTestStoreRequest(
        "ProtoStoreIndexTests");
    std::shared_ptr<IProtoStore> store;
};

ASYNC_TEST_F(ProtoStoreIndexTests, Can_create_two_indexes_before_reopen)
{
    CreateIndexRequest createIndex1;
    createIndex1.IndexName = "index1";
    createIndex1.Schema = Schema::Make(
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::Test_FlatStringKey_Object },
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::Test_FlatStringValue_Object }
    );

    auto index1 = throw_if_failed(co_await store->CreateIndex(createIndex1));

    CreateIndexRequest createIndex2;
    createIndex2.IndexName = "index2";
    createIndex2.Schema = Schema::Make(
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::Test_FlatStringKey_Object },
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::Test_FlatStringValue_Object }
    );

    auto index2 = throw_if_failed(co_await store->CreateIndex(createIndex2));

    auto key = JsonToProtoValue(
        FlatBuffersTestSchemas::TestSchema,
        FlatBuffersTestSchemas::Test_FlatStringKey_Object,
        R"({"value": "key"})"
    );

    auto value1 = JsonToProtoValue(
        FlatBuffersTestSchemas::TestSchema,
        FlatBuffersTestSchemas::Test_FlatStringValue_Object,
        R"({"value": "value1"})"
    );

    auto value2 = JsonToProtoValue(
        FlatBuffersTestSchemas::TestSchema,
        FlatBuffersTestSchemas::Test_FlatStringValue_Object,
        R"({"value": "value2"})"
    );

    throw_if_failed(co_await store->ExecuteTransaction(
        {},
        [&](ITransaction* transaction) -> status_task<>
    {
        co_await co_await transaction->AddRow(
            {},
            index1,
            key,
            value1
        );

        co_await co_await transaction->AddRow(
            {},
            index2,
            key,
            value2
        );

        co_return{};
    }));

    auto actualValue1 = co_await store->Read(
        {
            .Index = index1,
            .Key = key,
        });

    EXPECT_EQ("value1", actualValue1->Value.cast_if<FlatBuffers::FlatStringValue>()->value()->str());

    auto actualValue2 = co_await store->Read(
        {
            .Index = index2,
            .Key = key,
        });

    EXPECT_EQ("value2", actualValue2->Value.cast_if<FlatBuffers::FlatStringValue>()->value()->str());
}

ASYNC_TEST_F(ProtoStoreIndexTests, Can_create_two_indexes_after_two_reopens)
{
    co_await ReopenStore();

    CreateIndexRequest createIndex1;
    createIndex1.IndexName = "index1";
    createIndex1.Schema = Schema::Make(
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::Test_FlatStringKey_Object },
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::Test_FlatStringValue_Object }
    );

    throw_if_failed(co_await store->CreateIndex(createIndex1));
    
    co_await ReopenStore();

    CreateIndexRequest createIndex2;
    createIndex2.IndexName = "index2";
    createIndex2.Schema = Schema::Make(
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::Test_FlatStringKey_Object },
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::Test_FlatStringValue_Object }
    );

    auto index2 = throw_if_failed(co_await store->CreateIndex(createIndex2));
    auto index1 = throw_if_failed(co_await store->GetIndex(createIndex1));

    auto key = JsonToProtoValue(
        FlatBuffersTestSchemas::TestSchema,
        FlatBuffersTestSchemas::Test_FlatStringKey_Object,
        R"({"value": "key"})"
    );
    
    auto value1 = JsonToProtoValue(
        FlatBuffersTestSchemas::TestSchema,
        FlatBuffersTestSchemas::Test_FlatStringValue_Object,
        R"({"value": "value1"})"
    );
    
    auto value2 = JsonToProtoValue(
        FlatBuffersTestSchemas::TestSchema,
        FlatBuffersTestSchemas::Test_FlatStringValue_Object,
        R"({"value": "value2"})"
    );

    throw_if_failed(co_await store->ExecuteTransaction(
        {},
        [&](ITransaction * transaction) -> status_task<>
    {
        co_await co_await transaction->AddRow(
            {},
            index1,
            key,
            value1
        );

        co_await co_await transaction->AddRow(
            {},
            index2,
            key,
            value2
        );

        co_return{};
    }));

    auto actualValue1 = co_await store->Read(
        {
            .Index = index1,
            .Key = key,
        });
    
    EXPECT_EQ("value1", actualValue1->Value.cast_if<FlatBuffers::FlatStringValue>()->value()->str());

    auto actualValue2 = co_await store->Read(
        {
            .Index = index2,
            .Key = key,
        });

    EXPECT_EQ("value2", actualValue2->Value.cast_if<FlatBuffers::FlatStringValue>()->value()->str());
}

ASYNC_TEST_F(ProtoStoreIndexTests, Can_read_metadata_written_at_Create_time)
{
    FlatBuffers::MetadataT metadata;
    FlatBuffers::MetadataItemT metadataItem;
    metadataItem.key = "hello world";
    metadataItem.value = { 1, 2, 3 };
    metadata.items.push_back(std::make_unique< FlatBuffers::MetadataItemT>(metadataItem));

    CreateIndexRequest createIndex1;
    createIndex1.IndexName = "index1";
    createIndex1.Schema = Schema::Make(
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::Test_FlatStringKey_Object },
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::Test_FlatStringValue_Object }
    );
    createIndex1.Metadata = { &metadata };

    auto index1 = throw_if_failed(co_await store->CreateIndex(createIndex1));
    FlatBuffers::MetadataT actualMetadata;
    index1.Metadata()->UnPackTo(&actualMetadata);

    EXPECT_EQ(metadata, actualMetadata);

    co_await ReopenStore();

    index1 = throw_if_failed(co_await store->GetIndex(createIndex1));
    index1.Metadata()->UnPackTo(&actualMetadata);

    EXPECT_EQ(metadata, actualMetadata);
}
}