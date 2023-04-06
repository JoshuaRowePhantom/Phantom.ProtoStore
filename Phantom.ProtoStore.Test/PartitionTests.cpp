#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/MemoryExtentStore.h"
#include "Phantom.ProtoStore/src/MessageStoreImpl.h"
#include "Phantom.ProtoStore/src/PartitionImpl.h"
#include "Phantom.ProtoStore/src/PartitionWriterImpl.h"
#include "Phantom.ProtoStore/src/ProtocolBuffersValueComparer.h"
#include "Phantom.ProtoStore/src/Schema.h"
#include "Phantom.ProtoStore/src/ValueComparer.h"
#include "TestFactories.h"
#include "ProtoStoreInternal.pb.h"
#include "ProtoStoreTest.pb.h"
#include "Phantom.ProtoStore/ProtoStoreTest_generated.h"
#include <tuple>
#include <flatbuffers/minireflect.h>

namespace Phantom::ProtoStore
{

using namespace Serialization;

class PartitionTests : 
    public testing::Test,
    public SerializationTypes,
    public TestFactories
{
protected:
    PartitionTests()
    {
        schema = make_shared<Schema>
        (
            KeySchema { PartitionTestKey::descriptor() },
            ValueSchema {PartitionTestValue::descriptor() }
        );

        keyComparer = make_shared<ProtocolBuffersValueComparer>(
            PartitionTestKey::descriptor());

        extentStore = make_shared<MemoryExtentStore>(
            Schedulers::Inline());

        messageStore = make_shared<MessageStore>(
            Schedulers::Inline(),
            extentStore);
    }

    std::unique_ptr<DataValueT> ToSerializedValue(
        const Message& message
    )
    {
        auto serializedValue = message.SerializeAsString();

        auto dataValue = std::make_unique<DataValueT>();
        dataValue->flatbuffers_alignment = 1;
        dataValue->data.resize(serializedValue.size());
        std::copy_n(
            serializedValue.data(),
            serializedValue.size(),
            reinterpret_cast<char*>(dataValue->data.data())
        );

        return std::move(dataValue);
    }

    std::unique_ptr<DataValueT> ToSerializedKey(
        google::protobuf::int32 key)
    {
        PartitionTestKey protoKey;
        protoKey.set_key(key);
        
        return ToSerializedValue(
            protoKey);
    }

    std::unique_ptr<DataValueT> ToSerializedValue(
        google::protobuf::int32 key,
        google::protobuf::uint64 sequenceNumber)
    {
        PartitionTestValue protoValue;
        protoValue.set_key(key);
        protoValue.set_sequencenumber(sequenceNumber);

        return ToSerializedValue(
            protoValue);
    }

    typedef std::tuple<shared_ptr<PartitionTestKey>, SequenceNumber, shared_ptr<PartitionTestValue>> ScenarioRow;

    task<shared_ptr<Partition>> OpenPartition(
        const ExtentNameT& headerExtentName,
        const ExtentNameT& dataExtentName)
    {
        auto headerReader = co_await messageStore->OpenExtentForRandomReadAccess(
            FlatValue(headerExtentName));

        auto dataReader = co_await messageStore->OpenExtentForRandomReadAccess(
            FlatValue(dataExtentName));

        auto partition = make_shared<Partition>(
            schema,
            keyComparer,
            headerReader,
            dataReader
        );

        co_await partition->Open();
        
        co_return partition;
    }

    typedef optional<google::protobuf::int32> nil;
    typedef std::tuple<google::protobuf::int32, optional<google::protobuf::int32>, SequenceNumber> result_row_type;

    task<> AssertReadResult(
        const shared_ptr<Partition>& partition,
        google::protobuf::int32 key,
        SequenceNumber readSequenceNumber,
        result_row_type expectedResult)
    {
        PartitionTestKey keyMessage;
        keyMessage.set_key(key);
        auto keyProto = ProtoValue{ &keyMessage }.pack();

        auto enumeration = partition->Read(
            readSequenceNumber,
            keyProto,
            ReadValueDisposition::ReadValue
        );

        auto iterator = co_await enumeration.begin();
        if (get<1>(expectedResult))
        {
            EXPECT_NE(enumeration.end(), iterator);

            PartitionTestKey resultKeyMessage;
            iterator->Key.unpack(&resultKeyMessage);
            PartitionTestValue resultValueMessage;
            iterator->Value.unpack(&resultValueMessage);

            EXPECT_EQ(get<0>(expectedResult), resultKeyMessage.key());
            EXPECT_EQ(get<1>(expectedResult), resultValueMessage.key());
            EXPECT_EQ(get<2>(expectedResult), iterator->WriteSequenceNumber);
            co_await ++iterator;
        }

        EXPECT_EQ(enumeration.end(), iterator);
    }

    typedef optional<SequenceNumber> noWriteConflict;

    task<> AssertCheckForWriteConflictResult(
        const shared_ptr<Partition>& partition,
        string key,
        SequenceNumber readSequenceNumber,
        SequenceNumber writeSequenceNumber,
        optional<SequenceNumber> expectedResult)
    {
        StringKey keyMessage;
        keyMessage.set_value(key);
        auto keyProto = ProtoValue{ &keyMessage }.pack();

        auto actualResult = co_await partition->CheckForWriteConflict(
            readSequenceNumber,
            writeSequenceNumber,
            keyProto
        );

        EXPECT_EQ(actualResult, expectedResult);
    }

    shared_ptr<Schema> schema;
    shared_ptr<ValueComparer> keyComparer;
    shared_ptr<MemoryExtentStore> extentStore;
    shared_ptr<MessageStore> messageStore;
};

ASYNC_TEST_F(PartitionTests, Read_can_skip_from_bloom_filter)
{
    // This test writes a valid tree structure that _should_ find the message,
    // but a bloom filter that never hits.  Read() should therefore not find the message.
    auto headerExtentName = MakeLogExtentName(0);
    auto dataExtentName = MakeLogExtentName(1);

    auto headerWriter = co_await messageStore->OpenExtentForSequentialWriteAccess(
        FlatValue(headerExtentName));
    auto dataWriter = co_await messageStore->OpenExtentForSequentialWriteAccess(
        FlatValue(dataExtentName));

    PartitionMessageT treeMessage;
    auto& treeNode = treeMessage.tree_node = std::make_unique<PartitionTreeNodeT>();
    auto& treeEntry1 = treeNode->keys.emplace_back(std::make_unique<PartitionTreeEntryKeyT>());

    google::protobuf::int32 expectedKey = 1;
    treeEntry1->key = ToSerializedKey(expectedKey);
    auto& value = treeEntry1->values.emplace_back(std::make_unique<PartitionTreeEntryValueT>());
    value->value = ToSerializedValue(expectedKey, 5);
    value->write_sequence_number = 0;

    auto treeMessageWriteResult = co_await dataWriter->Write(
        FlatMessage{ &treeMessage }.data(),
        FlushBehavior::Flush);

    PartitionMessageT bloomFilterMessage;
    auto& bloomFilter = bloomFilterMessage.bloom_filter = std::make_unique<PartitionBloomFilterT>();
    bloomFilter->algorithm = FlatBuffers::PartitionBloomFilterHashAlgorithm::Version1;
    bloomFilter->hash_function_count = 1;
    bloomFilter->filter.push_back(
        0
        // Uncomment this next line to see that the test fails.
        //| 0xff
    );

    auto bloomFilterWriteResult = co_await dataWriter->Write(
        FlatMessage{ &bloomFilterMessage }.data(),
        FlushBehavior::Flush);

    PartitionMessageT partitionRootMessage;
    auto& partitionRoot = partitionRootMessage.root = std::make_unique<PartitionRootT>();
    partitionRoot->bloom_filter = copy_unique(*bloomFilterWriteResult->Reference_V1());
    partitionRoot->root_tree_node = copy_unique(*treeMessageWriteResult->Reference_V1());
    partitionRoot->row_count = 0;
    partitionRoot->earliest_sequence_number = 0;
    partitionRoot->latest_sequence_number = 10;

    auto partitionRootWriteResult = co_await dataWriter->Write(
        FlatMessage{ &partitionRootMessage }.data(),
        FlushBehavior::Flush);

    PartitionMessageT partitionHeaderMessage;
    auto& partitionHeader = partitionHeaderMessage.header = std::make_unique<PartitionHeaderT>();
    partitionHeader->partition_root = copy_unique(*partitionRootWriteResult->Reference_V1());

    co_await dataWriter->Write(
        FlatMessage{ &partitionHeaderMessage }.data(),
        FlushBehavior::Flush);

    co_await headerWriter->Write(
        FlatMessage{ &partitionHeaderMessage }.data(),
        FlushBehavior::Flush);

    auto partition = co_await OpenPartition(
        headerExtentName,
        dataExtentName);

    // The partition should fail an integrity check because
    // there is a key not present in the bloom filter.
    auto integrityCheckResults = co_await partition->CheckIntegrity(
        IntegrityCheckError{});
    EXPECT_EQ(1, integrityCheckResults.size());
    EXPECT_EQ(IntegrityCheckErrorCode::Partition_KeyNotInBloomFilter, integrityCheckResults[0].Code);

    co_await AssertReadResult(
        partition,
        expectedKey,
        ToSequenceNumber(6),
        { expectedKey, nil(), SequenceNumber::Earliest }
    );

    co_await AssertReadResult(
        partition,
        expectedKey,
        ToSequenceNumber(5),
        { expectedKey, nil(), SequenceNumber::Earliest }
    );

    co_await AssertReadResult(
        partition,
        expectedKey,
        ToSequenceNumber(4),
        { expectedKey, nil(), SequenceNumber::Earliest }
    );
}

//
//TEST_F(PartitionTests, CheckForWriteConflict_uses_WriteSequenceNumber_with_GreaterOrEqual)
//{
//    run_async([&]()->task<>
//    {
//        auto dataWriter = co_await dataMessageStore->OpenExtentForSequentialWriteAccess(0);
//        auto headerWriter = co_await dataHeaderMessageStore->OpenExtentForSequentialWriteAccess(0);
//
//        PartitionMessage treeMessage;
//        auto treeEntry1 = treeMessage.mutable_partitiontreenode()->add_treeentries();
//        treeEntry1->set_key(
//            ToSerializedStringKey("key1")
//        );
//        treeEntry1->mutable_value()->set_value(
//            ToSerializedStringValue("value1"));
//        treeEntry1->mutable_value()->set_writesequencenumber(5);
//        auto treeMessageWriteResult = co_await dataWriter->Write(
//            treeMessage,
//            FlushBehavior::Flush);
//
//        co_await WriteBloomFilterAndRootAndHeader(
//            treeMessageWriteResult,
//            1,
//            ToSequenceNumber(5),
//            ToSequenceNumber(5),
//            dataWriter,
//            headerWriter);
//
//        auto partition = co_await OpenPartition(0);
//        auto errorList = co_await partition->CheckIntegrity(IntegrityCheckError{});
//        EXPECT_EQ(0, errorList.size());
//
//        co_await AssertCheckForWriteConflictResult(
//            partition,
//            "key1",
//            SequenceNumber::Latest,
//            ToSequenceNumber(6),
//            noWriteConflict()
//        );
//
//        co_await AssertCheckForWriteConflictResult(
//            partition,
//            "key1",
//            SequenceNumber::Latest,
//            ToSequenceNumber(5),
//            ToSequenceNumber(5)
//        );
//
//        co_await AssertCheckForWriteConflictResult(
//            partition,
//            "key1",
//            SequenceNumber::Latest,
//            ToSequenceNumber(4),
//            ToSequenceNumber(5)
//        );
//    });
//}
//
//TEST_F(PartitionTests, CheckForWriteConflict_uses_ReadSequenceNumber_with_Greater)
//{
//    run_async([&]()->task<>
//    {
//        auto dataWriter = co_await dataMessageStore->OpenExtentForSequentialWriteAccess(0);
//        auto headerWriter = co_await dataHeaderMessageStore->OpenExtentForSequentialWriteAccess(0);
//
//        PartitionMessage treeMessage;
//        auto treeEntry1 = treeMessage.mutable_partitiontreenode()->add_treeentries();
//        treeEntry1->set_key(
//            ToSerializedStringKey("key1")
//        );
//        treeEntry1->mutable_value()->set_value(
//            ToSerializedStringValue("value1"));
//        treeEntry1->mutable_value()->set_writesequencenumber(5);
//        auto treeMessageWriteResult = co_await dataWriter->Write(
//            treeMessage,
//            FlushBehavior::Flush);
//
//        co_await WriteBloomFilterAndRootAndHeader(
//            treeMessageWriteResult,
//            1,
//            ToSequenceNumber(5),
//            ToSequenceNumber(5),
//            dataWriter,
//            headerWriter);
//
//        auto partition = co_await OpenPartition(0);
//        auto errorList = co_await partition->CheckIntegrity(IntegrityCheckError{});
//        EXPECT_EQ(0, errorList.size());
//
//        co_await AssertCheckForWriteConflictResult(
//            partition,
//            "key1",
//            ToSequenceNumber(6),
//            SequenceNumber::Latest,
//            noWriteConflict()
//        );
//
//        co_await AssertCheckForWriteConflictResult(
//            partition,
//            "key1",
//            ToSequenceNumber(5),
//            SequenceNumber::Latest,
//            noWriteConflict()
//        );
//
//        co_await AssertCheckForWriteConflictResult(
//            partition,
//            "key1",
//            ToSequenceNumber(4),
//            SequenceNumber::Latest,
//            ToSequenceNumber(5)
//        );
//    });
//}

ASYNC_TEST_F(PartitionTests, Can_point_read_single_existing_key)
{
    TestPartitionBuilder testPartitionBuilder(
        messageStore);
    co_await testPartitionBuilder.OpenForWrite(0, 0, 0, "");

    auto keyJsonBytes = JsonToJsonBytes<FlatBuffers::FlatStringKey>(
        R"({ value: "hello world" })");

    auto valueJsonBytes = JsonToJsonBytes<FlatBuffers::FlatStringValue>(
        R"({ value: "goodbye world" })");

    auto rootTreeNodeReference = co_await testPartitionBuilder.WriteData(
        "{ tree_node: { keys: [ { key: { data: " + keyJsonBytes + " }, single_value: { data: " + valueJsonBytes + " }, lowest_write_sequence_number_for_key: 5 } ] } }"
    );

    auto rootReference = co_await testPartitionBuilder.WriteData(
        "{ root: { root_tree_node: " + ToJson(&rootTreeNodeReference) + ", latest_sequence_number: 10 } }");

    co_await testPartitionBuilder.WriteHeader(
        "{ header: { partition_root: " + ToJson(&rootReference) + " } }");

    auto schema = Schema::Make(
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::Test_FlatStringKey_Object },
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::Test_FlatStringValue_Object }
    );

    auto partition = co_await testPartitionBuilder.OpenPartition(
        schema);

    auto keyProto = JsonToProtoValue<FlatBuffers::FlatStringKey>(R"({ value: "hello world" })");
    auto valueProto = JsonToProtoValue<FlatBuffers::FlatStringValue>(R"({ value: "goodbye world" })");

    auto readResult = partition->Read(
        ToSequenceNumber(10),
        keyProto,
        ReadValueDisposition::ReadValue);

    auto keyComparer = SchemaDescriptions::MakeKeyComparer(copy_shared(schema));
    auto valueComparer = SchemaDescriptions::MakeValueComparer(copy_shared(schema));

    auto iterator = co_await readResult.begin();
    EXPECT_FALSE(iterator == readResult.end());
    EXPECT_EQ(ToSequenceNumber(5), iterator->WriteSequenceNumber);
    EXPECT_TRUE(keyComparer->Equals(keyProto, iterator->Key));
    EXPECT_TRUE(valueComparer->Equals(valueProto, iterator->Value));

    co_await ++iterator;
    EXPECT_TRUE(iterator == readResult.end());
}

ASYNC_TEST_F(PartitionTests, Read_of_nonexistent_key_from_one_level_returns_nothing)
{
    TestPartitionBuilder testPartitionBuilder(
        messageStore);
    co_await testPartitionBuilder.OpenForWrite(0, 0, 0, "");

    auto key1JsonBytes = JsonToJsonBytes<FlatBuffers::FlatStringKey>(
        R"({ value: "hello world 1" })");

    auto value1JsonBytes = JsonToJsonBytes<FlatBuffers::FlatStringValue>(
        R"({ value: "goodbye world 1" })");
    
    auto key3JsonBytes = JsonToJsonBytes<FlatBuffers::FlatStringKey>(
        R"({ value: "hello world 3" })");

    auto value3JsonBytes = JsonToJsonBytes<FlatBuffers::FlatStringValue>(
        R"({ value: "goodbye world 3" })");

    auto rootTreeNodeReference = co_await testPartitionBuilder.WriteData(
        "{ tree_node: { keys: [ "
        "{ key: { data: " + key1JsonBytes + " }, single_value: { data: " + value1JsonBytes + " }, lowest_write_sequence_number_for_key: 5 }, "
        "{ key: { data: " + key3JsonBytes + " }, single_value: { data: " + value3JsonBytes + " }, lowest_write_sequence_number_for_key: 5 } "
        "] } }"
    );

    auto rootReference = co_await testPartitionBuilder.WriteData(
        "{ root: { root_tree_node: " + ToJson(&rootTreeNodeReference) + ", latest_sequence_number: 10 } }");

    co_await testPartitionBuilder.WriteHeader(
        "{ header: { partition_root: " + ToJson(&rootReference) + " } }");

    auto schema = Schema::Make(
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::Test_FlatStringKey_Object },
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::Test_FlatStringValue_Object }
    );

    auto partition = co_await testPartitionBuilder.OpenPartition(
        schema);

    auto key0Proto = JsonToProtoValue<FlatBuffers::FlatStringKey>(R"({ value: "hello world 0" })");
    auto key2Proto = JsonToProtoValue<FlatBuffers::FlatStringKey>(R"({ value: "hello world 2" })");
    auto key4Proto = JsonToProtoValue<FlatBuffers::FlatStringKey>(R"({ value: "hello world 4" })");

    {
        auto readResult = partition->Read(
            ToSequenceNumber(10),
            key0Proto,
            ReadValueDisposition::ReadValue);

        auto iterator = co_await readResult.begin();
        EXPECT_TRUE(iterator == readResult.end());
    }

    {
        auto readResult = partition->Read(
            ToSequenceNumber(10),
            key2Proto,
            ReadValueDisposition::ReadValue);

        auto iterator = co_await readResult.begin();
        EXPECT_TRUE(iterator == readResult.end());
    }

    {
        auto readResult = partition->Read(
            ToSequenceNumber(10),
            key4Proto,
            ReadValueDisposition::ReadValue);

        auto iterator = co_await readResult.begin();
        EXPECT_TRUE(iterator == readResult.end());
    }
}

ASYNC_TEST_F(PartitionTests, Can_EnumeratePrefix_from_one_tree_node)
{
    TestPartitionBuilder testPartitionBuilder(
        messageStore);
    co_await testPartitionBuilder.OpenForWrite(0, 0, 0, "");

    auto key_1_JsonBytes = JsonToJsonBytes<FlatBuffers::TestKey>(
        R"({ short_value: 1 })");

    auto value_1_JsonBytes = JsonToJsonBytes<FlatBuffers::FlatStringValue>(
        R"({ value: "1" })");
    
    auto key_2_1_JsonBytes = JsonToJsonBytes<FlatBuffers::TestKey>(
        R"({ short_value: 2, ushort_value: 1 })");

    auto value_2_1_JsonBytes = JsonToJsonBytes<FlatBuffers::FlatStringValue>(
        R"({ value: "2_1" })");
    
    auto key_2_2_JsonBytes = JsonToJsonBytes<FlatBuffers::TestKey>(
        R"({ short_value: 2, ushort_value: 2 })");

    auto value_2_2_JsonBytes = JsonToJsonBytes<FlatBuffers::FlatStringValue>(
        R"({ value: "2_2" })");

    auto key_3_JsonBytes = JsonToJsonBytes<FlatBuffers::TestKey>(
        R"({ short_value: 3 })");

    auto value_3_JsonBytes = JsonToJsonBytes<FlatBuffers::FlatStringValue>(
        R"({ value: "3" })");

    auto rootTreeNodeReference = co_await testPartitionBuilder.WriteData(
        "{ tree_node: { keys: [ "
        "{ key: { data: " + key_1_JsonBytes + " }, single_value : { data: " + value_1_JsonBytes + " }, lowest_write_sequence_number_for_key : 5 }, "
        "{ key: { data: " + key_2_1_JsonBytes + " }, single_value : { data: " + value_2_1_JsonBytes + " }, lowest_write_sequence_number_for_key : 5 }, "
        "{ key: { data: " + key_2_2_JsonBytes + " }, single_value : { data: " + value_2_2_JsonBytes + " }, lowest_write_sequence_number_for_key : 5 }, "
        "{ key: { data: " + key_3_JsonBytes + " }, single_value : { data: " + value_3_JsonBytes + " }, lowest_write_sequence_number_for_key : 5 }"
        "] } }"
    );

    auto rootReference = co_await testPartitionBuilder.WriteData(
        "{ root: { root_tree_node: " + ToJson(&rootTreeNodeReference) + ", latest_sequence_number: 10 } }");

    co_await testPartitionBuilder.WriteHeader(
        "{ header: { partition_root: " + ToJson(&rootReference) + " } }");

    auto schema = Schema::Make(
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::Test_TestKey_Object },
        { FlatBuffersTestSchemas::TestSchema, FlatBuffersTestSchemas::Test_FlatStringValue_Object }
    );

    auto partition = co_await testPartitionBuilder.OpenPartition(
        schema);

    auto key_2_1_protoValue = JsonToProtoValue<FlatBuffers::TestKey>(
        R"({ short_value: 2, ushort_value: 1 })");
    
    auto value_2_1_protoValue = JsonToProtoValue<FlatBuffers::FlatStringValue>(
        R"({ value: "2_1" })");

    auto key_2_2_protoValue = JsonToProtoValue<FlatBuffers::TestKey>(
        R"({ short_value: 2, ushort_value: 2 })");

    auto value_2_2_protoValue = JsonToProtoValue<FlatBuffers::FlatStringValue>(
        R"({ value: "2_2" })");

    auto readResult = partition->EnumeratePrefix(
        ToSequenceNumber(10),
        Prefix { key_2_2_protoValue, 4 },
        ReadValueDisposition::ReadValue);

    auto keyComparer = SchemaDescriptions::MakeKeyComparer(copy_shared(schema));
    auto valueComparer = SchemaDescriptions::MakeValueComparer(copy_shared(schema));

    auto iterator = co_await readResult.begin();
    EXPECT_FALSE(iterator == readResult.end());
    EXPECT_EQ(ToSequenceNumber(5), iterator->WriteSequenceNumber);
    EXPECT_TRUE(keyComparer->Equals(key_2_1_protoValue, iterator->Key));
    EXPECT_TRUE(valueComparer->Equals(value_2_1_protoValue, iterator->Value));

    co_await ++iterator;
    EXPECT_FALSE(iterator == readResult.end());
    EXPECT_EQ(ToSequenceNumber(5), iterator->WriteSequenceNumber);
    EXPECT_TRUE(keyComparer->Equals(key_2_2_protoValue, iterator->Key));
    EXPECT_TRUE(valueComparer->Equals(value_2_2_protoValue, iterator->Value));

    co_await ++iterator;
    EXPECT_TRUE(iterator == readResult.end());
}

}