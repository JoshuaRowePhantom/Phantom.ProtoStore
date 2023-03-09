#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/KeyComparer.h"
#include "Phantom.ProtoStore/src/PartitionImpl.h"
#include "Phantom.ProtoStore/src/PartitionWriterImpl.h"
#include "Phantom.ProtoStore/src/MemoryExtentStore.h"
#include "Phantom.ProtoStore/src/MessageStoreImpl.h"
#include "ProtoStoreInternal.pb.h"
#include "ProtoStoreTest.pb.h"
#include "Phantom.ProtoStore/src/Schema.h"
#include <tuple>

namespace Phantom::ProtoStore
{

using namespace Serialization;

class PartitionTests : 
    public testing::Test,
    public SerializationTypes
{
protected:
    PartitionTests()
    {
        keyComparer = make_shared<ProtoKeyComparer>(
            PartitionTestKey::descriptor());

        SchemaDescription keySchemaDescription;

        Schema::MakeSchemaDescription(
            keySchemaDescription,
            PartitionTestKey::descriptor());

        SchemaDescription valueSchemaDescription;

        Schema::MakeSchemaDescription(
            valueSchemaDescription,
            PartitionTestValue::descriptor());

        extentStore = make_shared<MemoryExtentStore>(
            Schedulers::Inline());

        messageStore = make_shared<MessageStore>(
            Schedulers::Inline(),
            extentStore);
    }

    std::unique_ptr<PartitionDataValueT> ToSerializedValue(
        const Message& message
    )
    {
        auto serializedValue = message.SerializeAsString();

        auto dataValue = std::make_unique<PartitionDataValueT>();
        dataValue->alignment = 1;
        dataValue->data.resize(serializedValue.size());
        std::copy_n(
            serializedValue.data(),
            serializedValue.size(),
            reinterpret_cast<char*>(dataValue->data.data())
        );

        return std::move(dataValue);
    }

    std::unique_ptr<PartitionDataValueT> ToSerializedKey(
        google::protobuf::int32 key)
    {
        PartitionTestKey protoKey;
        protoKey.set_key(key);
        
        return ToSerializedValue(
            protoKey);
    }

    std::unique_ptr<PartitionDataValueT> ToSerializedValue(
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
        ExtentName headerExtentName,
        ExtentName dataExtentName)
    {
        auto headerReader = co_await messageStore->OpenExtentForRandomReadAccess(
            headerExtentName);

        auto dataReader = co_await messageStore->OpenExtentForRandomReadAccess(
            dataExtentName);

        auto partition = make_shared<Partition>(
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
        ProtoValue keyProto{ &keyMessage, true };

        auto enumeration = partition->Read(
            readSequenceNumber,
            keyProto.as_bytes_if(),
            ReadValueDisposition::ReadValue
        );

        auto iterator = co_await enumeration.begin();
        if (get<1>(expectedResult))
        {
            EXPECT_NE(enumeration.end(), iterator);

            PartitionTestKey resultKeyMessage;
            ProtoValue{ iterator->Key }.unpack(&resultKeyMessage);
            PartitionTestValue resultValueMessage;
            ProtoValue{ iterator->Value }.unpack(&resultValueMessage);

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
        ProtoValue keyProto{ &keyMessage, true };

        auto actualResult = co_await partition->CheckForWriteConflict(
            readSequenceNumber,
            writeSequenceNumber,
            keyProto.as_bytes_if()
        );

        EXPECT_EQ(actualResult, expectedResult);
    }

    shared_ptr<KeyComparer> keyComparer;
    shared_ptr<MemoryExtentStore> extentStore;
    shared_ptr<MessageStore> messageStore;
};

TEST_F(PartitionTests, Read_can_skip_from_bloom_filter)
{
    run_async([&]()->task<>
    {
        // This test writes a valid tree structure that _should_ find the message,
        // but a bloom filter that never hits.  Read() should therefore not find the message.
        auto headerExtentName = MakeLogExtentName(0);
        auto dataExtentName = MakeLogExtentName(1);

        auto headerWriter = co_await messageStore->OpenExtentForSequentialWriteAccess(headerExtentName);
        auto dataWriter = co_await messageStore->OpenExtentForSequentialWriteAccess(dataExtentName);

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
    });
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

}