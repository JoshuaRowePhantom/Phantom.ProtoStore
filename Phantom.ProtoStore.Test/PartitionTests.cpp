#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/KeyComparer.h"
#include "Phantom.ProtoStore/src/PartitionImpl.h"
#include "Phantom.ProtoStore/src/PartitionWriterImpl.h"
#include "Phantom.ProtoStore/src/MemoryExtentStore.h"
#include "Phantom.ProtoStore/src/MessageStoreImpl.h"
#include "Phantom.ProtoStore/src/RandomMessageAccessorImpl.h"
#include "ProtoStoreTest.pb.h"
#include "Phantom.ProtoStore/src/Schema.h"
#include <tuple>

namespace Phantom::ProtoStore
{

class PartitionTests : 
    public testing::Test
{
protected:
    PartitionTests()
    {
        keyComparer = make_shared<KeyComparer>(
            StringKey::descriptor());

        MessageDescription keyMessageDescription;

        Schema::MakeMessageDescription(
            keyMessageDescription,
            StringKey::descriptor());

        MessageDescription valueMessageDescription;

        Schema::MakeMessageDescription(
            valueMessageDescription,
            StringValue::descriptor());

        keyFactory = Schema::MakeMessageFactory(
            keyMessageDescription);
        valueFactory = Schema::MakeMessageFactory(
            valueMessageDescription);

        dataMemoryExtentStore = make_shared<MemoryExtentStore>(
            Schedulers::Default());
        dataHeaderMemoryExtentStore = make_shared<MemoryExtentStore>(
            Schedulers::Default());

        dataMessageStore = make_shared<MessageStore>(
            Schedulers::Default(),
            dataMemoryExtentStore);
        dataHeaderMessageStore = make_shared<MessageStore>(
            Schedulers::Default(),
            dataHeaderMemoryExtentStore);

        dataMessageAccessor = make_shared<RandomMessageAccessor>(
            dataMessageStore);
        dataHeaderMessageAccessor = make_shared<RandomMessageAccessor>(
            dataHeaderMessageStore);
    }

    string ToSerializedStringKey(
        string value)
    {
        StringKey protoKey;
        protoKey.set_value(value);
        return protoKey.SerializeAsString();
    }

    string ToSerializedStringValue(
        string value)
    {
        StringValue protoValue;
        protoValue.set_value(value);
        return protoValue.SerializeAsString();
    }

    task<WriteMessageResult> WriteAlwaysHitBloomFilter(
        const shared_ptr<ISequentialMessageWriter>& sequentialMessageWriter
    )
    {
        PartitionMessage message;
        message.mutable_partitionbloomfilter()->set_filter("\xff");
        message.mutable_partitionbloomfilter()->set_algorithm(PartitionBloomFilterHashAlgorithm::Version1);
        message.mutable_partitionbloomfilter()->set_hashfunctioncount(1);

        co_return co_await sequentialMessageWriter->Write(
            message,
            FlushBehavior::Flush);
    }

    task<> WriteBloomFilterAndRootAndHeader(
        WriteMessageResult rootTreeNodeWriteResult,
        size_t rowCount,
        SequenceNumber earliestSequenceNumber,
        SequenceNumber latestSequenceNumber,
        const shared_ptr<ISequentialMessageWriter>& dataSequentialMessageWriter,
        const shared_ptr<ISequentialMessageWriter>& headerSequentialMessageWriter
    )
    {
        auto bloomFilterWriteResult = co_await WriteAlwaysHitBloomFilter(
            dataSequentialMessageWriter);

        PartitionMessage partitionRootMessage;
        auto partitionRoot = partitionRootMessage.mutable_partitionroot();
        partitionRoot->set_bloomfilteroffset(bloomFilterWriteResult.DataRange.Beginning);
        partitionRoot->set_roottreenodeoffset(rootTreeNodeWriteResult.DataRange.Beginning);
        partitionRoot->set_rowcount(0);
        partitionRoot->set_earliestsequencenumber(
            ToUint64(earliestSequenceNumber));
        partitionRoot->set_latestsequencenumber(
            ToUint64(latestSequenceNumber));

        auto partitionRootWriteResult = co_await dataSequentialMessageWriter->Write(
            partitionRootMessage,
            FlushBehavior::Flush);

        PartitionMessage partitionHeaderMessage;
        auto partitionHeader = partitionHeaderMessage.mutable_partitionheader();
        partitionHeader->set_partitionrootoffset(partitionRootWriteResult.DataRange.Beginning);

        co_await dataSequentialMessageWriter->Write(
            partitionHeaderMessage,
            FlushBehavior::Flush);

        co_await headerSequentialMessageWriter->Write(
            partitionHeaderMessage,
            FlushBehavior::Flush);
    }

    task<shared_ptr<Partition>> MakePartition(
        ExtentNumber extentNumber)
    {
        auto partition = make_shared<Partition>(
            keyComparer,
            keyFactory,
            valueFactory,
            dataHeaderMessageAccessor,
            dataMessageAccessor,
            ExtentLocation{ extentNumber, 0 },
            ExtentLocation{ extentNumber, 0 }
        );

        co_await partition->Open();

        co_return partition;
    }

    typedef optional<string> nil;
    typedef std::tuple<string, optional<string>, SequenceNumber> result_row_type;

    task<> AssertReadResult(
        const shared_ptr<Partition>& partition,
        string key,
        SequenceNumber readSequenceNumber,
        result_row_type expectedResult)
    {
        StringKey keyMessage;
        keyMessage.set_value(key);

        auto enumeration = partition->Read(
            readSequenceNumber,
            &keyMessage,
            ReadValueDisposition::ReadValue
        );

        auto iterator = co_await enumeration.begin();
        if (get<1>(expectedResult))
        {
            ASSERT_NE(enumeration.end(), iterator);

            auto keyMessage = static_cast<const StringKey*>((*iterator).Key);
            auto valueMessage = static_cast<const StringValue*>((*iterator).Value);

            ASSERT_EQ(get<0>(expectedResult), keyMessage->value());
            ASSERT_EQ(get<1>(expectedResult), valueMessage->value());
            ASSERT_EQ(get<2>(expectedResult), (*iterator).WriteSequenceNumber);
            co_await ++iterator;
        }

        ASSERT_EQ(enumeration.end(), iterator);
    }

    shared_ptr<KeyComparer> keyComparer;
    shared_ptr<MemoryExtentStore> dataMemoryExtentStore;
    shared_ptr<MessageStore> dataMessageStore;
    shared_ptr<MemoryExtentStore> dataHeaderMemoryExtentStore;
    shared_ptr<MessageStore> dataHeaderMessageStore;
    shared_ptr<IRandomMessageAccessor> dataMessageAccessor;
    shared_ptr<IRandomMessageAccessor> dataHeaderMessageAccessor;

    shared_ptr<IMessageFactory> keyFactory;
    shared_ptr<IMessageFactory> valueFactory;
};

TEST_F(PartitionTests, Read_expected_row_version_from_single_value_single_level_tree)
{
    run_async([&]()->task<>
    {
        auto dataWriter = co_await dataMessageStore->OpenExtentForSequentialWriteAccess(0);
        auto headerWriter = co_await dataHeaderMessageStore->OpenExtentForSequentialWriteAccess(0);

        PartitionMessage treeMessage;
        auto treeEntry1 = treeMessage.mutable_partitiontreenode()->add_treeentries();
        treeEntry1->set_key(
            ToSerializedStringKey("key1")
        );
        treeEntry1->mutable_value()->set_value(
            ToSerializedStringValue("value1"));
        treeEntry1->mutable_value()->set_writesequencenumber(5);
        auto treeMessageWriteResult = co_await dataWriter->Write(
            treeMessage,
            FlushBehavior::Flush);

        co_await WriteBloomFilterAndRootAndHeader(
            treeMessageWriteResult,
            1,
            ToSequenceNumber(5),
            ToSequenceNumber(5),
            dataWriter,
            headerWriter);

        auto partition = co_await MakePartition(0);

        co_await AssertReadResult(
            partition,
            "key1",
            ToSequenceNumber(6),
            { "key1", "value1", ToSequenceNumber(5) }
        );

        co_await AssertReadResult(
            partition,
            "key1",
            ToSequenceNumber(5),
            { "key1", "value1", ToSequenceNumber(5) }
        );

        co_await AssertReadResult(
            partition,
            "key1",
            ToSequenceNumber(4),
            { "key1", nil(), SequenceNumber::Earliest }
        );
    });
}

TEST_F(PartitionTests, Read_can_skip_from_bloom_filter)
{
    run_async([&]()->task<>
    {
        // This test writes a valid tree structure that _should_ find the message,
        // but a bloom filter that never hits.  Read() should therefore not find the message.

        auto dataWriter = co_await dataMessageStore->OpenExtentForSequentialWriteAccess(0);
        auto headerWriter = co_await dataHeaderMessageStore->OpenExtentForSequentialWriteAccess(0);

        PartitionMessage treeMessage;
        auto treeEntry1 = treeMessage.mutable_partitiontreenode()->add_treeentries();
        treeEntry1->set_key(
            ToSerializedStringKey("key1")
        );
        treeEntry1->mutable_value()->set_value(
            ToSerializedStringValue("value1"));
        treeEntry1->mutable_value()->set_writesequencenumber(5);
        auto treeMessageWriteResult = co_await dataWriter->Write(
            treeMessage,
            FlushBehavior::Flush);

        PartitionMessage bloomFilterMessage;
        
        using namespace std::string_literals;
        bloomFilterMessage.mutable_partitionbloomfilter()->set_filter("\x0"s);
        // Uncomment this next line to see that the test fails.
        //bloomFilterMessage.mutable_partitionbloomfilter()->set_filter("\xff");

        bloomFilterMessage.mutable_partitionbloomfilter()->set_algorithm(PartitionBloomFilterHashAlgorithm::Version1);
        bloomFilterMessage.mutable_partitionbloomfilter()->set_hashfunctioncount(1);

        auto bloomFilterWriteResult = co_await dataWriter->Write(
            bloomFilterMessage,
            FlushBehavior::Flush);

        PartitionMessage partitionRootMessage;
        auto partitionRoot = partitionRootMessage.mutable_partitionroot();
        partitionRoot->set_bloomfilteroffset(bloomFilterWriteResult.DataRange.Beginning);
        partitionRoot->set_roottreenodeoffset(treeMessageWriteResult.DataRange.Beginning);
        partitionRoot->set_rowcount(0);
        partitionRoot->set_earliestsequencenumber(
            5);
        partitionRoot->set_latestsequencenumber(
            5);

        auto partitionRootWriteResult = co_await dataWriter->Write(
            partitionRootMessage,
            FlushBehavior::Flush);

        PartitionMessage partitionHeaderMessage;
        auto partitionHeader = partitionHeaderMessage.mutable_partitionheader();
        partitionHeader->set_partitionrootoffset(partitionRootWriteResult.DataRange.Beginning);

        co_await dataWriter->Write(
            partitionHeaderMessage,
            FlushBehavior::Flush);

        co_await headerWriter->Write(
            partitionHeaderMessage,
            FlushBehavior::Flush);

        auto partition = co_await MakePartition(0);

        co_await AssertReadResult(
            partition,
            "key1",
            ToSequenceNumber(6),
            { "key1", nil(), SequenceNumber::Earliest }
        );

        co_await AssertReadResult(
            partition,
            "key1",
            ToSequenceNumber(5),
            { "key1", nil(), SequenceNumber::Earliest }
        );

        co_await AssertReadResult(
            partition,
            "key1",
            ToSequenceNumber(4),
            { "key1", nil(), SequenceNumber::Earliest }
        );
    });
}

TEST_F(PartitionTests, Read_expected_row_version_from_multiple_value_single_level_tree_with_sequence_number_hole)
{
    run_async([&]()->task<>
    {
        auto dataWriter = co_await dataMessageStore->OpenExtentForSequentialWriteAccess(0);
        auto headerWriter = co_await dataHeaderMessageStore->OpenExtentForSequentialWriteAccess(0);

        PartitionMessage treeMessage;
        auto treeEntry1 = treeMessage.mutable_partitiontreenode()->add_treeentries();
        treeEntry1->set_key(
            ToSerializedStringKey("key1"));
        
        auto value1 = treeEntry1->mutable_valueset()->add_values();
        value1->set_value(
            ToSerializedStringValue("value1"));
        value1->set_writesequencenumber(5);

        auto value2 = treeEntry1->mutable_valueset()->add_values();
        value2->set_value(
            ToSerializedStringValue("value2"));
        value2->set_writesequencenumber(3);

        auto treeMessageWriteResult = co_await dataWriter->Write(
            treeMessage,
            FlushBehavior::Flush);

        co_await WriteBloomFilterAndRootAndHeader(
            treeMessageWriteResult,
            1,
            ToSequenceNumber(5),
            ToSequenceNumber(5),
            dataWriter,
            headerWriter);

        auto partition = co_await MakePartition(0);

        co_await AssertReadResult(
            partition,
            "key1",
            ToSequenceNumber(6),
            { "key1", "value1", ToSequenceNumber(5) }
        );

        co_await AssertReadResult(
            partition,
            "key1",
            ToSequenceNumber(5),
            { "key1", "value1", ToSequenceNumber(5) }
        );

        co_await AssertReadResult(
            partition,
            "key1",
            ToSequenceNumber(4),
            { "key1", "value2", ToSequenceNumber(3) }
        );

        co_await AssertReadResult(
            partition,
            "key1",
            ToSequenceNumber(3),
            { "key1", "value2", ToSequenceNumber(3) }
        );

        co_await AssertReadResult(
            partition,
            "key1",
            ToSequenceNumber(2),
            { "key1", nil(), SequenceNumber::Earliest }
        );
    });
}

TEST_F(PartitionTests, Read_expected_row_version_from_multiple_value_single_level_tree_with_consecutive_sequence_numbers)
{
    run_async([&]()->task<>
    {
        auto dataWriter = co_await dataMessageStore->OpenExtentForSequentialWriteAccess(0);
        auto headerWriter = co_await dataHeaderMessageStore->OpenExtentForSequentialWriteAccess(0);

        PartitionMessage treeMessage;
        auto treeEntry1 = treeMessage.mutable_partitiontreenode()->add_treeentries();
        treeEntry1->set_key(
            ToSerializedStringKey("key1"));

        auto value1 = treeEntry1->mutable_valueset()->add_values();
        value1->set_value(
            ToSerializedStringValue("value1"));
        value1->set_writesequencenumber(5);

        auto value2 = treeEntry1->mutable_valueset()->add_values();
        value2->set_value(
            ToSerializedStringValue("value2"));
        value2->set_writesequencenumber(4);

        auto treeMessageWriteResult = co_await dataWriter->Write(
            treeMessage,
            FlushBehavior::Flush);

        co_await WriteBloomFilterAndRootAndHeader(
            treeMessageWriteResult,
            1,
            ToSequenceNumber(5),
            ToSequenceNumber(5),
            dataWriter,
            headerWriter);

        auto partition = co_await MakePartition(0);

        co_await AssertReadResult(
            partition,
            "key1",
            ToSequenceNumber(6),
            { "key1", "value1", ToSequenceNumber(5) }
        );

        co_await AssertReadResult(
            partition,
            "key1",
            ToSequenceNumber(5),
            { "key1", "value1", ToSequenceNumber(5) }
        );

        co_await AssertReadResult(
            partition,
            "key1",
            ToSequenceNumber(4),
            { "key1", "value2", ToSequenceNumber(4) }
        );

        co_await AssertReadResult(
            partition,
            "key1",
            ToSequenceNumber(3),
            { "key1", nil(), SequenceNumber::Earliest }
        );
    });
}

TEST_F(PartitionTests, Read_expected_row_version_from_multiple_tree_entries_single_level_tree_consecutive_sequence_numbers)
{
    run_async([&]()->task<>
    {
        auto dataWriter = co_await dataMessageStore->OpenExtentForSequentialWriteAccess(0);
        auto headerWriter = co_await dataHeaderMessageStore->OpenExtentForSequentialWriteAccess(0);

        PartitionMessage treeMessage;
        auto treeEntry1 = treeMessage.mutable_partitiontreenode()->add_treeentries();
        treeEntry1->set_key(
            ToSerializedStringKey("key1")
        );
        treeEntry1->mutable_value()->set_value(
            ToSerializedStringValue("value1"));
        treeEntry1->mutable_value()->set_writesequencenumber(5);
        
        auto treeEntry2 = treeMessage.mutable_partitiontreenode()->add_treeentries();
        treeEntry2->set_key(
            ToSerializedStringKey("key1")
        );
        treeEntry2->mutable_value()->set_value(
            ToSerializedStringValue("value2"));
        treeEntry2->mutable_value()->set_writesequencenumber(4);

        auto treeMessageWriteResult = co_await dataWriter->Write(
            treeMessage,
            FlushBehavior::Flush);

        co_await WriteBloomFilterAndRootAndHeader(
            treeMessageWriteResult,
            1,
            ToSequenceNumber(5),
            ToSequenceNumber(5),
            dataWriter,
            headerWriter);

        auto partition = co_await MakePartition(0);

        co_await AssertReadResult(
            partition,
            "key1",
            ToSequenceNumber(6),
            { "key1", "value1", ToSequenceNumber(5) }
        );

        co_await AssertReadResult(
            partition,
            "key1",
            ToSequenceNumber(5),
            { "key1", "value1", ToSequenceNumber(5) }
        );

        co_await AssertReadResult(
            partition,
            "key1",
            ToSequenceNumber(4),
            { "key1", "value2", ToSequenceNumber(4) }
        );

        co_await AssertReadResult(
            partition,
            "key1",
            ToSequenceNumber(3),
            { "key1", nil(), SequenceNumber::Earliest }
        );
    });
}

}