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
            PartitionTestKey::descriptor());

        MessageDescription keyMessageDescription;

        Schema::MakeMessageDescription(
            keyMessageDescription,
            PartitionTestKey::descriptor());

        MessageDescription valueMessageDescription;

        Schema::MakeMessageDescription(
            valueMessageDescription,
            PartitionTestValue::descriptor());

        keyFactory = Schema::MakeMessageFactory(
            keyMessageDescription);
        valueFactory = Schema::MakeMessageFactory(
            valueMessageDescription);

        dataMemoryExtentStore = make_shared<MemoryExtentStore>(
            Schedulers::Inline());
        dataHeaderMemoryExtentStore = make_shared<MemoryExtentStore>(
            Schedulers::Inline());

        dataMessageStore = make_shared<MessageStore>(
            Schedulers::Inline(),
            dataMemoryExtentStore);
        dataHeaderMessageStore = make_shared<MessageStore>(
            Schedulers::Inline(),
            dataHeaderMemoryExtentStore);

        dataMessageAccessor = make_shared<RandomMessageAccessor>(
            dataMessageStore);
        dataHeaderMessageAccessor = make_shared<RandomMessageAccessor>(
            dataHeaderMessageStore);
    }

    string ToSerializedKey(
        google::protobuf::int32 key)
    {
        PartitionTestKey protoKey;
        protoKey.set_key(key);
        return protoKey.SerializeAsString();
    }

    string ToSerializedValue(
        google::protobuf::int32 key,
        google::protobuf::uint64 sequenceNumber)
    {
        PartitionTestValue protoValue;
        protoValue.set_key(key);
        protoValue.set_sequencenumber(sequenceNumber);
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

    typedef std::tuple<shared_ptr<PartitionTestKey>, SequenceNumber, shared_ptr<PartitionTestValue>> ScenarioRow;

    struct Scenario
    {
        uint64_t ScenarioNumber;
        int32_t MinPossibleKey;
        int32_t MaxPossibleKey;
        SequenceNumber MinPossibleSequenceNumber;
        SequenceNumber MaxPossibleSequenceNumber;
        vector<ScenarioRow> AllRows;
        shared_ptr<Partition> Partition;

        optional<ScenarioRow> GetExpectedRow(
            int32_t key,
            SequenceNumber readSequenceNumber
        ) const
        {
            optional<ScenarioRow> result;

            for (auto& row : AllRows)
            {
                if (get<shared_ptr<PartitionTestKey>>(row)->key() != key)
                {
                    continue;
                }

                if (readSequenceNumber < (get<SequenceNumber>(row)))
                {
                    continue;
                }

                if (!result)
                {
                    result = row;
                    continue;
                }

                if (get<SequenceNumber>(*result) < get<SequenceNumber>(row))
                {
                    result = row;
                    continue;
                }
            }

            return result;
        }
    };

    shared_ptr<PartitionTestKey> MakeScenarioKey(
        google::protobuf::int32 key
    )
    {
        auto keyMessage = make_shared<PartitionTestKey>();
        keyMessage->set_key(
            key
        );
        return keyMessage;
    }

    shared_ptr<PartitionTestValue> MakeScenarioValue(
        google::protobuf::int32 key,
        SequenceNumber sequenceNumber
    )
    {
        auto valueMessage = make_shared<PartitionTestValue>();
        valueMessage->set_key(
            key);
        valueMessage->set_sequencenumber(
            ToUint64(sequenceNumber));
        return valueMessage;
    }

    async_generator<Scenario> GenerateAllScenarios()
    {
        const uint64_t scenarioCount =
            7
            * 7
            * 7
            * 7
            * 7
            * 7
            * 7;

        for (auto scenarioNumber = 0ULL; scenarioNumber < scenarioCount; scenarioNumber++)
        {
            co_yield co_await GenerateScenario(scenarioNumber);
        }
    }

    task<Scenario> GenerateScenario(
        uint64_t scenarioNumber
    )
    {
        Scenario scenario;
        scenario.ScenarioNumber = scenarioNumber;
        scenario.MinPossibleKey = 0;
        scenario.MaxPossibleKey = 4;
        scenario.MinPossibleSequenceNumber = SequenceNumber::Earliest;
        scenario.MaxPossibleSequenceNumber = ToSequenceNumber(4);

        auto dataWriter = co_await dataMessageStore->OpenExtentForSequentialWriteAccess(0);
        auto headerWriter = co_await dataHeaderMessageStore->OpenExtentForSequentialWriteAccess(0);
        PartitionTreeWriter partitionTreeWriter(dataWriter);

        vector<ScenarioRow> possibleRows =
        {
            ScenarioRow { MakeScenarioKey(1), ToSequenceNumber(3), MakeScenarioValue(1, ToSequenceNumber(3)), },
            ScenarioRow { MakeScenarioKey(1), ToSequenceNumber(2), MakeScenarioValue(1, ToSequenceNumber(2)), },
            ScenarioRow { MakeScenarioKey(1), ToSequenceNumber(1), MakeScenarioValue(1, ToSequenceNumber(1)), },
            ScenarioRow { MakeScenarioKey(2), ToSequenceNumber(2), MakeScenarioValue(2, ToSequenceNumber(2)), },
            ScenarioRow { MakeScenarioKey(3), ToSequenceNumber(3), MakeScenarioValue(3, ToSequenceNumber(3)), },
            ScenarioRow { MakeScenarioKey(3), ToSequenceNumber(2), MakeScenarioValue(3, ToSequenceNumber(2)), },
            ScenarioRow { MakeScenarioKey(3), ToSequenceNumber(1), MakeScenarioValue(3, ToSequenceNumber(1)), },
        };
        
        auto rowCount = 0;

        shared_ptr<PartitionTestKey> lastKeyInValueSet;

        for (auto rowIndex = 0; rowIndex < possibleRows.size(); rowIndex++)
        {
            auto rowScenarioNumber = scenarioNumber % 7;
            scenarioNumber /= 7;

            auto row = possibleRows[rowIndex];

            size_t level;
            bool startNewValueSet;

            switch (rowScenarioNumber)
            {
            case 0:
                continue;
            case 1:
            case 2:
            case 3:
                // The row is present in level 0-2 as a value row, start new value set
                level = rowScenarioNumber - 1;
                startNewValueSet = true;
                break;
            case 4:
            case 5:
            case 6:
                // The row is present in level 0-2 as a value row, share value set
                level = rowScenarioNumber - 4;
                startNewValueSet = false;
                break;
            default:
                assert(false);
                break;
            }

            scenario.AllRows.push_back(
                row);

            ++rowCount;

            auto rowKey = get<shared_ptr<PartitionTestKey>>(row);
            auto serializedRowKey = rowKey->SerializeAsString();

            if (startNewValueSet
                ||
                !partitionTreeWriter.IsCurrentKey(serializedRowKey))
            {
                partitionTreeWriter.StartTreeEntry(
                    move(serializedRowKey));
            }

            PartitionTreeEntryValue treeEntryValue;
            treeEntryValue.set_writesequencenumber(
                ToUint64(
                    get<SequenceNumber>(row))
            );
            get<shared_ptr<PartitionTestValue>>(row)->SerializeToString(
                treeEntryValue.mutable_value());
            partitionTreeWriter.AddTreeEntryValue(
                move(treeEntryValue));

            for (size_t flushLevel = 0; flushLevel < level; flushLevel++)
            {
                co_await partitionTreeWriter.Write(
                    flushLevel);
            }
        }

        auto rootTreeEntryWriteResult = co_await partitionTreeWriter.WriteRoot();

        co_await WriteBloomFilterAndRootAndHeader(
            rootTreeEntryWriteResult,
            rowCount,
            ToSequenceNumber(0),
            ToSequenceNumber(10),
            dataWriter,
            headerWriter);

        auto partition = co_await OpenPartition(0);
        auto errorList = co_await partition->CheckIntegrity(IntegrityCheckError{});
        assert(0 == errorList.size());

        scenario.Partition = partition;
        
        co_return scenario;
    }

    task<shared_ptr<Partition>> OpenPartition(
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

        auto enumeration = partition->Read(
            readSequenceNumber,
            &keyMessage,
            ReadValueDisposition::ReadValue
        );

        auto iterator = co_await enumeration.begin();
        if (get<1>(expectedResult))
        {
            EXPECT_NE(enumeration.end(), iterator);

            auto keyMessage = static_cast<const PartitionTestKey*>((*iterator).Key);
            auto valueMessage = static_cast<const PartitionTestValue*>((*iterator).Value);

            EXPECT_EQ(get<0>(expectedResult), keyMessage->key());
            EXPECT_EQ(get<1>(expectedResult), valueMessage->key());
            EXPECT_EQ(get<2>(expectedResult), (*iterator).WriteSequenceNumber);
            co_await ++iterator;
        }

        EXPECT_EQ(enumeration.end(), iterator);
    }

    task<> DoReadScenarioTest(
        Scenario scenarioArg)
    {
        Scenario& scenario(scenarioArg);

        for (auto key = scenario.MinPossibleKey;
            key <= scenario.MaxPossibleKey;
            key++)
        {
            for (auto readSequenceNumber = ToUint64(scenario.MinPossibleSequenceNumber);
                readSequenceNumber <= ToUint64(scenario.MaxPossibleSequenceNumber);
                readSequenceNumber++)
            {
                auto expectedRow = scenario.GetExpectedRow(
                    key,
                    ToSequenceNumber(readSequenceNumber));
                auto originalExpectedRow = expectedRow;

                PartitionTestKey keyMessage;
                keyMessage.set_key(key);

                auto actualRowGenerator = scenario.Partition->Read(
                    ToSequenceNumber(readSequenceNumber),
                    &keyMessage,
                    ReadValueDisposition::ReadValue);

                for (auto iterator = co_await actualRowGenerator.begin();
                    iterator != actualRowGenerator.end();
                    co_await ++iterator)
                {
                    auto actualKey = static_cast<const PartitionTestKey*>((*iterator).Key);
                    auto actualValue = static_cast<const PartitionTestValue*>((*iterator).Value);

                    EXPECT_TRUE(expectedRow.has_value());
                    EXPECT_EQ(actualKey->key(), get<shared_ptr<PartitionTestKey>>(*expectedRow)->key());
                    EXPECT_EQ(actualValue->key(), get<shared_ptr<PartitionTestValue>>(*expectedRow)->key());
                    EXPECT_EQ(actualValue->sequencenumber(), get<shared_ptr<PartitionTestValue>>(*expectedRow)->sequencenumber());
                    EXPECT_EQ((*iterator).WriteSequenceNumber, get<SequenceNumber>(*expectedRow));

                    expectedRow.reset();
                }

                EXPECT_FALSE(expectedRow.has_value());
            }
        }
    }

    task<> DoEnumerateScenarioTest(
        const Scenario& scenario,
        int32_t lowKey,
        Inclusivity lowKeyInclusivity,
        int32_t highKey,
        Inclusivity highKeyInclusivity,
        SequenceNumber readSequenceNumber
    )
    {
        vector<ScenarioRow> expectedScenarioRows;

        for (auto key = lowKeyInclusivity == Inclusivity::Inclusive ? lowKey : lowKey + 1;
            key < (highKeyInclusivity == Inclusivity::Inclusive ? highKey + 1 : highKey);
            key++)
        {
            auto expectedRow = scenario.GetExpectedRow(
                key,
                readSequenceNumber);

            if (expectedRow)
            {
                expectedScenarioRows.push_back(
                    *expectedRow);
            }
        }

        PartitionTestKey lowKeyMessage;
        lowKeyMessage.set_key(lowKey);

        PartitionTestKey highKeyMessage;
        highKeyMessage.set_key(highKey);
        
        auto enumeration = scenario.Partition->Enumerate(
            readSequenceNumber,
            { &lowKeyMessage, lowKeyInclusivity },
            { &highKeyMessage, highKeyInclusivity },
            ReadValueDisposition::ReadValue);

        auto expectedScenarioRowsIterator = expectedScenarioRows.begin();

        for (auto actualRowsIterator = co_await enumeration.begin();
            actualRowsIterator != enumeration.end();
            co_await ++actualRowsIterator)
        {
            auto& actualRow = *actualRowsIterator;

            auto actualKey = static_cast<const PartitionTestKey*>(actualRow.Key);
            auto actualValue = static_cast<const PartitionTestValue*>(actualRow.Value);

            EXPECT_TRUE(expectedScenarioRowsIterator != expectedScenarioRows.end());
            auto& expectedRow = *expectedScenarioRowsIterator;

            EXPECT_EQ(actualKey->key(), get<shared_ptr<PartitionTestKey>>(expectedRow)->key());
            EXPECT_EQ(actualValue->key(), get<shared_ptr<PartitionTestValue>>(expectedRow)->key());
            EXPECT_EQ(actualValue->sequencenumber(), get<shared_ptr<PartitionTestValue>>(expectedRow)->sequencenumber());
            EXPECT_EQ(actualRow.WriteSequenceNumber, get<SequenceNumber>(expectedRow));

            ++expectedScenarioRowsIterator;
        }

        EXPECT_EQ(expectedScenarioRowsIterator, expectedScenarioRows.end());
    }

    task<> DoEnumerateScenarioTest(
        Scenario scenario)
    {
        for (auto readSequenceNumber = ToUint64(scenario.MinPossibleSequenceNumber);
            readSequenceNumber <= ToUint64(scenario.MaxPossibleSequenceNumber);
            readSequenceNumber++)
        {
            for (auto lowKey = scenario.MinPossibleKey;
                lowKey <= scenario.MaxPossibleKey;
                lowKey++)
            {
                for (auto highKey = lowKey;
                    highKey <= scenario.MaxPossibleKey;
                    highKey++)
                {
                    co_await DoEnumerateScenarioTest(
                        scenario,
                        lowKey,
                        Inclusivity::Inclusive,
                        highKey,
                        Inclusivity::Inclusive,
                        ToSequenceNumber(readSequenceNumber)
                    );

                    co_await DoEnumerateScenarioTest(
                        scenario,
                        lowKey,
                        Inclusivity::Inclusive,
                        highKey,
                        Inclusivity::Exclusive,
                        ToSequenceNumber(readSequenceNumber)
                    );

                    co_await DoEnumerateScenarioTest(
                        scenario,
                        lowKey,
                        Inclusivity::Exclusive,
                        highKey,
                        Inclusivity::Inclusive,
                        ToSequenceNumber(readSequenceNumber)
                    );

                    co_await DoEnumerateScenarioTest(
                        scenario,
                        lowKey,
                        Inclusivity::Exclusive,
                        highKey,
                        Inclusivity::Exclusive,
                        ToSequenceNumber(readSequenceNumber)
                    );
                }
            }
        }
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

        auto actualResult = co_await partition->CheckForWriteConflict(
            readSequenceNumber,
            writeSequenceNumber,
            &keyMessage
        );

        EXPECT_EQ(actualResult, expectedResult);
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
        google::protobuf::int32 expectedKey = 1;
        treeEntry1->set_key(
            ToSerializedKey(expectedKey)
        );
        auto value = treeEntry1->mutable_valueset()->add_values();
        value->set_value(
            ToSerializedValue(expectedKey, 5));
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

        auto partition = co_await OpenPartition(0);
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

TEST_F(PartitionTests, Test_Enumerate_scenario)
{
    run_async([&]() -> task<>
        {
            co_await DoEnumerateScenarioTest(
                co_await GenerateScenario(352));
        });
}

TEST_F(PartitionTests, Test_Enumerate_scenario_352)
{
    run_async([&]() -> task<>
        {
            co_await DoEnumerateScenarioTest(
                co_await GenerateScenario(352));
        });
}

TEST_F(PartitionTests, DISABLED_Test_Enumerate_variations)
{
    run_async([&]() -> task<>
        {
            auto scenarios = GenerateAllScenarios();
            for (auto scenarioIterator = co_await scenarios.begin();
                scenarioIterator != scenarios.end();
                co_await ++scenarioIterator)
            {
                co_await DoEnumerateScenarioTest(
                    *scenarioIterator);
            }
        });
}

TEST_F(PartitionTests, Test_Read_scenario_352)
{
    run_async([&]() -> task<>
        {
            co_await DoReadScenarioTest(
                co_await GenerateScenario(352));
        });
}

TEST_F(PartitionTests, Test_Read_scenario)
{
    run_async([&]() -> task<>
    {
        co_await DoReadScenarioTest(
            co_await GenerateScenario(352));
    });
}

TEST_F(PartitionTests, DISABLED_Test_Read_variations)
{
    run_async([&]() -> task<>
    {
        auto scenarios = GenerateAllScenarios();
        for (auto scenarioIterator = co_await scenarios.begin();
            scenarioIterator != scenarios.end();
            co_await ++scenarioIterator)
        {
            co_await DoReadScenarioTest(
                *scenarioIterator);
        }
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