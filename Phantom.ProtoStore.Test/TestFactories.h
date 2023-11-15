#include "Phantom.ProtoStore/src/Index.h"
#include "Phantom.ProtoStore/src/Resources.h"
#include "Phantom.ProtoStore/src/ValueComparer.h"
#include "Phantom.System/concepts.h"
#include "Phantom.System/utility.h"
#include "Phantom.ProtoStore/ProtoStoreTest_generated.h"
#include "Resources.h"
#include <flatbuffers/minireflect.h>
#include <optional>
#include <string>
#include <vector>

namespace Phantom::ProtoStore
{

class IUnresolvedTransactionsTracker;

class TestAccessors
{
protected:
    static IUnresolvedTransactionsTracker* GetUnresolvedTransactionsTracker(
        const ProtoStore* protoStore);
};

class TestFactories
    :
public TestAccessors
{
protected:
    std::atomic<uint64_t> m_nextTestLocalTransactionId;
    std::atomic<uint64_t> m_nextTestWriteId;
    std::atomic<uint64_t> m_nextWriteSequenceNumber;

    task<std::shared_ptr<IIndexData>> MakeInMemoryIndex(
        IndexName indexName,
        const Schema& schema,
        FlatValue<FlatBuffers::Metadata> metadata = {}
    );

    task<OperationResult<>> AddRow(
        const std::shared_ptr<IIndexData>& index,
        ProtoValue key,
        ProtoValue value,
        std::optional<SequenceNumber> writeSequenceNumber = std::nullopt,
        SequenceNumber readSequenceNumber = SequenceNumber::Latest
    );

    static ProtoValue JsonToProtoValue(
        const reflection::Schema* schema,
        const reflection::Object* object,
        std::optional<string> json);

    template<
        IsFlatBufferTable Value
    > static const reflection::Schema* GetSchema()
    {
        if constexpr (is_in_types<
            Value,
            FlatBuffers::MergesKey,
            FlatBuffers::MergesValue,
            FlatBuffers::PartitionsKey,
            FlatBuffers::PartitionsValue
        >)
        {
            return FlatBuffersSchemas::ProtoStoreInternalSchema;
        }
        else if constexpr (is_in_types <
            Value, 
            FlatBuffers::TestKey,
            FlatBuffers::FlatStringKey,
            FlatBuffers::FlatStringValue
        >)
        {
            return FlatBuffersTestSchemas::TestSchema;
        }
        else
        {
            static_assert(always_false<Value>);
        }
    }

    template<
        IsFlatBufferTable Value
    > static const reflection::Object* GetObject()
    {
        if constexpr (std::same_as<Value, FlatBuffers::MergesKey>)
        {
            return FlatBuffersSchemas::MergesKey_Object;
        }
        else if constexpr (std::same_as<Value, FlatBuffers::MergesValue>)
        {
            return FlatBuffersSchemas::MergesValue_Object;
        }
        else if constexpr (std::same_as<Value, FlatBuffers::PartitionsKey>)
        {
            return FlatBuffersSchemas::PartitionsKey_Object;
        }
        else if constexpr (std::same_as<Value, FlatBuffers::PartitionsValue>)
        {
            return FlatBuffersSchemas::PartitionsValue_Object;
        }
        else if constexpr (std::same_as<Value, FlatBuffers::TestKey>)
        {
            return FlatBuffersTestSchemas::Test_TestKey_Object;
        }
        else if constexpr (std::same_as<Value, FlatBuffers::FlatStringKey>)
        {
            return FlatBuffersTestSchemas::Test_FlatStringKey_Object;
        }
        else if constexpr (std::same_as<Value, FlatBuffers::FlatStringValue>)
        {
            return FlatBuffersTestSchemas::Test_FlatStringValue_Object;
        }
        else
        {
            static_assert(always_false<Value>);
        }
    }

    template<
        IsFlatBufferTable Value
    > static const ProtoValueComparers& GetComparers()
    {

        if constexpr (std::same_as<Value, FlatBuffers::MergesKey>)
        {
            return FlatBuffersSchemas::MergesKey_Comparers;
        }
        else if constexpr (std::same_as<Value, FlatBuffers::MergesValue>)
        {
            return FlatBuffersSchemas::MergesValue_Comparers;
        }
        else if constexpr (std::same_as<Value, FlatBuffers::PartitionsKey>)
        {
            return FlatBuffersSchemas::PartitionsKey_Comparers;
        }
        else if constexpr (std::same_as<Value, FlatBuffers::PartitionsValue>)
        {
            return FlatBuffersSchemas::PartitionsValue_Comparers;
        }
        else
        {
            static_assert(always_false<Value>);
        }
    }

    using json_row_list = std::vector<std::pair<std::string, std::optional<std::string>>>;

    template<
        typename Value
    > static ProtoValue JsonToProtoValue(
        const std::string& string)
    {
        return JsonToProtoValue(
            GetSchema<Value>(),
            GetObject<Value>(),
            string);
    }

    template<
        typename Key,
        typename Value
    > static std::vector<row<FlatValue<Key>, FlatValue<Value>>> JsonToFlatRows(
        const json_row_list& rowStrings)
    {
        std::vector<row<FlatValue<Key>, FlatValue<Value>>> result;

        for (const auto& row : rowStrings)
        {
            ProtoValue key = JsonToProtoValue(
                GetSchema<Key>(),
                GetObject<Key>(),
                row.first);
            
            ProtoValue value = JsonToProtoValue(
                GetSchema<Value>(),
                GetObject<Value>(),
                row.second);

            result.push_back(
                {
                    key,
                    value,
                    SequenceNumber::Earliest,
                    SequenceNumber::Earliest,
                }
            );
        }

        return result;
    }

    template<
        typename Key,
        typename Value
    > static bool RowListsAreEqual(
        const std::vector<row<FlatValue<Key>, FlatValue<Value>>>& expected,
        const std::vector<row<FlatValue<Key>, FlatValue<Value>>>& actual
    )
    {
        if (expected.size() != actual.size())
        {
            return false;
        }

        const ProtoValueComparers& keyComparers = GetComparers<Key>();
        const ProtoValueComparers& valueComparers = GetComparers<Value>();

        for (int index = 0; index < expected.size(); ++index)
        {
            if (!keyComparers.equal_to(expected[index].Key, actual[index].Key)
                ||
                !valueComparers.equal_to(expected[index].Value, actual[index].Value))
            {
                return false;
            }
        }

        return true;
    }

    std::string JsonToJsonBytes(
        const reflection::Schema* schema,
        const reflection::Object* object,
        const std::string& json
    );

    template<
        IsFlatBufferObject Value
    > std::string ToJson(
        const Value* value
    )
    {
        flatbuffers::ToStringVisitor toStringVisitor("\n");
        flatbuffers::IterateObject(
            reinterpret_cast<const uint8_t*>(value),
            Value::MiniReflectTypeTable(),
            &toStringVisitor
        );
        return toStringVisitor.s;
    }

    template<
        typename Value
    > std::string JsonToJsonBytes(
        const std::string& json
    )
    {
        return JsonToJsonBytes(
            GetSchema<Value>(),
            GetObject<Value>(),
            json
        );
    }

    class TestPartitionBuilder
    {
        shared_ptr<IMessageStore> m_messageStore;
        ExtentNameT m_headerExtentName;
        ExtentNameT m_dataExtentName;
        shared_ptr<ISequentialMessageWriter> m_headerWriter;
        shared_ptr<ISequentialMessageWriter> m_dataWriter;

        task<FlatBuffers::MessageReference_V1> Write(
            const shared_ptr<ISequentialMessageWriter>& writer,
            const std::string& json
        );
    public:
        TestPartitionBuilder(
            shared_ptr<IMessageStore> messageStore
        );

        task<> OpenForWrite(
            IndexNumber indexNumber,
            PartitionNumber partitionNumber,
            LevelNumber levelNumber,
            std::string indexName
        );

        task<FlatBuffers::MessageReference_V1> WriteData(
            const std::string& json
        );

        task<FlatBuffers::MessageReference_V1> WriteHeader(
            const std::string& json
        );

        ExtentNameT HeaderExtentName() const;
        ExtentNameT DataExtentName() const;

        task<shared_ptr<IPartition>> OpenPartition(
            const Schema& schema);
    };

    CreateProtoStoreRequest GetCreateMemoryStoreRequest()
    {
        CreateProtoStoreRequest createRequest;

        createRequest.ExtentStore = UseMemoryExtentStore();

        return createRequest;
    }

    CreateProtoStoreRequest GetCreateFileStoreRequest(
        string testName)
    {
        CreateProtoStoreRequest createRequest;
        createRequest.ExtentStore = UseFilesystemStore(testName, "test", 4096);
        createRequest.Schedulers = Schedulers::Inline();

        return createRequest;
    }

    CreateProtoStoreRequest GetCreateTestStoreRequest(
        string testName)
    {
        return GetCreateFileStoreRequest(
            testName);
    }

    task<shared_ptr<IProtoStore>> CreateStore(
        const CreateProtoStoreRequest& createRequest)
    {
        auto storeFactory = MakeProtoStoreFactory();

        co_return co_await storeFactory->Create(
            createRequest);
    }

    task<shared_ptr<IProtoStore>> CreateMemoryStore()
    {
        co_return co_await CreateStore(
            GetCreateMemoryStoreRequest());
    }

    task<shared_ptr<IProtoStore>> OpenStore(
        const OpenProtoStoreRequest& request
    )
    {
        auto storeFactory = MakeProtoStoreFactory();

        co_return co_await storeFactory->Open(
            request);
    }

    shared_ptr<ProtoStore> ToProtoStore(
        shared_ptr<IProtoStore>
    );
};

}
