#include "StandardIncludes.h"

#include <google/protobuf/empty.pb.h>
#include "Phantom.ProtoStore/ProtoStoreTest_generated.h"
#include "ProtoStoreTest.pb.h"
#include "Phantom.System/lifetime_tracker.h"

namespace Phantom::ProtoStore
{

class ProtoValueTests :
    public testing::Test
{
protected:
    flatbuffers::FlatBufferBuilder nonNullFlatMessageBuilder;
    std::span<const std::byte> nonNullFlatMessageSpan;
    const FlatBuffers::ScalarTable* nonNullFlatTable;
    const flatbuffers::Table* nonNullFlatTableRaw;
    std::string nonNullFlatTableString;

    struct string_holder
    {
        lifetime_tracker tracker;
        std::string value;
    };

    lifetime_statistics nonNullFlatTableAlignedMessageLifetimeStatistics;
    std::shared_ptr<string_holder> nonNullFlatTableAlignedMessageString;
    AlignedMessageData nonNullFlatTableAlignedMessageData;
    const FlatBuffers::ScalarTable* nonNullAlignedMessageDataFlatTable;
    const flatbuffers::Table* nonNullAlignedMessageDataFlatTableRaw;

    StringKey nonNullProtocolMessage;
    std::string nonNullProtocolMessageString;
    std::span<const std::byte> nonNullProtocolMessageSpan;
    lifetime_statistics nonNullProtocolAlignedMessageLifetimeStatistics;
    std::shared_ptr<string_holder> nonNullProtocolAlignedMessageString;
    AlignedMessageData nonNullProtocolAlignedMessageData;

    std::shared_ptr<StringKey> nonNullProtocolMessageSharedPointer;

    ProtoValueTests()
    {
        auto rootOffset = FlatBuffers::CreateScalarTable(
            nonNullFlatMessageBuilder,
            5);
        nonNullFlatMessageBuilder.Finish(rootOffset);

        nonNullFlatMessageSpan = as_bytes(nonNullFlatMessageBuilder.GetBufferSpan());
        nonNullFlatTable = flatbuffers::GetRoot<FlatBuffers::ScalarTable>(
            nonNullFlatMessageSpan.data());
        nonNullFlatTableRaw = reinterpret_cast<const flatbuffers::Table*>(
            nonNullFlatTable);
        nonNullFlatTableString = std::string(
            reinterpret_cast<const char*>(nonNullFlatMessageSpan.data()),
            nonNullFlatMessageSpan.size()
        );

        nonNullFlatTableAlignedMessageString = std::make_shared<string_holder>(
            nonNullFlatTableAlignedMessageLifetimeStatistics.tracker(),
            nonNullFlatTableString);
        nonNullFlatTableAlignedMessageData = AlignedMessageData(
            DataReference<char>(nonNullFlatTableAlignedMessageString, 1),
            AlignedMessage(
                16,
                get_byte_span(nonNullFlatTableAlignedMessageString->value)));
        nonNullAlignedMessageDataFlatTable = flatbuffers::GetRoot<FlatBuffers::ScalarTable>(
            nonNullFlatTableAlignedMessageData->Payload.data());
        nonNullAlignedMessageDataFlatTableRaw = flatbuffers::GetRoot<flatbuffers::Table>(
            nonNullFlatTableAlignedMessageData->Payload.data());

        nonNullProtocolMessage.set_value("hello world");
        nonNullProtocolMessage.SerializeToString(
            &nonNullProtocolMessageString);
        nonNullProtocolMessageSpan = get_byte_span(
            nonNullProtocolMessageString);

        nonNullProtocolAlignedMessageString = std::make_shared<string_holder>(
            nonNullProtocolAlignedMessageLifetimeStatistics.tracker(),
            nonNullProtocolMessageString);
        nonNullProtocolAlignedMessageData = AlignedMessageData(
            DataReference<char>(nonNullProtocolAlignedMessageString, 1),
            AlignedMessage(
                1,
                get_byte_span(nonNullProtocolAlignedMessageString->value)));

        nonNullProtocolMessageSharedPointer = std::make_shared<StringKey>(
            nonNullProtocolMessage);
    }
};

void ExpectEmptyData(
    const ProtoValue& protoValue)
{
    EXPECT_EQ(nullptr, protoValue.as_flat_buffer_bytes_if().data());
    EXPECT_EQ(nullptr, protoValue.as_protocol_buffer_bytes_if().data());
    EXPECT_EQ(nullptr, protoValue.as_aligned_message_if().Payload.data());
}

void ExpectIsNotProtocolBuffer(
    const ProtoValue& protoValue
)
{
    EXPECT_FALSE(protoValue.is_protocol_buffer());
    EXPECT_EQ(nullptr, protoValue.as_message_if());
    EXPECT_EQ(nullptr, protoValue.cast_if<google::protobuf::Message>());
    EXPECT_EQ(nullptr, protoValue.as_protocol_buffer_bytes_if().data());
    EXPECT_EQ(0, protoValue.as_protocol_buffer_bytes_if().size());
}

void ExpectIsNotTable(
    const ProtoValue& protoValue
)
{
    EXPECT_FALSE(protoValue.is_flat_buffer());
    EXPECT_EQ(nullptr, protoValue.as_table_if());
    EXPECT_EQ(nullptr, protoValue.cast_if<flatbuffers::Table>());
    EXPECT_EQ(nullptr, protoValue.as_flat_buffer_bytes_if().data());
    EXPECT_EQ(0, protoValue.as_flat_buffer_bytes_if().size());
}

void ExpectEmpty(
    const ProtoValue& protoValue)
{
    ExpectIsNotTable(protoValue);
    ExpectIsNotProtocolBuffer(protoValue);
    EXPECT_FALSE(protoValue.has_value());
}

void ExpectIsTable(
    const ProtoValue& protoValue,
    const flatbuffers::Table* table
)
{
    ExpectIsNotProtocolBuffer(protoValue);
    EXPECT_NE(nullptr, table);
    EXPECT_TRUE(protoValue.is_flat_buffer());
    EXPECT_EQ(table, protoValue.as_table_if());
    EXPECT_EQ(table, protoValue.cast_if<flatbuffers::Table>());
    EXPECT_EQ(5, protoValue.cast_if<FlatBuffers::ScalarTable>()->item());
}

void ExpectIsProtocolBuffer(
    const ProtoValue& protoValue,
    const StringKey* message
)
{
    ExpectIsNotTable(protoValue);
    EXPECT_TRUE(protoValue.is_protocol_buffer());
    EXPECT_EQ(message, protoValue.cast_if<StringKey>());
    ProtoValue unpacked = ProtoValue(protoValue).unpack<StringKey>();
    EXPECT_EQ("hello world", unpacked.cast_if<StringKey>()->value());
}

TEST_F(ProtoValueTests, constructor)
{
    ProtoValue protoValue;
    ExpectEmpty(protoValue);
}

TEST_F(ProtoValueTests, constructor_flatmessage_native_table)
{
    FlatBuffers::ScalarTableT table;
    table.item = 5;
    ProtoValue protoValue(&table);
    ExpectIsTable(
        protoValue,
        protoValue.as_table_if());
}

TEST_F(ProtoValueTests, constructor_flatmessage_native_table_null)
{
    FlatBuffers::ScalarTableT* table = nullptr;
    ProtoValue protoValue(table);
    ExpectEmpty(protoValue);
}

TEST_F(ProtoValueTests, constructor_flatmessage_monostate)
{
    ProtoValue protoValue(ProtoValue::flat_buffer_message{});
    ExpectEmpty(protoValue);
}

TEST_F(ProtoValueTests, constructor_flatmessage_nullptr)
{
    ProtoValue protoValue(ProtoValue::flat_buffer_message{ nullptr });
    ExpectEmpty(protoValue);
}

TEST_F(ProtoValueTests, constructor_flatmessage_pointer)
{
    ProtoValue protoValue(ProtoValue::flat_buffer_message{ nonNullFlatTableRaw });
    ExpectIsTable(protoValue, nonNullFlatTableRaw);
}

TEST_F(ProtoValueTests, constructor_backing_store_monostate_flatmessage_monostate)
{
    ProtoValue protoValue(ProtoValue::backing_store{}, ProtoValue::flat_buffer_message{});
    ExpectEmpty(protoValue);
}

TEST_F(ProtoValueTests, constructor_backing_store_span_flatmessage_monostate)
{
    ProtoValue protoValue(nonNullFlatMessageSpan, ProtoValue::flat_buffer_message{});
    EXPECT_EQ(nonNullFlatMessageSpan.data(), protoValue.as_flat_buffer_bytes_if().data());
    ExpectIsTable(protoValue, nonNullFlatTableRaw);
}

TEST_F(ProtoValueTests, constructor_backing_store_string_flatmessage_monostate)
{
    ProtoValue protoValue(nonNullFlatTableString, ProtoValue::flat_buffer_message{});
    ExpectIsTable(
        protoValue,
        // 12 bytes for root, file type, root vtable.
        reinterpret_cast<const flatbuffers::Table*>(protoValue.as_flat_buffer_bytes_if().data() + 12));
}

TEST_F(ProtoValueTests, constructor_backing_store_aligned_message_data_flatmessage_monostate)
{
    ProtoValue protoValue(
        std::move(nonNullFlatTableAlignedMessageData),
        ProtoValue::flat_buffer_message{});

    EXPECT_EQ(16, protoValue.as_aligned_message_if().Alignment);
    EXPECT_EQ(
        reinterpret_cast<const std::byte*>(nonNullFlatTableAlignedMessageString->value.data()),
        protoValue.as_aligned_message_if().Payload.data());

    ExpectIsTable(
        protoValue,
        nonNullAlignedMessageDataFlatTableRaw);

    nonNullFlatTableAlignedMessageString = nullptr;
    EXPECT_EQ(1, nonNullFlatTableAlignedMessageLifetimeStatistics.instance_count);
    protoValue = {};
    EXPECT_EQ(0, nonNullFlatTableAlignedMessageLifetimeStatistics.instance_count);
}

TEST_F(ProtoValueTests, constructor_backing_store_monostate_flatmessage_pointer)
{
    ProtoValue protoValue(ProtoValue::backing_store{}, ProtoValue::flat_buffer_message{ nonNullFlatTableRaw });
    ExpectIsTable(protoValue, nonNullFlatTableRaw);
}

TEST_F(ProtoValueTests, constructor_backing_store_span_flatmessage_pointer)
{
    ProtoValue protoValue(nonNullFlatMessageSpan, ProtoValue::flat_buffer_message{ nonNullFlatTableRaw });
    EXPECT_EQ(nonNullFlatMessageSpan.data(), protoValue.as_flat_buffer_bytes_if().data());
    ExpectIsTable(protoValue, nonNullFlatTableRaw);
}

TEST_F(ProtoValueTests, constructor_backing_store_string_flatmessage_pointer)
{
    ProtoValue protoValue(nonNullFlatTableString, ProtoValue::flat_buffer_message{ nonNullFlatTableRaw });
    ExpectIsTable(
        protoValue,
        nonNullFlatTableRaw
    );
}

TEST_F(ProtoValueTests, constructor_backing_store_aligned_message_data_flatmessage_pointer)
{
    ProtoValue protoValue(
        std::move(nonNullFlatTableAlignedMessageData),
        ProtoValue::flat_buffer_message{ nonNullFlatTableRaw });

    ExpectIsTable(
        protoValue,
        nonNullFlatTableRaw);

    nonNullFlatTableAlignedMessageString = nullptr;
    EXPECT_EQ(1, nonNullFlatTableAlignedMessageLifetimeStatistics.instance_count);
    protoValue = {};
    EXPECT_EQ(0, nonNullFlatTableAlignedMessageLifetimeStatistics.instance_count);
}


TEST_F(ProtoValueTests, constructor_protocolmessage_monostate)
{
    ProtoValue protoValue(ProtoValue::protocol_buffer_message{});
    ExpectEmpty(protoValue);
}

TEST_F(ProtoValueTests, constructor_protocolmessage_nullptr)
{
    ProtoValue protoValue(ProtoValue::protocol_buffer_message{ nullptr });
    ExpectEmpty(protoValue);
}

TEST_F(ProtoValueTests, constructor_protocolmessage_pointer)
{
    ProtoValue protoValue(ProtoValue::protocol_buffer_message{ &nonNullProtocolMessage });
    ExpectIsProtocolBuffer(protoValue, &nonNullProtocolMessage);
    ExpectEmptyData(protoValue);
}

TEST_F(ProtoValueTests, constructor_backing_store_monostate_protocolmessage_monostate)
{
    ProtoValue protoValue(ProtoValue::backing_store{}, ProtoValue::protocol_buffer_message{});
    ExpectEmpty(protoValue);
}

TEST_F(ProtoValueTests, constructor_backing_store_span_protocolmessage_monostate)
{
    ProtoValue protoValue(nonNullProtocolMessageSpan, ProtoValue::protocol_buffer_message{});
    EXPECT_EQ(nonNullProtocolMessageSpan.data(), protoValue.as_protocol_buffer_bytes_if().data());
    ExpectIsProtocolBuffer(protoValue, nullptr);
}

TEST_F(ProtoValueTests, constructor_backing_store_string_protocolmessage_monostate)
{
    ProtoValue protoValue(nonNullProtocolMessageString, ProtoValue::protocol_buffer_message{});
    ExpectIsProtocolBuffer(
        protoValue,
        nullptr);
}

TEST_F(ProtoValueTests, constructor_backing_store_aligned_message_data_protocolmessage_monostate)
{
    ProtoValue protoValue(
        std::move(nonNullProtocolAlignedMessageData),
        ProtoValue::protocol_buffer_message{});

    ExpectIsProtocolBuffer(
        protoValue,
        nullptr);

    nonNullProtocolAlignedMessageString = nullptr;
    EXPECT_EQ(1, nonNullProtocolAlignedMessageLifetimeStatistics.instance_count);
    protoValue = {};
    EXPECT_EQ(0, nonNullProtocolAlignedMessageLifetimeStatistics.instance_count);
}

TEST_F(ProtoValueTests, constructor_backing_store_monostate_protocolmessage_pointer)
{
    ProtoValue protoValue(ProtoValue::backing_store{}, ProtoValue::protocol_buffer_message{ &nonNullProtocolMessage });
    ExpectIsProtocolBuffer(protoValue, &nonNullProtocolMessage);
    ExpectEmptyData(protoValue);
}

TEST_F(ProtoValueTests, constructor_backing_store_span_protocolmessage_pointer)
{
    ProtoValue protoValue(nonNullProtocolMessageSpan, ProtoValue::protocol_buffer_message{ &nonNullProtocolMessage });
    EXPECT_EQ(nonNullProtocolMessageSpan.data(), protoValue.as_protocol_buffer_bytes_if().data());
    ExpectIsProtocolBuffer(protoValue, &nonNullProtocolMessage);
}

TEST_F(ProtoValueTests, constructor_backing_store_string_protocolmessage_pointer)
{
    ProtoValue protoValue(nonNullProtocolMessageString, ProtoValue::protocol_buffer_message{ &nonNullProtocolMessage });
    ExpectIsProtocolBuffer(
        protoValue,
        &nonNullProtocolMessage
    );
}

TEST_F(ProtoValueTests, constructor_backing_store_aligned_message_data_protocolmessage_pointer)
{
    ProtoValue protoValue(
        std::move(nonNullProtocolAlignedMessageData),
        ProtoValue::protocol_buffer_message{ &nonNullProtocolMessage });

    ExpectIsProtocolBuffer(
        protoValue,
        &nonNullProtocolMessage);

    nonNullProtocolAlignedMessageString = nullptr;
    EXPECT_EQ(1, nonNullProtocolAlignedMessageLifetimeStatistics.instance_count);
    protoValue = {};
    EXPECT_EQ(0, nonNullProtocolAlignedMessageLifetimeStatistics.instance_count);
}

TEST_F(ProtoValueTests, constructor_backing_store_monostate_protocolmessage_shared_ptr_null)
{
    ProtoValue protoValue(ProtoValue::backing_store{}, ProtoValue::protocol_buffer_message{ std::shared_ptr<const google::protobuf::Message>() });
    ExpectEmpty(protoValue);
}

TEST_F(ProtoValueTests, constructor_backing_store_monostate_protocolmessage_shared_ptr)
{
    ProtoValue protoValue(ProtoValue::backing_store{}, ProtoValue::protocol_buffer_message{ nonNullProtocolMessageSharedPointer });
    ExpectIsProtocolBuffer(protoValue, nonNullProtocolMessageSharedPointer.get());
    ExpectEmptyData(protoValue);
}

TEST_F(ProtoValueTests, constructor_backing_store_span_protocolmessage_shared_ptr)
{
    ProtoValue protoValue(nonNullProtocolMessageSpan, ProtoValue::protocol_buffer_message{ nonNullProtocolMessageSharedPointer });
    EXPECT_EQ(nonNullProtocolMessageSpan.data(), protoValue.as_protocol_buffer_bytes_if().data());
    ExpectIsProtocolBuffer(protoValue, nonNullProtocolMessageSharedPointer.get());
}

TEST_F(ProtoValueTests, constructor_backing_store_string_protocolmessage_shared_ptr)
{
    ProtoValue protoValue(nonNullProtocolMessageString, ProtoValue::protocol_buffer_message{ nonNullProtocolMessageSharedPointer });
    ExpectIsProtocolBuffer(
        protoValue,
        nonNullProtocolMessageSharedPointer.get()
    );
}

TEST_F(ProtoValueTests, constructor_backing_store_aligned_message_data_protocolmessage_shared_ptr)
{
    ProtoValue protoValue(
        std::move(nonNullProtocolAlignedMessageData),
        ProtoValue::protocol_buffer_message{ nonNullProtocolMessageSharedPointer });

    EXPECT_EQ(1, protoValue.as_aligned_message_if().Alignment);
    EXPECT_EQ(
        reinterpret_cast<const std::byte*>(nonNullProtocolAlignedMessageString->value.data()),
        protoValue.as_aligned_message_if().Payload.data());

    ExpectIsProtocolBuffer(
        protoValue,
        nonNullProtocolMessageSharedPointer.get());

    nonNullProtocolAlignedMessageString = nullptr;
    EXPECT_EQ(1, nonNullProtocolAlignedMessageLifetimeStatistics.instance_count);
    protoValue = {};
    EXPECT_EQ(0, nonNullProtocolAlignedMessageLifetimeStatistics.instance_count);
}

TEST_F(ProtoValueTests, pack_does_nothing_on_packed_message)
{
    ProtoValue protoValue(
        nonNullProtocolMessageSpan,
        &nonNullProtocolMessage
    );
    protoValue.pack();
    EXPECT_EQ(nonNullProtocolMessageSpan.data(), protoValue.as_protocol_buffer_bytes_if().data());
}

TEST_F(ProtoValueTests, pack_does_nothing_on_flat_message)
{
    ProtoValue protoValue(
        ProtoValue::backing_store{},
        nonNullFlatTable
    );
    protoValue.pack();
    ExpectEmptyData(protoValue);
    ExpectIsTable(protoValue, nonNullFlatTableRaw);
}

TEST_F(ProtoValueTests, pack_does_nothing_on_key_min)
{
    auto protoValue = ProtoValue::KeyMin();
    EXPECT_TRUE(protoValue.IsKeyMin());
    EXPECT_FALSE(protoValue.IsKeyMax());
    EXPECT_FALSE(protoValue.is_flat_buffer());
    EXPECT_FALSE(protoValue.is_protocol_buffer());

    protoValue.pack();
    EXPECT_TRUE(protoValue.IsKeyMin());
    EXPECT_FALSE(protoValue.IsKeyMax());
    EXPECT_FALSE(protoValue.is_flat_buffer());
    EXPECT_FALSE(protoValue.is_protocol_buffer());

    protoValue = ProtoValue::KeyMin().pack();
    EXPECT_TRUE(protoValue.IsKeyMin());
    EXPECT_FALSE(protoValue.IsKeyMax());
    EXPECT_FALSE(protoValue.is_flat_buffer());
    EXPECT_FALSE(protoValue.is_protocol_buffer());
}

TEST_F(ProtoValueTests, pack_does_nothing_on_key_max)
{
    auto protoValue = ProtoValue::KeyMax();
    EXPECT_FALSE(protoValue.IsKeyMin());
    EXPECT_TRUE(protoValue.IsKeyMax());
    EXPECT_FALSE(protoValue.is_flat_buffer());
    EXPECT_FALSE(protoValue.is_protocol_buffer());

    protoValue.pack();
    EXPECT_FALSE(protoValue.IsKeyMin());
    EXPECT_TRUE(protoValue.IsKeyMax());
    EXPECT_FALSE(protoValue.is_flat_buffer());
    EXPECT_FALSE(protoValue.is_protocol_buffer());

    protoValue = ProtoValue::KeyMax().pack();
    EXPECT_FALSE(protoValue.IsKeyMin());
    EXPECT_TRUE(protoValue.IsKeyMax());
    EXPECT_FALSE(protoValue.is_flat_buffer());
    EXPECT_FALSE(protoValue.is_protocol_buffer());
}

TEST_F(ProtoValueTests, pack_allocates_data_on_packed_message)
{
    ProtoValue protoValue(
        {},
        &nonNullProtocolMessage
    );
    protoValue.pack();

    auto copy = ProtoValue::ProtocolBuffer(
        protoValue.as_protocol_buffer_bytes_if());

    ExpectIsProtocolBuffer(copy, nullptr);
}

TEST_F(ProtoValueTests, unpack_table_null)
{
    ProtoValue protoValue;

    FlatBuffers::ScalarTableT native;
    EXPECT_EQ(false, protoValue.unpack(&native));
    EXPECT_EQ(0, native.item);
}

TEST_F(ProtoValueTests, unpack_table_get_table)
{
    ProtoValue protoValue = ProtoValue::FlatBuffer(
        nonNullFlatMessageSpan
    );

    FlatBuffers::ScalarTableT native;
    EXPECT_EQ(true, protoValue.unpack(&native));
    EXPECT_EQ(5, native.item);

    native = {};
    EXPECT_EQ(false, protoValue.unpack<FlatBuffers::ScalarTableT>(nullptr));
    EXPECT_EQ(0, native.item);
}

TEST_F(ProtoValueTests, assignment_copies_data_reference)
{
    StringKey key;
    key.set_value("hello world");

    ProtoValue protoValue = ProtoValue::ProtocolBuffer(
        { &key }
    ).pack().unpack<StringKey>();

    ProtoValue protoValue2;
    protoValue2 = protoValue;

    EXPECT_EQ(
        protoValue.as_aligned_message_if().Payload.data(),
        protoValue2.as_aligned_message_if().Payload.data()
    );
}
}