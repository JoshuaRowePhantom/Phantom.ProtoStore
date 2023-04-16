#pragma once

#include <assert.h>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <stdint.h>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>
#include <cppcoro/async_generator.hpp>
#include <cppcoro/generator.hpp>
#include <cppcoro/task.hpp>
#include <cppcoro/shared_task.hpp>
#include "Phantom.System/pooled_ptr.h"
#include <flatbuffers/flatbuffers.h>
#include <Phantom.ProtoStore/Phantom.ProtoStore.h>
#include "FlatMessage.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"

namespace google::protobuf
{
class FileDescriptor;
class FileDescriptorSet;
class FileDescriptorProto;
class Message;
class Descriptor;
typedef uint8_t uint8;
}

namespace google::protobuf::io
{
class ZeroCopyInputStream;
class ZeroCopyOutputStream;
class CodedInputStream;
}

namespace Phantom::ProtoStore
{

using std::byte;
using std::forward;
using std::function;
using std::make_shared;
using std::make_unique;
using std::map;
using std::move;
using std::optional;
using std::range_error;
using std::shared_ptr;
using std::span;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using std::weak_ptr;
using cppcoro::shared_task;
using Phantom::pooled_ptr;
using Phantom::Coroutines::reusable_task;
using google::protobuf::Message;
using google::protobuf::Descriptor;
using google::protobuf::io::ZeroCopyInputStream;
using google::protobuf::io::ZeroCopyOutputStream;

typedef string IndexName;
typedef uint64_t IndexNumber;
typedef uint64_t LevelNumber;
typedef uint64_t PartitionNumber;
typedef uint64_t LogExtentSequenceNumber;

class IInternalProtoStore;
class IInternalTransaction;
class IExtentStore;
class IMessageStore;
class IChecksumAlgorithm;
class IChecksumAlgorithmFactory;
class ISequentialMessageWriter;
class IPartition;
class IPartitionWriter;
class IMessageFactory;
class IUnresolvedTransactionsTracker;
class ValueComparer;
class RowMerger;
struct MemoryTableTransactionOutcome;

enum class FlushBehavior
{
    DontFlush = 0,
    Flush = 1,
};

class WriteRowsResult;
class MergeParameters;

namespace Serialization
{
enum TransactionOutcome;
class UnresolvedTransactionKey;
class UnresolvedTransactionValue;
}

namespace FlatBuffers
{
struct ProtocolBuffersMessageDescription;
struct ProtocolBuffersSchemaDescription;
struct FlatBuffersSchemaDescription;
struct IndexSchemaDescription;
struct IndexesByNumberKey;
struct IndexesByNumberValue;

struct SchemaDescriptionT;
struct ProtocolBuffersMessageDescriptionT;
struct ProtocolBuffersSchemaDescriptionT;
struct FlatBuffersSchemaDescriptionT;
struct IndexSchemaDescriptionT;
struct IndexesByNumberKeyT;
struct IndexesByNumberValue;

struct DatabaseHeaderExtentName;
struct DatabaseHeaderExtentNameBuilder;
struct IndexExtentName;
struct IndexExtentNameBuilder;
struct IndexDataExtentName;
struct IndexDataExtentNameBuilder;
struct IndexHeaderExtentName;
struct IndexHeaderExtentNameBuilder;
struct LogExtentName;
struct LogExtentNameT;
struct LogExtentNameBuilder;
struct DatabaseHeader;
struct DatabaseHeaderT;
struct DatabaseHeaderBuilder;
struct ExtentHeader;
struct ExtentHeaderBuilder;
struct MessageHeader_V1;
struct LogRecord;
struct LogEntry;
enum class LogEntryUnion : uint8_t;
struct LoggedRowWrite;
struct LoggedCommitLocalTransaction;
struct LoggedCheckpoint;
struct LoggedCheckpointT;
struct LoggedAction;
struct LoggedCreateIndex;
struct LoggedCreateExtent;
struct LoggedCreateMemoryTable;
struct LoggedCreatePartition;
struct LoggedUpdatePartitions;
struct LoggedCommitExtent;
struct LoggedDeleteExtentPendingPartitionsUpdated;
struct LoggedUnresolvedTransactions;
struct LoggedPartitionsData;
struct LoggedPartitionsDataT;

struct MessageReference_V1;

struct PartitionMessage;
struct PartitionMessageT;
struct PartitionHeader;
struct PartitionHeaderT;
struct PartitionRoot;
struct PartitionRootT;
struct PartitionTreeNode;
struct PartitionTreeNodeT;
struct DataValue;
struct DataValueT;
struct PartitionTreeEntryKey;
struct PartitionTreeEntryKeyT;
struct PartitionTreeEntryValue;
struct PartitionTreeEntryValueT;
struct PartitionBloomFilter;
struct PartitionBloomFilterT;

struct IndexesByNameKey;
struct IndexesByNameKeyT;
struct IndexesByNameValue;
struct IndexesByNameValueT;

struct PartitionsKey;
struct PartitionsKeyT;
struct PartitionsValue;
struct PartitionsValueT;

enum class ExtentFormatVersion : int8_t;

struct ExtentName;
struct ExtentNameT;
std::weak_ordering operator<=>(
    const ExtentNameT&,
    const ExtentNameT&
    );

struct DatabaseHeaderExtentNameT;
std::weak_ordering operator<=>(
    const DatabaseHeaderExtentNameT&,
    const DatabaseHeaderExtentNameT&
    );

struct IndexDataExtentNameT;
std::weak_ordering operator<=>(
    const IndexDataExtentNameT&,
    const IndexDataExtentNameT&
    );

struct IndexExtentNameT;
std::weak_ordering operator<=>(
    const IndexExtentNameT&,
    const IndexExtentNameT&
    );

struct IndexHeaderExtentNameT;
std::weak_ordering operator<=>(
    const IndexHeaderExtentNameT&,
    const IndexHeaderExtentNameT&
    );

struct LogExtentNameT;
std::weak_ordering operator<=>(
    const LogExtentNameT&,
    const LogExtentNameT&
    );
}

class SerializationTypes
{
public:
    using uoffset_t = flatbuffers::uoffset_t;
    
    using IndexesByNameKey = FlatBuffers::IndexesByNameKey;
    using IndexesByNameKeyT = FlatBuffers::IndexesByNameKeyT;
    using IndexesByNameValue = FlatBuffers::IndexesByNameValue;
    using IndexesByNameValueT = FlatBuffers::IndexesByNameValueT;

    using PartitionsKey = FlatBuffers::PartitionsKey;
    using PartitionsKeyT = FlatBuffers::PartitionsKeyT;
    using PartitionsValue = FlatBuffers::PartitionsValue;
    using PartitionsValueT = FlatBuffers::PartitionsValueT;

    using IndexesByNumberKey = FlatBuffers::IndexesByNumberKey;
    using IndexesByNumberValue = FlatBuffers::IndexesByNumberValue;
    using LogRecord = FlatBuffers::LogRecord;
    using LogEntry = FlatBuffers::LogEntry;
    using LogEntryUnion = FlatBuffers::LogEntryUnion;
    using LoggedRowWrite = FlatBuffers::LoggedRowWrite;
    using LoggedCommitLocalTransaction = FlatBuffers::LoggedCommitLocalTransaction;
    using LoggedCheckpoint = FlatBuffers::LoggedCheckpoint;
    using LoggedCheckpointT = FlatBuffers::LoggedCheckpointT;
    using LoggedAction = FlatBuffers::LoggedAction;
    using LoggedCreateIndex = FlatBuffers::LoggedCreateIndex;
    using LoggedCreateExtent = FlatBuffers::LoggedCreateExtent;
    using LoggedCreateMemoryTable = FlatBuffers::LoggedCreateMemoryTable;
    using LoggedCreatePartition = FlatBuffers::LoggedCreatePartition;
    using LoggedUpdatePartitions = FlatBuffers::LoggedUpdatePartitions;
    using LoggedCommitExtent = FlatBuffers::LoggedCommitExtent;
    using LoggedDeleteExtentPendingPartitionsUpdated = FlatBuffers::LoggedDeleteExtentPendingPartitionsUpdated;
    using LoggedUnresolvedTransactions = FlatBuffers::LoggedUnresolvedTransactions;
    using LoggedPartitionsData = FlatBuffers::LoggedPartitionsData;
    using LoggedPartitionsDataT = FlatBuffers::LoggedPartitionsDataT;
    using MergesKey = FlatBuffers::MergesKey;
    using MergesValue = FlatBuffers::MergesValue;
    using MergeProgressKey = FlatBuffers::MergeProgressKey;
    using MergeProgressValue = FlatBuffers::MergeProgressValue;
    using PartitionMessage = FlatBuffers::PartitionMessage;
    using PartitionMessageT = FlatBuffers::PartitionMessageT;
    using PartitionRoot = FlatBuffers::PartitionRoot;
    using PartitionRootT = FlatBuffers::PartitionRootT;
    using PartitionTreeNode = FlatBuffers::PartitionTreeNode;
    using PartitionTreeNodeT = FlatBuffers::PartitionTreeNodeT;
    using PartitionTreeEntryKey = FlatBuffers::PartitionTreeEntryKey;
    using PartitionTreeEntryKeyT = FlatBuffers::PartitionTreeEntryKeyT;
    using PartitionTreeEntryValue= FlatBuffers::PartitionTreeEntryValue;
    using PartitionTreeEntryValueT = FlatBuffers::PartitionTreeEntryValueT;
    using PartitionBloomFilter = FlatBuffers::PartitionBloomFilter;
    using PartitionBloomFilterT = FlatBuffers::PartitionBloomFilterT;
    using PartitionHeader = FlatBuffers::PartitionHeader;
    using PartitionHeaderT = FlatBuffers::PartitionHeaderT;
    using DataValue = FlatBuffers::DataValue;
    using DataValueT = FlatBuffers::DataValueT;
    using MessageReference_V1 = FlatBuffers::MessageReference_V1;
    using MessageHeader_V1 = FlatBuffers::MessageHeader_V1;
    using UnresolvedTransactionKey = Serialization::UnresolvedTransactionKey;
    using UnresolvedTransactionValue = Serialization::UnresolvedTransactionValue;

    //using ExtentName = FlatBuffers::ExtentName;
    //using ExtentNameT = FlatBuffers::ExtentNameT;
    //using LogExtentName = FlatBuffers::LogExtentName;
    //using LogExtentNameT = FlatBuffers::LogExtentNameT;
    //using LogExtentNameBuilder = FlatBuffers::LogExtentNameBuilder;
    using DatabaseHeader = FlatBuffers::DatabaseHeader;
    using DatabaseHeaderT = FlatBuffers::DatabaseHeaderT;
    using DatabaseHeaderBuilder = FlatBuffers::DatabaseHeaderBuilder;

    template<
        typename T
    >
    using Offset = flatbuffers::Offset<T>;
};

class ExtentName;
using TransactionId = std::string_view;
using TransactionIdReference = std::shared_ptr<const flatbuffers::String>;
typedef ExtentName MergeId;
using LocalTransactionNumber = uint64_t;

struct ResultRow
{
    ProtoValue Key;
    SequenceNumber WriteSequenceNumber;
    ProtoValue Value;
    TransactionIdReference TransactionId;
};

typedef cppcoro::async_generator<ResultRow> row_generator;
typedef row_generator::iterator row_generator_iterator;
typedef cppcoro::generator<row_generator> row_generators;
template<typename T> struct tag {};

TransactionIdReference MakeTransactionIdReference(
    const DataReference<StoredMessage>& message,
    const flatbuffers::String* transactionId
);

template<
    typename TKey,
    typename TValue
> struct row
{
    TKey Key;
    TValue Value;
    SequenceNumber WriteSequenceNumber;
    SequenceNumber ReadSequenceNumber;

    static row FromResultRow(
        auto&& resultRow
    )
    {
        return
        {
            .Key = std::forward_like<decltype(resultRow)>(resultRow.Key),
            .Value = std::forward_like<decltype(resultRow)>(resultRow.Value),
            .WriteSequenceNumber = std::forward_like<decltype(resultRow)>(resultRow.WriteSequenceNumber),
            .ReadSequenceNumber = std::forward_like<decltype(resultRow)>(resultRow.WriteSequenceNumber),
        };
    }
};

typedef row<FlatValue<FlatBuffers::PartitionsKey>, FlatValue<FlatBuffers::PartitionsValue>> partition_row_type;
typedef vector<partition_row_type> partition_row_list_type;
typedef row<FlatValue<FlatBuffers::MergesKey>, FlatValue<FlatBuffers::MergesValue>> merges_row_type;
typedef vector<merges_row_type> merges_row_list_type;
typedef row<FlatValue<FlatBuffers::MergeProgressKey>, FlatValue<FlatBuffers::MergeProgressValue>> merge_progress_row_type;
typedef vector<merge_progress_row_type> merge_progress_row_list_type;

flatbuffers::Offset<FlatBuffers::DataValue> CreateDataValue(
    flatbuffers::FlatBufferBuilder& builder,
    const AlignedMessage& message
);

AlignedMessage GetAlignedMessage(
    const FlatBuffers::DataValue* data
);

AlignedMessageData GetAlignedMessageData(
    DataReference<StoredMessage> reference,
    const FlatBuffers::DataValue* data
);

AlignedMessageData GetAlignedMessageData(
    const IsFlatMessage auto& reference,
    const FlatBuffers::DataValue* data
)
{
    return GetAlignedMessageData(
        static_cast<DataReference<StoredMessage>>(reference),
        data);
}

}
