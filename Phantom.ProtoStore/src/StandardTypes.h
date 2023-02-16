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
#include <Phantom.ProtoStore/Phantom.ProtoStore.h>

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

typedef google::protobuf::uint64 CheckpointNumber;

struct ExtentOffsetRange
{
    ExtentOffset Beginning;
    ExtentOffset End;
};

typedef string IndexName;
typedef google::protobuf::uint64 IndexNumber;
typedef google::protobuf::uint64 LevelNumber;
typedef google::protobuf::uint64 PartitionNumber;
typedef google::protobuf::uint64 LogExtentSequenceNumber;

class IInternalProtoStore;
class IInternalTransaction;
class IExtentStore;
class IMessageStore;
class IRandomMessageAccessor;
class IHeaderAccessor;
class IChecksumAlgorithm;
class IChecksumAlgorithmFactory;
class ISequentialMessageWriter;
class IPartition;
class IPartitionWriter;
class IMessageFactory;
class IUnresolvedTransactionsTracker;
class KeyComparer;
class RowMerger;
struct MemoryTableOperationOutcome;
typedef cppcoro::shared_task<MemoryTableOperationOutcome> MemoryTableOperationOutcomeTask;

enum class FlushBehavior
{
    DontFlush = 0,
    Flush = 1,
};

class WriteRowsResult;
class MergeParameters;
class MessageDescription;

namespace Serialization
{
class Header;
class LogRecord;
class IndexesByNameKey;
class IndexesByNameValue;
class IndexesByNumberKey;
class IndexesByNumberValue;
class LoggedRowWrite;
class LoggedCheckpoint;
class LoggedAction;
class LoggedCreateIndex;
class LoggedCreateDataExtent;
class LoggedCreatePartition;
class LoggedUpdatePartitions;
class LoggedCommitExtent;
class LoggedUnresolvedTransactions;
class PartitionHeader;
class PartitionRoot;
class PartitionBloomFilter;
enum PartitionBloomFilterHashAlgorithm;
class PartitionTreeNode;
class PartitionTreeEntry;
class PartitionTreeEntryValue;
class PartitionTreeEntryValueSet;
class PartitionsKey;
class PartitionsValue;
class MergesKey;
class MergesValue;
class MergeProgressKey;
class MergeProgressValue;
class PlaceholderKey;
class PartitionMessage;
class LoggedPartitionsData;
enum TransactionOutcome;
class UnresolvedTransactionKey;
class UnresolvedTransactionValue;
}

class SerializationTypes
{
public:
    using Header = Serialization::Header;
    using LogRecord = Serialization::LogRecord;
    using IndexesByNameKey = Serialization::IndexesByNameKey;
    using IndexesByNameValue = Serialization::IndexesByNameValue;
    using IndexesByNumberKey = Serialization::IndexesByNumberKey;
    using IndexesByNumberValue = Serialization::IndexesByNumberValue;
    using LoggedRowWrite = Serialization::LoggedRowWrite;
    using LoggedCheckpoint = Serialization::LoggedCheckpoint;
    using LoggedAction = Serialization::LoggedAction;
    using LoggedCreateIndex = Serialization::LoggedCreateIndex;
    using LoggedCreateDataExtent = Serialization::LoggedCreateDataExtent;
    using LoggedCreatePartition = Serialization::LoggedCreatePartition;
    using LoggedUpdatePartitions = Serialization::LoggedUpdatePartitions;
    using LoggedCommitExtent = Serialization::LoggedCommitExtent;
    using LoggedUnresolvedTransactions = Serialization::LoggedUnresolvedTransactions;
    using PartitionHeader = Serialization::PartitionHeader;
    using PartitionRoot = Serialization::PartitionRoot;
    using PartitionBloomFilter = Serialization::PartitionBloomFilter;
    using PartitionBloomFilterHashAlgorithm = Serialization::PartitionBloomFilterHashAlgorithm;
    using PartitionTreeNode = Serialization::PartitionTreeNode;
    using PartitionTreeEntry = Serialization::PartitionTreeEntry;
    using PartitionTreeEntryValue = Serialization::PartitionTreeEntryValue;
    using PartitionTreeEntryValueSet = Serialization::PartitionTreeEntryValueSet;
    using PartitionsKey = Serialization::PartitionsKey;
    using PartitionsValue = Serialization::PartitionsValue;
    using MergesKey = Serialization::MergesKey;
    using MergesValue = Serialization::MergesValue;
    using MergeProgressKey = Serialization::MergeProgressKey;
    using MergeProgressValue = Serialization::MergeProgressValue;
    using PlaceholderKey = Serialization::PlaceholderKey;
    using PartitionMessage = Serialization::PartitionMessage;
    using LoggedPartitionsData = Serialization::LoggedPartitionsData;
    using UnresolvedTransactionKey = Serialization::UnresolvedTransactionKey;
    using UnresolvedTransactionValue = Serialization::UnresolvedTransactionValue;
};

using TransactionId = std::string;
typedef ExtentName MergeId;


struct ResultRow
{
    const Message* Key;
    SequenceNumber WriteSequenceNumber;
    const Message* Value = nullptr;
    const TransactionId* TransactionId = nullptr;
};

typedef cppcoro::async_generator<ResultRow> row_generator;
typedef row_generator::iterator row_generator_iterator;
typedef cppcoro::generator<row_generator> row_generators;

template<
    typename TKey,
    typename TValue
> struct row
{
    TKey Key;
    TValue Value;
    SequenceNumber WriteSequenceNumber;
    SequenceNumber ReadSequenceNumber;
};

typedef row<Serialization::PartitionsKey, Serialization::PartitionsValue> partition_row_type;
typedef vector<partition_row_type> partition_row_list_type;
typedef row<Serialization::MergesKey, Serialization::MergesValue> merges_row_type;
typedef vector<merges_row_type> merges_row_list_type;
typedef row<Serialization::MergeProgressKey, Serialization::MergeProgressValue> merge_progress_row_type;
typedef vector<merge_progress_row_type> merge_progress_row_list_type;

}
