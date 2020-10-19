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
using cppcoro::task;
using cppcoro::shared_task;
using Phantom::pooled_ptr;
using google::protobuf::Message;
using google::protobuf::Descriptor;
using google::protobuf::io::ZeroCopyInputStream;
using google::protobuf::io::ZeroCopyOutputStream;

typedef google::protobuf::uint64 CheckpointNumber;
typedef google::protobuf::uint64 MergeId;

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
class KeyComparer;
class RowMerger;
struct MemoryTableOperationOutcome;
typedef cppcoro::shared_task<MemoryTableOperationOutcome> MemoryTableOperationOutcomeTask;

enum class FlushBehavior
{
    DontFlush = 0,
    Flush = 1,
};

class Header;
class LogRecord;
class IndexesByNameKey;
class IndexesByNameValue;
class IndexesByNumberKey;
class IndexesByNumberValue;
class MessageDescription;
class LoggedRowWrite;
class LoggedCheckpoint;
class LoggedAction;
class LoggedCreateIndex;
class LoggedCreateDataExtent;
class LoggedUpdatePartitions;
class PartitionTreeNode;
class PartitionTreeEntry;
class PartitionsKey;
class PartitionsValue;
class MergesKey;
class MergesValue;
class MergeProgressKey;
class MergeProgressValue;
class MergeParameters;
class WriteRowsResult;
class PlaceholderKey;

class ResultRow;
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

typedef row<PartitionsKey, PartitionsValue> partition_row_type;
typedef vector<partition_row_type> partition_row_list_type;
typedef row<MergesKey, MergesValue> merges_row_type;
typedef vector<merges_row_type> merges_row_list_type;
typedef row<MergeProgressKey, MergeProgressValue> merge_progress_row_type;
typedef vector<merge_progress_row_type> merge_progress_row_list_type;

}