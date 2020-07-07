#pragma once

#include <assert.h>
#include <exception>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <stdint.h>
#include <type_traits>
#include <utility>
#include <vector>
#include <cppcoro/task.hpp>
#include "Phantom.System/pooled_ptr.h"
#include <Phantom.ProtoStore/include/Phantom.ProtoStore.h>

namespace google::protobuf
{
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
using std::make_shared;
using std::make_unique;
using std::map;
using std::move;
using std::optional;
using std::range_error;
using std::shared_ptr;
using std::span;
using std::unique_ptr;
using std::vector;
using std::weak_ptr;
using cppcoro::task;
using Phantom::pooled_ptr;
using google::protobuf::Message;
using google::protobuf::io::ZeroCopyInputStream;
using google::protobuf::io::ZeroCopyOutputStream;

typedef std::uint64_t ExtentNumber;
typedef std::uint64_t ExtentOffset;

struct ExtentLocation
{
    ExtentNumber extentNumber;
    ExtentOffset extentOffset;
};

class IExtentStore;
class IMessageStore;
class IRandomMessageAccessor;
class IHeaderAccessor;
class IChecksumAlgorithm;
class IChecksumAlgorithmFactory;

enum class FlushBehavior
{
    DontFlush = 0,
    Flush = 1,
};

class LogRecord;

}