#pragma once

#include <cppcoro/async_generator.hpp>
#include <cppcoro/shared_task.hpp>
#include <cppcoro/task.hpp>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <google/protobuf/any.pb.h>
#include <google/protobuf/message.h>

namespace Phantom::ProtoStore
{
class IProtoStore;
class IOperation;
}

namespace Phantom::Scalable
{
using cppcoro::async_generator;
using cppcoro::shared_task;
using cppcoro::task;
using std::forward;
using std::make_shared;
using std::move;
using std::optional;
using std::size_t;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using google::protobuf::Any;
using google::protobuf::Message;

using Phantom::ProtoStore::IProtoStore;
typedef Phantom::ProtoStore::IOperation IProtoStoreOperation;

}
