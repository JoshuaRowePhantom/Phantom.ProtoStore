#pragma once

#include <cppcoro/sync_wait.hpp>
#include <gtest/gtest.h>
#include "Phantom.ProtoStore/include/Phantom.ProtoStore.h"
#include "Phantom.ProtoStore/src/StandardTypes.h"
#include <google/protobuf/util/message_differencer.h>

namespace Phantom::ProtoStore
{

using google::protobuf::util::MessageDifferencer;

std::function<task<shared_ptr<IExtentStore>>()> UseMemoryExtentStore();

}