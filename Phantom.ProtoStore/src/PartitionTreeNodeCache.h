#include "StandardTypes.h"
#include "SkipList.h"
#include <atomic>
#include "Phantom.System/async_reader_writer_lock.h"

namespace Phantom::ProtoStore
{

class PartitionTreeNodeCacheEntry
{
    google::protobuf::Arena m_arena;
    shared_ptr<IMessageFactory> m_keyFactory;

    shared_task<const PartitionTreeNode*> m_readTreeNodeTask;
    std::vector<shared_task<const Message*>> m_keys;
    
    shared_task<const PartitionTreeNode*> ReadTreeNodeInternal(
        shared_ptr<IRandomMessageAccessor> messageAccessor,
        ExtentLocation messageLocation);

    shared_task<const Message*> GetKeyInternal(
        size_t index);

public:
    PartitionTreeNodeCacheEntry(
        shared_ptr<IMessageFactory> keyFactory,
        shared_ptr<IRandomMessageAccessor> messageAccessor,
        ExtentLocation messageLocation
    );

    shared_task<const PartitionTreeNode*> ReadTreeNode();
    task<const Message*> GetKey(
        size_t index);
};

class PartitionTreeNodeCache
{
    async_reader_writer_lock m_cacheLock;
    std::atomic<size_t> m_approximateCache1Size;
    typedef SkipList<ExtentOffset, shared_ptr<PartitionTreeNodeCacheEntry>, 16> SkipListType;
    shared_ptr<SkipListType> m_cache1;
    shared_ptr<SkipListType> m_cache2;
    shared_ptr<IMessageFactory> m_keyFactory;
    shared_ptr<IRandomMessageAccessor> m_messageAccessor;

public:
    PartitionTreeNodeCache(
        shared_ptr<IMessageFactory> keyFactory,
        shared_ptr<IRandomMessageAccessor> messageAccessor
    );

    task<shared_ptr<PartitionTreeNodeCacheEntry>> GetPartitionTreeNodeCacheEntry(
        ExtentLocation location);
};

}