#include "PartitionTreeNodeCache.h"
#include "RandomMessageAccessor.h"
#include "src/ProtoStoreInternal.pb.h"
#include "Schema.h"

namespace Phantom::ProtoStore
{

PartitionTreeNodeCacheEntry::PartitionTreeNodeCacheEntry(
    shared_ptr<IMessageFactory> keyFactory,
    shared_ptr<IRandomMessageAccessor> messageAccessor,
    ExtentLocation messageLocation
) :
    m_keyFactory(keyFactory)
{
    m_readTreeNodeTask = ReadTreeNodeInternal(
        messageAccessor,
        messageLocation);
}

shared_task<const PartitionTreeNode*> PartitionTreeNodeCacheEntry::ReadTreeNodeInternal(
    shared_ptr<IRandomMessageAccessor> messageAccessor,
    ExtentLocation messageLocation)
{
    auto treeNode = google::protobuf::Arena::CreateMessage<PartitionTreeNode>(
        &m_arena);

    co_await messageAccessor->ReadMessage(
        messageLocation,
        *treeNode
    );

    m_keys.resize(treeNode->treeentries_size());
    for (size_t index = 0; index < treeNode->treeentries_size(); index++)
    {
        m_keys[index] = GetKeyInternal(
            index);
    }

    co_return treeNode;
}

shared_task<const Message*> PartitionTreeNodeCacheEntry::GetKeyInternal(
    size_t index)
{
    auto treeNode = co_await m_readTreeNodeTask;
    auto key = m_keyFactory->GetPrototype()->New(&m_arena);
    key->ParseFromString(treeNode->treeentries(index).key());

    co_return key;
}

shared_task<const PartitionTreeNode*> PartitionTreeNodeCacheEntry::ReadTreeNode()
{
    return m_readTreeNodeTask;
}

task<const Message*> PartitionTreeNodeCacheEntry::GetKey(
    size_t index)
{
    co_await m_readTreeNodeTask;
    co_return co_await m_keys[index];
}

PartitionTreeNodeCache::PartitionTreeNodeCache(
    shared_ptr<IMessageFactory> keyFactory,
    shared_ptr<IRandomMessageAccessor> messageAccessor
) :
    m_keyFactory(keyFactory),
    m_messageAccessor(messageAccessor),
    m_approximateCache1Size(0),
    m_cache1(make_shared<SkipListType>()),
    m_cache2(make_shared<SkipListType>())
{}

task<shared_ptr<PartitionTreeNodeCacheEntry>> PartitionTreeNodeCache::GetPartitionTreeNodeCacheEntry(
    ExtentLocation location)
{
    shared_ptr<SkipListType> skipListToDestroy;

    if (m_approximateCache1Size.load() > 1000)
    {
        auto writeLock = co_await m_cacheLock.writer().scoped_lock_async();
        m_approximateCache1Size.store(0);
        skipListToDestroy = m_cache2;
        m_cache2 = m_cache1;
    }

    skipListToDestroy.reset();

    shared_ptr<SkipListType> skipList1;
    shared_ptr<SkipListType> skipList2;
    
    {
        auto lock = co_await m_cacheLock.reader().scoped_lock_async();
        skipList1 = m_cache1;
        skipList2 = m_cache2;
    }

    auto cacheEntryFindResult1 = skipList1->find(
        location.extentOffset);

    if (cacheEntryFindResult1.second == std::weak_ordering::equivalent)
    {
        co_return cacheEntryFindResult1.first->second;
    }

    auto cacheEntryFindResult2 = skipList2->find(
        location.extentOffset);

    if (cacheEntryFindResult2.second == std::weak_ordering::equivalent)
    {
        co_return cacheEntryFindResult2.first->second;
    }

    auto cacheEntry = make_shared<PartitionTreeNodeCacheEntry>(
        m_keyFactory,
        m_messageAccessor,
        location
        );

    auto insertionResult = skipList1->insert_with_hint(
        location.extentOffset,
        cacheEntry,
        cacheEntryFindResult1.first);

    co_return insertionResult.first->second;
}
}
