#include "PartitionTreeNodeCache.h"
#include "RandomMessageAccessor.h"
#include "ProtoStoreInternal.pb.h"
#include "Schema.h"

namespace Phantom::ProtoStore
{

PartitionTreeNodeCacheEntry::PartitionTreeNodeCacheEntry(
    shared_ptr<IMessageFactory> keyFactory,
    shared_ptr<IRandomMessageAccessor> messageAccessor,
    ExtentLocation messageLocation
) :
    m_keyFactory(keyFactory),
    m_arena(std::make_shared<google::protobuf::Arena>())
{
    m_readTreeNodeTask = ReadTreeNodeInternal(
        messageAccessor,
        messageLocation);
}


shared_task<> PartitionTreeNodeCacheEntry::ReadTreeNodeInternal(
    shared_ptr<IRandomMessageAccessor> messageAccessor,
    ExtentLocation messageLocation)
{
    auto treeNodeMessage = google::protobuf::Arena::CreateMessage<PartitionMessage>(
        m_arena.get());

    co_await messageAccessor->ReadMessage(
        messageLocation,
        *treeNodeMessage
    );

    assert(treeNodeMessage->has_partitiontreenode());
    auto treeNode = treeNodeMessage->mutable_partitiontreenode();

    m_treeNode = { m_arena, treeNode };
}

DataReference<const Serialization::PartitionTreeNode*> PartitionTreeNodeCacheEntry::ReadTreeNode() const
{
    return m_treeNode;
}

RawData PartitionTreeNodeCacheEntry::GetKey(
    size_t index
) const
{
    auto key = as_bytes(std::span{ (*m_treeNode)->treeentries().Get(index).key() });
    return RawData
    {
        m_treeNode,
        key
    };
}

PartitionTreeNodeCacheEntry::iterator_type PartitionTreeNodeCacheEntry::begin() const
{
    return iterator_type(
        this,
        *m_treeNode,
        0
    );
}

PartitionTreeNodeCacheEntry::iterator_type PartitionTreeNodeCacheEntry::end() const
{
    return iterator_type(
        this,
        *m_treeNode,
        (*m_treeNode)->treeentries_size());
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
        co_await cacheEntryFindResult1.first->second->m_readTreeNodeTask;
        co_return cacheEntryFindResult1.first->second;
    }

    auto cacheEntryFindResult2 = skipList2->find(
        location.extentOffset);

    if (cacheEntryFindResult2.second == std::weak_ordering::equivalent)
    {
        co_await cacheEntryFindResult2.first->second->m_readTreeNodeTask;
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

    co_await insertionResult.first->second->m_readTreeNodeTask;
    co_return insertionResult.first->second;
}

PartitionTreeNodeCacheEntry::iterator_type::iterator_type(
    const PartitionTreeNodeCacheEntry* cacheEntry,
    const PartitionTreeNode* treeNode,
    int index)
    :
    m_cacheEntry(cacheEntry),
    m_treeNode(treeNode),
    m_value { nullptr, nullptr },
    m_index(index)
{
    // This sets m_value.
    *this += 0;
}

PartitionTreeNodeCacheEntry::iterator_type& PartitionTreeNodeCacheEntry::iterator_type::operator++()
{
    return *this += 1;
}

PartitionTreeNodeCacheEntry::iterator_type& PartitionTreeNodeCacheEntry::iterator_type::operator++(int)
{
    return *this += 1;
}


PartitionTreeNodeCacheEntry::iterator_type& PartitionTreeNodeCacheEntry::iterator_type::operator+=(
    int offset)
{
    m_index += offset;
    if (m_index < m_treeNode->treeentries_size())
    {
        m_value = value_type
        {
            &m_treeNode->treeentries(m_index),
            m_cacheEntry->GetKey(m_index),
        };
    }
    return *this;
}

PartitionTreeNodeCacheEntry::iterator_type PartitionTreeNodeCacheEntry::iterator_type::operator+(
    int offset
    ) const
{
    iterator_type result(*this);
    return result += offset;
}

int PartitionTreeNodeCacheEntry::iterator_type::operator-(
    const iterator_type& other
    ) const
{
    return m_index - other.m_index;
}

const PartitionTreeNodeCacheEntry::value_type& PartitionTreeNodeCacheEntry::iterator_type::operator*() const
{
    return m_value;
}

const PartitionTreeNodeCacheEntry::value_type* PartitionTreeNodeCacheEntry::iterator_type::operator->() const
{
    return &m_value;
}

bool PartitionTreeNodeCacheEntry::iterator_type::operator==(
    const iterator_type& other
    ) const
{
    return other.m_index == m_index;
}

bool PartitionTreeNodeCacheEntry::iterator_type::operator!=(
    const iterator_type& other
    ) const
{
    return other.m_index != m_index;
}
}
