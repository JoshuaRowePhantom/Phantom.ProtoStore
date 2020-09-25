#pragma once

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

    struct value_type
    {
        const PartitionTreeEntry* TreeEntry;
        const shared_task<const Message*>* Key;
    };

    class iterator_type
    {
        static PartitionTreeEntry EmptyTreeEntry;
        static shared_task<const Message*> EmptyKeyTask;

        const PartitionTreeNodeCacheEntry* m_cacheEntry;
        const PartitionTreeNode* m_treeNode;
        value_type m_value;
        int m_index;

        friend class PartitionTreeNodeCacheEntry;
        iterator_type(
            const PartitionTreeNodeCacheEntry* cacheEntry,
            const PartitionTreeNode* treeNode,
            int index);
    public:
        typedef value_type value_type;

        bool operator==(const iterator_type& other) const;
        bool operator!=(const iterator_type& other) const;

        iterator_type& operator++();
        iterator_type& operator++(int);
        iterator_type operator+(
            int offset
            ) const;
        iterator_type& operator+=(
            int offset);

        int operator-(
            const iterator_type& other
            ) const;

        const value_type* operator->() const;
        const value_type& operator*() const;
    };

    task<iterator_type> begin() const;
    task<iterator_type> end() const;

    shared_task<const PartitionTreeNode*> ReadTreeNode() const;
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