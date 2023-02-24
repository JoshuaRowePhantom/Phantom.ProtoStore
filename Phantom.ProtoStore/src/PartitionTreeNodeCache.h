#pragma once

#include "StandardTypes.h"
#include "SkipList.h"
#include <atomic>
#include "Phantom.System/async_reader_writer_lock.h"

namespace Phantom::ProtoStore
{

class PartitionTreeNodeCacheEntry : 
    public SerializationTypes
{
    friend class PartitionTreeNodeCache;

    std::shared_ptr<google::protobuf::Arena> m_arena;
    shared_ptr<IMessageFactory> m_keyFactory;
    DataReference<const Serialization::PartitionTreeNode*> m_treeNode;

    shared_task<> m_readTreeNodeTask;
    
    shared_task<> ReadTreeNodeInternal(
        shared_ptr<IRandomMessageAccessor> messageAccessor,
        ExtentLocation messageLocation);

public:
    PartitionTreeNodeCacheEntry(
        shared_ptr<IMessageFactory> keyFactory,
        shared_ptr<IRandomMessageAccessor> messageAccessor,
        ExtentLocation messageLocation
    );

    struct value_type
    {
        const PartitionTreeEntry* TreeEntry;
        RawData Key;
    };

    class iterator_type
    {
        static PartitionTreeEntry EmptyTreeEntry;
        static shared_task<RawData> EmptyKeyTask;

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

    iterator_type begin() const;
    iterator_type end() const;

    DataReference<const Serialization::PartitionTreeNode*> ReadTreeNode() const;
    RawData GetKey(
        size_t index
    ) const;
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