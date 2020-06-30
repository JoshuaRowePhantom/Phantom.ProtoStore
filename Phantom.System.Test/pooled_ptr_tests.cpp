#include "Phantom.System/pooled_ptr.h"
#include "gtest/gtest.h"

namespace Phantom::System
{
    struct MySelfReturnable
    {
        bool& m_returnedToPool;
        bool& m_destructorInvoked;

        MySelfReturnable(
            bool& returnedToPool,
            bool& destructorInvoked
        )
            :
            m_returnedToPool(returnedToPool),
            m_destructorInvoked(destructorInvoked)
        {}

        void ReturnToPool()
        {
            m_returnedToPool = true;
        }

        ~MySelfReturnable()
        {
            m_destructorInvoked = true;
        }
    };

    TEST(pooled_ptr_tests, ReturnsToPoolOnSelfReturnableObject)
    {
        bool returnedToPool = false;
        bool destructorInvoked = false;

        {
            auto pooled = make_pooled<MySelfReturnable>(
                returnedToPool,
                destructorInvoked);
        }

        ASSERT_TRUE(returnedToPool);
        ASSERT_FALSE(destructorInvoked);
    }

    struct MyReturnable
    {
        bool& m_returnedToPool;
        bool& m_destructorInvoked;

        MyReturnable(
            bool& returnedToPool,
            bool& destructorInvoked
        )
            :
            m_returnedToPool(returnedToPool),
            m_destructorInvoked(destructorInvoked)
        {}

        ~MyReturnable()
        {
            m_destructorInvoked = true;
        }
    };

    void ReturnToPool(
        MyReturnable* p)
    {
        p->m_returnedToPool = true;
    }

    TEST(pooled_ptr_tests, ReturnsToPoolOnReturnableObject)
    {
        bool returnedToPool = false;
        bool destructorInvoked = false;

        {
            auto pooled = make_pooled<MyReturnable>(
                returnedToPool,
                destructorInvoked);
        }

        ASSERT_TRUE(returnedToPool);
        ASSERT_FALSE(destructorInvoked);
    }
}