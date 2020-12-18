#pragma once

namespace Phantom
{

struct move_and_copy_counter
{
    struct counts
    {
        mutable int m_movedCount;
        mutable int m_copiedCount;
        mutable int m_moveAssignedCount;
        mutable int m_copyAssignedCount;

        counts()
            :
            m_moveFromCount(0),
            m_copyFromCount(0),
            m_movedCount(0),
            m_copiedCount(0),
            m_moveAssignedFromCount(0),
            m_moveAssignedCount(0),
            m_copyAssignedFromCount(0),
            m_copyAssignedCount(0)
        {
        }
    };

    shared_ptr<counts> m_counts;

    move_and_copy_counter()
        :
        m_counts(std::make_shared<counts>())
    {}

    move_and_copy_counter(
        const move_and_copy_counter& other
    ) :
        m_counts(other.m_counts)
    {
        m_counts->m_copiedCount++;
        m_counts->m_copyFromCount++;
    }

    move_and_copy_counter(
        move_and_copy_counter&& other
    ) :
        m_counts(other.m_counts)
    {
        m_counts.m_movedCount++;
        other.m_counts.m_moveFromCount++;
    }

};
}