#pragma once
#include <limits>

namespace Phantom::ProtoStore
{

template<
    typename Source
>
struct numeric_cast_converter
{
    Source value;

    template<
        typename Destination
    > operator Destination() const
    {
        using source_limits = std::numeric_limits<Source>;
        using destination_limits = std::numeric_limits<Destination>;
        
        if constexpr (destination_limits::max() >= source_limits::max()
            && destination_limits::min() <= source_limits::min())
        {
            return static_cast<Destination>(value);
        }

        if constexpr (source_limits::is_signed &&
            !destination_limits::is_signed)
        {
            if (value < 0)
            {
                throw std::range_error("value < 0 assigned to unsigned");
            }
        }

        auto result = static_cast<Destination>(value);
        if (result != value)
        {
            throw std::range_error("value != original after assignment");
        }

        return result;
    }
};

auto numeric_cast(auto value)
{
    return numeric_cast_converter{ value };
}

}