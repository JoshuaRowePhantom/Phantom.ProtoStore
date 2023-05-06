#pragma once

namespace Phantom
{

void hash_combine(size_t& seed, const auto& value)
{
    std::hash<std::remove_cvref_t<decltype(value)>> hasher;
    if constexpr (sizeof(size_t) >= 8)
    {
        seed ^= hasher(value) + 0x9ddfea08eb382d69ULL + (seed << 17) + (seed >> 47);
    }
    else
    {
        seed ^= hasher(value) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
}

size_t hash(const auto& value, const auto&... values)
{
    std::hash<std::remove_cvref_t<decltype(value)>> hasher;
    size_t seed = hasher(value);
    (hash_combine(seed, values), ...);
    return seed;
}

}
