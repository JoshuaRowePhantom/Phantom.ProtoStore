#pragma once

#include <functional>

namespace Phantom
{

template<
    typename TOperation,
    typename TConverter
>
class ArgumentConversionAdapter
{
    TOperation m_operation;
    TConverter m_converter;

public:
    ArgumentConversionAdapter(
        const TOperation& operation,
        const TConverter& converter
    ) : m_converter(converter)
    {}

    template<
        typename ... TArgs
    > decltype(auto) operator()(
        TArgs&& ... args
        )
    {
        return m_operation(
            m_converter(std::forward<TArgs>(args))...
        );
    }
};

class IdentityConverter
{
public:
    template<
        typename T
    > decltype(auto) operator()(
        T&& arg)
    {
        return std::forward<T>(arg);
    }
};

template<
    typename TConverter,
    typename ... TConverters
> class ArgumentConverter
    : public ArgumentConverter<TConverters...>
{
    TConverter m_converter;
public:
    ArgumentConverter(
        TConverter converter,
        TConverters ... converters
    ) : m_converter(converter),
        ArgumentConverter<TConverters...>(converters...)
    {}

    template<
        typename T
    > decltype(auto) operator()(
        T&& arg,
        typename std::enable_if<std::is_invocable_v<TConverter, T>, int>::type = 0
        )
    {
        return m_converter(std::forward<T>(arg));
    }

    template<
        typename T
    > decltype(auto) operator()(
        T&& arg,
        typename std::enable_if<!std::is_invocable_v<TConverter, T>, int>::type = 0
        )
    {
        return ArgumentConverter<TConverters...>::operator()(
            std::forward<T>(arg));
    }
};

template<
> class ArgumentConverter<IdentityConverter>
    : public IdentityConverter
{
public:
    ArgumentConverter(
        IdentityConverter
    )
    {}
};

template<
    typename TConverter
> class ArgumentConverter<TConverter>
    : public ArgumentConverter<TConverter, IdentityConverter>
{
public:
    ArgumentConverter(
        TConverter converter)
        : ArgumentConverter<TConverter, IdentityConverter>(
            converter,
            IdentityConverter()
            )
    {}
};
}
