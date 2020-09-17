#pragma once

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
    > auto operator()(
        TArgs...&& args
        )
    {
        return m_operation(
            converter(std::forward<TArgs>(args))...
        );
    }
};

template<
    typename TConverter1,
    typename TConverter2
> class ArgumentConverter
{
public:

};

}
