#pragma once

#include "StandardIncludes.h"
//#include "PhantomScalableGrpcInternal.pb.h"
//
//namespace Phantom::Scalable::Grpc::Internal
//{
//
//DataComponentString ToDataComponentStringInternal(
//    DataComponentString& string
//)
//{
//    return string;
//}
//
//void ToDataComponentInternal(
//    DataComponent* component,
//    string value
//)
//{
//    component->set_string(move(value));
//}
//
//void ToDataComponentInternal(
//    DataComponent* component,
//    google::protobuf::int64 value
//)
//{
//    component->set_int64(value);
//}
//
//void ToDataComponentInternal(
//    DataComponent* component,
//    const google::protobuf::Message& value
//)
//{
//    component->mutable_any()->PackFrom(
//        value);
//}
//
//void ToDataComponentInternal(
//    DataComponent* component,
//    DataComponent value
//)
//{
//    *component = move(value);
//}
//
//template<
//    typename TComponent,
//    typename ... TComponents
//>
//void ToDataComponentStringInternal(
//    DataComponentString& dataComponentString,
//    TComponent&& component,
//    TComponents&& ... components
//)
//{
//    ToDataComponentInternal(
//        *dataComponentString.add_component(),
//        std::forward<TComponent>(component)
//    );
//
//    ToDataComponentStringInternal(
//        dataComponentString,
//        std::forward<TComponents>(components)...
//    );
//}
//
//template<
//    typename ... TComponents
//> DataComponentString ToDataComponentString(
//    TComponents&& ... components)
//{
//    DataComponentString dataComponentString;
//    return ToDataComponentStringInternal(
//        dataComponentString,
//        std::forward<TComponents>(components)...
//    );
//}

}