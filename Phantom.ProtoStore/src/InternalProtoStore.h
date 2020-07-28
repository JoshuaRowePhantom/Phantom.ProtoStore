#pragma once

#include "StandardTypes.h"

namespace Phantom::ProtoStore
{

class IInternalOperation
    :
    public IOperationTransaction
{
public:
    virtual LogRecord& LogRecord(
    ) = 0;
};

typedef std::function<task<>(Operation*)> InternalOperationVisitor;

class IInternalProtoStore
    :
    public IProtoStore
{
public:
    virtual task<OperationResult> InternalExecuteOperation(
        const BeginTransactionRequest beginRequest,
        InternalOperationVisitor visitor
    ) = 0;

    virtual task<ExtentNumber> AllocateDataExtent(
    ) = 0;

    virtual task<> LogCommitDataExtent(
        LogRecord& logRecord,
        ExtentNumber extentNumber
    ) = 0;
};

}