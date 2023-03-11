#pragma once

#include "StandardTypes.h"
#include "MemoryTable.h"

namespace Phantom::ProtoStore
{

struct PartitionCheckpointStartKey
{
    ProtoValue Key;
    SequenceNumber WriteSequenceNumber;
};

class IPartition
{
public:
    virtual task<size_t> GetRowCount(
    ) = 0;

    virtual task<ExtentOffset> GetApproximateDataSize(
    ) = 0;

    // Do a point read of the requested key.
    // This method should return 0 or 1 ResultRow.
    // If the partition has a deleted row, it should be returned,
    // so that the WriteSequenceNumber of the delete can be compared
    // with the WriteSequenceNumber the same key from other sources.
    virtual row_generator Read(
        SequenceNumber readSequenceNumber,
        const ProtoValue& key,
        ReadValueDisposition readValueDisposition
    ) = 0;

    // Do an enumeration of the requested range
    // This method should return 1 ResultRow for each key in the range,
    // that being the row with the largest writeSequenceNumber <= the readSequenceNumber.
    // If the partition has a deleted row, it should be returned,
    // so that the WriteSequenceNumber of the delete can be compared
    // with the WriteSequenceNumber the same key from other sources.
    virtual row_generator Enumerate(
        SequenceNumber readSequenceNumber,
        KeyRangeEnd low,
        KeyRangeEnd high,
        ReadValueDisposition readValueDisposition
    ) = 0;

    // Begin enumerating from the requested checkpoint start key,
    // returning all versions of all rows.
    virtual row_generator Checkpoint(
        optional<PartitionCheckpointStartKey> startKey
    ) = 0;

    virtual SequenceNumber GetLatestSequenceNumber(
    ) = 0;

    // Given a key, return a SequenceNumber of any write conflicts.
    virtual task<optional<SequenceNumber>> CheckForWriteConflict(
        SequenceNumber readSequenceNumber,
        SequenceNumber writeSequenceNumber,
        const ProtoValue& key
    ) = 0;

    virtual task<IntegrityCheckErrorList> CheckIntegrity(
        const IntegrityCheckError& errorPrototype
    ) = 0;
};
}