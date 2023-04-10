---- MODULE UnresolvedTransactionsTracker ----
EXTENDS Sequences, TLC, Integers

CONSTANT Transactions, Tables, CheckpointNumbers, PartitionNumbers, Keys, SequenceNumberType

VARIABLE 
    Log,
    Memory,
    Partitions,
    NextSequenceNumber

Max(S) == CHOOSE s \in S : ~ \E t \in S : t > s
Min(S) == CHOOSE s \in S : ~ \E t \in S : t < s

IsIndexSequence(S) == 
    /\  S \in SUBSET Nat 
    /\  1..Max(S) = S

ASSUME IsIndexSequence(Transactions)
ASSUME IsIndexSequence(Tables)
ASSUME IsIndexSequence(CheckpointNumbers)
ASSUME IsIndexSequence(PartitionNumbers)
ASSUME IsIndexSequence(Keys)
ASSUME IsIndexSequence(SequenceNumberType)

vars == << Log, Memory, Partitions, NextSequenceNumber >>

Nil == << >>
MapType(KeyType, ValueType) == UNION { [keys -> ValueType] : keys \in SUBSET KeyType }
NillableTransactionIdType == { Nil } \union Transactions
RowType == [Key : Keys, Transaction : NillableTransactionIdType, SequenceNumber : SequenceNumberType ]
TableType == SUBSET RowType
MemoryTablesType == MapType(CheckpointNumbers, TableType)
MemoryType == [Tables -> MemoryTablesType]
CurrentCheckpoint(table) == Max(DOMAIN Memory[table])
CurrentMemoryTable(table) == Memory[table][CurrentCheckpoint(table)]
LoggedWriteType == [
    Type : { "Write" },
    Table : Tables,
    CheckpointNumber : CheckpointNumbers,
    Row : RowType
]
LoggedCheckpointType == [
    Type : { "Checkpoint" },
    Table : Tables,
    CheckpointNumbers : SUBSET CheckpointNumbers
]
LogRecordType == LoggedWriteType \union LoggedCheckpointType
LogType == Seq(LogRecordType)

PartitionsType ==
    [ Tables -> MapType(PartitionNumbers, TableType) ]

TypeOk ==
    /\  Log \in LogType
    /\  Memory \in MemoryType
    /\  Partitions \in PartitionsType
    /\  NextSequenceNumber \in SequenceNumberType
    
NextPartitionNumber == Max(
    { 1 } \union UNION { DOMAIN Partitions[table] : table \in Tables }
) + 1

Init ==
    /\  Log = << >>
    /\  Memory = [table \in Tables |-> << {} >>]
    /\  Partitions = [table \in Tables |-> << >>]
    /\  NextSequenceNumber \in SequenceNumberType

Write(key, transaction, table) ==
    /\  NextSequenceNumber' \in SequenceNumberType
    /\  NextSequenceNumber' > NextSequenceNumber
    /\  LET row ==  [Key |-> key, Transaction |-> transaction, SequenceNumber |-> NextSequenceNumber] IN
        /\  Memory' = [Memory EXCEPT ![table][CurrentCheckpoint(table)] = @ \union { row }]
        /\  Log' = Log \o << [Type |-> "Write", Table |-> table, Row |-> row, CheckpointNumber |-> CurrentCheckpoint(table)] >>
    /\  UNCHANGED << Partitions >>

StartCheckpoint(table) ==
    /\  \E nextCheckpointNumber \in CheckpointNumbers :
        /\  nextCheckpointNumber = CurrentCheckpoint(table) + 1
        /\  Memory[table][CurrentCheckpoint(table)] # { }
        /\  Memory' = [Memory EXCEPT ![table] = nextCheckpointNumber :> << >> @@ @]
        /\  UNCHANGED << Log, NextSequenceNumber, Partitions >>

Outcome(transaction) == "Committed"

ReadKey(key, sources) ==
    LET allValues == UNION { sources }
        matchingValues == { value \in allValues : value.Key = key }
        nonAbortedValues == { value \in matchingValues : Outcome(value.Transaction) # "Aborted" } IN
    IF nonAbortedValues = {} 
    THEN << >>
    ELSE CHOOSE highest \in nonAbortedValues : ~\E lower \in nonAbortedValues : lower.SequenceNumber > highest.SequenceNumber

CopyRowsToNewTable(sources) ==
    LET nonAbortedRows ==         
        {
            nonAbortedRow \in UNION sources
            :
            Outcome(nonAbortedRow) # "Aborted"
        }
        IN
    {
        highestRow \in nonAbortedRows :
            ~ \E lowerRow \in nonAbortedRows :
                /\  lowerRow.Key = highestRow.Key
                /\  lowerRow.SequenceNumber > highestRow.SequenceNumber
    }

Checkpoint(table) ==
    /\  LET checkpointNumbers == { checkpointNumber \in DOMAIN Memory[table] : checkpointNumber # Max(DOMAIN Memory[table]) }
            partitionNumber == NextPartitionNumber IN
        /\  checkpointNumbers # { }
        /\  partitionNumber \in PartitionNumbers
        /\  Partitions' = [Partitions EXCEPT ![table] =
                partitionNumber :> CopyRowsToNewTable(
                    { Memory[table][checkpointNumber] : checkpointNumber \in checkpointNumbers }
                ) @@ @
            ]
        /\  Memory' = [Memory EXCEPT ![table] =
                [checkpointNumber \in (DOMAIN Memory[table] \ checkpointNumbers) |-> Memory[table][checkpointNumber]]
            ]
        /\  Log' = Log \o << [Type |-> "Checkpoint", Table |-> table, CheckpointNumbers |-> checkpointNumbers ] >>
        /\  UNCHANGED << NextSequenceNumber >>

Truncate ==
    /\  Len(Log) > 0
    /\  Log[1].Type = "Write" =>
            Log[1].CheckpointNumber \notin DOMAIN Memory[Log[1].Table]
    /\  Log' = Tail(Log)
    /\  UNCHANGED << Memory, Partitions, NextSequenceNumber >>

LogRecords == { Log[index] : index \in DOMAIN Log }
LoggedWrites(table) == { logRecord \in LogRecords : 
    /\  logRecord.Type = "Write"
    /\  logRecord.Table = table
}
LoggedWritesForCheckpoint(table, checkpointNumber) == {
    loggedWrite \in LoggedWrites(table) :
        loggedWrite.CheckpointNumber = checkpointNumber
}
LoggedCheckpoints(table) == {
    logRecord \in LogRecords :
        /\  logRecord.Type = "Checkpoint"
        /\  logRecord.Table = table
}
LoggedCheckpointNumbers(table) == UNION 
{
    loggedCheckpoint.CheckpointNumbers : loggedCheckpoint \in LoggedCheckpoints(table)
}

Replay ==
    /\  UNCHANGED << Log, Partitions >>
    /\  LET memoryTablesFromWrites == 
            [ table \in Tables |->
                [ checkpointNumber \in { write.CheckpointNumber : write \in LoggedWrites(table) } |-> 
                    {
                        loggedWrite.Row : loggedWrite \in LoggedWritesForCheckpoint(table, checkpointNumber)
                    }
                ]
            ] IN 
        LET memoryTablesWithoutCheckpoints ==
            [ table \in Tables |->
                [
                    checkpointNumber \in ((DOMAIN memoryTablesFromWrites[table]) \ LoggedCheckpointNumbers(table)) 
                    |->
                    memoryTablesFromWrites[table][checkpointNumber]
                ]
            ] IN 
        LET memoryTablesWithActiveTables ==
            [ table \in Tables |->
                IF memoryTablesWithoutCheckpoints[table] # << >> 
                THEN 
                    memoryTablesWithoutCheckpoints[table]
                ELSE
                    [
                        checkpointNumber \in { Max(DOMAIN memoryTablesFromWrites[table] \union { 1 }) } |-> { }
                    ]
            ] IN 
        /\  Memory' = memoryTablesWithActiveTables
        /\  NextSequenceNumber' \in SequenceNumberType
        /\  ~ \E loggedWrite \in LogRecords :
            /\  loggedWrite.Type = "Write"
            /\  loggedWrite.Row.SequenceNumber >= NextSequenceNumber'

Merge(table) ==
    /\  \E sourcePartitions \in SUBSET DOMAIN Partitions[table] :
        \E nextPartitionNumber \in PartitionNumbers :
        /\  sourcePartitions # {}
        /\  nextPartitionNumber = NextPartitionNumber
        /\  Partitions' = [Partitions EXCEPT ![table] =
                nextPartitionNumber :> CopyRowsToNewTable(
                    { Partitions[table][partitionNumber] : partitionNumber \in sourcePartitions }
                ) @@
                [
                    partitionNumber \in (DOMAIN Partitions[table] \ sourcePartitions) |-> Partitions[table][partitionNumber]
                ]
            ]
        /\  UNCHANGED << Memory, NextSequenceNumber, Log >>

Next ==
    \E key \in Keys, transaction \in Transactions, table \in Tables :
    \/  Write(key, transaction, table)
    \/  StartCheckpoint(table)
    \/  Checkpoint(table)
    \/  Merge(table)
    \/  Replay

Spec ==
    Init /\ [][Next]_vars

Alias ==
[
    Log |-> Log,
    Memory |-> Memory,
    Partitions |-> Partitions,
    NextSequenceNumber |-> NextSequenceNumber
]
====
