---- MODULE LogManager ----
EXTENDS TLC, Sequences, Integers

CONSTANT 
    Writes,
    PartitionIds

VARIABLE
    Log,
    Memory,
    Partitions,
    CommittedWrites,
    CurrentMemory,
    NextPartition,
    CurrentDiskPartitions

vars == <<
    Log,
    Memory,
    Partitions,
    CommittedWrites,
    CurrentMemory,
    NextPartition,
    CurrentDiskPartitions
>>

Max(S) == CHOOSE s \in S : ~ \E t \in S : t > s
Min(S) == CHOOSE s \in S : ~ \E t \in S : t < s

Tables == { "Partitions", "Data" }

AppendLog(logEntries) ==
    Log' = Log \o logEntries

AllocatePartition(newPartition) ==
    /\  NextPartition = newPartition
    /\  NextPartition' = newPartition + 1

Init ==
    /\  Log = << >>
    /\  CurrentMemory = [table \in Tables |-> << >>]
    /\  Memory = [table \in Tables |-> << >>]
    /\  Partitions = [table \in Tables |-> << >>]
    /\  CommittedWrites = {}
    /\  NextPartition = Min(PartitionIds)
    /\  CurrentDiskPartitions = [table \in Tables |-> {}]

StartCheckpoint(table, newPartition) == 
    /\  AllocatePartition(newPartition)
    /\  \/  Memory[table] = << >>
        \/  Memory[table] # << >> /\ Memory[table][CurrentMemory[table]] # {}
    /\  Memory' = [Memory EXCEPT ![table] = newPartition :> {} @@ @]
    /\  CurrentMemory' = [CurrentMemory EXCEPT ![table] = newPartition]
    /\  AppendLog(<< [ Type |-> "CreateMemoryTable", Table |-> table, NewPartition |-> newPartition ] >>)
    /\  UNCHANGED << Partitions, CommittedWrites, CurrentDiskPartitions >>

Write(table, write, partition) ==
    /\  partition \in DOMAIN Memory[table]
    /\  partition = CurrentMemory[table]
    /\  Memory' = [Memory EXCEPT ![table][partition] = @ \union { write }]
    /\  AppendLog(<< [ Type |-> "Write", Table |-> table, Partition |-> partition, Value |-> write ] >>)
    /\  UNCHANGED << Partitions, CurrentMemory, NextPartition, CurrentDiskPartitions >>

WriteData(write, partition) ==
    /\  write \notin CommittedWrites 
    /\  CommittedWrites' = CommittedWrites \union { write }
    /\  Write("Data", write, partition)

CompleteCheckpoint(table, diskPartition) ==
    /\  \E memoryPartitions \in SUBSET DOMAIN Memory[table] :
        /\  memoryPartitions # { }
        /\  memoryPartitions \subseteq DOMAIN Memory[table]
        /\  AllocatePartition(diskPartition)
        /\  CurrentMemory[table] \notin memoryPartitions
        /\  LET writes ==  UNION { Memory[table][memoryPartition] : memoryPartition \in memoryPartitions } IN
            /\  writes # {}
            /\  CurrentDiskPartitions' = [CurrentDiskPartitions EXCEPT ![table] = @ \union { diskPartition }]
            /\  AppendLog(<< [ 
                    Type |-> "Checkpoint", 
                    Table |-> table, 
                    RemovedPartitions |-> memoryPartitions, 
                    DiskPartitions |-> CurrentDiskPartitions'[table] 
                ] >>)
            /\  Memory' = [Memory EXCEPT ![table] = [ partition \in DOMAIN Memory[table] \ memoryPartitions |-> Memory[table][partition]]]
            /\  Partitions' = [Partitions EXCEPT ![table] = diskPartition :> writes
                    @@ Partitions[table]
                ]
            /\  UNCHANGED << CommittedWrites, CurrentMemory >>

AllLogRecords == { Log[index] : index \in DOMAIN Log }

AllCreateMemoryTableLogRecords ==
    { logRecord \in AllLogRecords : logRecord.Type = "CreateMemoryTable" }

AllWriteLogRecords ==
    { logRecord \in AllLogRecords : logRecord.Type = "Write" }

AllCheckpointLogRecords ==
    { logRecord \in AllLogRecords : logRecord.Type = "Checkpoint" }

RECURSIVE ReplayLogEntry(_, _, _, _, _)

ReplayLogEntry(
    currentMemory,
    memory,
    nextPartition,
    currentDiskPartitions,
    logIndex
)  ==
    IF logIndex > Len(Log) THEN
        /\  CurrentMemory' = currentMemory
        /\  Memory' = memory
        /\  NextPartition' = nextPartition
        /\  CurrentDiskPartitions' = currentDiskPartitions
    ELSE
        LET logEntry == Log[logIndex] IN 
        IF logEntry.Type = "CreateMemoryTable" THEN
            ReplayLogEntry(
                [currentMemory EXCEPT ![logEntry.Table] = logEntry.NewPartition],
                [memory EXCEPT ![logEntry.Table] = logEntry.NewPartition :> {} @@ @],
                Max({nextPartition, logEntry.NewPartition}) + 1,
                currentDiskPartitions,
                logIndex + 1
            )
        ELSE IF logEntry.Type = "Write" THEN
            ReplayLogEntry(
                currentMemory,
                [memory EXCEPT ![logEntry.Table][logEntry.Partition] = memory[logEntry.Table][logEntry.Partition] \union { logEntry.Value }],
                nextPartition,
                currentDiskPartitions,
                logIndex + 1
            )
        ELSE IF logEntry.Type = "Checkpoint" THEN 
            ReplayLogEntry(
                currentMemory,
                [memory EXCEPT ![logEntry.Table] = [partition \in DOMAIN @ \ logEntry.RemovedPartitions |-> @[partition]]],
                Max({ nextPartition } \union logEntry.DiskPartitions) + 1,
                [currentDiskPartitions EXCEPT ![logEntry.Table] = logEntry.DiskPartitions],
                logIndex + 1
            )
        ELSE
            Assert(FALSE, "Invalid logEntry.Type")

Replay == 
    /\  ReplayLogEntry(
            [table \in Tables |-> << >>],
            [table \in Tables |-> << >>],
            Min(PartitionIds),
            [table \in Tables |-> {}],
            1
        )
    /\  UNCHANGED << CommittedWrites, Log, Partitions >>

Next ==
    \E  write \in Writes,
        partition \in PartitionIds,
        table \in Tables
        :
        \/  StartCheckpoint(table, partition)
        \/  WriteData(write, partition)
        \/  CompleteCheckpoint(table, partition)
        \/  Replay

Spec ==
    /\  Init
    /\  [][Next]_vars

Symmetry ==
    Permutations(Writes)

CanRead(write) ==
    \/  \E partition \in DOMAIN Memory["Data"] :
            write \in Memory["Data"][partition]
    \/  \E partition \in CurrentDiskPartitions["Data"] :
            write \in Partitions["Data"][partition]

CanAlwaysReadCommittedWrites ==
    \A write \in CommittedWrites :
        CanRead(write)

Alias ==
    [
        Log |-> Log,
        Memory |-> Memory,
        Partitions |-> Partitions,
        CommittedWrites |-> CommittedWrites,
        CurrentMemory |-> CurrentMemory,
        NextPartition |-> NextPartition,
        CurrentDiskPartitions |-> CurrentDiskPartitions
    ]

====
