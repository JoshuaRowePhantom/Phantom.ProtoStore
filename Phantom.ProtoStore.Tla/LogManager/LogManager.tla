---- MODULE LogManager ----
EXTENDS TLC, Sequences, Integers

CONSTANT 
    Writes,
    PartitionIds,
    Tables

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

NonExistentPartition == Min(PartitionIds) - 1

AppendLog(logEntries) ==
    Log' = Log \o logEntries

AllocatePartition(newPartition) ==
    /\  NextPartition = newPartition
    /\  NextPartition' = newPartition + 1

Init ==
    /\  Log = << >>
    /\  CurrentMemory = [table \in Tables |-> NonExistentPartition]
    /\  Memory = [table \in Tables |-> << >>]
    /\  Partitions = [table \in Tables |-> << >>]
    /\  CommittedWrites = << >>
    /\  NextPartition = Min(PartitionIds)
    /\  CurrentDiskPartitions = [table \in Tables |-> {}]

StartCheckpoint(table, newPartition) == 
    /\  AllocatePartition(newPartition)
    /\  \/  Memory[table] = << >>
        \/  /\  CurrentMemory[table] \in DOMAIN Memory[table]
            /\  Memory[table][CurrentMemory[table]] # {}
    /\  Memory' = [Memory EXCEPT ![table] = newPartition :> {} @@ @]
    /\  CurrentMemory' = [CurrentMemory EXCEPT ![table] = newPartition]
    /\  UNCHANGED << Log, Partitions, CommittedWrites, CurrentDiskPartitions >>

Write(table, write, partition) ==
    /\  partition \in DOMAIN Memory[table]
    /\  partition = CurrentMemory[table]
    /\  Memory' = [Memory EXCEPT ![table][partition] = @ \union { write }]
    /\  AppendLog(<< [ Type |-> "Write", Table |-> table, Partition |-> partition, Value |-> write ] >>)
    /\  UNCHANGED << Partitions, CurrentMemory, NextPartition, CurrentDiskPartitions >>

WriteData(table, write, partition) ==
    /\  write \notin DOMAIN CommittedWrites 
    /\  CommittedWrites' = write :> table @@ CommittedWrites
    /\  Write(table, write, partition)

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

Merge(table, mergedPartitions, diskPartition) ==
    /\  mergedPartitions # {}
    /\  mergedPartitions \subseteq CurrentDiskPartitions[table]
    /\  AllocatePartition(diskPartition)
    /\  Partitions' = [Partitions EXCEPT ![table] =
            diskPartition :> UNION { Partitions[table][mergedPartition] : mergedPartition \in mergedPartitions }
            @@
            [
                oldPartition \in DOMAIN @ \ mergedPartitions |-> @[oldPartition]
            ]
        ]
    /\  CurrentDiskPartitions' = [CurrentDiskPartitions EXCEPT ![table] = 
            (@ \ mergedPartitions) \union { diskPartition }]
    /\  AppendLog(<< [
                Type |-> "Checkpoint",
                Table |-> table,
                RemovedPartitions |-> mergedPartitions,
                DiskPartitions |-> CurrentDiskPartitions'[table]
            ] >>)
    /\  UNCHANGED << Memory, CommittedWrites, CurrentMemory >>

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
        IF logEntry.Type = "Write" THEN
            ReplayLogEntry(
                
                IF logEntry.Partition \in DOMAIN memory[logEntry.Table] 
                THEN currentMemory
                ELSE [currentMemory EXCEPT ![logEntry.Table] = logEntry.Partition],

                [memory EXCEPT ![logEntry.Table] =
                    [
                        @ @@ logEntry.Partition :> {}
                        EXCEPT ![logEntry.Partition]
                        =
                        @ \union { logEntry.Value }
                    ]
                ],
                Max({nextPartition, logEntry.Partition + 1}),
                currentDiskPartitions,
                logIndex + 1
            )
        ELSE IF logEntry.Type = "Checkpoint" THEN 
            ReplayLogEntry(
                IF
                    /\  currentMemory[logEntry.Table] \in logEntry.RemovedPartitions
                THEN
                    [currentMemory EXCEPT ![logEntry.Table] = NonExistentPartition]
                ELSE
                    currentMemory,

                [memory EXCEPT ![logEntry.Table] = [partition \in DOMAIN @ \ logEntry.RemovedPartitions |-> @[partition]]],
                Max({ nextPartition } \union {
                    diskPartition + 1 : diskPartition \in logEntry.DiskPartitions
                    }),
                [currentDiskPartitions EXCEPT ![logEntry.Table] = logEntry.DiskPartitions],
                logIndex + 1
            )
        ELSE
            Assert(FALSE, "Invalid logEntry.Type")

Replay == 
    /\  ReplayLogEntry(
            [table \in Tables |-> NonExistentPartition],
            [table \in Tables |-> << >>],
            Min(PartitionIds),
            [table \in Tables |-> {}],
            1
        )
    /\  UNCHANGED << CommittedWrites, Log, Partitions >>

TruncateLog ==
    /\  Log # << >>
    /\  Log' = Tail(Log)
    /\  Log[1].Type = "Write" =>
        \E index \in 2..Len(Log):
            /\  Log[index].Type = "Checkpoint"
            /\  Log[index].Table = Log[1].Table
            /\  Log[1].Partition \in Log[index].RemovedPartitions
    /\  Log[1].Type = "Checkpoint" =>
        \E index \in 2..Len(Log):
            /\  Log[index].Type = "Checkpoint"
            /\  Log[index].Table = Log[1].Table
    /\  UNCHANGED << CommittedWrites, Partitions, CurrentMemory, CurrentDiskPartitions, NextPartition, Memory >>

Next ==
    \E  write \in Writes,
        partition \in PartitionIds,
        partitions \in SUBSET PartitionIds,
        table \in Tables
        :
        \/  StartCheckpoint(table, partition)
        \/  WriteData(table, write, partition)
        \/  CompleteCheckpoint(table, partition)
        \/  Replay
        \/  TruncateLog
        \/  Merge(table, partitions, partition)

Spec ==
    /\  Init
    /\  [][Next]_vars

Symmetry ==
    Permutations(Writes)

CanRead(table, write) ==
    \/  \E partition \in DOMAIN Memory[table] :
            write \in Memory[table][partition]
    \/  \E partition \in CurrentDiskPartitions[table] :
            write \in Partitions[table][partition]

CanAlwaysReadCommittedWrites ==
    \A write \in DOMAIN CommittedWrites :
        CanRead(CommittedWrites[write], write)

Alias ==
    [
        Log |-> Log,
        Memory |-> Memory,
        Partitions |-> Partitions,
        CommittedWrites |-> CommittedWrites,
        CurrentMemory |-> CurrentMemory,
        NextPartition |-> NextPartition,
        CurrentDiskPartitions |-> CurrentDiskPartitions,
        CanRead |-> [ write \in DOMAIN CommittedWrites |-> CanRead(CommittedWrites[write], write) ]
    ]

====
