---- MODULE LogManager ----
EXTENDS TLC, Sequences, Integers

CONSTANT 
    Writes,
    PartitionIds,
    UserTables

VARIABLE
    Log,
    Memory,
    Partitions,
    CommittedWrites,
    CurrentMemory,
    NextPartition,
    CurrentDiskPartitions,
    CurrentReplayIndex,
    CurrentReplayPhase

vars == <<
    Log,
    Memory,
    Partitions,
    CommittedWrites,
    CurrentMemory,
    NextPartition,
    CurrentDiskPartitions,
    CurrentReplayIndex,
    CurrentReplayPhase
>>

Tables == UserTables \union { "Tables" }

Max(S) == CHOOSE s \in S : ~ \E t \in S : t > s
Min(S) == CHOOSE s \in S : ~ \E t \in S : t < s

NonExistentPartition == Min(PartitionIds) - 1

CanReadEphemeral(memory, currentDiskPartitions, table, write) ==
    \/  \E partition \in DOMAIN memory[table] :
            write \in Memory[table][partition]
    \/  \E partition \in currentDiskPartitions[table] :
            write \in Partitions[table][partition]

CanRead(table, write) ==
    CanReadEphemeral(
        Memory,
        CurrentDiskPartitions,
        table,
        write
    )

AppendLog(logEntries) ==
    Log' = Log \o logEntries

AllocatePartition(newPartition) ==
    /\  NextPartition = newPartition
    /\  NextPartition' = newPartition + 1

IsReplaying == CurrentReplayPhase > 0

EmptyPartitionSet == [table \in Tables |-> << >>]

Init ==
    /\  Log = << >>
    /\  CurrentMemory = [table \in Tables |-> NonExistentPartition]
    /\  Memory = EmptyPartitionSet
    /\  Partitions = EmptyPartitionSet
    /\  CommittedWrites = << >>
    /\  NextPartition = Min(PartitionIds)
    /\  CurrentDiskPartitions = [table \in Tables |-> {}]
    /\  CurrentReplayIndex = 0
    /\  CurrentReplayPhase = 0

StartCheckpoint(table, newPartition) == 
    /\  ~IsReplaying
    /\  AllocatePartition(newPartition)
    /\  \/  Memory[table] = << >>
        \/  /\  CurrentMemory[table] \in DOMAIN Memory[table]
            /\  Memory[table][CurrentMemory[table]] # {}
    /\  Memory' = [Memory EXCEPT ![table] = newPartition :> {} @@ @]
    /\  CurrentMemory' = [CurrentMemory EXCEPT ![table] = newPartition]
    /\  UNCHANGED << Log, Partitions, CommittedWrites, CurrentDiskPartitions, CurrentReplayIndex, CurrentReplayPhase >>

Write(table, write, partition) ==
    /\  ~IsReplaying
    /\  partition \in DOMAIN Memory[table]
    /\  partition = CurrentMemory[table]
    /\  Memory' = [Memory EXCEPT ![table][partition] = @ \union { write }]
    /\  AppendLog(<< [ Type |-> "Write", Table |-> table, Partition |-> partition, Value |-> write ] >>)
    /\  UNCHANGED << Partitions, CurrentMemory, NextPartition, CurrentDiskPartitions, CurrentReplayIndex, CurrentReplayPhase >>

CreateTable(table) ==
    /\  ~IsReplaying
    /\  table \in UserTables
    /\  ~ CanRead("Tables", table)
    /\  Write("Tables", table, CurrentMemory["Tables"])
    /\  UNCHANGED << CommittedWrites, CurrentReplayIndex, CurrentReplayPhase >>

WriteData(table, write, partition) ==
    /\  ~IsReplaying
    /\  table \in UserTables
    /\  CanRead("Tables", table)
    /\  write \notin DOMAIN CommittedWrites 
    /\  CommittedWrites' = write :> table @@ CommittedWrites
    /\  Write(table, write, partition)

CompleteCheckpoint(table, diskPartition) ==
    /\  ~IsReplaying
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
            /\  UNCHANGED << CommittedWrites, CurrentMemory, CurrentReplayIndex, CurrentReplayPhase >>

Merge(table, mergedPartitions, diskPartition) ==
    /\  ~IsReplaying
    /\  mergedPartitions # {}
    /\  mergedPartitions \subseteq CurrentDiskPartitions[table]
    /\  AllocatePartition(diskPartition)
    /\  Partitions' = [Partitions EXCEPT ![table] =
            diskPartition :> UNION { Partitions[table][mergedPartition] : mergedPartition \in mergedPartitions }
            @@
            Partitions[table]
        ]
    /\  CurrentDiskPartitions' = [CurrentDiskPartitions EXCEPT ![table] = 
            (@ \ mergedPartitions) \union { diskPartition }]
    /\  AppendLog(<< [
                Type |-> "Checkpoint",
                Table |-> table,
                RemovedPartitions |-> mergedPartitions,
                DiskPartitions |-> CurrentDiskPartitions'[table]
            ] >>)
    /\  UNCHANGED << Memory, CommittedWrites, CurrentMemory, CurrentReplayIndex, CurrentReplayPhase >>

RemovePartition(table, partition) ==
    /\  ~IsReplaying
    /\  partition \notin CurrentDiskPartitions[table]
    /\  partition \in DOMAIN Partitions[table]
    /\  Partitions' = [Partitions EXCEPT ![table] = 
            [
                existingPartition \in DOMAIN @ \ { partition } |-> @[existingPartition]
            ]
        ]
    /\  UNCHANGED << Memory, CommittedWrites, CurrentMemory, CurrentDiskPartitions, NextPartition, Log, CurrentReplayIndex, CurrentReplayPhase >>

CheckTableExistence(table) ==
    /\  Assert(
            table \in UserTables => CanRead("Tables", table),
            "Table metadata not found")

Replay_LogEntry(logEntry) ==
        IF logEntry.Type = "Write" THEN
            /\  CurrentMemory' = 
                    IF logEntry.Partition \in DOMAIN Memory[logEntry.Table] 
                    THEN CurrentMemory
                    ELSE [CurrentMemory EXCEPT ![logEntry.Table] = logEntry.Partition]
            /\  Memory' =
                    [Memory EXCEPT ![logEntry.Table] =
                        [
                            @ @@ logEntry.Partition :> {}
                            EXCEPT ![logEntry.Partition]
                            =
                            @ \union { logEntry.Value }
                        ]
                    ]
            /\  NextPartition' =
                    Max({NextPartition, logEntry.Partition + 1})
            /\  UNCHANGED CurrentDiskPartitions
        ELSE IF logEntry.Type = "Checkpoint" THEN 
            /\  CurrentMemory' =
                    IF
                        /\  CurrentMemory[logEntry.Table] \in logEntry.RemovedPartitions
                    THEN
                        [CurrentMemory EXCEPT ![logEntry.Table] = NonExistentPartition]
                    ELSE
                        CurrentMemory
            /\  Memory' =
                    [Memory EXCEPT ![logEntry.Table] = [partition \in DOMAIN @ \ logEntry.RemovedPartitions |-> @[partition]]]
            /\  NextPartition' =
                    Max({ NextPartition } \union {
                        diskPartition + 1 : diskPartition \in logEntry.DiskPartitions
                        })
            /\  CurrentDiskPartitions' =
                    [CurrentDiskPartitions EXCEPT ![logEntry.Table] = logEntry.DiskPartitions]
        ELSE
            TRUE

StartReplay ==
    /\  CurrentReplayIndex' = 1
    /\  CurrentReplayPhase' = 1
    /\  CurrentMemory' = [table \in Tables |-> NonExistentPartition]
    /\  Memory' = EmptyPartitionSet
    /\  NextPartition' = Min(PartitionIds)
    /\  CurrentDiskPartitions' = [table \in Tables |-> {}]
    /\  UNCHANGED << CommittedWrites, Log, Partitions >>

ReplayLogEntry_Phase1 ==
    /\  CurrentReplayPhase = 1
    /\  CurrentReplayIndex <= Len(Log)
    /\  CurrentReplayIndex' = CurrentReplayIndex + 1
    /\  UNCHANGED << CommittedWrites, Log, Partitions, CurrentReplayPhase >>
    /\  LET logEntry == Log[CurrentReplayIndex] IN 
        IF logEntry.Table = "Tables" 
        THEN Replay_LogEntry(logEntry)
        ELSE 
        /\  UNCHANGED << Memory, CurrentMemory, CurrentDiskPartitions, NextPartition >>

FinishReplay_Phase1 ==
    /\  CurrentReplayPhase = 1
    /\  CurrentReplayIndex > Len(Log)
    /\  CurrentReplayIndex' = 1
    /\  CurrentReplayPhase' = 2
    /\  UNCHANGED << CommittedWrites, Memory, CurrentMemory, CurrentDiskPartitions, Partitions, NextPartition, Log >>

ReplayLogEntry_Phase2 ==
    /\  CurrentReplayPhase = 2
    /\  CurrentReplayIndex <= Len(Log)
    /\  CurrentReplayIndex' = CurrentReplayIndex + 1
    /\  UNCHANGED << CommittedWrites, Log, Partitions, CurrentReplayPhase >>
    /\  LET logEntry == Log[CurrentReplayIndex] IN 
        IF logEntry.Table # "Tables" 
        THEN 
        /\  Replay_LogEntry(logEntry)
        /\  CheckTableExistence(logEntry.Table)
        ELSE 
        /\  UNCHANGED << Memory, CurrentMemory, CurrentDiskPartitions, NextPartition >>

FinishReplay_Phase2 ==
    /\  CurrentReplayPhase = 2
    /\  CurrentReplayIndex > Len(Log)
    /\  CurrentReplayIndex' = 1
    /\  CurrentReplayPhase' = 0
    /\  UNCHANGED << CommittedWrites, Memory, CurrentMemory, CurrentDiskPartitions, Partitions, NextPartition, Log >>

TruncateLog ==
    /\  ~IsReplaying
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
    /\  UNCHANGED << CommittedWrites, Partitions, CurrentMemory, CurrentDiskPartitions, NextPartition, Memory, CurrentReplayIndex, CurrentReplayPhase >>

Next ==
    \E  write \in Writes,
        partition \in PartitionIds,
        partitions \in SUBSET PartitionIds,
        table \in Tables
        :
        \/  CreateTable(table)
        \/  StartCheckpoint(table, partition)
        \/  WriteData(table, write, partition)
        \/  CompleteCheckpoint(table, partition)
        \/  TruncateLog
        \/  Merge(table, partitions, partition)
        \/  RemovePartition(table, partition)
        \/  StartReplay
        \/  ReplayLogEntry_Phase1
        \/  ReplayLogEntry_Phase2
        \/  FinishReplay_Phase1
        \/  FinishReplay_Phase2

Spec ==
    /\  Init
    /\  [][Next]_vars

Symmetry ==
    Permutations(Writes)

CanAlwaysReadCommittedWrites ==
    ~ IsReplaying => \A write \in DOMAIN CommittedWrites :
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
        CanRead |-> [ write \in DOMAIN CommittedWrites |-> CanRead(CommittedWrites[write], write) ],
        CanReadMetadata |-> [ userTable \in UserTables |-> CanRead("Tables", userTable) ],
        CurrentReplayIndex |-> CurrentReplayIndex
    ]

====
