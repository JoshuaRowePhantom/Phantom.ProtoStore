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

ASSUME UserTables \intersect { "Tables", "Partitions" } = {}
Tables == UserTables \union { "Tables", "Partitions" }

Max(S) == CHOOSE s \in S : ~ \E t \in S : t > s
Min(S) == CHOOSE s \in S : ~ \E t \in S : t < s

NonExistentPartition == Min(PartitionIds) - 1

ReadTable(table, memory, currentDiskPartitions) ==
    UNION { memory[table][partition] : partition \in DOMAIN memory[table] }
    \union
    UNION { Partitions[table][partition] : partition \in currentDiskPartitions[table] }

ReadPartitions(table, memory, currentDiskPartitions) ==
    LET tableValues ==
        { 
            value \in ReadTable("Partitions", memory, currentDiskPartitions) : value.Table = table 
        }
        deletes ==
        {
            value \in tableValues : value.IsDelete = TRUE
        }
        withoutDeletes == tableValues \ deletes
        withoutDeletedValues == withoutDeletes \ { [delete EXCEPT !.IsDelete = TRUE] : delete \in deletes }
    IN
        withoutDeletedValues

CanReadEphemeral(table, write, memory, currentDiskPartitions) ==
    write \in ReadTable(table, memory, currentDiskPartitions)

CanRead(table, write) ==
    CanReadEphemeral(
        table,
        write,
        Memory,
        CurrentDiskPartitions
    )

CanWrite(table) ==
    CurrentMemory[table] \in DOMAIN Memory[table]

AppendLog(logEntries) ==
    Log' = Log \o logEntries

AllocatePartition(newPartition) ==
    /\  NextPartition = newPartition
    /\  NextPartition' = newPartition + 1

UpdateNextPartition(existingPartitions) ==
    NextPartition' = Max({ NextPartition } \union 
    {
        existingPartition + 1 : existingPartition \in existingPartitions
    })

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
    /\  CurrentReplayIndex = 1
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
    /\  AppendLog(<< [ 
                Type |-> "Write", 
                Table |-> table, 
                Partition |-> partition, 
                Value |-> write
            ] >>)
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
        /\  CanWrite("Partitions")
        /\  CurrentDiskPartitions' = [CurrentDiskPartitions EXCEPT ![table] = @ \union { diskPartition }]
        /\  LET writes ==  UNION { Memory[table][memoryPartition] : memoryPartition \in memoryPartitions }
                partitionsValue == [ Table |-> table, Partition |-> diskPartition, IsDelete |-> FALSE ]
                partitionsLogEntry == 
                    IF table # "Partitions"
                    THEN << >>
                    ELSE << [
                        Type |-> "PartitionsData",
                        DiskPartitions |-> CurrentDiskPartitions'["Partitions"]
                    ] >> IN
            /\  writes # {}
            /\  AppendLog(<< [
                    Type |-> "Write",
                    Table |-> "Partitions",
                    Partition |-> CurrentMemory["Partitions"],
                    Value |-> partitionsValue
                ], [ 
                    Type |-> "Checkpoint", 
                    Table |-> table, 
                    RemovedPartitions |-> memoryPartitions
                ] >> \o partitionsLogEntry)
            /\  LET updatedPartitionsMemory == [Memory EXCEPT 
                        ![table] = [ partition \in DOMAIN Memory[table] \ memoryPartitions |-> Memory[table][partition]]
                    ]
                    updatedPartitionsValueMemory == [updatedPartitionsMemory EXCEPT
                        !["Partitions"][CurrentMemory["Partitions"]] = @ \union { partitionsValue }
                    ]
                IN
                    Memory' = updatedPartitionsValueMemory
            /\  Partitions' = [Partitions EXCEPT ![table] = diskPartition :> writes
                    @@ Partitions[table]
                ]
            /\  UNCHANGED << CommittedWrites, CurrentMemory, CurrentReplayIndex, CurrentReplayPhase >>

RECURSIVE GetDeletedPartitionsValuesLogEntries(_, _, _)

GetDeletedPartitionsValuesLogEntries(table, removedPartitions, partitionsValuesSequence) ==
    IF removedPartitions = {} THEN partitionsValuesSequence ELSE 
    LET removedPartition == CHOOSE partition \in removedPartitions : TRUE IN
    GetDeletedPartitionsValuesLogEntries(table, removedPartitions \ { removedPartition }, partitionsValuesSequence \o <<
        [
            Type |-> "Write",
            Table |-> "Partitions",
            Partition |-> CurrentMemory["Partitions"],
            Value |-> [ Table |-> table, Partition |-> removedPartition, IsDelete |-> TRUE ]
        ]        
    >>)

Merge(table, mergedPartitions, diskPartition) ==
    /\  ~IsReplaying
    /\  mergedPartitions # {}
    /\  mergedPartitions \subseteq CurrentDiskPartitions[table]
    /\  AllocatePartition(diskPartition)
    /\  CanWrite("Partitions")
    /\  Partitions' = [Partitions EXCEPT ![table] =
            diskPartition :> UNION { Partitions[table][mergedPartition] : mergedPartition \in mergedPartitions }
            @@
            Partitions[table]
        ]
    /\  CurrentDiskPartitions' = [CurrentDiskPartitions EXCEPT ![table] = 
            (@ \ mergedPartitions) \union { diskPartition }]
    /\  LET
            addedPartitionsValue == [ Table |-> table, Partition |-> diskPartition, IsDelete |-> FALSE ]
            deletedPartitionsValuesLogEntries == GetDeletedPartitionsValuesLogEntries(table, mergedPartitions, << >>)
        IN
        /\  AppendLog(<< [
                    Type |-> "Checkpoint",
                    Table |-> table,
                    RemovedPartitions |-> mergedPartitions
                ] >>
            \o deletedPartitionsValuesLogEntries)
        /\  Memory' = [Memory EXCEPT !["Partitions"][CurrentMemory["Partitions"]] = 
                @ 
                \union 
                { deletedPartitionsValuesLogEntries[index].Value : index \in 1..Len(deletedPartitionsValuesLogEntries) }
                \union
                { addedPartitionsValue }
            ]
    /\  UNCHANGED << CommittedWrites, CurrentMemory, CurrentReplayIndex, CurrentReplayPhase >>

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
            /\  logEntry.Table \in UserTables => CheckTableExistence(logEntry.Table)
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
            /\  IF logEntry.Table # "Partitions" 
                THEN
                /\  UpdateNextPartition({ logEntry.Partition })
                /\  UNCHANGED << CurrentDiskPartitions >>
                ELSE
                LET tableWithUpdatedPartitions == logEntry.Value.Table
                    updatedPartitions == ReadPartitions(
                            logEntry.Value.Table,
                            Memory',
                            CurrentDiskPartitions)
                    diskPartitions == { partition.Partition : partition \in updatedPartitions } IN
                /\  tableWithUpdatedPartitions \in UserTables => CheckTableExistence(logEntry.Table)
                /\  CurrentDiskPartitions' = [
                        CurrentDiskPartitions EXCEPT ![tableWithUpdatedPartitions] = diskPartitions
                    ]
                /\  UpdateNextPartition(
                        { logEntry.Partition }
                        \union
                        diskPartitions
                    )
                /\  UNCHANGED CurrentDiskPartitions
        ELSE IF logEntry.Type = "Checkpoint" THEN 
            /\  logEntry.Table \in UserTables => CheckTableExistence(logEntry.Table)
            /\  CurrentMemory' =
                    IF
                        /\  CurrentMemory[logEntry.Table] \in logEntry.RemovedPartitions
                    THEN
                        [CurrentMemory EXCEPT ![logEntry.Table] = NonExistentPartition]
                    ELSE
                        CurrentMemory
            /\  Memory' =
                    [Memory EXCEPT ![logEntry.Table] = [partition \in DOMAIN @ \ logEntry.RemovedPartitions |-> @[partition]]]
            /\  UNCHANGED << CurrentDiskPartitions, NextPartition >>
        ELSE IF logEntry.Type = "PartitionsData" THEN
            /\  CurrentDiskPartitions' = [CurrentDiskPartitions EXCEPT !["Partitions"] = logEntry.DiskPartitions]
            /\  UpdateNextPartition(
                    logEntry.DiskPartitions)
            /\  UNCHANGED << CurrentMemory, Memory >>
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

IsPhase1LogEntry(logEntry) ==
    \/  logEntry.Table = "Tables"
    \/  /\  logEntry.Table = "Partitions" 
        /\  logEntry.Type = "Write"
        /\  logEntry.Value.Table = "Tables"
    \/  /\  logEntry.Type = "PartitionsData"

IsPhase2LogEntry(logEntry) ==
    /\  ~ IsPhase1LogEntry(logEntry)
    /\  logEntry.Table = "Partitions"

IsPhase3LogEntry(logEntry) ==
    /\  ~ IsPhase1LogEntry(logEntry)
    /\  ~ IsPhase2LogEntry(logEntry)

ReplayLogEntry_Phase1 ==
    /\  CurrentReplayPhase = 1
    /\  CurrentReplayIndex <= Len(Log)
    /\  CurrentReplayIndex' = CurrentReplayIndex + 1
    /\  UNCHANGED << CommittedWrites, Log, Partitions, CurrentReplayPhase >>
    /\  LET logEntry == Log[CurrentReplayIndex] IN
        /\  IF IsPhase1LogEntry(logEntry)
            THEN Replay_LogEntry(logEntry)
            ELSE
            UNCHANGED << CurrentMemory, Memory, CurrentDiskPartitions, NextPartition >>

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
        /\  IF IsPhase2LogEntry(logEntry)
            THEN Replay_LogEntry(logEntry)
            ELSE
            UNCHANGED << CurrentMemory, Memory, CurrentDiskPartitions, NextPartition >>

FinishReplay_Phase2 ==
    /\  CurrentReplayPhase = 2
    /\  CurrentReplayIndex > Len(Log)
    /\  CurrentReplayIndex' = 1
    /\  CurrentReplayPhase' = 3
    /\  UNCHANGED << CommittedWrites, Memory, CurrentMemory, CurrentDiskPartitions, Partitions, NextPartition, Log >>

ReplayLogEntry_Phase3 ==
    /\  CurrentReplayPhase = 3
    /\  CurrentReplayIndex <= Len(Log)
    /\  CurrentReplayIndex' = CurrentReplayIndex + 1
    /\  UNCHANGED << CommittedWrites, Log, Partitions, CurrentReplayPhase >>
    /\  LET logEntry == Log[CurrentReplayIndex] IN
        /\  IF IsPhase3LogEntry(logEntry)
            THEN Replay_LogEntry(logEntry)
            ELSE
            UNCHANGED << CurrentMemory, Memory, CurrentDiskPartitions, NextPartition >>

FinishReplay_Phase3 ==
    /\  CurrentReplayPhase = 3
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
    /\  Log[1].Type = "PartitionsData" =>
        \E index \in 2..Len(Log):
            /\  Log[index].Type = "PartitionsData"
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
        \/  FinishReplay_Phase1
        \/  ReplayLogEntry_Phase2
        \/  FinishReplay_Phase2
        \/  ReplayLogEntry_Phase3
        \/  FinishReplay_Phase3

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
        CurrentReplayIndex |-> CurrentReplayIndex,
        TablePartitions |-> [
            table \in Tables |-> ReadPartitions(table, Memory, CurrentDiskPartitions)
        ],
        TableRows |-> [
            table \in Tables |-> ReadTable(table, Memory, CurrentDiskPartitions)
        ]
    ]

====
