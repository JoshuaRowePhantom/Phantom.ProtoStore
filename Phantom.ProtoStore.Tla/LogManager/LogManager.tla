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
        deletesAsAdds ==
        {
            [delete EXCEPT !.IsDelete = FALSE] : delete \in deletes
        }
        withoutDeletes == tableValues \ deletes
        withoutDeletedValues == withoutDeletes \ deletesAsAdds
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
    \* Don't start checkpointing tables that are empty.
    /\  CurrentMemory[table] \in DOMAIN Memory[table] => 
        Memory[table][CurrentMemory[table]] # {}
    /\  
        \* Don't start checkpointing the Partitions table if it
        \* doesn't have any data other than for the Partitions table.
        \* Technically this isn't necessary, but cuts down on the state space.
        (
            /\  table = "Partitions" 
            /\  CurrentMemory[table] \in DOMAIN Memory[table]
        ) => (
            \E value \in Memory[table][CurrentMemory[table]] :
                \/  value.Table # "Partitions"
        )
    /\  AllocatePartition(newPartition)
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
        /\  CurrentMemory[table] \notin memoryPartitions
        /\  CanWrite("Partitions")
        /\  LET writes ==  UNION { Memory[table][memoryPartition] : memoryPartition \in memoryPartitions }
                partitionsValue == [ Table |-> table, Partition |-> diskPartition, IsDelete |-> FALSE ] IN 
            /\  AllocatePartition(diskPartition)
            /\  CurrentDiskPartitions' = [CurrentDiskPartitions EXCEPT ![table] = @ \union { diskPartition }]
            /\  LET 
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
    /\  CanWrite("Partitions")
    /\  AllocatePartition(diskPartition)
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
            \o << [
                Type |-> "Write",
                Table |-> "Partitions",
                Partition |-> CurrentMemory["Partitions"],
                Value |-> addedPartitionsValue
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
    /\  ~ \E logIndex \in 1..Len(Log) :
        LET logEntry == Log[logIndex] IN
        /\  logEntry.Type = "Write"
        /\  logEntry.Table = "Partitions"
        /\  \/  /\  logEntry.Value.Table \in UserTables
                /\  logEntry.Value.IsDelete = FALSE
            \/  /\  logEntry.Value.Table \notin UserTables
        /\  logEntry.Value.Partition = partition
    /\  Partitions' = [Partitions EXCEPT ![table] = 
            [
                existingPartition \in DOMAIN Partitions[table] \ { partition } |-> Partitions[table][existingPartition]
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
                    /\  CurrentDiskPartitions' = [
                            CurrentDiskPartitions EXCEPT ![tableWithUpdatedPartitions] = diskPartitions
                        ]
                    /\  logEntry.Value.Table \in UserTables => CheckTableExistence(logEntry.Value.Table)
                    /\  UpdateNextPartition(
                            { logEntry.Partition }
                            \union
                            diskPartitions
                        )
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
            LET currentDiskPartitionsWithUpdatedPartitionsTable == [
                CurrentDiskPartitions EXCEPT !["Partitions"] = logEntry.DiskPartitions
            ] IN
            \* Because the partitions table has changed in a dramatic way,
            \* all tables may have changed their current partitions.
            \* Therefore, reread all the partitions for all tables.
            /\  CurrentDiskPartitions' = [
                    table \in Tables |->
                        IF table = "Partitions" THEN logEntry.DiskPartitions ELSE
                        { partition.Partition : partition \in ReadPartitions(table, Memory, currentDiskPartitionsWithUpdatedPartitionsTable) }
                ]
            /\  UpdateNextPartition(
                    logEntry.DiskPartitions)
            /\  UNCHANGED << CurrentMemory, Memory >>
        ELSE
            Assert(FALSE, "Invalid log entry type")

StartReplay ==
    \* Technically, we could start replay at any time,
    \* but this reduces the number of states that TLC needs to check,
    \* and by checking for deadlock we guarantee that log replay can always complete.
    /\  CurrentReplayPhase = 0

    /\  CurrentReplayIndex' = 1
    /\  CurrentReplayPhase' = 1
    /\  CurrentMemory' = [table \in Tables |-> NonExistentPartition]
    /\  Memory' = EmptyPartitionSet
    /\  NextPartition' = Min(PartitionIds)
    /\  CurrentDiskPartitions' = [table \in Tables |-> {}]
    /\  UNCHANGED << CommittedWrites, Log, Partitions >>

ReplayLogEntry ==
    /\  CurrentReplayPhase = 1
    /\  CurrentReplayIndex <= Len(Log)
    /\  CurrentReplayIndex' = CurrentReplayIndex + 1
    /\  UNCHANGED << CommittedWrites, Log, Partitions, CurrentReplayPhase >>
    /\  LET logEntry == Log[CurrentReplayIndex] IN
        Replay_LogEntry(logEntry)

FinishReplay ==
    /\  CurrentReplayPhase = 1
    /\  CurrentReplayIndex > Len(Log)
    /\  CurrentReplayIndex' = 1
    /\  CurrentReplayPhase' = 0
    /\  UNCHANGED << CommittedWrites, Memory, CurrentMemory, CurrentDiskPartitions, Partitions, NextPartition, Log >>

TruncateLog ==
    /\  ~IsReplaying
    /\  \E truncateIndex \in 2..Len(Log) :
        \A checkIndex \in 1 .. truncateIndex - 1 :
        /\  Log[truncateIndex].Type = "PartitionsData"
        /\  Log[checkIndex].Type = "Write" =>
            \E index \in checkIndex..Len(Log):
                /\  Log[index].Type = "Checkpoint"
                /\  Log[index].Table = Log[checkIndex].Table
                /\  Log[checkIndex].Partition \in Log[index].RemovedPartitions
        /\  Log[checkIndex].Type = "Checkpoint" =>
            \E index \in checkIndex..Len(Log):
                /\  Log[index].Type = "PartitionsData"
        /\  Log' = SubSeq(Log, truncateIndex, Len(Log))
    /\  UNCHANGED << CommittedWrites, Partitions, CurrentMemory, CurrentDiskPartitions, NextPartition, Memory, CurrentReplayIndex, CurrentReplayPhase >>

RemoveSomePartition ==
    \/  \E  table \in Tables :
        \E  partition \in DOMAIN Partitions[table] :
        \/  RemovePartition(table, partition)

MergeSomePartition ==
    \/  \E  table \in Tables,
            partition \in PartitionIds :
        \E  partitions \in SUBSET DOMAIN Partitions[table]
        :
        \/  Merge(table, partitions, partition)
        
Next ==
    \/  TruncateLog
    \/  StartReplay
    \/  ReplayLogEntry
    \/  FinishReplay
    \/  \E  write \in Writes,
        partition \in PartitionIds,
        table \in Tables
        :
        \/  CreateTable(table)
        \/  StartCheckpoint(table, partition)
        \/  WriteData(table, write, partition)
        \/  CompleteCheckpoint(table, partition)
    \/  RemoveSomePartition
    \/  MergeSomePartition

Spec ==
    /\  Init
    /\  [][Next]_vars

Symmetry ==
    Permutations(Writes) \union Permutations(UserTables)

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
        CurrentReplayPhase |-> CurrentReplayPhase,
        TablePartitions |-> [
            table \in Tables |-> ReadPartitions(table, Memory, CurrentDiskPartitions)
        ],
        TableRows |-> [
            table \in Tables |-> ReadTable(table, Memory, CurrentDiskPartitions)
        ]
    ]

\* Use these operators to determine if RemovePartition is being
\* used meaningfully.
FailIfRemovedUserPartition == [][~(
    \E table \in UserTables :
    \E partition \in PartitionIds :
        /\  partition \in DOMAIN Partitions[table]
        /\  partition \notin DOMAIN Partitions[table]'
)]_vars

FailIfRemovedTablesPartition == [][~(
    \E partition \in PartitionIds :
        /\  partition \in DOMAIN Partitions["Tables"]
        /\  partition \notin DOMAIN Partitions["Tables"]'
)]_vars

FailIfRemovedPartitionsPartition == [][~(
    \E partition \in PartitionIds :
        /\  partition \in DOMAIN Partitions["Partitions"]
        /\  partition \notin DOMAIN Partitions["Partitions"]'
)]_vars

DoesntHaveTwoPartitionsDataItems == 
    ~ \E index1 \in 1..Len(Log),
         index2 \in 1..Len(Log) :
         /\ index1 # index2
         /\ Log[index1].Type = "PartitionsData"
         /\ Log[index2].Type = "PartitionsData"

NoPartitionIsEmpty == 
    ~ 
    \E table \in Tables :
    \E partition \in DOMAIN Partitions[table] : 
        Partitions[table][partition] = {}

====
