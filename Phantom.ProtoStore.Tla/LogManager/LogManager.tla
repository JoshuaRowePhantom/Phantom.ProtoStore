---- MODULE LogManager ----
EXTENDS TLC, Sequences, Integers

CONSTANT 
    Writes,
    PartitionIds

VARIABLE
    LogExtents,
    Memory,
    Partitions,
    CommittedWrites,
    CurrentMemory,
    NextPartition

vars == <<
    LogExtents,
    Memory,
    Partitions,
    CommittedWrites,
    CurrentMemory,
    NextPartition
>>

Max(S) == CHOOSE s \in S : ~ \E t \in S : t > s
Min(S) == CHOOSE s \in S : ~ \E t \in S : t < s

Tables == { "Partitions", "Data" }

CurrentLogExtent == Max(DOMAIN(LogExtents))

AppendLog(logEntries) ==
    LogExtents' = [LogExtents EXCEPT ![CurrentLogExtent] = @ \o logEntries]

AllocatePartition(newPartition) ==
    /\  NextPartition = newPartition
    /\  NextPartition' = newPartition + 1

Init ==
    /\  LogExtents = << << >> >>
    /\  CurrentMemory = [table \in Tables |-> << >>]
    /\  Memory = [table \in Tables |-> << >>]
    /\  Partitions = [table \in Tables |-> << >>]
    /\  CommittedWrites = {}
    /\  NextPartition = Min(PartitionIds)

StartCheckpoint(table, newPartition) == 
    /\  AllocatePartition(newPartition)
    /\  \/  Memory[table] = << >>
        \/  Memory[table] # << >> /\ Memory[table][CurrentMemory[table]] # {}
    /\  Memory' = [Memory EXCEPT ![table] = newPartition :> {} @@ @]
    /\  CurrentMemory' = [CurrentMemory EXCEPT ![table] = newPartition]
    /\  AppendLog(<< [ Type |-> "CreateMemoryTable", Table |-> table, NewPartition |-> newPartition ] >>)
    /\  UNCHANGED << Partitions, CommittedWrites >>

Write(table, write, partition) ==
    /\  partition \in DOMAIN Memory[table]
    /\  partition = CurrentMemory[table]
    /\  Memory' = [Memory EXCEPT ![table][partition] = @ \union { write }]
    /\  AppendLog(<< [ Type |-> "Write", Table |-> table, Partition |-> partition, Value |-> write ] >>)
    /\  UNCHANGED << Partitions, CurrentMemory, NextPartition >>

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
            /\  AppendLog(<< [ Type |-> "Checkpoint", Table |-> table, Partitions |-> memoryPartitions, NewPartition |-> diskPartition ] >>)
            /\  Memory' = [Memory EXCEPT ![table] = [ partition \in DOMAIN Memory[table] \ memoryPartitions |-> Memory[table][partition]]]
            /\  Partitions' = [Partitions EXCEPT ![table] = diskPartition :> writes
                    @@ Partitions[table]
                ]
            /\  UNCHANGED << CommittedWrites, CurrentMemory >>

AllLogRecords ==
    UNION { { LogExtents[extent][index] : index \in DOMAIN LogExtents[extent] } : extent \in DOMAIN LogExtents }

AllCreateMemoryTableLogRecords ==
    { logRecord \in AllLogRecords : logRecord.Type = "CreateMemoryTable" }

AllWriteLogRecords ==
    { logRecord \in AllLogRecords : logRecord.Type = "Write" }

AllCheckpointLogRecords ==
    { logRecord \in AllLogRecords : logRecord.Type = "Checkpoint" }

RECURSIVE FindLastCreateMemoryTable(_, _, _, _)
FindLastCreateMemoryTable(table, extent, index, value) == 
    IF extent \notin DOMAIN LogExtents THEN value
    ELSE IF index \notin DOMAIN LogExtents[extent] THEN FindLastCreateMemoryTable(table, extent + 1, 1, value)
    ELSE 
        LET newValue == 
            IF
                /\  LogExtents[extent][index].Type = "CreateMemoryTable" 
                /\  LogExtents[extent][index].Table = table
            THEN LogExtents[extent][index].NewPartition 
            ELSE value
        IN FindLastCreateMemoryTable(table, extent, index + 1, newValue)

Replay ==
    LET uncheckpointedMemory == [
        table \in Tables |->
            [
                partition \in { 
                        logRecord.NewPartition : logRecord \in { forTable \in AllCreateMemoryTableLogRecords : forTable.Table = table }
                    } |-> 
                    { selectedWrite.Value : selectedWrite \in { write \in AllWriteLogRecords : 
                        /\  write.Partition = partition
                        /\  write.Table = table
                    } }
            ]
        ]
        checkpointedPartitions == [
            table \in Tables |->
                UNION { logRecord.Partitions : logRecord \in { checkpoint \in AllCheckpointLogRecords : checkpoint.Table = table } }
        ]
        createdPartitions == { 
            newPartitionRecord.NewPartition : newPartitionRecord \in AllCreateMemoryTableLogRecords \union AllCheckpointLogRecords
        }
    IN 
        /\  Memory' = [table \in Tables |-> 
                [ partition \in DOMAIN uncheckpointedMemory[table] \ checkpointedPartitions[table] |-> uncheckpointedMemory[table][partition] ]
            ]
        /\  CurrentMemory' = [ table \in Tables |-> FindLastCreateMemoryTable(table, 1, 1, {}) ]
        /\  NextPartition' = IF createdPartitions = {} THEN Min(PartitionIds) ELSE Max(createdPartitions) + 1
        /\  UNCHANGED << CommittedWrites, LogExtents, Partitions >>

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

Alias ==
    [
        LogExtents |-> LogExtents,
        Memory |-> Memory,
        Partitions |-> Partitions,
        CommittedWrites |-> CommittedWrites,
        CurrentMemory |-> CurrentMemory
    ]

====
