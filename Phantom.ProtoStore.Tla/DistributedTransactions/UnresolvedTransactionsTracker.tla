---- MODULE UnresolvedTransactionsTracker ----
EXTENDS Sequences, TLC, Integers

CONSTANT 
    Transactions,
    PartitionNumbers,
    Keys

VARIABLE 
    Partitions,
    StartedTransactions,
    WrittenKeys,
    TransactionReferences,
    TransactionOutcomes,
    MergingPartitions

vars == << Partitions, WrittenKeys, StartedTransactions, TransactionReferences, TransactionOutcomes, MergingPartitions >>

Max(S) == CHOOSE s \in S : ~ \E t \in S : t > s
Min(S) == CHOOSE s \in S : ~ \E t \in S : t < s

IsIndexSequence(S) == 
    /\  S \in SUBSET Nat 
    /\  1..Max(S) = S

Outcomes == { "Aborted", "Committed" }

Outcome(nillableTransaction) ==
    IF nillableTransaction = << >> THEN "Committed"
    ELSE IF \E transactionOutcome \in TransactionOutcomes : transactionOutcome.Transaction = nillableTransaction[1]
    THEN (CHOOSE transactionOutcome \in TransactionOutcomes : transactionOutcome.Transaction = nillableTransaction[1]).Outcome
    ELSE "Committed"

Init ==
    /\  TransactionOutcomes = {}
    /\  TransactionReferences = {}
    /\  Partitions = << >>
    /\  StartedTransactions = [ transaction \in Transactions |-> "NotStarted" ]
    /\  WrittenKeys = [ key \in Keys |-> << >> ]
    /\  MergingPartitions = [ partition \in PartitionNumbers |-> << >> ]

WriteReference(transaction, partition) ==
    /\  partition \in DOMAIN Partitions
    /\  MergingPartitions[partition] = << >>
    /\  StartedTransactions[transaction] = "Unknown"
    /\  TransactionReferences' = TransactionReferences \union { [ Transaction |-> transaction, Partition |-> partition ] }
    /\  UNCHANGED << Partitions, WrittenKeys, StartedTransactions, TransactionOutcomes, MergingPartitions >>

StartTransaction(transaction) ==
    /\  StartedTransactions[transaction] = "NotStarted"
    /\  StartedTransactions' = [ StartedTransactions EXCEPT ![transaction] = "Unknown" ]
    /\  TransactionOutcomes' = TransactionOutcomes \union { [ Transaction |-> transaction, Outcome |-> "Unknown" ] }
    /\  UNCHANGED << Partitions, WrittenKeys, TransactionReferences, MergingPartitions >>

Write(key, transaction, partition) ==
    /\  partition \in DOMAIN Partitions
    /\  MergingPartitions[partition] = << >>
    /\  StartedTransactions[transaction] = "Unknown"
    /\  [ Transaction |-> transaction, Partition |-> partition ] \in TransactionReferences
    /\  [ Transaction |-> transaction, Outcome |-> "Unknown" ] \in TransactionOutcomes
    /\  WrittenKeys[key] = << >>
    /\  Partitions' = [Partitions EXCEPT ![partition] = @ \union { [Key |-> key, Transaction |-> << transaction >>] }]
    /\  WrittenKeys' = [WrittenKeys EXCEPT ![key] = << transaction >>]
    /\  UNCHANGED << StartedTransactions, TransactionReferences, TransactionOutcomes, MergingPartitions >>

CollectReference(transaction, partition) ==
    /\  partition \notin DOMAIN Partitions
    /\  [ Transaction |-> transaction, Partition |-> partition ] \in TransactionReferences
    /\  TransactionReferences' = TransactionReferences \ { [ Transaction |-> transaction, Partition |-> partition ] }
    /\  UNCHANGED << Partitions, WrittenKeys, StartedTransactions, TransactionOutcomes, MergingPartitions >>

CollectOutcome(transaction) ==
    /\  \E outcome \in TransactionOutcomes :
        outcome.Transaction = transaction
    /\  ~ \E reference \in TransactionReferences :
        reference.Transaction = transaction
    /\  TransactionOutcomes' = {
            outcome \in TransactionOutcomes : outcome.Transaction # transaction
        }
    /\  UNCHANGED << Partitions, WrittenKeys, StartedTransactions, TransactionReferences, MergingPartitions >>

StartPartition(newPartition) ==
    /\  newPartition \notin DOMAIN Partitions
    /\  ~ \E reference \in TransactionReferences : reference.Partition = newPartition
    /\  Partitions' = 
            newPartition :> {}
            @@
            Partitions
    /\  UNCHANGED << WrittenKeys, StartedTransactions, TransactionReferences, TransactionOutcomes, MergingPartitions >>

StartMerge(sourcePartition, destinationPartition) ==
    /\  MergingPartitions[sourcePartition] = << >>
    /\  MergingPartitions[destinationPartition] = << >>
    /\  sourcePartition # destinationPartition
    /\  sourcePartition \in DOMAIN Partitions
    /\  destinationPartition \in DOMAIN Partitions
    /\  MergingPartitions' = [MergingPartitions EXCEPT ![sourcePartition] = << destinationPartition >>]
    /\  UNCHANGED << WrittenKeys, StartedTransactions, TransactionOutcomes, TransactionReferences, Partitions >>

MergeReference(key, transaction, sourcePartition, destinationPartition) ==
    /\  sourcePartition # destinationPartition
    /\  MergingPartitions[sourcePartition] = << destinationPartition >>
    /\  [ Key |-> key, Transaction |-> transaction ] \in Partitions[sourcePartition]
    /\  Outcome(<< transaction >>) = "Unknown"
    /\  TransactionReferences' = TransactionReferences \union { [
            Transaction |-> transaction,
            Partition |-> destinationPartition
        ] }
    /\  UNCHANGED << WrittenKeys, StartedTransactions, TransactionOutcomes, Partitions, MergingPartitions >>

Merge(key, sourcePartition, destinationPartition) ==
    /\  MergingPartitions[sourcePartition] = << destinationPartition >>
    /\  \E value \in Partitions[sourcePartition] : value.Key = key
    /\  LET transaction == (CHOOSE value \in Partitions[sourcePartition] : value.Key = key).Transaction IN
        /\  Outcome(transaction) = "Unknown" => 
            [ Transaction |-> transaction[1], Partition |-> destinationPartition] \in TransactionReferences
        /\  Partitions' = [Partitions EXCEPT 
                ![sourcePartition] = { value \in Partitions[sourcePartition] : value.Key # key },
                ![destinationPartition] = Partitions[destinationPartition] \union 
                    {
                        value \in 
                        {
                            [ 
                                Key |-> key, 
                                Transaction |-> IF Outcome(transaction) = "Committed" THEN << >> ELSE transaction
                            ]
                        }
                        :
                        Outcome(value.Transaction) # "Aborted"
                    }
                ]
    /\  UNCHANGED << WrittenKeys, StartedTransactions, TransactionOutcomes, TransactionReferences, MergingPartitions >>

RemovePartition(oldPartition) ==
    /\  MergingPartitions[oldPartition] # << >>
    /\  ~ \E partition \in DOMAIN Partitions : MergingPartitions[partition] = << oldPartition >>
    /\  Partitions[oldPartition] = {}
    /\  MergingPartitions' = [MergingPartitions EXCEPT ![oldPartition] = << >>]
    /\  Partitions' = [ partition \in DOMAIN Partitions \ { oldPartition } |-> Partitions[partition] ]
    /\  UNCHANGED << WrittenKeys, StartedTransactions, TransactionReferences, TransactionOutcomes >>

Resolve(transaction, outcome) ==
    /\  StartedTransactions[transaction] = "Unknown"
    /\  StartedTransactions' = [StartedTransactions EXCEPT ![transaction] = outcome]
    /\  TransactionOutcomes' = { transactionOutcomes \in TransactionOutcomes : transactionOutcomes.Transaction # transaction } \union {
            [ Transaction |-> transaction, Outcome |-> outcome ]
        }
    /\  UNCHANGED << WrittenKeys, TransactionReferences, Partitions, MergingPartitions >>

Next ==
    \E  sourcePartition \in PartitionNumbers,
        destinationPartition \in PartitionNumbers,
        key \in Keys,
        transaction \in Transactions,
        outcome \in Outcomes
        :
        \/  WriteReference(transaction, sourcePartition)
        \/  StartTransaction(transaction)
        \/  Write(key, transaction, sourcePartition)
        \/  CollectReference(transaction, sourcePartition)
        \/  CollectOutcome(transaction)
        \/  StartPartition(sourcePartition)
        \/  StartMerge(sourcePartition, destinationPartition)
        \/  Merge(key, sourcePartition, destinationPartition)
        \/  RemovePartition(sourcePartition)
        \/  Resolve(transaction, outcome)

ReadKey(key) ==
    {
        value \in UNION { Partitions[partition] : partition \in DOMAIN Partitions }
        :
        /\  value.Key = key
        /\  Outcome(value.Transaction) \in { "Committed", "Unknown" }
    }

Spec ==
    /\  Init
    /\  [][Next]_vars

Fairness ==
    /\  \A  transaction \in Transactions,
            outcome \in Outcomes :
        SF_vars(
            \/  Resolve(transaction, outcome)
        )
    /\  SF_vars(
        \E  transaction \in Transactions,
            sourcePartition \in PartitionNumbers :
                \/  CollectOutcome(transaction)
                \/  CollectReference(transaction, sourcePartition)
        )
    /\  \A key \in Keys :
        SF_vars(
            \E  sourcePartition \in PartitionNumbers,
                destinationPartition \in PartitionNumbers :
            \/  Merge(key, sourcePartition, destinationPartition)
        )
    /\  \A sourcePartition \in PartitionNumbers :
        /\  SF_vars(
                \/  RemovePartition(sourcePartition)
            )
        /\  SF_vars(
                \E  destinationPartition \in PartitionNumbers :
                \/  StartMerge(sourcePartition, destinationPartition)
            )
    /\  WF_vars(
            \E  sourcePartition \in PartitionNumbers :
            \/  StartPartition(sourcePartition)
        )

SpecWithFairness ==
    /\  Spec
    /\  Fairness

Consistent ==
    \A key \in Keys :
        WrittenKeys[key] # << >> =>
        LET outcome == StartedTransactions[WrittenKeys[key][1]] IN
            /\  outcome = "Aborted" =>
                ReadKey(key) = {}
            /\  outcome = "Unknown" =>
                \A value \in ReadKey(key) : Outcome(value.Transaction) = "Unknown"
            /\  outcome = "Committed" =>
                { Outcome(value.Transaction) : value \in ReadKey(key) } = { "Committed" }

Symmetry == Permutations(Keys) \union Permutations(PartitionNumbers) \union Permutations(Transactions)

TransactionsAreCollected ==
    <>[](
        /\  TransactionOutcomes = {}
        /\  TransactionReferences = {}
        )

Alias ==
    [
        WrittenKeys |-> WrittenKeys,
        Partitions |-> Partitions,
        TransactionOutcomes |-> TransactionOutcomes,
        TransactionReferences |-> TransactionReferences,
        StartedTransactions |-> StartedTransactions,
        MergingPartitions |-> MergingPartitions
    ]

====
