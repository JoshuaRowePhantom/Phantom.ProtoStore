---- MODULE LogManager_Correctness ----
EXTENDS LogManager, FiniteSets
CONSTANT t1, w1

Constraint ==
    Cardinality(UNION { DOMAIN Partitions[table] : table \in Tables }) +
    Cardinality(UNION { DOMAIN Memory[table] : table \in Tables }) < 8
====
