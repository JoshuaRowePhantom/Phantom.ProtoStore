------------------------------ MODULE SkipList ------------------------------

EXTENDS Sequences, Integers
CONSTANTS max_level, key_space

NodeType == [
    Value : key_space \union { {} },
    Next : Seq(Nat)
]

NodesType == Seq(NodeType)

(* --algorithm SkipList

variables 
    Nodes = <<[
        Value |-> {},
        Next |-> << 0 >>
    ]>>, 
    SearcherResult = {}, 
    InsertedValues = {},
    FindNodeResults = [ process \in ProcSet |-> 0 ];

procedure FindNode(
    CurrentNode,
    Value)
variable Level = 0;
begin
FindNode_Start:
    Level := Len(Nodes[CurrentNode].Next);
    
    FindNodeLoop:
    while (Level > 0)
    do
        if (/\  Nodes[CurrentNode].Next[Level] /= 0
            /\  Nodes[Nodes[CurrentNode].Next[Level]].Value <= Value) then
            call FindNode(
                Nodes[CurrentNode].Next[Level],
                Value);
            return;
        end if;
    end while;
    
    FindNodeResults[self] := [
        Current |-> CurrentNode,
        Next |-> Nodes[CurrentNode].Next[1]
    ];
    
end procedure;

process inserter \in (key_space \X { 1, 2 })
variables NewNode = 0, NewNodeValue = {};
begin
    Inserter_Start:
    call FindNode(
        Nodes[1],
        Key);
    
    Inserter_MakeNode:
    if (Nodes[FindNodeResults[self].Current].Value /= self[1]) then
        
        NewNodeValue := [
            Value: self[1],
            Previous: FindNodeResults[self].Current,
            Next: Nodes[FindNodeResults[self].Next[1]]
        ];
        
        if NewNode = 0 then
            Nodes := Nodes \o NewNodeValue;
            NewNode := Len(Nodes);
        else
            Nodes[NewNode] := NewNodeValue;
        end if;
        
        Inserter_UpdatePreviousNode:
        if (Nodes[FindNodeResults[self].Current].Next[1] /= FindNodeResults[self].Next) then
            goto Inserter_Start;
        else
            Nodes[FindNodeResults[self].Current].Next[1] := NewNode;
            InsertedValues := InsertedValues \union { NewNode.Value }
        end if;
        
    end if;
    
end process

process searcher = "searcher"
variables Key
begin
    Searcher_Start:
    with key \in key_space do
        Key := key;
    end with;
    
    call FindNode(
        Nodes[1],
        Key);
end process

end algorithm
*)
\* BEGIN TRANSLATION - the hash of the PCal code: PCal-50880309eec455b559ff7d6effc84560
CONSTANT defaultInitValue
VARIABLES Nodes, SearcherResult, InsertedValues, FindNodeResults, pc, stack, 
          CurrentNode, Value, Level, NewNode, NewNodeValue, Key

vars == << Nodes, SearcherResult, InsertedValues, FindNodeResults, pc, stack, 
           CurrentNode, Value, Level, NewNode, NewNodeValue, Key >>

ProcSet == ((key_space \X { 1, 2 })) \cup {"searcher"}

Init == (* Global variables *)
        /\ Nodes = <<>>
        /\ SearcherResult = {}
        /\ InsertedValues = {}
        /\ FindNodeResults = [ process \in ProcSet |-> 0 ]
        (* Procedure FindNode *)
        /\ CurrentNode = [ self \in ProcSet |-> defaultInitValue]
        /\ Value = [ self \in ProcSet |-> defaultInitValue]
        /\ Level = [ self \in ProcSet |-> 0]
        (* Process inserter *)
        /\ NewNode = [self \in (key_space \X { 1, 2 }) |-> 0]
        /\ NewNodeValue = [self \in (key_space \X { 1, 2 }) |-> {}]
        (* Process searcher *)
        /\ Key = defaultInitValue
        /\ stack = [self \in ProcSet |-> << >>]
        /\ pc = [self \in ProcSet |-> CASE self \in (key_space \X { 1, 2 }) -> "Inserter_Start"
                                        [] self = "searcher" -> "Searcher_Start"]

FindNode_Start(self) == /\ pc[self] = "FindNode_Start"
                        /\ Level' = [Level EXCEPT ![self] = Len(Nodes[CurrentNode[self]].Next)]
                        /\ pc' = [pc EXCEPT ![self] = "FindNodeLoop"]
                        /\ UNCHANGED << Nodes, SearcherResult, InsertedValues, 
                                        FindNodeResults, stack, CurrentNode, 
                                        Value, NewNode, NewNodeValue, Key >>

FindNodeLoop(self) == /\ pc[self] = "FindNodeLoop"
                      /\ IF (Level[self] > 0)
                            THEN /\ IF (Nodes[Nodes[CurrentNode[self]].Next[Level[self]]].Value <= Value[self])
                                       THEN /\ /\ CurrentNode' = [CurrentNode EXCEPT ![self] = Nodes[CurrentNode[self]].Next[Level[self]]]
                                               /\ Value' = [Value EXCEPT ![self] = Value[self]]
                                            /\ Level' = [Level EXCEPT ![self] = 0]
                                            /\ pc' = [pc EXCEPT ![self] = "FindNode_Start"]
                                       ELSE /\ pc' = [pc EXCEPT ![self] = "FindNodeLoop"]
                                            /\ UNCHANGED << CurrentNode, Value, 
                                                            Level >>
                                 /\ UNCHANGED FindNodeResults
                            ELSE /\ FindNodeResults' = [FindNodeResults EXCEPT ![self] =                          [
                                                                                             Current |-> CurrentNode[self],
                                                                                             Next |-> Nodes[CurrentNode[self]].Next[1]
                                                                                         ]]
                                 /\ pc' = [pc EXCEPT ![self] = "Error"]
                                 /\ UNCHANGED << CurrentNode, Value, Level >>
                      /\ UNCHANGED << Nodes, SearcherResult, InsertedValues, 
                                      stack, NewNode, NewNodeValue, Key >>

FindNode(self) == FindNode_Start(self) \/ FindNodeLoop(self)

Inserter_Start(self) == /\ pc[self] = "Inserter_Start"
                        /\ /\ CurrentNode' = [CurrentNode EXCEPT ![self] = Nodes[1]]
                           /\ Value' = [Value EXCEPT ![self] = Key]
                           /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "FindNode",
                                                                    pc        |->  "Inserter_MakeNode",
                                                                    Level     |->  Level[self],
                                                                    CurrentNode |->  CurrentNode[self],
                                                                    Value     |->  Value[self] ] >>
                                                                \o stack[self]]
                        /\ Level' = [Level EXCEPT ![self] = 0]
                        /\ pc' = [pc EXCEPT ![self] = "FindNode_Start"]
                        /\ UNCHANGED << Nodes, SearcherResult, InsertedValues, 
                                        FindNodeResults, NewNode, NewNodeValue, 
                                        Key >>

Inserter_MakeNode(self) == /\ pc[self] = "Inserter_MakeNode"
                           /\ IF (Nodes[FindNodeResults[self].Current].Value /= self[1])
                                 THEN /\ NewNodeValue' = [NewNodeValue EXCEPT ![self] =                 [
                                                                                            Value: self[1],
                                                                                            Previous: FindNodeResults[self].Current,
                                                                                            Next: Nodes[FindNodeResults[self].Next]
                                                                                        ]]
                                      /\ IF NewNode[self] = 0
                                            THEN /\ Nodes' = Nodes \o NewNodeValue'[self]
                                                 /\ NewNode' = [NewNode EXCEPT ![self] = Len(Nodes')]
                                            ELSE /\ Nodes' = [Nodes EXCEPT ![NewNode[self]] = NewNodeValue'[self]]
                                                 /\ UNCHANGED NewNode
                                      /\ pc' = [pc EXCEPT ![self] = "Inserter_UpdatePreviousNode"]
                                 ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                                      /\ UNCHANGED << Nodes, NewNode, 
                                                      NewNodeValue >>
                           /\ UNCHANGED << SearcherResult, InsertedValues, 
                                           FindNodeResults, stack, CurrentNode, 
                                           Value, Level, Key >>

Inserter_UpdatePreviousNode(self) == /\ pc[self] = "Inserter_UpdatePreviousNode"
                                     /\ IF (Nodes[FindNodeResults[self].Current].Next[1] /= FindNodeResults[self].Next)
                                           THEN /\ pc' = [pc EXCEPT ![self] = "Inserter_Start"]
                                                /\ Nodes' = Nodes
                                           ELSE /\ Nodes' = [Nodes EXCEPT ![FindNodeResults[self].Current].Next[1] = NewNode[self]]
                                                /\ pc' = [pc EXCEPT ![self] = "Done"]
                                     /\ UNCHANGED << SearcherResult, 
                                                     InsertedValues, 
                                                     FindNodeResults, stack, 
                                                     CurrentNode, Value, Level, 
                                                     NewNode, NewNodeValue, 
                                                     Key >>

inserter(self) == Inserter_Start(self) \/ Inserter_MakeNode(self)
                     \/ Inserter_UpdatePreviousNode(self)

Searcher_Start == /\ pc["searcher"] = "Searcher_Start"
                  /\ \E key \in key_space:
                       Key' = key
                  /\ /\ CurrentNode' = [CurrentNode EXCEPT !["searcher"] = Nodes[1]]
                     /\ Value' = [Value EXCEPT !["searcher"] = Key']
                     /\ stack' = [stack EXCEPT !["searcher"] = << [ procedure |->  "FindNode",
                                                                    pc        |->  "Done",
                                                                    Level     |->  Level["searcher"],
                                                                    CurrentNode |->  CurrentNode["searcher"],
                                                                    Value     |->  Value["searcher"] ] >>
                                                                \o stack["searcher"]]
                  /\ Level' = [Level EXCEPT !["searcher"] = 0]
                  /\ pc' = [pc EXCEPT !["searcher"] = "FindNode_Start"]
                  /\ UNCHANGED << Nodes, SearcherResult, InsertedValues, 
                                  FindNodeResults, NewNode, NewNodeValue >>

searcher == Searcher_Start

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == searcher
           \/ (\E self \in ProcSet: FindNode(self))
           \/ (\E self \in (key_space \X { 1, 2 }): inserter(self))
           \/ Terminating

Spec == Init /\ [][Next]_vars

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION - the hash of the generated TLA code (remove to silence divergence warnings): TLA-9f0c024ab76360066db8f7f41bfa4b2e

=============================================================================
\* Modification History
\* Last modified Wed Jul 08 13:33:17 PDT 2020 by jrowe
\* Created Wed Jul 08 11:25:16 PDT 2020 by jrowe
