---- MODULE DeadlockDetection ----
EXTENDS Integers, TLC

CONSTANT Threads

ASSUME Threads \in SUBSET Nat

(* --algorithm DeadlockDetection 

variable 
    WaitOperations = [thread \in Threads |-> << >>],
    Outcomes = [thread \in Threads |-> "Unresolved"];

fair+ process Thread \in Threads
variable
    latestThread = << >>,
    deadlockedThread = << >>
begin
DoWork:
    while Outcomes[self] = "Unresolved" do
        either
            with otherThread \in Threads \ { self } do
                await Outcomes[otherThread] # "Completed";
                WaitOperations[self] := << otherThread >>;
                deadlockedThread := << otherThread >>;
                latestThread := << otherThread >>;
            end with;
FindLatestThread:
            while
                /\  deadlockedThread # << self >>
                /\  deadlockedThread # << >>
                do

                if deadlockedThread[1] > latestThread[1] then
                    latestThread := deadlockedThread;
                end if;
                deadlockedThread := WaitOperations[deadlockedThread[1]];
            end while;

Interrupt:
            if deadlockedThread = << self >> then
                Outcomes[latestThread[1]] := "Completed";
            end if;

WaitForResolution:
            await Outcomes[WaitOperations[self][1]] = "Completed";
            deadlockedThread := << >>;
            latestThread :=  << >>;
            WaitOperations[self] := << >>;

        or
            Outcomes[self] := "Completed";
            deadlockedThread := << >>;
            latestThread :=  << >>;
        end either;
    end while;
end process;

end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "e2881f17" /\ chksum(tla) = "b9c5e345")
VARIABLES WaitOperations, Outcomes, pc, latestThread, deadlockedThread

vars == << WaitOperations, Outcomes, pc, latestThread, deadlockedThread >>

ProcSet == (Threads)

Init == (* Global variables *)
        /\ WaitOperations = [thread \in Threads |-> << >>]
        /\ Outcomes = [thread \in Threads |-> "Unresolved"]
        (* Process Thread *)
        /\ latestThread = [self \in Threads |-> << >>]
        /\ deadlockedThread = [self \in Threads |-> << >>]
        /\ pc = [self \in ProcSet |-> "DoWork"]

DoWork(self) == /\ pc[self] = "DoWork"
                /\ IF Outcomes[self] = "Unresolved"
                      THEN /\ \/ /\ \E otherThread \in Threads \ { self }:
                                      /\ Outcomes[otherThread] # "Completed"
                                      /\ WaitOperations' = [WaitOperations EXCEPT ![self] = << otherThread >>]
                                      /\ deadlockedThread' = [deadlockedThread EXCEPT ![self] = << otherThread >>]
                                      /\ latestThread' = [latestThread EXCEPT ![self] = << otherThread >>]
                                 /\ pc' = [pc EXCEPT ![self] = "FindLatestThread"]
                                 /\ UNCHANGED Outcomes
                              \/ /\ Outcomes' = [Outcomes EXCEPT ![self] = "Completed"]
                                 /\ deadlockedThread' = [deadlockedThread EXCEPT ![self] = << >>]
                                 /\ latestThread' = [latestThread EXCEPT ![self] = << >>]
                                 /\ pc' = [pc EXCEPT ![self] = "DoWork"]
                                 /\ UNCHANGED WaitOperations
                      ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                           /\ UNCHANGED << WaitOperations, Outcomes, 
                                           latestThread, deadlockedThread >>

FindLatestThread(self) == /\ pc[self] = "FindLatestThread"
                          /\ IF /\  deadlockedThread[self] # << self >>
                                /\  deadlockedThread[self] # << >>
                                THEN /\ IF deadlockedThread[self][1] > latestThread[self][1]
                                           THEN /\ latestThread' = [latestThread EXCEPT ![self] = deadlockedThread[self]]
                                           ELSE /\ TRUE
                                                /\ UNCHANGED latestThread
                                     /\ deadlockedThread' = [deadlockedThread EXCEPT ![self] = WaitOperations[deadlockedThread[self][1]]]
                                     /\ pc' = [pc EXCEPT ![self] = "FindLatestThread"]
                                ELSE /\ pc' = [pc EXCEPT ![self] = "Interrupt"]
                                     /\ UNCHANGED << latestThread, 
                                                     deadlockedThread >>
                          /\ UNCHANGED << WaitOperations, Outcomes >>

Interrupt(self) == /\ pc[self] = "Interrupt"
                   /\ IF deadlockedThread[self] = << self >>
                         THEN /\ Outcomes' = [Outcomes EXCEPT ![latestThread[self][1]] = "Completed"]
                         ELSE /\ TRUE
                              /\ UNCHANGED Outcomes
                   /\ pc' = [pc EXCEPT ![self] = "WaitForResolution"]
                   /\ UNCHANGED << WaitOperations, latestThread, 
                                   deadlockedThread >>

WaitForResolution(self) == /\ pc[self] = "WaitForResolution"
                           /\ Outcomes[WaitOperations[self][1]] = "Completed"
                           /\ deadlockedThread' = [deadlockedThread EXCEPT ![self] = << >>]
                           /\ latestThread' = [latestThread EXCEPT ![self] = << >>]
                           /\ WaitOperations' = [WaitOperations EXCEPT ![self] = << >>]
                           /\ pc' = [pc EXCEPT ![self] = "DoWork"]
                           /\ UNCHANGED Outcomes

Thread(self) == DoWork(self) \/ FindLatestThread(self) \/ Interrupt(self)
                   \/ WaitForResolution(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == (\E self \in Threads: Thread(self))
           \/ Terminating

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in Threads : SF_vars(Thread(self))

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION 

====
