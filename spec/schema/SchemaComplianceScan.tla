---- MODULE SchemaComplianceScan ----
EXTENDS Naturals, FiniteSets

\* One tracked compliance scan of one governed tree: the policy read once at
\* the start, an ungoverned tree answered at once, the live-entry count taken
\* before an ordered, non-snapshot pass over a writable tree, per-value
\* verdicts tallied as compliant or by failure reason, an undecodable stored
\* value counted non-compliant with enforcement's reason, cancellation and a
\* lost runner ending the scan without a report, and the report's rule count
\* (the policy's rules) kept apart from its failure-reason count.

CONSTANTS NKeys, MaxWrites, MaxPolicyChanges

Keys == 1..NKeys
\* "Undec" is a stored value the read path cannot decode to the tree's target
\* shape (no upcast hop, or stamped newer than the target).
Values == {"Absent", "Good", "BadA", "BadB", "Undec"}
\* "Strict" has two rules and rejects both bad shapes; "Loose" has one rule
\* and rejects only BadA.
Policies == {"None", "Strict", "Loose"}
Reasons == {"RA", "RB", "RU"}
Phases == {"Idle", "Counting", "Scanning", "Succeeded", "Failed", "Cancelled"}

RuleCount(p) == CASE p = "Strict" -> 2 [] p = "Loose" -> 1 [] OTHER -> 0
\* The verdict of a governed policy on a value: "Ok", or the failure reason.
\* An undecodable value fails with enforcement's decode reason under any policy.
Verdict(p, v) ==
    CASE v = "Good" -> "Ok"
      [] v = "BadA" -> "RA"
      [] v = "BadB" -> IF p = "Strict" THEN "RB" ELSE "Ok"
      [] OTHER -> "RU"

VARIABLES value, policy, writes, policyChanges, phase, scanPolicy, cursor,
          total, totalKnown, compliant, nonCompliant, reasonCount, seen,
          touched, lastValue, lastVerdict, lost, hasReport, repHasPolicy,
          repRuleCount, repReasons
vars == <<value, policy, writes, policyChanges, phase, scanPolicy, cursor,
          total, totalKnown, compliant, nonCompliant, reasonCount, seen,
          touched, lastValue, lastVerdict, lost, hasReport, repHasPolicy,
          repRuleCount, repReasons>>
envVars == <<value, policy, writes, policyChanges, touched>>
scanVars == <<phase, scanPolicy, cursor, total, totalKnown, compliant,
              nonCompliant, reasonCount, seen, lastValue, lastVerdict, lost>>
reportVars == <<hasReport, repHasPolicy, repRuleCount, repReasons>>

Scanned == compliant + nonCompliant
Running == phase \in {"Counting", "Scanning"}
Terminal == phase \in {"Succeeded", "Failed", "Cancelled"}
LiveCount == Cardinality({k \in Keys : value[k] # "Absent"})
FailureReasons == Cardinality({r \in Reasons : reasonCount[r] > 0})
ReasonTotal == reasonCount["RA"] + reasonCount["RB"] + reasonCount["RU"]

TypeOK ==
    /\ value \in [Keys -> Values]
    /\ policy \in Policies /\ scanPolicy \in Policies
    /\ writes \in 0..MaxWrites /\ policyChanges \in 0..MaxPolicyChanges
    /\ phase \in Phases
    /\ cursor \in 0..NKeys
    /\ total \in 0..NKeys /\ totalKnown \in BOOLEAN
    /\ compliant \in 0..NKeys /\ nonCompliant \in 0..NKeys
    /\ reasonCount \in [Reasons -> 0..NKeys]
    /\ seen \subseteq Keys /\ touched \subseteq Keys
    /\ lastValue \in Values /\ lastVerdict \in Reasons \cup {"Ok"}
    /\ lost \in BOOLEAN
    /\ hasReport \in BOOLEAN /\ repHasPolicy \in BOOLEAN
    /\ repRuleCount \in 0..2 /\ repReasons \in 0..Cardinality(Reasons)

\* Every value the scan judged is counted exactly once, as compliant or not.
CountsAddUp == Scanned = Cardinality(seen)

\* The failure-reason breakdown accounts for every non-compliant value.
ReasonsSumToNonCompliant == ReasonTotal = nonCompliant

\* A governed report's rule count is the number of rules in the policy it
\* scanned against, not the number of failure reasons (#4966).
RuleCountIsPolicyRules ==
    (hasReport /\ repHasPolicy) => repRuleCount = RuleCount(scanPolicy)

\* The failure-reason count sizes the breakdown, not the policy.
FailureReasonCountIsBreakdown ==
    hasReport => repReasons = FailureReasons

\* An ungoverned tree's report has no policy and every count zero.
UngovernedReportEmpty ==
    (hasReport /\ ~repHasPolicy) =>
        /\ Scanned = 0 /\ seen = {} /\ repRuleCount = 0 /\ repReasons = 0

\* A completed governed scan counted every key that was live and untouched
\* for the whole scan.
StableKeysCounted ==
    (hasReport /\ repHasPolicy) =>
        \A k \in Keys : (k \notin touched /\ value[k] # "Absent") => k \in seen

\* A known total never reads below the entries already scanned.
TotalNeverBelowScanned == totalKnown => total >= Scanned

\* Only a completed scan publishes a report.
ReportOnlyWhenSucceeded == hasReport => phase = "Succeeded"

\* A scan fails only when its runner is lost: no stored value faults it.
FailsOnlyWhenLost == phase = "Failed" => lost

\* A running scan keeps the policy it read at its start, whatever the tree's
\* policy becomes meanwhile.
PolicyFixedForScan == [][Running => scanPolicy' = scanPolicy]_vars

\* Every judged value is judged by the policy read at the start of the scan.
JudgedByStartPolicy ==
    [][Scanned' # Scanned => lastVerdict' = Verdict(scanPolicy, lastValue')]_vars

\* An undecodable value is counted non-compliant under enforcement's reason.
UndecodableCountedNonCompliant ==
    [][(Scanned' # Scanned /\ lastValue' = "Undec") =>
        (nonCompliant' = nonCompliant + 1 /\ reasonCount'["RU"] = reasonCount["RU"] + 1)]_vars

\* A running scan reaches a terminal state.
ScanTerminates == Running ~> Terminal

Init ==
    /\ value \in [Keys -> Values]
    /\ policy \in Policies
    /\ writes = 0 /\ policyChanges = 0
    /\ phase = "Idle" /\ scanPolicy = "None" /\ cursor = 0
    /\ total = 0 /\ totalKnown = FALSE
    /\ compliant = 0 /\ nonCompliant = 0
    /\ reasonCount = [r \in Reasons |-> 0]
    /\ seen = {} /\ touched = {}
    /\ lastValue = "Absent" /\ lastVerdict = "Ok"
    /\ lost = FALSE
    /\ hasReport = FALSE /\ repHasPolicy = FALSE
    /\ repRuleCount = 0 /\ repReasons = 0

\* The scan reads the tree's cached policy once. An ungoverned tree succeeds
\* at once with an ungoverned report; a governed one starts counting.
Start ==
    /\ phase = "Idle"
    /\ IF policy = "None"
       THEN /\ phase' = "Succeeded"
            /\ hasReport' = TRUE /\ repHasPolicy' = FALSE
            /\ repRuleCount' = 0 /\ repReasons' = 0
            /\ UNCHANGED scanPolicy
       ELSE /\ phase' = "Counting" /\ scanPolicy' = policy
            /\ UNCHANGED reportVars
    /\ UNCHANGED envVars
    /\ UNCHANGED <<cursor, total, totalKnown, compliant, nonCompliant,
                   reasonCount, seen, lastValue, lastVerdict, lost>>

\* The tracked scan counts the tree's live entries, then starts scanning.
Count ==
    /\ phase = "Counting"
    /\ phase' = "Scanning" /\ total' = LiveCount /\ totalKnown' = TRUE
    /\ cursor' = 0
    /\ UNCHANGED envVars /\ UNCHANGED reportVars
    /\ UNCHANGED <<scanPolicy, compliant, nonCompliant, reasonCount, seen,
                   lastValue, lastVerdict, lost>>

\* The ordered scan reads the next key's current value, if live, and tallies
\* its verdict under the start policy. A total the scan has passed turns
\* unknown rather than reading below the entries scanned.
Step ==
    /\ phase = "Scanning" /\ cursor < NKeys
    /\ LET k == cursor + 1
           v == value[k]
           d == Verdict(scanPolicy, v)
           n == Scanned + 1
       IN /\ cursor' = k
          /\ IF v = "Absent"
             THEN UNCHANGED <<compliant, nonCompliant, reasonCount, seen,
                              lastValue, lastVerdict, total, totalKnown>>
             ELSE /\ seen' = seen \cup {k}
                  /\ lastValue' = v /\ lastVerdict' = d
                  /\ IF d = "Ok"
                     THEN /\ compliant' = compliant + 1
                          /\ UNCHANGED <<nonCompliant, reasonCount>>
                     ELSE /\ nonCompliant' = nonCompliant + 1
                          /\ reasonCount' = [reasonCount EXCEPT ![d] = @ + 1]
                          /\ UNCHANGED compliant
                  /\ IF totalKnown /\ n > total
                     THEN /\ totalKnown' = FALSE /\ UNCHANGED total
                     ELSE UNCHANGED <<total, totalKnown>>
    /\ UNCHANGED envVars /\ UNCHANGED reportVars
    /\ UNCHANGED <<phase, scanPolicy, lost>>

\* The pass ends and the report is published with the start policy's rule
\* count and the number of distinct failure reasons.
Finish ==
    /\ phase = "Scanning" /\ cursor = NKeys
    /\ phase' = "Succeeded"
    /\ hasReport' = TRUE /\ repHasPolicy' = TRUE
    /\ repRuleCount' = RuleCount(scanPolicy)
    /\ repReasons' = FailureReasons
    /\ UNCHANGED envVars
    /\ UNCHANGED <<scanPolicy, cursor, total, totalKnown, compliant,
                   nonCompliant, reasonCount, seen, lastValue, lastVerdict, lost>>

\* A cancel request observed by the running scan ends it with no report.
Cancel ==
    /\ Running
    /\ phase' = "Cancelled"
    /\ UNCHANGED envVars /\ UNCHANGED reportVars
    /\ UNCHANGED <<scanPolicy, cursor, total, totalKnown, compliant,
                   nonCompliant, reasonCount, seen, lastValue, lastVerdict, lost>>

\* Environment: the scan's runner silo is lost; its record reads failed and
\* there is no report. A fresh scan is a new operation.
Lose ==
    /\ Running
    /\ phase' = "Failed" /\ lost' = TRUE
    /\ UNCHANGED envVars /\ UNCHANGED reportVars
    /\ UNCHANGED <<scanPolicy, cursor, total, totalKnown, compliant,
                   nonCompliant, reasonCount, seen, lastValue, lastVerdict>>

\* Environment: a write or delete to a key, concurrent with the scan or not.
Write(k, v) ==
    /\ writes < MaxWrites /\ value[k] # v
    /\ value' = [value EXCEPT ![k] = v]
    /\ writes' = writes + 1
    /\ touched' = IF phase = "Idle" THEN touched ELSE touched \cup {k}
    /\ UNCHANGED <<policy, policyChanges>>
    /\ UNCHANGED scanVars /\ UNCHANGED reportVars

\* Environment: the tree's policy is changed or cleared.
ChangePolicy(p) ==
    /\ policyChanges < MaxPolicyChanges /\ policy # p
    /\ policy' = p
    /\ policyChanges' = policyChanges + 1
    /\ UNCHANGED <<value, writes, touched>>
    /\ UNCHANGED scanVars /\ UNCHANGED reportVars

Stutter == UNCHANGED vars

Next ==
    \/ Start
    \/ Count
    \/ Step
    \/ Finish
    \/ Cancel
    \/ Lose
    \/ \E k \in Keys, v \in Values : Write(k, v)
    \/ \E p \in Policies : ChangePolicy(p)
    \/ Stutter

Spec ==
    /\ Init /\ [][Next]_vars
    /\ WF_vars(Count) /\ WF_vars(Step) /\ WF_vars(Finish)
====
