-- Per-tree shard-healing decision census.
--
-- Emits one pipe-delimited row per healing orchestrator that has persisted
-- state:
--   <treeId>|<lastDecision>|<lastBacklog>|<inFlightCount>
--
-- WHY THIS EXISTS. ShardHealingOrchestratorState persists the decision the most
-- recent sweep reached and the backlog it observed, precisely so that a
-- reactivated orchestrator reports its last real observation rather than
-- "not observed". That makes the state the durable, offline answer to the two
-- questions S14 has to settle about a tree that has not healed:
--
--   * did the orchestrator sweep at all (is there a row), and
--   * if it swept, what did it DECIDE - "not_over_split" (it does not believe
--     the tree is damaged), "backpressure" (it is yielding to load), or
--     "admitted" (it is folding).
--
-- Backlog reaching zero is the machine-checkable "this tree is healed" signal.
-- A row whose decision is NotOverSplit while the tree carries a thousand
-- physical leaves means the orchestrator's view of the BASE shard count
-- disagrees with reality, which is the registry-pin case S11 documented.
--
-- The grain key is the tree id and the state is the JSON-serialised
-- ShardHealingOrchestratorState, so both are read straight out of the row
-- without activating anything.
.mode list
.headers off

WITH healing AS (
    SELECT
        GrainIdN1 AS grainKey,
        CAST(PayloadBinary AS TEXT) AS doc
    FROM OrleansStorage
    WHERE GrainTypeString = 'shard-healing'
      AND PayloadBinary IS NOT NULL
),
decided AS (
    SELECT
        grainKey,
        doc,
        CASE WHEN instr(doc, '"LastDecision":') > 0
             THEN substr(doc, instr(doc, '"LastDecision":') + 15)
             ELSE '' END AS decisionRest,
        CASE WHEN instr(doc, '"LastBacklog":') > 0
             THEN substr(doc, instr(doc, '"LastBacklog":') + 14)
             ELSE '' END AS backlogRest,
        CASE WHEN instr(doc, '"InFlightDonorShardIndices":[') > 0
             THEN substr(doc, instr(doc, '"InFlightDonorShardIndices":[') + 29)
             ELSE '' END AS inFlightRest
    FROM healing
)
SELECT
    grainKey || '|' ||
    CASE WHEN decisionRest = '' THEN 'none'
         ELSE rtrim(substr(decisionRest, 1, 40), '}') END || '|' ||
    CASE WHEN backlogRest = '' THEN '?'
         ELSE substr(backlogRest, 1, 12) END || '|' ||
    CASE WHEN inFlightRest = '' THEN '?'
         ELSE substr(inFlightRest, 1, 40) END
FROM decided
ORDER BY grainKey;
