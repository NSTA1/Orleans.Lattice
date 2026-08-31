-- Leaf count per tree.
--
-- Emits one pipe-delimited row per tree:
--   <treeId>|<leafGrainRows>
--
-- The `leaf` grain state (LeafNodeState) carries its own TreeId, so the
-- physical leaf count of each tree is read straight out of the persisted
-- state without activating anything. This is the figure that shows a tree has
-- been shattered by adaptive split (S4 / S5): every healthy tree sits at the
-- initial physical shard count, and a runaway one does not.
.mode list
.headers off

WITH leaf AS (
    SELECT CAST(PayloadBinary AS TEXT) AS doc
    FROM OrleansStorage
    WHERE GrainTypeString = 'leaf'
      AND PayloadBinary IS NOT NULL
),
tagged AS (
    SELECT substr(doc, instr(doc, '"TreeId":"') + 10) AS rest
    FROM leaf
    WHERE instr(doc, '"TreeId":"') > 0
),
named AS (
    SELECT substr(rest, 1, instr(rest, '"') - 1) AS treeId
    FROM tagged
)
SELECT treeId || '|' || COUNT(*)
FROM named
GROUP BY treeId
ORDER BY COUNT(*) DESC, treeId;
