-- Physical shard-root count per tree.
--
-- Emits one pipe-delimited row per tree:
--   <treeId>|<shardRootRows>
--
-- WHY THIS MATTERS, AND WHY IT IS NOT THE LEAF COUNT. leaf-count-by-tree.sql
-- counts LEAF grains; this counts SHARD ROOTS. They are different populations
-- and the distinction decides whether shard consolidation (S5 / S11) has
-- anything to do at all:
--
--   * A shard is the unit adaptive split creates and consolidation folds. The
--     healing orchestrator compares the PHYSICAL SHARD count against the
--     registry-pinned base and reports NotOverSplit when it is not above it.
--   * A leaf is a B+ tree node inside a shard. Many leaves per shard is normal
--     and is not something consolidation addresses.
--
-- So a tree can carry a thousand leaves while sitting at exactly its base shard
-- count, in which case "not over-split" is the CORRECT decision and the leaf
-- population is a separate concern with a separate remedy. Reading the leaf
-- count as though it were the shard count is the mistake this query exists to
-- prevent.
.mode list
.headers off

WITH roots AS (
    SELECT GrainIdExtensionString AS grainKey
    FROM OrleansStorage
    WHERE GrainTypeString = 'shardroot'
      AND GrainIdExtensionString IS NOT NULL
),
named AS (
    -- The shard-root grain key is "{treeId}/{shardIndex}" and a tree id never
    -- contains a slash, so the tree is everything before the first separator.
    SELECT substr(grainKey, 1, instr(grainKey, '/') - 1) AS treeId
    FROM roots
    WHERE instr(grainKey, '/') > 0
)
SELECT treeId || '|' || COUNT(*)
FROM named
GROUP BY treeId
ORDER BY COUNT(*) DESC, treeId;
