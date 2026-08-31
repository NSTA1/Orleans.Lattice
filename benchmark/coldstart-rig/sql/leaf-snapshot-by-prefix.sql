-- Leaf-snapshot rows and bytes by repository-context key prefix.
--
-- Emits one pipe-delimited row per key prefix:
--   <keyPrefix>|<snapshotRows>|<totalBytes>
--
-- A leaf-snapshot blob holds the rows of one leaf, and every repository-context
-- key has the shape repo/<repoId>/<prefix>/... , so the prefix of the blob's
-- FIRST row attributes the snapshot to its logical plane (vpay = vector
-- payload, vec = vector metadata, vmem = vector membership, plus symbol,
-- content and the structural planes).
--
-- Only the leading bytes of each blob are cast to text: the first row's key
-- appears within the first couple of hundred characters, so casting the whole
-- multi-hundred-kilobyte blob would cost a great deal and buy nothing.
.mode list
.headers off

WITH raw AS (
    SELECT
        LENGTH(PayloadBinary) AS bytes,
        CAST(substr(PayloadBinary, 1, 4096) AS TEXT) AS head
    FROM OrleansStorage
    WHERE GrainTypeString = 'leaf-snapshot'
      AND PayloadBinary IS NOT NULL
),
keyed AS (
    SELECT
        bytes,
        CASE WHEN instr(head, '"Key":"') > 0
             THEN substr(head, instr(head, '"Key":"') + 7)
             ELSE '' END AS rest
    FROM raw
),
full_key AS (
    SELECT
        bytes,
        CASE WHEN rest = '' THEN '' ELSE substr(rest, 1, instr(rest, '"') - 1) END AS k
    FROM keyed
),
after_repo AS (
    SELECT bytes, CASE WHEN instr(k, '/') > 0 THEN substr(k, instr(k, '/') + 1) ELSE '' END AS r
    FROM full_key
),
after_repo_id AS (
    SELECT bytes, CASE WHEN instr(r, '/') > 0 THEN substr(r, instr(r, '/') + 1) ELSE '' END AS r
    FROM after_repo
),
prefixed AS (
    SELECT
        bytes,
        (CASE
             WHEN r = '' THEN '(no-rows)'
             WHEN instr(r, '/') > 0 THEN substr(r, 1, instr(r, '/') - 1)
             ELSE r
         END) AS prefix
    FROM after_repo_id
)
SELECT prefix || '|' || COUNT(*) || '|' || SUM(bytes)
FROM prefixed
GROUP BY prefix
ORDER BY SUM(bytes) DESC;
