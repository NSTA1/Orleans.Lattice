-- The design criteria for this table are:
--
-- 1. It can contain arbitrary content serialized as binary, XML or JSON. These formats
-- are supported to allow one to take advantage of in-storage processing capabilities for
-- these types if required. This should not incur extra cost on storage.
--
-- 2. The table design should scale with the idea of tens or hundreds (or even more) types
-- of grains that may operate with even hundreds of thousands of grain IDs within each
-- type of a grain.
--
-- 3. The table and its associated operations should remain stable. There should not be
-- structural reason for unexpected delays in operations. It should be possible to also
-- insert data reasonably fast without resource contention.
--
-- 4. For reasons in 2. and 3., the index should be as narrow as possible so it fits well in
-- memory and should it require maintenance, isn't resource intensive. For this
-- reason the index is narrow by design (ideally non-clustered). Currently the entity
-- is recognized in the storage by the grain type and its ID, which are unique in Orleans silo.
-- The ID is the grain ID bytes (if string type UTF-8 bytes) and possible extension key as UTF-8
-- bytes concatenated with the ID and then hashed.
--
-- Reason for hashing: Database engines usually limit the length of the column sizes, which
-- would artificially limit the length of IDs or types. Even when within limitations, the
-- index would be thick and consume more memory.
--
-- In the current setup the ID and the type are hashed into two INT type instances, which
-- are made a compound index. When there are no collisions, the index can quickly locate
-- the unique row. Along with the hashed index values, the NVARCHAR(nnn) values are also
-- stored and they are used to prune hash collisions down to only one result row.
--
-- 5. The design leads to duplication in the storage. It is reasonable to assume there will
-- a low number of services with a given service ID operational at any given time. Or that
-- compared to the number of grain IDs, there are a fairly low number of different types of
-- grain. The catch is that were these data separated to another table, it would make INSERT
-- and UPDATE operations complicated and would require joins, temporary variables and additional
-- indexes or some combinations of them to make it work. It looks like fitting strategy
-- could be to use table compression.
--
-- 6. Upstream, grain state DELETE sets NULL to the data fields and updates the Version
-- number normally, on the reasoning that this alleviates the need for index or statistics
-- maintenance at the cost of some bytes of storage space, and that the table can be
-- scrubbed in a separate maintenance operation.
--
-- This deployment deliberately does NOT do that. It defines DeleteStorageKey below and
-- runs with AdoNetGrainStorageOptions.DeleteStateOnClear enabled, so a cleared grain row
-- is removed outright. The upstream reasoning does not hold for this workload: several of
-- its grain types are keyed generationally (a per-leaf generation counter, a per-cursor
-- GUID), so a nulled row is never revisited by a later write and the population only ever
-- grows. Measured on the deployed container, 29.8% of OrleansStorage rows were nulled
-- tombstones accumulating at roughly 3,000/day, which costs row count, IX_OrleansStorage
-- depth and scan cost, and makes it impossible to tell a live grain from a dead one.
-- The separate maintenance operation upstream assumes is not available here: this is an
-- embedded SQLite file inside a running container, with no scheduled maintenance window.
-- Note this reclaims no space on its own - a nulled row holds no payload bytes by
-- construction - it bounds the row count.
--
-- 7. In the storage operations queries the columns need to be in the exact same order
-- since the storage table operations support optionally streaming.
CREATE TABLE IF NOT EXISTS OrleansStorage
(
    -- These are for the book keeping. Orleans calculates
    -- these hashes (see RelationalStorageProvide implementation),
    -- which are signed 32 bit integers mapped to the *Hash fields.
    -- The mapping is done in the code. The
    -- *String columns contain the corresponding clear name fields.
    --
    -- If there are duplicates, they are resolved by using GrainIdN0,
    -- GrainIdN1, GrainIdExtensionString and GrainTypeString fields.
    -- It is assumed these would be rarely needed.
    GrainIdHash                INT NOT NULL,
    GrainIdN0                BIGINT NOT NULL,
    GrainIdN1                BIGINT NOT NULL,
    GrainTypeHash            INT NOT NULL,
    GrainTypeString            NVARCHAR(512) NOT NULL,
    GrainIdExtensionString    NVARCHAR(512) NULL,
    ServiceId                NVARCHAR(150) NOT NULL,
    -- Payload
    PayloadBinary    BLOB NULL,
    -- Informational field, no other use.
    ModifiedOn DATETIME NOT NULL,
    -- The version of the stored payload.
    Version INT NULL
    -- The following would in principle be the primary key, but it would be too thick
    -- to be indexed, so the values are hashed and only collisions will be solved
    -- by using the fields. That is, after the indexed queries have pinpointed the right
    -- rows down to [0, n] relevant ones, n being the number of collided value pairs.
);

CREATE INDEX IF NOT EXISTS IX_OrleansStorage ON OrleansStorage(GrainIdHash, GrainTypeHash);


-- Updates an existing grain state with optimistic concurrency control or inserts it if it does not exist.
--
-- The mutation runs as ONE statement. The batch stages its parameters as a single row
-- in a connection-scoped temp table, OrleansStorageWriteRequest, and a TEMP trigger on
-- that table performs the UPDATE or the conditional INSERT against OrleansStorage.
-- Trigger actions belong to the statement that fired them, so the staging INSERT, the
-- storage mutation and the outcome bookkeeping commit or roll back together, in one
-- implicit transaction that takes the database write lock once.
--
-- This replaces an earlier form that ran the UPDATE and the conditional INSERT as two
-- separate auto-committing statements (issue #3512). There the UPDATE committed on its
-- own, and the INSERT after it - a write statement even when its WHERE matches nothing -
-- had to take the write lock again. When that wait outran the busy timeout the command
-- threw SQLITE_BUSY ("database is locked") for a write that was already durable, so
-- Orleans kept the old ETag and the grain's next write failed with
-- InconsistentStateException: Version conflict (WriteState). No write statement may run
-- after the one that mutates the row: everything after it here reads only the
-- connection-private temp table, which needs no database lock at all.
--
-- The batch deliberately does NOT use an explicit BEGIN TRANSACTION / COMMIT (or
-- BEGIN IMMEDIATE), which would also make the write atomic. SQLite forbids nested
-- transactions, and under Microsoft.Data.Sqlite connection pooling a batch that fails
-- before reaching its COMMIT leaves the transaction open on the pooled connection; the
-- pool does not roll it back, so the next reuse of that connection fails at BEGIN with
-- "cannot start a transaction within a transaction", cascading across a burst of
-- concurrent writes. A single statement's implicit transaction cannot leak that way:
-- SQLite ends it when the statement completes or fails.
--
-- The staging table and trigger are TEMP objects, so they are private to the connection
-- and created on its first write. A TEMP trigger is allowed to modify a table in the main
-- database. Bound parameters are not visible inside a trigger body, which is why they
-- travel as the staged row's columns (NEW.*). The trigger records whether it mutated a
-- row in the staged row's Applied column, using changes() of the trigger step just
-- completed, and nulls the staged payload in the same statement so a pooled connection
-- never pins the last grain state it wrote.
--
-- The two trailing SELECTs report the new version to Orleans, which reads them
-- with SingleOrDefault(): more than one returned row throws
-- InvalidOperationException("Sequence contains more than one element") out of
-- AdoNetGrainStorage.WriteStateAsync. Both therefore compute the version as a
-- scalar rather than selecting Version back out of OrleansStorage, because that
-- read returns one row PER STORAGE ROW matching the grain identity. Nothing in
-- this schema constrains that to one row - IX_OrleansStorage is deliberately
-- non-unique (see design criterion 4 above), so a grain that ever acquires a
-- second row becomes permanently unwritable while still reading cleanly, since
-- ReadFromStorageKey below caps itself with LIMIT 1. Observed in production as a
-- storm of write failures against leaf, internal and leaf-snapshot rows. Each
-- SELECT reads the staging table, which holds exactly one row after the DELETE and
-- INSERT above it, so each returns at most one row. The same property rules out an
-- INSERT ... ON CONFLICT upsert: it needs a unique index this schema cannot carry.
--
-- This is a deliberate divergence from the upstream script this file is derived
-- from, which reads the version back out of the table in both queries and is
-- affected. Reported as dotnet/orleans#11303; every other Orleans dialect
-- (PostgreSQL, SQL Server, MySQL, Oracle) already computes the version as a
-- scalar, so revert this local change once upstream SQLite does the same.
--
-- The scalar is exact, not an approximation of the stored value. The UPDATE
-- matches only on Version = @GrainStateVersion and sets Version = Version + 1,
-- so a successful update always lands @GrainStateVersion + 1; the INSERT fires
-- only when @GrainStateVersion IS NULL and always writes 1. Exactly one of the
-- two can fire - the UPDATE cannot match when @GrainStateVersion is NULL
-- (Version = NULL is never true) and the INSERT is gated on it being NULL - so
-- @GrainStateVersion IS NULL discriminates them precisely. This also removes a
-- redundant indexed lookup from every write.
INSERT OR REPLACE INTO OrleansQuery (QueryKey, QueryText) VALUES 
('WriteToStorageKey', '
    CREATE TEMP TABLE IF NOT EXISTS OrleansStorageWriteRequest
    (
        GrainIdHash                INT NOT NULL,
        GrainIdN0                BIGINT NOT NULL,
        GrainIdN1                BIGINT NOT NULL,
        GrainTypeHash            INT NOT NULL,
        GrainTypeString            NVARCHAR(512) NOT NULL,
        GrainIdExtensionString    NVARCHAR(512) NULL,
        ServiceId                NVARCHAR(150) NOT NULL,
        PayloadBinary    BLOB NULL,
        GrainStateVersion INT NULL,
        Applied INT NOT NULL DEFAULT 0
    );

    CREATE TEMP TRIGGER IF NOT EXISTS OrleansStorageWriteApply
    AFTER INSERT ON OrleansStorageWriteRequest
    BEGIN
        UPDATE OrleansStorage
        SET
            PayloadBinary = NEW.PayloadBinary,
            ModifiedOn = datetime(''now''),
            Version = Version + 1
        WHERE
            GrainIdHash = NEW.GrainIdHash AND GrainTypeHash = NEW.GrainTypeHash
            AND GrainIdN0 = NEW.GrainIdN0 AND GrainIdN1 = NEW.GrainIdN1
            AND GrainTypeString = NEW.GrainTypeString
            AND (GrainIdExtensionString = NEW.GrainIdExtensionString OR (GrainIdExtensionString IS NULL AND NEW.GrainIdExtensionString IS NULL))
            AND ServiceId = NEW.ServiceId
            AND Version = NEW.GrainStateVersion;

        UPDATE OrleansStorageWriteRequest SET Applied = changes() WHERE rowid = NEW.rowid;

        INSERT INTO OrleansStorage (GrainIdHash, GrainIdN0, GrainIdN1, GrainTypeHash, GrainTypeString, GrainIdExtensionString, ServiceId, PayloadBinary, ModifiedOn, Version)
        SELECT NEW.GrainIdHash, NEW.GrainIdN0, NEW.GrainIdN1, NEW.GrainTypeHash, NEW.GrainTypeString, NEW.GrainIdExtensionString, NEW.ServiceId, NEW.PayloadBinary, datetime(''now''), 1
        WHERE NEW.GrainStateVersion IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM OrleansStorage
            WHERE GrainIdHash = NEW.GrainIdHash AND GrainTypeHash = NEW.GrainTypeHash
            AND GrainIdN0 = NEW.GrainIdN0 AND GrainIdN1 = NEW.GrainIdN1
            AND GrainTypeString = NEW.GrainTypeString
            AND (GrainIdExtensionString = NEW.GrainIdExtensionString OR (GrainIdExtensionString IS NULL AND NEW.GrainIdExtensionString IS NULL))
            AND ServiceId = NEW.ServiceId
        );

        UPDATE OrleansStorageWriteRequest SET Applied = Applied + changes(), PayloadBinary = NULL WHERE rowid = NEW.rowid;
    END;

    DELETE FROM OrleansStorageWriteRequest;

    INSERT INTO OrleansStorageWriteRequest (GrainIdHash, GrainIdN0, GrainIdN1, GrainTypeHash, GrainTypeString, GrainIdExtensionString, ServiceId, PayloadBinary, GrainStateVersion)
    VALUES (@GrainIdHash, @GrainIdN0, @GrainIdN1, @GrainTypeHash, @GrainTypeString, @GrainIdExtensionString, @ServiceId, @PayloadBinary, @GrainStateVersion);

    SELECT (CASE WHEN @GrainStateVersion IS NULL THEN 1 ELSE @GrainStateVersion + 1 END) AS NewGrainStateVersion
    FROM OrleansStorageWriteRequest
    WHERE Applied > 0;

    SELECT @GrainStateVersion AS NewGrainStateVersion
    FROM OrleansStorageWriteRequest
    WHERE Applied = 0
        AND @GrainStateVersion IS NOT NULL;
');

-- Retrieves the binary payload and the current version of a specific grain state.
INSERT OR REPLACE INTO OrleansQuery (QueryKey, QueryText) VALUES 
('ReadFromStorageKey', '
    SELECT
        PayloadBinary,
        Version AS Version
    FROM
        OrleansStorage
    WHERE
        GrainIdHash = @GrainIdHash AND GrainTypeHash = @GrainTypeHash
        AND GrainIdN0 = @GrainIdN0 AND GrainIdN1 = @GrainIdN1
        AND GrainTypeString = @GrainTypeString
        AND (GrainIdExtensionString = @GrainIdExtensionString OR (GrainIdExtensionString IS NULL AND @GrainIdExtensionString IS NULL))
        AND ServiceId = @ServiceId
    LIMIT 1;
');

-- Clears the grain state by setting the payload to null and incrementing the version for consistency.
-- The version is computed as a scalar for the same reason as WriteToStorageKey
-- above: selecting Version back out of OrleansStorage returns one row per
-- storage row, and Orleans' SingleOrDefault() throws on more than one. The
-- UPDATE matches only on Version = @GrainStateVersion and sets Version + 1, so
-- a cleared row is always at @GrainStateVersion + 1.
INSERT OR REPLACE INTO OrleansQuery (QueryKey, QueryText) VALUES 
('ClearStorageKey', '
    UPDATE OrleansStorage
    SET
        PayloadBinary = NULL,
        ModifiedOn = datetime(''now''),
        Version = Version + 1
    WHERE
        GrainIdHash = @GrainIdHash AND GrainTypeHash = @GrainTypeHash
        AND GrainIdN0 = @GrainIdN0 AND GrainIdN1 = @GrainIdN1
        AND GrainTypeString = @GrainTypeString
        AND (GrainIdExtensionString = @GrainIdExtensionString OR (GrainIdExtensionString IS NULL AND @GrainIdExtensionString IS NULL))
        AND ServiceId = @ServiceId
        AND Version = @GrainStateVersion;

    SELECT @GrainStateVersion + 1 AS NewGrainStateVersion
    WHERE changes() > 0;

    SELECT @GrainStateVersion AS NewGrainStateVersion
    WHERE changes() = 0
        AND @GrainStateVersion IS NOT NULL;
');

-- Removes the grain state row outright. Orleans issues this instead of ClearStorageKey
-- when AdoNetGrainStorageOptions.DeleteStateOnClear is enabled, which this host sets for
-- the SQLite branch in DurabilitySelector.ConfigureGrainStorage. See design criterion 6
-- above for why this deployment deletes rather than nulls.
--
-- The two queries are coupled and must land together. Orleans looks the query text up by
-- the exact key literal 'DeleteStorageKey' during AdoNetGrainStorage.Init - the key it
-- selects for in its public DefaultInitializationQuery constant - and throws at SILO
-- STARTUP, not at the first clear, when the option is enabled and the key is absent.
-- ClearStorageKey above is retained because Orleans resolves it unconditionally at Init
-- whether or not the delete path is in use.
--
-- The version is reported as a scalar for exactly the same reason as WriteToStorageKey
-- and ClearStorageKey above, and the reason bites harder here. The upstream PostgreSQL
-- form of this query uses DELETE ... RETURNING Version + 1, which emits one row PER
-- DELETED ROW. Nothing in this schema constrains a grain identity to a single row -
-- IX_OrleansStorage is deliberately non-unique (design criterion 4) - so against a grain
-- that had acquired a duplicate row, RETURNING would hand Orleans two rows and its
-- SingleOrDefault() would throw InvalidOperationException("Sequence contains more than
-- one element"), reintroducing the precise wedge the scalar form was adopted to remove.
-- Computing the version as a scalar returns exactly one row however many were deleted.
--
-- The scalar is exact rather than an approximation. The DELETE matches only on
-- Version = @GrainStateVersion, so the row it removed was at @GrainStateVersion and the
-- version Orleans should carry forward for its CheckVersionInconsistency check is
-- @GrainStateVersion + 1. When nothing matched, reporting @GrainStateVersion back
-- unchanged is what makes Orleans raise an InconsistentStateException, since a reported
-- version equal to the version held in memory is its definition of a conflict.
INSERT OR REPLACE INTO OrleansQuery (QueryKey, QueryText) VALUES 
('DeleteStorageKey', '
    DELETE FROM OrleansStorage
    WHERE
        GrainIdHash = @GrainIdHash AND GrainTypeHash = @GrainTypeHash
        AND GrainIdN0 = @GrainIdN0 AND GrainIdN1 = @GrainIdN1
        AND GrainTypeString = @GrainTypeString
        AND (GrainIdExtensionString = @GrainIdExtensionString OR (GrainIdExtensionString IS NULL AND @GrainIdExtensionString IS NULL))
        AND ServiceId = @ServiceId
        AND Version = @GrainStateVersion;

    SELECT @GrainStateVersion + 1 AS NewGrainStateVersion
    WHERE changes() > 0;

    SELECT @GrainStateVersion AS NewGrainStateVersion
    WHERE changes() = 0
        AND @GrainStateVersion IS NOT NULL;
');
