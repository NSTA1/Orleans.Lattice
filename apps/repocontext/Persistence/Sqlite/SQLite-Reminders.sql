-- Orleans Reminders table for the SQLite ADO.NET invariant
-- (https://learn.microsoft.com/dotnet/orleans/grains/timers-and-reminders).
--
-- Orleans does not ship a first-party SQLite reminders script (it ships
-- SQL Server, PostgreSQL, MySQL, and Oracle), so the RepoContext container host
-- owns this one. It mirrors the column contract and the named-query keys of the
-- PostgreSQL script exactly (the AdoNet reminder table reads results by the
-- column names GrainId, ReminderName, StartTime, Period, Version and binds
-- parameters by @Name), and expresses the upsert and delete with SQLite's native
-- ON CONFLICT ... DO UPDATE ... RETURNING and changes() rather than a stored
-- procedure, so no server-side function is required.
CREATE TABLE OrleansRemindersTable
(
    ServiceId TEXT NOT NULL,
    GrainId TEXT NOT NULL,
    ReminderName TEXT NOT NULL,
    StartTime TEXT NOT NULL,
    Period BIGINT NOT NULL,
    GrainHash INT NOT NULL,
    Version INT NOT NULL,

    CONSTRAINT PK_RemindersTable_ServiceId_GrainId_ReminderName PRIMARY KEY(ServiceId, GrainId, ReminderName)
);

-- Inserts a new reminder row at version 0 or bumps the version of an existing
-- one, returning the resulting version.
INSERT INTO OrleansQuery (QueryKey, QueryText) VALUES
('UpsertReminderRowKey', '
    INSERT INTO OrleansRemindersTable
    (
        ServiceId,
        GrainId,
        ReminderName,
        StartTime,
        Period,
        GrainHash,
        Version
    )
    VALUES
    (
        @ServiceId,
        @GrainId,
        @ReminderName,
        @StartTime,
        @Period,
        @GrainHash,
        0
    )
    ON CONFLICT (ServiceId, GrainId, ReminderName)
    DO UPDATE SET
        StartTime = excluded.StartTime,
        Period = excluded.Period,
        GrainHash = excluded.GrainHash,
        Version = OrleansRemindersTable.Version + 1
    RETURNING Version AS Version;
');

-- Reads every reminder row for a grain.
INSERT INTO OrleansQuery (QueryKey, QueryText) VALUES
('ReadReminderRowsKey', '
    SELECT
        GrainId,
        ReminderName,
        StartTime,
        Period,
        Version
    FROM OrleansRemindersTable
    WHERE
        ServiceId = @ServiceId AND @ServiceId IS NOT NULL
        AND GrainId = @GrainId AND @GrainId IS NOT NULL;
');

-- Reads a single reminder row.
INSERT INTO OrleansQuery (QueryKey, QueryText) VALUES
('ReadReminderRowKey', '
    SELECT
        GrainId,
        ReminderName,
        StartTime,
        Period,
        Version
    FROM OrleansRemindersTable
    WHERE
        ServiceId = @ServiceId AND @ServiceId IS NOT NULL
        AND GrainId = @GrainId AND @GrainId IS NOT NULL
        AND ReminderName = @ReminderName AND @ReminderName IS NOT NULL;
');

-- Reads the reminder rows whose grain hash falls in the open-closed range
-- (BeginHash, EndHash].
INSERT INTO OrleansQuery (QueryKey, QueryText) VALUES
('ReadRangeRows1Key', '
    SELECT
        GrainId,
        ReminderName,
        StartTime,
        Period,
        Version
    FROM OrleansRemindersTable
    WHERE
        ServiceId = @ServiceId AND @ServiceId IS NOT NULL
        AND GrainHash > @BeginHash AND @BeginHash IS NOT NULL
        AND GrainHash <= @EndHash AND @EndHash IS NOT NULL;
');

-- Reads the reminder rows whose grain hash falls in the wrap-around range
-- (GrainHash > BeginHash OR GrainHash <= EndHash).
INSERT INTO OrleansQuery (QueryKey, QueryText) VALUES
('ReadRangeRows2Key', '
    SELECT
        GrainId,
        ReminderName,
        StartTime,
        Period,
        Version
    FROM OrleansRemindersTable
    WHERE
        ServiceId = @ServiceId AND @ServiceId IS NOT NULL
        AND ((GrainHash > @BeginHash AND @BeginHash IS NOT NULL)
        OR (GrainHash <= @EndHash AND @EndHash IS NOT NULL));
');

-- Deletes a reminder row at a specific version, returning 1 when a row was
-- removed and 0 otherwise.
INSERT INTO OrleansQuery (QueryKey, QueryText) VALUES
('DeleteReminderRowKey', '
    DELETE FROM OrleansRemindersTable
    WHERE
        ServiceId = @ServiceId AND @ServiceId IS NOT NULL
        AND GrainId = @GrainId AND @GrainId IS NOT NULL
        AND ReminderName = @ReminderName AND @ReminderName IS NOT NULL
        AND Version = @Version AND @Version IS NOT NULL;
    SELECT changes();
');

-- Clears every reminder row for the service (used by the testing/reset path).
INSERT INTO OrleansQuery (QueryKey, QueryText) VALUES
('DeleteReminderRowsKey', '
    DELETE FROM OrleansRemindersTable
    WHERE
        ServiceId = @ServiceId AND @ServiceId IS NOT NULL;
');
