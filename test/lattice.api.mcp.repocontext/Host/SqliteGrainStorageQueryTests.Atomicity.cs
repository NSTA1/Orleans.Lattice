using Microsoft.Data.Sqlite;
using SQLitePCL;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Atomicity of <c>WriteToStorageKey</c> under writer contention (issue #3512).
/// </summary>
/// <remarks>
/// <para>
/// Orleans treats a write that throws as a write that did not happen: the grain keeps
/// its old ETag. So a batch that commits its mutation and then throws leaves the grain
/// one version behind the stored row, and its next write fails with
/// <c>InconsistentStateException: Version conflict (WriteState)</c>. The batch
/// auto-commits each statement, so any write statement that runs after the one that
/// mutated the row must take the database write lock again, and can lose it for
/// longer than the busy timeout after the mutation is already durable.
/// </para>
/// <para>
/// These tests make that window deterministic rather than racing for it. An update
/// hook on the writer's connection records the moment <c>OrleansStorage</c> is
/// mutated; a trace hook, which fires as each later statement starts, then has a
/// second connection seize the write lock with <c>BEGIN IMMEDIATE</c>. Every statement
/// the batch runs after its mutation therefore runs against a held write lock. The
/// contract asserted is the one Orleans relies on: the write either commits and
/// reports the new version, or fails and leaves the stored version unchanged - never
/// the mixed case.
/// </para>
/// </remarks>
public sealed partial class SqliteGrainStorageQueryTests
{
    [Test]
    public void An_update_that_loses_the_write_lock_after_mutating_the_row_is_never_reported_as_failed()
    {
        Write(version: null);

        var outcome = WriteWhileTheWriteLockIsSeizedAfterTheRowIsMutated(version: 1);

        AssertCommitsAndReportsOrFailsAndLeavesTheRowUnchanged(outcome, versionBefore: 1, expectedNewVersion: 2);
    }

    [Test]
    public void A_first_write_that_loses_the_write_lock_after_inserting_the_row_is_never_reported_as_failed()
    {
        var outcome = WriteWhileTheWriteLockIsSeizedAfterTheRowIsMutated(version: null);

        AssertCommitsAndReportsOrFailsAndLeavesTheRowUnchanged(outcome, versionBefore: null, expectedNewVersion: 1);
    }

    [Test]
    public void A_write_leaves_no_payload_bytes_held_on_the_pooled_connection()
    {
        using var connection = Open();
        RunOn(connection, _writeSql, version: null, payload: true);

        // The batch stages its parameters in a connection-scoped temp table so the
        // mutation can run as one statement. That table lives as long as the pooled
        // connection, so a payload left in it would pin the last grain state written
        // on every connection in the pool.
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM temp.OrleansStorageWriteRequest WHERE PayloadBinary IS NOT NULL;";
        Assert.That(Convert.ToInt64(command.ExecuteScalar()), Is.EqualTo(0));
    }

    private void AssertCommitsAndReportsOrFailsAndLeavesTheRowUnchanged(
        WriteUnderContention outcome, int? versionBefore, int expectedNewVersion)
    {
        var stored = StoredVersion();

        Assert.Multiple(() =>
        {
            Assert.That(outcome.LockSeized, Is.True,
                "the harness must actually have held the write lock after the mutation, "
                + "otherwise nothing here was contended");

            if (outcome.Failure is not null)
            {
                Assert.That(stored, Is.EqualTo(versionBefore),
                    "the write threw '" + outcome.Failure.Message + "' but its mutation was "
                    + "already durable: Orleans keeps the old ETag and the grain's next write "
                    + "fails with a version conflict.");
            }
            else
            {
                Assert.That(outcome.Reported, Is.EqualTo(new[] { expectedNewVersion }));
                Assert.That(stored, Is.EqualTo(expectedNewVersion));
            }
        });
    }

    private WriteUnderContention WriteWhileTheWriteLockIsSeizedAfterTheRowIsMutated(int? version)
    {
        using var holder = Open();
        using var writer = Open();
        var holderHandle = holder.Handle!;
        var writerHandle = writer.Handle!;
        raw.sqlite3_busy_timeout(holderHandle, 0);
        raw.sqlite3_busy_timeout(writerHandle, 0);

        var rowMutated = false;
        var lockSeized = false;

        // The update hook fires inside the mutating statement; the trace hook fires as
        // each subsequent statement (or trigger program) starts. A seize attempted while
        // the writer still holds the lock returns SQLITE_BUSY immediately and is retried
        // at the next statement, so the lock is taken at the first point the writer has
        // released it.
        strdelegate_update onUpdate = (_, _, database, table, _) =>
        {
            if (database == "main" && table == "OrleansStorage")
            {
                rowMutated = true;
            }
        };
        strdelegate_trace onStatement = (_, _) =>
        {
            if (rowMutated && !lockSeized)
            {
                lockSeized = raw.sqlite3_exec(holderHandle, "BEGIN IMMEDIATE;") == raw.SQLITE_OK;
            }
        };

        raw.sqlite3_update_hook(writerHandle, onUpdate, null);
        raw.sqlite3_trace(writerHandle, onStatement, null);
        try
        {
            var reported = RunOn(writer, _writeSql, version, payload: true, commandTimeoutSeconds: 1);
            return new WriteUnderContention(reported, null, lockSeized);
        }
        catch (SqliteException failure)
        {
            return new WriteUnderContention(null, failure, lockSeized);
        }
        finally
        {
            raw.sqlite3_trace(writerHandle, (strdelegate_trace)null!, null);
            raw.sqlite3_update_hook(writerHandle, (strdelegate_update)null!, null);
            if (lockSeized)
            {
                raw.sqlite3_exec(holderHandle, "ROLLBACK;");
            }

            GC.KeepAlive(onUpdate);
            GC.KeepAlive(onStatement);
        }
    }

    private int? StoredVersion()
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT MAX(Version) FROM OrleansStorage;";
        var value = command.ExecuteScalar();
        return value is null or DBNull ? null : Convert.ToInt32(value);
    }

    private sealed record WriteUnderContention(List<int>? Reported, SqliteException? Failure, bool LockSeized);
}
