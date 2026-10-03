using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Reports the startup <c>auto_vacuum</c> outcome and, when the database runs in
/// <see cref="SqliteAutoVacuumMode.Incremental"/> mode, returns freed pages to the
/// filesystem in small paced batches.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why incremental rather than full.</b> <c>auto_vacuum=FULL</c> relocates and
/// truncates pages inside every commit, so every grain-state write pays for it.
/// Incremental mode adds no work to a commit; the reclaim happens here instead, at a
/// bounded rate: at most <see cref="MaxPagesPerStep"/> pages every
/// <see cref="StepInterval"/>, each step one short write transaction. A large delete
/// (an index reset, say) is therefore given back over hours rather than in one burst
/// that competes with foreground writes.
/// </para>
/// <para>
/// A step that faults - characteristically a busy database under a write burst - is
/// logged and retried on the next interval. Reclaim is housekeeping and must never
/// fail the host.
/// </para>
/// </remarks>
public sealed class SqliteAutoVacuumService(
    RepoContextHostConfiguration config,
    SqliteAutoVacuumOutcome outcome,
    ILogger<SqliteAutoVacuumService> logger,
    TimeProvider? timeProvider = null,
    SqliteSnapshotSweepOutcome? snapshotSweep = null) : BackgroundService
{
    /// <summary>The spacing between reclaim steps.</summary>
    public static readonly TimeSpan StepInterval = TimeSpan.FromMinutes(1);

    /// <summary>The most freelist pages one step returns to the filesystem (16 MiB at the default 4 KiB page).</summary>
    public const int MaxPagesPerStep = 4096;

    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    /// <summary>
    /// Runs one bounded reclaim step against the database at <paramref name="connectionString"/>.
    /// </summary>
    /// <param name="connectionString">The SQLite connection string.</param>
    /// <param name="maxPages">The most freelist pages to release in this step.</param>
    /// <returns>The freelist page count before and after the step.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="connectionString"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxPages"/> is not positive.</exception>
    public static (long FreeBefore, long FreeAfter) ReclaimStep(string connectionString, int maxPages)
    {
        ArgumentNullException.ThrowIfNull(connectionString);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxPages);

        using var connection = new SqliteConnection(connectionString);
        connection.Open();

        var before = FreelistCount(connection);
        if (before == 0)
        {
            return (0, 0);
        }

        using (var command = connection.CreateCommand())
        {
            command.CommandText = $"PRAGMA incremental_vacuum({maxPages});";

            // The pragma returns one row per page released; drain them so the step
            // actually runs to its bound rather than stopping at the first row.
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
            }
        }

        return (before, FreelistCount(connection));
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        LogOutcome();

        if (config.SqliteAutoVacuum != SqliteAutoVacuumMode.Incremental)
        {
            return;
        }

        var connectionString = SqliteSchemaInitializer.BuildConnectionString(config.SqlitePath);
        var reclaimedSinceReport = 0L;
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(StepInterval, _time, stoppingToken).ConfigureAwait(false);

                try
                {
                    var (before, after) = ReclaimStep(connectionString, MaxPagesPerStep);
                    reclaimedSinceReport += Math.Max(0, before - after);
                    if (before > 0 && after == 0)
                    {
                        logger.LogInformation(
                            "SQLite incremental vacuum caught up: {Pages} freed page(s) returned to the filesystem.",
                            reclaimedSinceReport);
                        reclaimedSinceReport = 0;
                    }
                    else if (before > 0)
                    {
                        logger.LogDebug(
                            "SQLite incremental vacuum step: freelist {Before} -> {After} page(s).", before, after);
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    logger.LogWarning(
                        ex, "SQLite incremental vacuum step failed (non-fatal); retrying in {Interval}.", StepInterval);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // The host is stopping.
        }
    }

    private void LogOutcome()
    {
        if (outcome.Converted)
        {
            logger.LogInformation(
                "SQLite database converted from auto_vacuum={Previous} to auto_vacuum={Requested} by a one-time VACUUM "
                + "in {Elapsed} ms: {Before} -> {After} bytes.",
                outcome.Previous,
                outcome.Requested,
                (long)outcome.Elapsed.TotalMilliseconds,
                outcome.BytesBefore,
                outcome.BytesAfter);
        }
        else
        {
            logger.LogInformation(
                "SQLite auto_vacuum={Mode} ({Key}); database file is {Bytes} bytes.",
                outcome.Requested,
                RepoContextHostConfiguration.SqliteAutoVacuumKey,
                outcome.BytesAfter);
        }

        if (snapshotSweep is { Mode: not SqliteSnapshotSweepMode.Off } sweep)
        {
            logger.LogInformation(
                "SQLite snapshot sweep ({Key}={Mode}) in {Elapsed} ms: {Stranded} of {Scanned} leaf identities are "
                + "reached by no live row, holding {LeafRows} leaf row(s), {ManifestRows} snapshot manifest(s) and "
                + "{SegmentRows} snapshot segment(s), {PayloadBytes} payload bytes; {Deleted} row(s) deleted; "
                + "database file {Before} -> {After} bytes.",
                RepoContextHostConfiguration.SqliteSnapshotSweepKey,
                sweep.Mode,
                (long)sweep.Elapsed.TotalMilliseconds,
                sweep.StrandedLeaves,
                sweep.LeavesScanned,
                sweep.LeafRows,
                sweep.ManifestRows,
                sweep.SegmentRows,
                sweep.PayloadBytes,
                sweep.RowsDeleted,
                sweep.BytesBefore,
                sweep.BytesAfter);
        }
    }

    private static long FreelistCount(SqliteConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA freelist_count;";
        return Convert.ToInt64(command.ExecuteScalar(), System.Globalization.CultureInfo.InvariantCulture);
    }
}
