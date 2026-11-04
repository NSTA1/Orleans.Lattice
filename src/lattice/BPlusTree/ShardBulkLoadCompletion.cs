using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Issues a shard's raw bulk load and, when the call outruns the response timeout,
/// waits for the load the shard root is still running instead of re-issuing it
/// (#5812). A load's size is unbounded, so no response timeout fits every load; a
/// re-issued call cannot help either, because the shard root is not reentrant and the
/// second call only queues behind the first until it, too, times out.
/// </summary>
/// <remarks>
/// After a timeout the caller polls
/// <see cref="IShardRootGrain.GetBulkOperationStatusAsync(string)"/> for the same
/// operation id. <see cref="BulkOperationStatus.Completed"/> returns;
/// <see cref="BulkOperationStatus.Running"/> keeps waiting; and
/// <see cref="BulkOperationStatus.NotRunning"/> - the call was dropped, faulted, or the
/// shard root's activation was lost - re-issues the load, at most
/// <see cref="MaxReissues"/> times. The wait is bounded twice over: a running load
/// ends, because every await inside it is itself a bounded grain call or storage
/// operation over a finite batch, and <see cref="DefaultWaitCeiling"/> is a hang guard
/// that rethrows the timeout should it not. A shard root that cannot answer the status
/// read - a silo still on lattice 10.2.0, which does not know the method - rethrows
/// the original timeout, which is exactly what the caller saw before.
/// </remarks>
internal static class ShardBulkLoadCompletion
{
    /// <summary>The delays between status reads while a load runs.</summary>
    internal static readonly IReadOnlyList<TimeSpan> DefaultPollBackoff =
        [TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(5)];

    /// <summary>
    /// The longest one load is waited for, across every timeout and re-issue. Half of
    /// a restore prepare's one-hour progress deadline, so a load that hangs fails the
    /// prepare with its cause rather than the coordinator's deadline.
    /// </summary>
    internal static readonly TimeSpan DefaultWaitCeiling = TimeSpan.FromMinutes(30);

    /// <summary>How many times a load the shard root no longer runs is issued again.</summary>
    internal const int MaxReissues = 2;

    /// <summary>
    /// Loads <paramref name="entries"/> into <paramref name="shard"/>, waiting out a
    /// response timeout as described on <see cref="ShardBulkLoadCompletion"/>.
    /// </summary>
    /// <param name="shard">The shard root.</param>
    /// <param name="operationId">The load's operation id; a re-issue reuses it, so the load stays idempotent.</param>
    /// <param name="entries">The entries, sorted ascending by key.</param>
    /// <param name="logger">Where entering and leaving a wait is logged, or <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels a wait between status reads.</param>
    /// <param name="pollBackoff">The delays between status reads; <see cref="DefaultPollBackoff"/> by default.</param>
    /// <param name="waitCeiling">The hang guard; <see cref="DefaultWaitCeiling"/> by default.</param>
    /// <returns>A task that completes when the shard has completed the load.</returns>
    /// <exception cref="TimeoutException">
    /// The load timed out and the shard could not report its status, the hang guard
    /// expired, or the re-issues were spent; the inner exception is the last timeout
    /// where there is one.
    /// </exception>
    internal static async Task BulkLoadRawAsync(
        IShardRootGrain shard,
        string operationId,
        List<LwwEntry> entries,
        ILogger? logger,
        CancellationToken cancellationToken = default,
        IReadOnlyList<TimeSpan>? pollBackoff = null,
        TimeSpan? waitCeiling = null)
    {
        ArgumentNullException.ThrowIfNull(shard);
        ArgumentNullException.ThrowIfNull(entries);
        long waitStarted = 0;
        for (var reissues = 0; ; reissues++)
        {
            TimeoutException timeout;
            try
            {
                await shard.BulkLoadRawAsync(operationId, entries).ConfigureAwait(false);
                if (waitStarted != 0)
                {
                    LogLeft(logger, operationId, BulkOperationStatus.Completed, waitStarted);
                }

                return;
            }
            catch (TimeoutException ex)
            {
                timeout = ex;
            }

            if (waitStarted == 0)
            {
                waitStarted = Stopwatch.GetTimestamp();
                logger?.LogWarning(
                    "Bulk load {OperationId} outran the response timeout; waiting for the shard root to finish it rather than re-issuing it.",
                    operationId);
            }

            var status = await WaitWhileRunningAsync(
                shard, operationId, timeout, waitStarted, pollBackoff ?? DefaultPollBackoff,
                waitCeiling ?? DefaultWaitCeiling, cancellationToken).ConfigureAwait(false);
            if (status == BulkOperationStatus.Completed)
            {
                LogLeft(logger, operationId, status, waitStarted);
                return;
            }

            if (reissues >= MaxReissues)
            {
                throw new TimeoutException(
                    $"Bulk load '{operationId}' timed out and was re-issued {reissues} time(s) without completing.", timeout);
            }

            logger?.LogWarning(
                "Bulk load {OperationId} is no longer running on its shard root and did not complete; issuing it again.",
                operationId);
        }
    }

    private static async Task<BulkOperationStatus> WaitWhileRunningAsync(
        IShardRootGrain shard,
        string operationId,
        TimeoutException timeout,
        long waitStarted,
        IReadOnlyList<TimeSpan> pollBackoff,
        TimeSpan waitCeiling,
        CancellationToken cancellationToken)
    {
        for (var attempt = 0; ; attempt++)
        {
            BulkOperationStatus status;
            try
            {
                status = await shard.GetBulkOperationStatusAsync(operationId).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                throw new TimeoutException(
                    $"Bulk load '{operationId}' timed out and its shard root could not report its status: {ex.Message}",
                    timeout);
            }

            if (status != BulkOperationStatus.Running)
            {
                return status;
            }

            var delay = pollBackoff[Math.Min(attempt, pollBackoff.Count - 1)];
            if (Stopwatch.GetElapsedTime(waitStarted) + delay > waitCeiling)
            {
                throw new TimeoutException(
                    $"Bulk load '{operationId}' was still running on its shard root after {waitCeiling}.", timeout);
            }

            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
        }
    }

    private static void LogLeft(ILogger? logger, string operationId, BulkOperationStatus status, long waitStarted) =>
        logger?.LogInformation(
            "Bulk load {OperationId} left its completion wait as {Status} after {Elapsed}.",
            operationId, status, Stopwatch.GetElapsedTime(waitStarted));
}
