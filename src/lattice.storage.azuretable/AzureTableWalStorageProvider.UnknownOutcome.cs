using System.Diagnostics;
using Azure;
using Azure.Data.Tables;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// Phase-1 faults whose outcome is unknown (issue #6116). A phase-1 transaction
/// the client abandoned on a per-attempt network timeout, or one answered by a
/// transient fault, may still commit on the service after the call has surfaced
/// the fault. Reported as a plain failure, the WAL shard's resync re-seeded its
/// allocator from a tail read taken before that late landing, reissued the window,
/// and the overlap guard then refused every append until a resync settled: a
/// sticky-failure storm. The provider instead marks the surfaced fault through
/// <see cref="WalAppendOutcomeUnknown"/> with a settle signal it owns, and the
/// shard fences the window as an abandoned flush until the signal completes.
/// </summary>
public sealed partial class AzureTableWalStorageProvider
{
    /// <summary>
    /// Default for <see cref="PhaseOneUnknownOutcomeSettleHorizon"/>: 60 seconds,
    /// matching <see cref="PhaseTwoWorker.DefaultAbandonedSubmitSettleCap"/>. Azure
    /// Tables bounds a request's server-side execution well inside it.
    /// </summary>
    internal static readonly TimeSpan DefaultPhaseOneUnknownOutcomeSettleHorizon = TimeSpan.FromSeconds(60);

    /// <summary>The first wait before the settle signal probes for a late landing.</summary>
    internal static readonly TimeSpan DefaultPhaseOneUnknownOutcomeFirstProbeDelay = TimeSpan.FromMilliseconds(250);

    /// <summary>The longest wait between two probes for a late landing.</summary>
    private static readonly TimeSpan PhaseOneUnknownOutcomeMaxProbeDelay = TimeSpan.FromSeconds(5);

    private static readonly string[] PhaseOneSettleProbeColumns = ["RowKey"];

    /// <summary>
    /// How long after a phase-1 fault with an unknown outcome the batch may still
    /// land. The settle signal completes as soon as the batch's first entry row is
    /// present (the transaction is atomic, so the whole batch landed), or once this
    /// horizon has passed with it absent, after which the window is treated as a
    /// hole. A landing later still lands below every offset the shard allocated
    /// since, so it cannot collide with one; it is reclaimed with the failed batches.
    /// </summary>
    internal TimeSpan PhaseOneUnknownOutcomeSettleHorizon { get; set; } = DefaultPhaseOneUnknownOutcomeSettleHorizon;

    /// <summary>The first wait before the settle signal probes; a test seam.</summary>
    internal TimeSpan PhaseOneUnknownOutcomeFirstProbeDelay { get; set; } = DefaultPhaseOneUnknownOutcomeFirstProbeDelay;

    /// <summary>
    /// Whether <paramref name="fault"/> leaves open that the request which raised it
    /// was, or will be, executed by the service: a timeout or cancellation, a
    /// transient status, or a transport failure.
    /// </summary>
    private static bool MayHaveLanded(Exception fault) => fault switch
    {
        AggregateException aggregate => aggregate.InnerExceptions.Any(MayHaveLanded),
        RequestFailedException { Status: 0 } => true,
        HttpRequestException or IOException => true,
        _ => IsTransientPhaseOneFault(fault) || (fault.InnerException is { } inner && MayHaveLanded(inner)),
    };

    /// <summary>
    /// Whether <paramref name="fault"/> is the client giving up on an attempt it had
    /// sent: a timeout or a cancellation, directly or wrapped.
    /// </summary>
    private static bool IsClientAbandonment(Exception fault) => fault switch
    {
        OperationCanceledException or TimeoutException => true,
        AggregateException aggregate => aggregate.InnerExceptions.Any(IsClientAbandonment),
        _ => fault.InnerException is { } inner && IsClientAbandonment(inner),
    };

    /// <summary>
    /// Marks <paramref name="fault"/> as a phase-1 fault whose batch may still land,
    /// and starts the settle signal the WAL shard waits on. Runs only on the fault
    /// path.
    /// </summary>
    private void MarkPhaseOneOutcomeUnknown(
        Exception fault,
        TableClient table,
        IReadOnlyList<TableTransactionAction> actions,
        string treeId,
        int shardIndex,
        int attempts,
        int clientAbandonedAttempts)
    {
        if (actions.Count == 0 || actions[0].Entity is not AzureTableWalEntity first)
        {
            return;
        }

        var endOffsetInclusive = actions[^1].Entity is AzureTableWalEntity last ? last.Offset : first.Offset;
        var activity = GetShardActivity(BuildManifestPartitionKey(treeId, shardIndex));
        var settled = SettleUnknownPhaseOneOutcomeAsync(
            table, first.PartitionKey, first.RowKey, activity, first.Offset, endOffsetInclusive);
        WalAppendOutcomeUnknown.Mark(fault, settled, attempts, clientAbandonedAttempts);
    }

    /// <summary>
    /// Completes once the batch whose first entry row is <paramref name="rowKey"/>
    /// in <paramref name="partitionKey"/> has landed, or once
    /// <see cref="PhaseOneUnknownOutcomeSettleHorizon"/> has passed without it.
    /// Probes with a doubling back-off and never faults. Either way the batch is
    /// then recorded with the shard's failed batches: it has no manifest row, so
    /// the next reconciliation reclaims its rows if no roll-forward committed it.
    /// </summary>
    private async Task SettleUnknownPhaseOneOutcomeAsync(
        TableClient table,
        string partitionKey,
        string rowKey,
        WalShardActivity activity,
        long startOffset,
        long endOffsetInclusive)
    {
        var started = Stopwatch.GetTimestamp();
        var delay = PhaseOneUnknownOutcomeFirstProbeDelay;
        try
        {
            while (Volatile.Read(ref _disposed) == 0)
            {
                var remaining = PhaseOneUnknownOutcomeSettleHorizon - Stopwatch.GetElapsedTime(started);
                if (remaining <= TimeSpan.Zero)
                {
                    return;
                }

                await Task.Delay(delay < remaining ? delay : remaining).ConfigureAwait(false);
                delay = delay * 2 < PhaseOneUnknownOutcomeMaxProbeDelay ? delay * 2 : PhaseOneUnknownOutcomeMaxProbeDelay;
                try
                {
                    await table.GetEntityAsync<AzureTableWalEntity>(
                        partitionKey, rowKey, PhaseOneSettleProbeColumns, CancellationToken.None).ConfigureAwait(false);
                    return;
                }
                catch (Exception)
                {
                    // Absent (404) or unreadable: keep probing until the horizon.
                }
            }
        }
        finally
        {
            activity.FailedBatches.Add(startOffset, endOffsetInclusive);
        }
    }
}
