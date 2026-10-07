using Microsoft.Extensions.Logging;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Receiver side of the forced-gap re-seed (issue #4534). A sender that lost
/// records to a write-ahead-log trim before shipping them withholds saga
/// records from this receiver and stamps its pushes with the export epoch the
/// receiver must bootstrap past (<see cref="ReplicationBatch.ReseedAfterEpoch"/>).
/// The receiver answers with the epoch of the last full bootstrap it completed
/// from that sender, and starts one when it has none past the requested epoch
/// and none is already running.
/// </summary>
internal static class ReplicationReseedResponder
{
    /// <summary>
    /// Returns the epoch to echo in <see cref="ReplicationAck.BootstrapEpoch"/>
    /// and, when <paramref name="autoBootstrap"/> allows it, starts the
    /// bootstrap the sender is waiting for. Never throws: a failed lookup echoes
    /// nothing, which keeps the sender withholding (fail closed).
    /// </summary>
    public static async Task<long?> RespondAsync(
        IGrainFactory grainFactory,
        string treeName,
        string sourceClusterId,
        long reseedAfterEpoch,
        bool autoBootstrap,
        ILogger logger)
    {
        try
        {
            var coordinator = grainFactory.GetGrain<ILatticeBootstrapCoordinatorGrain>(treeName);
            var completed = await coordinator.GetCompletedExportEpochAsync(sourceClusterId).ConfigureAwait(false);
            if ((completed ?? 0) > reseedAfterEpoch)
            {
                return completed;
            }

            var status = await coordinator.GetStatusAsync().ConfigureAwait(false);
            if (status.SourceClusterId is null && autoBootstrap)
            {
                logger.LogWarning(
                    "Tree '{TreeName}': sender '{Source}' lost records to a WAL trim before shipping them and withholds saga "
                    + "records until this receiver re-seeds past export epoch {Epoch}; starting a bootstrap.",
                    treeName, sourceClusterId, reseedAfterEpoch);
            }

            // Records the request even when no bootstrap starts here (one already
            // running from the sender, or automatic bootstrap off), so the drain
            // that serves it clears the stale pending buckets (#4533).
            _ = StartBootstrapAsync(coordinator, treeName, sourceClusterId, reseedAfterEpoch, autoBootstrap, logger);
            return completed;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Tree '{TreeName}': could not answer sender '{Source}''s re-seed request; it keeps withholding saga records.",
                treeName, sourceClusterId);
            return null;
        }
    }

    /// <summary>
    /// <see langword="true"/> when <paramref name="entries"/> carries a saga
    /// record and <paramref name="sourceClusterId"/> has a re-seed request this
    /// receiver has not yet drained and cleared past (issue #4533): such a
    /// record was pushed before the sender's re-seed marker, and applying it
    /// after the stale-bucket clear could stage a purged saga's prepare that
    /// nothing settles. The caller refuses the batch with a not-accepted ack.
    /// A failed lookup refuses too (fail closed).
    /// </summary>
    public static async Task<bool> RefusesStragglerAsync(
        IGrainFactory grainFactory,
        string treeName,
        string sourceClusterId,
        IReadOnlyList<WalRecord> entries,
        ILogger logger)
    {
        var carriesSaga = false;
        for (var i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            if (entry.IsPrepared || entry.Op is MutationKind.TxCommit or MutationKind.TxAbort)
            {
                carriesSaga = true;
                break;
            }
        }

        if (!carriesSaga)
        {
            return false;
        }

        try
        {
            return await grainFactory.GetGrain<ILatticeBootstrapCoordinatorGrain>(treeName)
                .IsReseedPendingAsync(sourceClusterId)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Tree '{TreeName}': could not tell whether sender '{Source}' awaits a re-seed; its saga records are refused "
                + "until it can.",
                treeName, sourceClusterId);
            return true;
        }
    }

    private static async Task StartBootstrapAsync(
        ILatticeBootstrapCoordinatorGrain coordinator, string treeName, string sourceClusterId, long reseedAfterEpoch, bool start, ILogger logger)
    {
        try
        {
            await coordinator.BootstrapForReseedAsync(sourceClusterId, reseedAfterEpoch, start).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // An already-running bootstrap, or one that failed: the sender's next
            // stamped push asks again.
            logger.LogDebug(ex, "Tree '{TreeName}': re-seed bootstrap from '{Source}' did not complete.", treeName, sourceClusterId);
        }
    }
}
