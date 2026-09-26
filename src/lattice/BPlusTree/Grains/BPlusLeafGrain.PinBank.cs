using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

internal sealed partial class BPlusLeafGrain
{
    /// <inheritdoc />
    public async Task BankDurablePinAsync()
    {
        // Answers without waiting for the activation replay, and does nothing
        // while one is outstanding: the replay owns the checkpoint until it
        // latches, and the caller (the WAL GC's arm-2 first tier) grades the
        // call by the pin and escalates when nothing moved.
        if (string.IsNullOrEmpty(state.State.TreeId)
            || (_replayBarrierArmed && !_replayBarrierSatisfied && !_replayBarrierRetired))
        {
            return;
        }

        var options = await GetOptionsAsync();
        await BankDurablePinCoreAsync(
            Math.Max(1, options.WalPartitions),
            flushPendingCheckpoint: true,
            CancellationToken.None);
    }

    /// <summary>
    /// The permit-free bank step (issue #3599): commits any pending checkpoint
    /// advance, captures a snapshot when a partition's checkpoint has run ahead
    /// of its durable coverage and the existing no-loss precondition holds, and
    /// publishes the durable pin when a partition's
    /// <c>min(persisted checkpoint, durable coverage)</c> is above what an
    /// awaited flush is known to have published.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>It acquires no replay permit and replays nothing.</b> Every step works
    /// only on state this activation already holds: the pending advance was
    /// banked by an earlier apply, the capture serialises the resident cache,
    /// and the publish reads the persisted checkpoint and coverage. That is what
    /// distinguishes it from the starvation drive, whose replay is the expensive,
    /// permit-gated part; this is the drive's tail without its replay.
    /// </para>
    /// <para>
    /// <b>The pin is never loosened.</b> The capture runs through
    /// <see cref="MaybeRunPeriodicSnapshotRecheckAsync"/>, so every no-loss
    /// precondition is the existing one, and coverage is restamped only by a
    /// save the store kept. The publish resolves each partition through
    /// <c>ResolveDurablePinForPartition</c>, which clamps it at the coverage
    /// read AFTER the capture's outcome, so a capture that is declined, throws,
    /// or is not attempted still publishes
    /// <c>min(persisted, existing coverage)</c> and never more. The WAL between
    /// coverage and the checkpoint is the only durable copy of those rows and
    /// deletes, so a higher pin would license a trim a rehydrate could not
    /// recover from.
    /// </para>
    /// <para>
    /// Declines while a starvation drive or warm rescue is in flight on this
    /// activation: both end in this same flush-recheck-publish tail, and both
    /// interleave, so running it underneath them would only race their own
    /// publish. The capture and publish are each contained; the pending-advance
    /// flush is not, exactly as on every other caller of it.
    /// </para>
    /// </remarks>
    /// <param name="partitionCount">The configured WAL partition count.</param>
    /// <param name="flushPendingCheckpoint">
    /// Whether to commit a pending checkpoint advance first. The WAL GC's
    /// explicit call always does; the coverage-lag tick does only when the
    /// checkpoint-coalescing predicate says the advance is due (issue #3608),
    /// so a periodic tick never defeats the coalescing window that
    /// deliberately holds an advance pending, yet cannot leave one pending for
    /// ever on a write-idle leaf. The tick still banks everything already
    /// persisted.
    /// </param>
    /// <param name="cancellationToken">Bounds the flush, the recheck and the publish.</param>
    private async Task BankDurablePinCoreAsync(
        int partitionCount,
        bool flushPendingCheckpoint,
        CancellationToken cancellationToken)
    {
        if (_starvationDriveInFlight || _warmRescueInFlight)
        {
            return;
        }

        if (flushPendingCheckpoint)
        {
            await ((ILeafProjection)this).FlushCheckpointAsync(cancellationToken);
        }

        try
        {
            await MaybeRunPeriodicSnapshotRecheckAsync(
                fromCheckpointPersist: false,
                cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            ResolveLogger()?.LogWarning(
                ex,
                "Leaf {GrainId}: snapshot recheck failed during the durable pin bank step; the pin is still "
                + "published at the existing coverage (#3599).",
                context.GrainId);
        }

        if (!IsDurablePinBehindBankableOffset(partitionCount))
        {
            return;
        }

        try
        {
            await FlushDurableMaterialiserFrontierAsync(cancellationToken);
            _durableFrontierBarriered = true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            ResolveLogger()?.LogWarning(
                ex,
                "Leaf {GrainId}: publishing the durable pin during the bank step failed; the checkpoint and "
                + "coverage are persisted and the next bank step retries (#3599).",
                context.GrainId);
        }
    }
}
