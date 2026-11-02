using System.Runtime.CompilerServices;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The stamped dispatch of a fenced WAL append (epic #5605), called by
/// <see cref="WalCommitLogWriter"/> from inside its outbound shard-RPC dispatch
/// sites when the append carries a leaf fence. It asks the fence for a stamp,
/// calls the stamped shard method, and handles every fence outcome:
/// </summary>
/// <remarks>
/// <list type="bullet">
///   <item><description><b>Gate closed:</b> the existing, unstamped method, then the
///   leaf's self-report of its own unstamped append.</description></item>
///   <item><description><b>Unloaded entry:</b> the partition restarted or moved since
///   the claim; the fence re-presents its claim, which loads the entry, and the append
///   is retried. Nothing was allocated.</description></item>
///   <item><description><b>Stale:</b> the activation was superseded; the fence stops
///   it and the append fails with <see cref="WalAppendEpochStaleException"/>, which the
///   seam that called the leaf retries against the live activation.</description></item>
///   <item><description><b>Method unknown:</b> the partition is hosted by a silo
///   running an older build that joined before this silo's gate observed it; only on
///   that definite failure is the append re-issued unstamped, and the fence closes its
///   view of the gate. Any other failure, above all a timeout, may have been allocated,
///   so it propagates unchanged.</description></item>
/// </list>
/// <para>
/// <b>Why <c>ConfigureAwait(false)</c>.</b> Every await here is part of the outbound
/// shard-RPC dispatch that the writer's wedge-attribution sites await with
/// <c>ConfigureAwait(false)</c>, so its continuations must not be queued back onto a
/// possibly-wedged caller grain context either. The fence's mutable state is guarded
/// by its own lock, not by the grain turn.
/// </para>
/// </remarks>
internal static class WalFencedAppendDispatch
{
    /// <summary>How many times an append re-presents a claim after an unloaded-entry rejection.</summary>
    internal const int MaxUnloadedRetries = 8;

    /// <summary>Appends one entry through <paramref name="fence"/>.</summary>
    /// <param name="grain">The partition's WAL shard.</param>
    /// <param name="entry">The record to append.</param>
    /// <param name="batched">Use the interleaving batched overloads, as the writer's point path does.</param>
    /// <param name="partition">The partition index.</param>
    /// <param name="fence">The leaf activation's fence.</param>
    /// <param name="waitForDeadline">
    /// Also abandon each shard call when <paramref name="cancellationToken"/> fires,
    /// whether or not the callee honours it, exactly as the writer's bounded
    /// dispatch does for an unfenced append.
    /// </param>
    /// <param name="cancellationToken">The dispatch's cancellation, or its deadline.</param>
    /// <remarks>
    /// Built on the pooling builder, so a fenced append whose stamp is already
    /// claimed allocates no state machine of its own (the note's cost bound).
    /// </remarks>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal static async ValueTask<long> AppendPointAsync(
        IWalShardGrain grain,
        WalRecord entry,
        bool batched,
        int partition,
        ILeafAppendFence fence,
        bool waitForDeadline,
        CancellationToken cancellationToken)
    {
        for (var attempt = 0; ; attempt++)
        {
            var stamp = await fence.StampForAsync(grain, partition, cancellationToken).ConfigureAwait(false);
            if (stamp is null)
            {
                long offset;
                if (batched)
                {
                    var offsets = await Bound(grain.AppendBatchAsync(new[] { entry }, cancellationToken), waitForDeadline, cancellationToken).ConfigureAwait(false);
                    offset = offsets[0];
                }
                else
                {
                    offset = await Bound(grain.AppendAsync(entry, cancellationToken), waitForDeadline, cancellationToken).ConfigureAwait(false);
                }

                fence.OnUnstampedAppended(partition, offset);
                return offset;
            }

            WalAppendFenceOutcome outcome;
            long admitted;
            long watermark;
            try
            {
                if (batched)
                {
                    var result = await Bound(grain.AppendBatchStampedAsync(new[] { entry }, stamp.Value, cancellationToken), waitForDeadline, cancellationToken).ConfigureAwait(false);
                    outcome = result.Outcome;
                    admitted = result.Offsets.Length == 0 ? WalStampedBatchResult.Rejected : result.Offsets[0];
                    watermark = result.UnstampedWatermark;
                }
                else
                {
                    var result = await Bound(grain.AppendStampedAsync(entry, stamp.Value, cancellationToken), waitForDeadline, cancellationToken).ConfigureAwait(false);
                    outcome = result.Outcome;
                    admitted = result.Offset;
                    watermark = result.UnstampedWatermark;
                }
            }
            catch (Exception ex) when (WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(ex))
            {
                fence.OnStampedMethodUnknown();
                continue;
            }

            if (outcome == WalAppendFenceOutcome.Admitted)
            {
                fence.OnStampedAppended(partition, watermark);
                return admitted;
            }

            HandleRejection(fence, partition, stamp.Value, outcome, attempt);
        }
    }

    /// <summary>Appends one partition's slice of a batch through <paramref name="fence"/>.</summary>
    /// <param name="grain">The partition's WAL shard.</param>
    /// <param name="entries">The partition's slice.</param>
    /// <param name="partition">The partition index.</param>
    /// <param name="fence">The leaf activation's fence.</param>
    /// <param name="waitForDeadline">As for <see cref="AppendPointAsync"/>.</param>
    /// <param name="cancellationToken">The dispatch's cancellation, or its deadline.</param>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    internal static async ValueTask<IReadOnlyList<long>> AppendBatchAsync(
        IWalShardGrain grain,
        IReadOnlyList<WalRecord> entries,
        int partition,
        ILeafAppendFence fence,
        bool waitForDeadline,
        CancellationToken cancellationToken)
    {
        for (var attempt = 0; ; attempt++)
        {
            var stamp = await fence.StampForAsync(grain, partition, cancellationToken).ConfigureAwait(false);
            if (stamp is null)
            {
                var offsets = await Bound(grain.AppendBatchAsync(entries, cancellationToken), waitForDeadline, cancellationToken).ConfigureAwait(false);
                if (offsets.Count > 0)
                {
                    fence.OnUnstampedAppended(partition, offsets[offsets.Count - 1]);
                }

                return offsets;
            }

            WalStampedBatchResult result;
            try
            {
                result = await Bound(grain.AppendBatchStampedAsync(entries, stamp.Value, cancellationToken), waitForDeadline, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(ex))
            {
                fence.OnStampedMethodUnknown();
                continue;
            }

            if (result.Outcome == WalAppendFenceOutcome.Admitted)
            {
                fence.OnStampedAppended(partition, result.UnstampedWatermark);
                return result.Offsets;
            }

            // A partially admitted batch is a stale one: an unloaded entry is only
            // ever met before the first allocation.
            HandleRejection(fence, partition, stamp.Value, result.Outcome, attempt);
        }
    }

    private static Task<T> Bound<T>(Task<T> call, bool waitForDeadline, CancellationToken cancellationToken) =>
        waitForDeadline ? call.WaitAsync(cancellationToken) : call;

    private static void HandleRejection(ILeafAppendFence fence, int partition, WalAppendStamp stamp, WalAppendFenceOutcome outcome, int attempt)
    {
        if (outcome == WalAppendFenceOutcome.UnloadedEntry && attempt < MaxUnloadedRetries)
        {
            fence.OnUnloaded(partition, stamp);
            return;
        }

        throw fence.OnStale(stamp);
    }
}
