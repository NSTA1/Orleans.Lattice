using Microsoft.Extensions.Logging;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The discard of the prepares an atomic-write saga left on the copies it
/// re-bound away from (issue #4689, spec/shard-ownership/ShardOwnershipCutover.tla).
/// <para>
/// A saga re-binds when the tree moves off its bound copy and that copy mirrors
/// nowhere - the copy a shadow-cutover restore retains. It re-dispatches its
/// whole batch onto the copy the tree resolves to, but the prepares it had
/// already taken on the copy it left stay there as buckets of a saga that goes on
/// to commit. A stale reader served that copy before the restore arms its
/// redirect, or any reader once a revert makes it live again, would be served
/// those buckets beside the other keys' pre-saga values: a torn batch.
/// </para>
/// <para>
/// So before any decision the saga drops its buckets on every recorded copy
/// (<see cref="State.AtomicWriteState.AbandonedCopyShards"/>): each shard it
/// touched there and every shard a split of them leads to, every leaf of each.
/// The calls are addressed to the physical copy directly, with no routed-logical
/// stamp, so a retained redirect admits them as it admits a direct terminal.
/// <see cref="IBPlusLeafGrain.DiscardPendingTransactionAsync"/> drops the bucket
/// without recording a decision or logging a terminal, remembers the
/// transaction as terminal so a routed prepare of it still on the wire is
/// refused, and durably marks a leaf that held a bucket so a replay does not
/// recreate it. A copy that was purged, or that a resize undo discarded, serves
/// no one, so it counts as done.
/// </para>
/// </summary>
internal sealed partial class AtomicWriteGrain
{
    /// <summary>
    /// Discards the saga's prepares on every copy it re-bound away from, then
    /// clears the record in memory; the next state write persists that. Throws
    /// when a discard fails, so the saga does not reach its decision with an
    /// orphan left behind: the execute phase is re-entered and the discard,
    /// which is idempotent, runs again.
    /// </summary>
    private async Task DiscardAbandonedCopiesAsync()
    {
        if (state.State.AbandonedCopyShards is not { Count: > 0 } abandoned
            || state.State.TransactionId == Guid.Empty)
        {
            return;
        }

        // Addressed to each physical copy directly, never through the alias: a
        // routed-logical stamp inherited from the call that started the saga
        // would make a retained redirect refuse it.
        RequestContext.Remove(LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey);

        foreach (var (copy, shards) in abandoned)
        {
            if (string.Equals(copy, state.State.BoundPhysicalTreeId, StringComparison.Ordinal))
            {
                continue;
            }

            await DiscardOnCopyAsync(copy, shards).ConfigureAwait(true);
        }

        state.State.AbandonedCopyShards = null;
    }

    /// <summary>
    /// <see cref="DiscardAbandonedCopiesAsync"/> for the abort path, where the
    /// buckets are never surfaced and the abort must proceed: a failure is
    /// logged and the record kept.
    /// </summary>
    private async Task TryDiscardAbandonedCopiesAsync()
    {
        try
        {
            await DiscardAbandonedCopiesAsync().ConfigureAwait(true);
        }
        catch (Exception ex) when (!GrainStateWriteFaults.IsTranslatedConflict(ex))
        {
            Logger.LogWarning(ex,
                "Atomic-write saga {OperationKey}: could not discard its prepares on a copy it re-bound away from before aborting; they stay stranded there (issue #4689).",
                OperationKey);
        }
    }

    private async Task DiscardOnCopyAsync(string copy, List<int> touched)
    {
        var transactionId = state.State.TransactionId;
        try
        {
            var shards = await TerminalFanOutResolver.ResolveTransitiveAsync(
                grainFactory, copy, touched, CancellationToken.None).ConfigureAwait(true);
            foreach (var shardIndex in shards)
            {
                var shard = grainFactory.GetGrain<IShardRootGrain>($"{copy}/{shardIndex}");
                var leafId = await shard.GetLeftmostLeafIdAsync().ConfigureAwait(true);
                while (leafId is { } id)
                {
                    var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(id);
                    await leaf.DiscardPendingTransactionAsync(transactionId).ConfigureAwait(true);
                    leafId = await leaf.GetNextSiblingAsync().ConfigureAwait(true);
                }
            }
        }
        catch (LatticeTreePurgedException)
        {
            // A purged copy refuses whoever still addresses it and serves no one.
        }
        catch (InvalidOperationException)
        {
            // A copy a resize undo discarded serves no one either; any other
            // refusal keeps the saga from deciding.
            if (!await TerminalCopyWasDiscardedAsync(copy).ConfigureAwait(true))
                throw;
        }

        Logger.LogInformation(
            "Atomic-write saga {OperationKey}: discarded its prepares on {Copy}, a copy it re-bound away from, before its decision (issue #4689).",
            OperationKey, copy);
    }
}
