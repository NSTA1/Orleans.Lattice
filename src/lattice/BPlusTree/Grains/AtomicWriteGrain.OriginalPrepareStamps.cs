using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The coordinator half of applying a saga's committed value at its original
/// prepare stamp (issue #4522).
/// <para>
/// A terminal's committed-values backstop installs a key's value on a leaf that
/// holds no bucket for it - the sibling a leaf split moved the key's range to,
/// or a split destination the key moved to after the decision. Installed at a
/// fresh, dominating stamp, it overwrote any write acknowledged on that leaf
/// after the prepare. The leaf applies a backstop key under last-writer-wins at
/// its original stamp P instead when the delivery carries P
/// (<see cref="LatticeOriginalPrepareStampContext"/>), so the coordinator reads
/// each key's P back from the buckets that hold it and carries it on every
/// terminal.
/// </para>
/// <para>
/// <b>Complete or not at all.</b> After its prepares, and before the checkpoint
/// that ends the execute phase, the coordinator asks every shard a prepare can
/// have reached - the touched shards and every shard a split of them leads to
/// - for the saga's buckets
/// (<see cref="IShardRootGrain.GetOriginalPrepareStampsAsync"/>). Each shard
/// first reads the leaves it recorded the prepares reaching; for any key that
/// pass did not find, the coordinator asks again exhaustively, adding each
/// key's owner under refreshed routing, and each shard reads its whole leaf
/// chain, whose buckets and marks are replayed from the
/// write-ahead log. Every entry must be accounted for - marked with its stamp,
/// or unmarked - before the checkpoint is written. A read that faults or still
/// misses a key fails the batch, which the execute loop retries like any other
/// batch failure and, once its retries are spent, aborts: the saga never
/// commits without each key's stamp, so no backstop ever falls back to a
/// dominating stamp. The stamps are persisted with the checkpoint, so a
/// coordinator that reactivates after the decision still carries them.
/// </para>
/// <para>
/// <b>Lineage.</b> P orders writes only on the copy whose clocks minted it, so it
/// is carried only to a shard of that physical tree. A terminal re-resolved to
/// another copy (an alias swap), redelivered to a resized copy because the old
/// one was purged (#4475), or mirrored by a shard root to a resize destination
/// carries none.
/// </para>
/// </summary>
internal sealed partial class AtomicWriteGrain
{
    /// <summary>
    /// Reads back the original prepare stamp of every entry from the bound copy
    /// and records them in the saga state, in memory; the caller persists them
    /// with the batch checkpoint. Throws when any read faults or any entry is
    /// not found in a bucket, so the caller treats the batch as failed.
    /// </summary>
    private async Task ReadBackOriginalPrepareStampsAsync()
    {
        var physicalTreeId = state.State.BoundPhysicalTreeId;
        var transactionId = state.State.TransactionId;
        if (string.IsNullOrEmpty(physicalTreeId) || transactionId == Guid.Empty || state.State.Entries.Count == 0)
            return;

        var found = new Dictionary<string, HybridLogicalClock?>(state.State.Entries.Count, StringComparer.Ordinal);
        var shards = await ReadBackShardsAsync(physicalTreeId, withCurrentOwners: false).ConfigureAwait(true);
        await ReadBackPassAsync(physicalTreeId, transactionId, shards, exhaustive: false, found).ConfigureAwait(true);

        if (CountMissing(found) > 0)
        {
            // The fast pass missed a key: a shard that reactivated after the
            // prepares, or a bucket a split moved off the shard that recorded
            // it. Re-resolve with fresh routing and read every leaf.
            RecordReadBackSlowPath(physicalTreeId, LatticeMetrics.PrepareStampReadBackExhaustive);
            shards = await ReadBackShardsAsync(physicalTreeId, withCurrentOwners: true).ConfigureAwait(true);
            await ReadBackPassAsync(physicalTreeId, transactionId, shards, exhaustive: true, found).ConfigureAwait(true);
        }

        var missing = CountMissing(found);
        if (missing > 0)
        {
            RecordReadBackSlowPath(physicalTreeId, LatticeMetrics.PrepareStampReadBackIncomplete);
            throw new InvalidOperationException(
                $"Atomic-write saga '{OperationKey}' found no prepared bucket for {missing} of its {state.State.Entries.Count} keys on '{physicalTreeId}' when reading back their original prepare stamps (issue #4522); the batch is retried.");
        }

        Dictionary<string, HybridLogicalClock>? stamps = null;
        foreach (var (key, stamp) in found)
        {
            if (stamp is { } p)
                (stamps ??= new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal))[key] = p;
        }

        state.State.OriginalPrepareStamps = stamps;
        state.State.OriginalPrepareStampsPhysicalTreeId = physicalTreeId;
    }

    /// <summary>
    /// The read-back for a committed saga whose execute phase recorded none -
    /// its state was persisted by a silo that predates it. The decision is
    /// recorded, so a failed read cannot abort the saga: it is logged and
    /// counted, and the broadcast proceeds without stamps, as it did on that
    /// silo.
    /// </summary>
    private async Task ReadBackBeforeBroadcastAsync()
    {
        try
        {
            await ReadBackOriginalPrepareStampsAsync().ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            state.State.OriginalPrepareStamps = null;
            state.State.OriginalPrepareStampsPhysicalTreeId = null;
            Logger.LogWarning(ex,
                "Saga {OperationKey} was prepared by a silo that predates the original-prepare-stamp read-back and its read-back before the commit broadcast failed; the broadcast proceeds without stamps (issue #4522).",
                OperationKey);
        }
    }

    /// <summary>
    /// The shards of <paramref name="physicalTreeId"/> to read: the touched
    /// shards and every shard a split of them leads to, and, on the exhaustive
    /// pass (<paramref name="withCurrentOwners"/>), each entry's owner under
    /// freshly refreshed routing as well, in case a prepare was routed by a map
    /// newer than the one the touched shards were captured from.
    /// </summary>
    private async Task<List<int>> ReadBackShardsAsync(string physicalTreeId, bool withCurrentOwners)
    {
        var seed = new SortedSet<int>(state.State.TouchedShards);
        if (withCurrentOwners)
        {
            var routing = await grainFactory.GetGrain<ILattice>(state.State.TreeId)
                .GetRoutingAsync(forceRefresh: true).ConfigureAwait(true);
            if (string.Equals(routing.PhysicalTreeId, physicalTreeId, StringComparison.Ordinal))
            {
                foreach (var entry in state.State.Entries)
                    seed.Add(routing.Map.Resolve(entry.Key));
            }
        }

        return await TerminalFanOutResolver.ResolveTransitiveAsync(
            grainFactory, physicalTreeId, seed, CancellationToken.None).ConfigureAwait(true);
    }

    private async Task ReadBackPassAsync(
        string physicalTreeId,
        Guid transactionId,
        List<int> shards,
        bool exhaustive,
        Dictionary<string, HybridLogicalClock?> found)
    {
        var reads = new Task<Dictionary<string, HybridLogicalClock?>>[shards.Count];
        for (var i = 0; i < shards.Count; i++)
        {
            reads[i] = grainFactory
                .GetGrain<IShardRootGrain>($"{physicalTreeId}/{shards[i]}")
                .GetOriginalPrepareStampsAsync(transactionId, exhaustive);
        }

        try
        {
            await Task.WhenAll(reads).ConfigureAwait(true);
        }
        catch
        {
            RecordReadBackSlowPath(physicalTreeId, LatticeMetrics.PrepareStampReadBackFailed);
            throw;
        }

        var entryKeys = EntryKeys();
        foreach (var read in reads)
        {
            if (read.Result is not { } shardStamps) continue;
            foreach (var (key, stamp) in shardStamps)
            {
                // A shard reports every bucket of the saga it holds; only this
                // saga's entries are of interest.
                if (!entryKeys.Contains(key)) continue;
                found[key] = found.TryGetValue(key, out var existing)
                    ? ShardRootGrain.MergeOriginalStamp(existing, stamp)
                    : stamp;
            }
        }
    }

    private HashSet<string> EntryKeys()
    {
        var keys = new HashSet<string>(state.State.Entries.Count, StringComparer.Ordinal);
        foreach (var entry in state.State.Entries)
            keys.Add(entry.Key);
        return keys;
    }

    private int CountMissing(Dictionary<string, HybridLogicalClock?> found)
    {
        var missing = 0;
        foreach (var entry in state.State.Entries)
        {
            if (!found.ContainsKey(entry.Key))
                missing++;
        }
        return missing;
    }

    private void RecordReadBackSlowPath(string physicalTreeId, string reason)
    {
        LatticeMetrics.AtomicWritePrepareStampReadBackSlowPath.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, _metricTreeId ?? state.State.TreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagReason, reason),
            LatticeTenantLabel.ForTree(state.State.TreeId));
        if (reason != LatticeMetrics.PrepareStampReadBackExhaustive)
        {
            Logger.LogWarning(
                "Saga {OperationKey} could not read back the original prepare stamps of its keys on {PhysicalTreeId} ({Reason}); the batch is retried, and the saga aborts if its retries are spent (issue #4522).",
                OperationKey, physicalTreeId, reason);
        }
    }

    /// <summary>
    /// The original prepare stamps a committed terminal delivery to a shard of
    /// <paramref name="physicalTreeId"/> carries for <paramref name="committedValues"/>,
    /// or <see langword="null"/>: on an abort, with no backstop, or when the
    /// stamps were minted on another copy (the lineage guard).
    /// </summary>
    internal Dictionary<string, HybridLogicalClock>? CarriedOriginalStamps(
        string physicalTreeId,
        bool committed,
        IReadOnlyDictionary<string, byte[]>? committedValues)
    {
        if (!committed
            || committedValues is not { Count: > 0 }
            || state.State.OriginalPrepareStamps is not { Count: > 0 } stamps
            || !string.Equals(state.State.OriginalPrepareStampsPhysicalTreeId, physicalTreeId, StringComparison.Ordinal))
        {
            return null;
        }

        Dictionary<string, HybridLogicalClock>? carried = null;
        foreach (var key in committedValues.Keys)
        {
            if (stamps.TryGetValue(key, out var stamp))
                (carried ??= new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal))[key] = stamp;
        }

        return carried;
    }
}
