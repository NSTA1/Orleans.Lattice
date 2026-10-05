using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The online-resize mirror of a key-routed write, made at this copy's own
/// stamps (issue #4522).
/// <para>
/// A resize mirrors each write this shard applies onto the destination copy R,
/// which becomes authoritative at the swap. It used to forward the operation
/// itself, in parallel with the local apply, so R re-minted every mirrored
/// write on its own clock. R's clock does not order those writes against a
/// saga's prepare stamp P minted here: a plain write acknowledged here below P
/// could land on R above P and beat the saga's value there (a torn batch on R,
/// and once acknowledged a lost committed write); an unmarked mirrored prepare
/// fell back to R's dominating drain.
/// </para>
/// <para>
/// The mirror now runs after the local apply and reads back what the apply
/// stored, so R holds exactly this copy's history:
/// </para>
/// <list type="bullet">
/// <item><description>
/// A saga prepare is forwarded as the same operation carrying each key's
/// original prepare stamp, so R buckets it AT P and marks it, and R's leaf
/// clock merges past P.
/// </description></item>
/// <item><description>
/// Any other write is forwarded as the rows it left here - value, tombstone and
/// expiry at this copy's stamp - through a last-writer-wins merge, so R never
/// re-mints it. The merge advances R's leaf clock past each stamp, so a write R
/// mints after the swap still sorts above them.
/// </description></item>
/// </list>
/// <para>
/// Range deletes and merges already carry their stamps (the routing tier issues
/// a range delete's stamp under an override; a merge ships its rows' own
/// stamps), so they forward the operation unchanged. With R on this copy's
/// lineage, the stamps a saga's terminal carries are carried to R too.
/// </para>
/// </summary>
internal sealed partial class ShardRootGrain
{
    /// <summary>
    /// Mirrors the write this shard has just applied to <paramref name="keys"/>
    /// to the resize destination, at this copy's own stamps. A saga prepare is
    /// forwarded as <paramref name="preparedForward"/> carrying each key's
    /// original prepare stamp; any other write as the rows the apply stored. A
    /// no-op when no resize mirror is active. Throws when a prepared key's bucket
    /// cannot be found, so the write fails rather than mirror an unmarked
    /// prepare that R would stamp on its own clock.
    /// </summary>
    private async Task MirrorAppliedWritesAsync<TState>(
        IReadOnlyCollection<string> keys,
        TState preparedState,
        Func<IShardRootGrain, TState, Task> preparedForward,
        Func<TState, IReadOnlyList<TState>>? preparedSplitPerKey = null)
    {
        if (keys.Count == 0 || TryGetShadowTarget() is null)
            return;

        var transactionId = LatticeTransactionContext.Current;
        if (LatticePreparedContext.Current && transactionId != Guid.Empty)
        {
            var stamps = await MirroredPrepareStampsAsync(transactionId, keys);
            using (LatticeOriginalPrepareStampContext.WithoutPreparedRoute())
            using (LatticeOriginalPrepareStampContext.With(stamps))
            {
                await TrackShadowForward(preparedState, preparedForward, preparedSplitPerKey);
            }

            return;
        }

        var rows = await AppliedRowsAsync(keys);
        if (rows.Count == 0)
            return;

        await TrackShadowForward(
            rows,
            static (t, s) => t.MergeManyAsync(s),
            static s => ShadowForwardRefusal.PerEntry(s));
    }

    /// <summary>
    /// The original prepare stamp of each of <paramref name="keys"/> this shard
    /// just prepared under <paramref name="transactionId"/>, or
    /// <see langword="null"/> when none is marked (a CRDT-delta prepare folds at
    /// the terminal stamp, so it has none to carry).
    /// </summary>
    private async Task<Dictionary<string, HybridLogicalClock>?> MirroredPrepareStampsAsync(
        Guid transactionId,
        IReadOnlyCollection<string> keys)
    {
        var buckets = await GetOriginalPrepareStampsAsync(transactionId, exhaustive: false);
        Dictionary<string, HybridLogicalClock>? stamps = null;
        foreach (var key in keys)
        {
            if (!buckets.TryGetValue(key, out var stamp))
            {
                throw new InvalidOperationException(
                    $"Shard '{context.GrainId}' found no prepared bucket for key '{key}' of transaction {transactionId} to mirror to the resize destination with its prepare stamp (issue #4522); the write is retried.");
            }

            if (stamp is { } p)
                (stamps ??= new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal))[key] = p;
        }

        return stamps;
    }

    /// <summary>
    /// The distinct keys of <paramref name="entries"/>, in first-seen order.
    /// </summary>
    private static List<string> DistinctKeys(List<KeyValuePair<string, byte[]>> entries)
    {
        var seen = new HashSet<string>(entries.Count, StringComparer.Ordinal);
        var keys = new List<string>(entries.Count);
        foreach (var entry in entries)
        {
            if (seen.Add(entry.Key))
                keys.Add(entry.Key);
        }

        return keys;
    }

    /// <summary>
    /// The rows this shard's leaves hold for <paramref name="keys"/>, tombstones
    /// included, at their own stamps. A key with no row is omitted.
    /// </summary>
    private async Task<Dictionary<string, LwwValue<byte[]>>> AppliedRowsAsync(IReadOnlyCollection<string> keys)
    {
        var reads = new List<(string Key, Task<LwwEntry?> Read)>(keys.Count);
        foreach (var key in keys)
        {
            var leafId = RootIsLeafTyped
                ? state.State.RootNodeId!.Value
                : await TraverseToLeafAsync(key);
            reads.Add((key, grainFactory.GetGrain<IBPlusLeafGrain>(leafId).GetRawEntryAsync(key)));
        }

        var rows = new Dictionary<string, LwwValue<byte[]>>(reads.Count, StringComparer.Ordinal);
        foreach (var (key, read) in reads)
        {
            if (await read is { } row)
                rows[key] = row.ToLwwValue();
        }

        return rows;
    }
}
