using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Original-prepare-stamp partial for <see cref="BPlusLeafGrain"/> (issue #4522).
/// <para>
/// <b>The defect.</b> A saga's terminal installed its committed values under a
/// stamp minted at terminal time, above every row the leaf held - the drain by a
/// counter bump past the leaf clock, the committed-values backstop by
/// <c>Tick(max(clock, row))</c>. A write acknowledged after the prepare that
/// reached the leaf as a cross-shard migration import (a split's shadow-forward
/// of a plain write) was overwritten by the drain, because the orphan guard
/// exempts a migrated row; and the backstop overwrote any later write on a leaf
/// that held no bucket. The acknowledged write was lost.
/// </para>
/// <para>
/// <b>The rule.</b> A <i>marked</i> prepare - one whose stamp P is its prepare's
/// original stamp - is applied under last-writer-wins at P: installed only when
/// the key has no row or a row stamped below P, and stored AT P. Any write
/// stamped at or above P survives, migrated or not, and in whichever order it
/// lands relative to the terminal. This rests on property H: every write
/// acknowledged after a prepare is stamped above that prepare's P, which holds
/// because the leaf that holds the key has a clock at or above P (it minted P,
/// or merged it when the prepare was forwarded or replayed, or inherited it
/// from its split donor). The read gate's supersession test is the exact
/// complement: a row stamped at or above P supersedes a marked prepare.
/// </para>
/// <para>
/// <b>Rolling upgrade.</b> A prepare is marked only on positive evidence that
/// its stamp is original (see <see cref="LatticeOriginalPrepareStampContext"/>).
/// An unmarked prepare - written by an older silo, forwarded without its
/// stamp, or replayed from a record that predates the marker - may be stamped
/// with the destination's own clock, below a pre-saga migrated value it must
/// still beat, so it keeps the pre-#4522 drain and read gate unchanged,
/// including the migrated-row carve-out (Fix M).
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// The prepared keys, per saga, whose bucketed stamp is NOT known to be the
    /// prepare's original stamp. Tracked as the complement so the steady state
    /// (every prepare marked) allocates nothing: a key absent from this map is
    /// marked. Activation-scoped like <see cref="_pendingTx"/>; replay rebuilds
    /// it from each record's <see cref="LatticeMutation.PrepareStampOriginal"/>.
    /// </summary>
    private Dictionary<Guid, HashSet<string>>? _unmarkedPrepares;

    /// <summary>
    /// This leaf's own shard key, <c>{treeId}/{shardIndex}</c>, cached for the
    /// prepared-route comparison. Rebuilt when the tree id or shard index the
    /// cache was built from no longer match the leaf's state.
    /// </summary>
    private string? _ownShardKey;
    private string? _ownShardKeyTreeId;
    private int _ownShardKeyIndex = -1;

    /// <summary>
    /// Whether <paramref name="key"/>'s bucketed prepare under
    /// <paramref name="transactionId"/> carries its prepare's original stamp.
    /// </summary>
    private bool IsPrepareStampOriginal(Guid transactionId, string key) =>
        _unmarkedPrepares is null
        || !_unmarkedPrepares.TryGetValue(transactionId, out var unmarked)
        || !unmarked.Contains(key);

    /// <summary>
    /// Records <paramref name="key"/>'s classification under
    /// <paramref name="transactionId"/>.
    /// </summary>
    private void SetPrepareStampOriginal(Guid transactionId, string key, bool original)
    {
        if (original)
        {
            if (_unmarkedPrepares is not null
                && _unmarkedPrepares.TryGetValue(transactionId, out var set)
                && set.Remove(key)
                && set.Count == 0)
            {
                _unmarkedPrepares.Remove(transactionId);
            }

            return;
        }

        _unmarkedPrepares ??= new Dictionary<Guid, HashSet<string>>();
        if (!_unmarkedPrepares.TryGetValue(transactionId, out var unmarked))
        {
            unmarked = new HashSet<string>(StringComparer.Ordinal);
            _unmarkedPrepares[transactionId] = unmarked;
        }

        unmarked.Add(key);
    }

    /// <summary>
    /// Drops every classification recorded under <paramref name="transactionId"/>,
    /// once its bucket has drained or been discarded.
    /// </summary>
    private void ForgetPrepareStampClassification(Guid transactionId) =>
        _unmarkedPrepares?.Remove(transactionId);

    /// <summary>
    /// Mints the stamp for a saga prepare-phase write of <paramref name="key"/>
    /// and classifies it (issue #4522).
    /// <list type="bullet">
    /// <item><description>
    /// A forwarder carried the prepare's original stamp P: the prepare is
    /// bucketed AT P (the leaf clock merged past it, keeping property H) and is
    /// marked.
    /// </description></item>
    /// <item><description>
    /// The routing tier dispatched the write to this leaf's own shard, and no
    /// override stamp is in force: the leaf mints the original stamp itself and
    /// the prepare is marked.
    /// </description></item>
    /// <item><description>
    /// Otherwise the stamp is minted as before and the prepare is unmarked.
    /// </description></item>
    /// </list>
    /// <c>Carried</c> reports the first case: the stamp was minted on another
    /// shard, so the value is on that shard's clock lineage and is stored
    /// migrated (issue #4564).
    /// </summary>
    private (HybridLogicalClock Stamp, bool Original, bool Carried) MintPreparedStamp(string key)
    {
        if (LatticeOriginalPrepareStampContext.TryGetStamp(key, out var carried))
        {
            state.State.Clock = HybridLogicalClock.Merge(state.State.Clock, carried);
            return (carried, true, true);
        }

        var original = LatticeHlcOverrideContext.Current is null
            && IsPreparedRouteToThisShard();
        return (AdvanceClockOrOverride(), original, false);
    }

    /// <summary>
    /// Whether the routing tier stamped this leaf's own shard as the target of
    /// the ambient prepared write.
    /// </summary>
    private bool IsPreparedRouteToThisShard()
    {
        if (LatticeOriginalPrepareStampContext.PreparedRoute is not { Length: > 0 } route)
            return false;

        var treeId = state.State.TreeId;
        if (string.IsNullOrEmpty(treeId) || state.State.ShardIndex is not { } shardIndex)
            return false;

        if (_ownShardKey is null
            || _ownShardKeyIndex != shardIndex
            || !string.Equals(_ownShardKeyTreeId, treeId, StringComparison.Ordinal))
        {
            _ownShardKey = $"{treeId}/{shardIndex}";
            _ownShardKeyTreeId = treeId;
            _ownShardKeyIndex = shardIndex;
        }

        return string.Equals(route, _ownShardKey, StringComparison.Ordinal);
    }

    /// <summary>
    /// Whether <paramref name="key"/>'s prepare under
    /// <paramref name="transactionId"/> is a marked last-writer-wins prepare,
    /// applied at its own stamp. A CRDT-delta prepare folds at the terminal
    /// stamp whatever its classification.
    /// </summary>
    private bool IsMarkedLwwPrepare(
        Guid transactionId,
        string key,
        Dictionary<string, (byte[] Delta, LatticeMergeMode Mode)>? deltaBucket) =>
        (deltaBucket is null || !deltaBucket.ContainsKey(key))
        && IsPrepareStampOriginal(transactionId, key);

    /// <summary>
    /// The rule (d) install test for a value carrying its prepare's original
    /// stamp <paramref name="prepareStamp"/>: install only when the key has no
    /// row or a row stamped below it. A row stamped at or above it is a write
    /// acknowledged after the prepare (property H) and survives.
    /// </summary>
    private bool IsRowAtOrAboveOriginalStamp(string key, HybridLogicalClock prepareStamp) =>
        Cache.TryGetRow(key, out var row) && row.Timestamp.CompareTo(prepareStamp) >= 0;

    /// <summary>
    /// Installs <paramref name="value"/> for <paramref name="key"/> AT its own
    /// stamp under rule (d). The caller has established the row is stamped below
    /// it, so the last-writer-wins merge inside <see cref="StoreEntry"/> keeps
    /// this value. The value keeps the migration provenance its prepare was
    /// bucketed with: migrated when its original stamp was carried from another
    /// shard, so a later migration import of a write acknowledged after the
    /// prepare competes with it by last-writer-wins instead of being dropped
    /// (issue #4564); otherwise not migrated, which clears any stale marker.
    /// Returns the stamp the value was stored at.
    /// </summary>
    private HybridLogicalClock StoreAtOriginalStamp(string key, in LwwValue<byte[]> value)
    {
        StoreEntry(key, value);
        return value.Timestamp;
    }

    /// <summary>
    /// The original prepare stamp of each committed-values backstop key that
    /// has one, or <see langword="null"/> when none does: a stamp carried by the
    /// terminal delivery (a forwarder's <see cref="LatticeOriginalPrepareStampContext"/>
    /// map), else, for a stranded prepared key, its marked last-writer-wins
    /// bucket entry's own stamp. Called before the bucket's classification is
    /// discarded by the drain. <paramref name="migratedKeys"/> receives the keys
    /// whose value is stored migrated (issue #4564): a stamp carried by the terminal
    /// delivery was minted on another shard, and a stranded bucket entry keeps the
    /// provenance it was bucketed with.
    /// </summary>
    private Dictionary<string, HybridLogicalClock>? CollectBackstopOriginalStamps(
        Guid transactionId,
        List<KeyValuePair<string, byte[]>>? missingKeys,
        Dictionary<string, LwwValue<byte[]>>? bucket,
        HashSet<string>? strandedPrepared,
        out HashSet<string>? migratedKeys)
    {
        migratedKeys = null;
        if (missingKeys is not { Count: > 0 })
            return null;

        var carried = LatticeOriginalPrepareStampContext.HasStamps;
        if (!carried && strandedPrepared is null)
            return null;

        Dictionary<string, (byte[] Delta, LatticeMergeMode Mode)>? deltaBucket = null;
        _pendingTxDeltas?.TryGetValue(transactionId, out deltaBucket);

        Dictionary<string, HybridLogicalClock>? stamps = null;
        foreach (var kvp in missingKeys)
        {
            if (carried && LatticeOriginalPrepareStampContext.TryGetStamp(kvp.Key, out var stamp))
            {
                (stamps ??= new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal))[kvp.Key] = stamp;
                (migratedKeys ??= new HashSet<string>(StringComparer.Ordinal)).Add(kvp.Key);
                continue;
            }

            if (strandedPrepared is not null
                && strandedPrepared.Contains(kvp.Key)
                && bucket is not null
                && bucket.TryGetValue(kvp.Key, out var prepared)
                && IsMarkedLwwPrepare(transactionId, kvp.Key, deltaBucket))
            {
                (stamps ??= new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal))[kvp.Key] = prepared.Timestamp;
                if (prepared.IsMigrated)
                    (migratedKeys ??= new HashSet<string>(StringComparer.Ordinal)).Add(kvp.Key);
            }
        }

        return stamps;
    }

    /// <inheritdoc />
    public async Task<Dictionary<string, HybridLogicalClock?>?> GetOriginalPrepareStampsAsync(Guid transactionId)
    {
        await AwaitReplayBarrierAsync();
        EnsureInternalOrigin(LatticeOperation.RangeRead);

        if (_pendingTx is null || !_pendingTx.TryGetValue(transactionId, out var bucket) || bucket.Count == 0)
            return null;

        Dictionary<string, (byte[] Delta, LatticeMergeMode Mode)>? deltaBucket = null;
        _pendingTxDeltas?.TryGetValue(transactionId, out deltaBucket);

        var stamps = new Dictionary<string, HybridLogicalClock?>(bucket.Count, StringComparer.Ordinal);
        foreach (var (key, prepared) in bucket)
        {
            stamps[key] = IsMarkedLwwPrepare(transactionId, key, deltaBucket)
                ? prepared.Timestamp
                : null;
        }

        return stamps;
    }

    /// <summary>
    /// The subset of <paramref name="stamps"/> for <paramref name="keys"/>, or
    /// <see langword="null"/> when none of them has an original stamp, so a
    /// forward carries exactly its own keys' stamps.
    /// </summary>
    private static Dictionary<string, HybridLogicalClock>? SelectStamps(
        Dictionary<string, HybridLogicalClock>? stamps,
        IEnumerable<string> keys)
    {
        if (stamps is null)
            return null;

        Dictionary<string, HybridLogicalClock>? selected = null;
        foreach (var key in keys)
        {
            if (stamps.TryGetValue(key, out var stamp))
                (selected ??= new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal))[key] = stamp;
        }

        return selected;
    }
}
