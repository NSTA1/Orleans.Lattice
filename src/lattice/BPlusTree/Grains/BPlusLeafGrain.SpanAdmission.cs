using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Declared-span admission for the leaf write path: the rule that a leaf only
/// commits a key its own <c>[LowKeyInclusive, HighKeyExclusive)</c> range
/// covers, and forwards anything else to the leaf that does cover it.
/// <para>
/// Two independent rules used to decide whether a leaf owns a key, and they
/// could disagree. The <b>write</b> path admitted purely by <i>routing</i>: a
/// write descended the internal separators, landed on whichever leaf they
/// currently named, and was acknowledged and WAL-appended without the leaf ever
/// consulting its own declared range. <b>Replay</b> admits by <i>declared
/// span</i> - <c>ShouldApplyDuringReplay</c> gates every Set / Delete /
/// Tombstone through <see cref="SplitBoundary.Owns"/>. A leaf could therefore
/// accept and acknowledge a row that its own replay would refuse to reinstate.
/// </para>
/// <para>
/// The window that produces the disagreement is the split. <c>CompleteSplit</c>
/// narrows the donor's <c>HighKeyExclusive</c> to the split key inside the
/// donor's own turn, but the separator that redirects routing to the new
/// sibling is installed by the shard root <em>after</em> that turn returns.
/// Between those two points routing still names the donor for keys the donor
/// has already stopped declaring, and every such write became an <i>orphan
/// row</i>: held and acknowledged by a leaf whose replay filter drops it.
/// </para>
/// <para>
/// An orphan is not by itself a lost write, and this file should not be read as
/// claiming it is. The WAL is shard-wide, so the leaf that legitimately declares
/// the key sees the same record during its own replay and a rebuild relocates
/// the row rather than dropping it. Durability only fails when a second,
/// uncontrolled condition also holds - the declaring leaf's projection
/// checkpoint is already past the offset the orphan occupies, so its replay
/// never reaches the record. The span disagreement creates the orphan; a
/// checkpoint accident decides whether it is recoverable. Removing the
/// disagreement removes the dependence on the accident.
/// </para>
/// <para>
/// <b>Why this is keyed off the declared span and not off
/// <see cref="SplitState"/>.</b> The pre-existing forwarding in
/// <c>SetCoreAsync</c> and <c>MergeManyAsync</c> fires only while
/// <c>SplitState == SplitInProgress</c>, and that condition is unreachable on
/// any leaf that has already split once. <see cref="SplitState"/> is a
/// join-merged one-way ratchet (<c>Unsplit &lt; SplitInProgress &lt;
/// SplitComplete</c>) and nothing ever writes <c>Unsplit</c> back, so a leaf
/// sits at <see cref="SplitState.SplitComplete"/> permanently after its first
/// split and a later <c>BeginSplit</c> cannot lower it again. Those guards are
/// therefore dead code exactly on the leaves that have split most, which is the
/// opposite of the population that needs them. The declared span carries no such
/// history: it is the same value the replay filter reads, so keying admission
/// off it is what makes the write path and the replay path agree by
/// construction.
/// </para>
/// <para>
/// <b>Termination.</b> The chain invariant is that a leaf's
/// <c>HighKeyExclusive</c> equals its successor's <c>LowKeyInclusive</c>.
/// Forwarding rightwards therefore lands on a leaf whose low bound is at most
/// the key, so only the high test can fail again and the next hop is strictly
/// rightwards; the leftwards case is symmetric. A forward can never bounce back
/// to its sender, so this introduces no reentrant grain cycle.
/// </para>
/// <para>
/// <b>Fail-open, deliberately.</b> When a key is out of span but no sibling
/// pointer names a leaf to forward it to, the write is committed locally as
/// before. That is the status quo rather than an improvement, and it is chosen
/// over failing the write: a leaf with a torn chain pointer would otherwise turn
/// a latent recoverability hazard into an outright write outage.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Whether this leaf declares any range bound at all. A leaf with two null
    /// bounds owns the whole keyspace (the single-leaf tree, the chain's outer
    /// ends before any split, and legacy state rows that pre-date the persisted
    /// range), so no key can be out of span and every scan below can be skipped
    /// outright. This keeps the batch paths allocation-free and comparison-free
    /// on the overwhelmingly common shape.
    /// </summary>
    private bool HasDeclaredSpan =>
        state.State.LowKeyInclusive is not null || state.State.HighKeyExclusive is not null;

    /// <summary>
    /// Whether this leaf's declared range covers <paramref name="key"/>. This is
    /// the identical call <c>ShouldApplyDuringReplay</c> makes, which is the
    /// point: the write path and the replay path now execute one shared rule.
    /// </summary>
    private bool DeclaresKey(string key) =>
        SplitBoundary.Owns(key, state.State.LowKeyInclusive, state.State.HighKeyExclusive);

    /// <summary>
    /// Resolves the leaf that should receive <paramref name="key"/> when this
    /// leaf's declared range excludes it, returning <see langword="false"/> when
    /// the key is in span (the common case) or when no forward target can be
    /// resolved.
    /// <para>
    /// The successor pointer is preferred over <c>SplitSiblingId</c> because it
    /// is the live chain pointer: split maintains it, empty-leaf reclaim
    /// maintains it in the same persist that widens the predecessor's high
    /// bound, and <c>TryClearAbsorbedSplitBoundary</c> nulls <c>SplitKey</c> and
    /// <c>SplitSiblingId</c> once a fold has absorbed the boundary.
    /// <c>SplitSiblingId</c> is kept only as a fallback for the narrow window in
    /// which a split has recorded the sibling identity but the successor pointer
    /// has not yet been persisted.
    /// </para>
    /// </summary>
    private bool TryResolveSpanForwardTarget(string key, out GrainId target)
    {
        target = default;

        var low = state.State.LowKeyInclusive;
        var high = state.State.HighKeyExclusive;
        if (SplitBoundary.Owns(key, low, high))
        {
            return false;
        }

        var candidate = high is not null && string.CompareOrdinal(key, high) >= 0
            ? state.State.NextSibling ?? state.State.SplitSiblingId
            : state.State.PrevSibling;

        // A self-reference would spin the forward on this same grain, and a
        // missing pointer means there is nowhere better to put the row than
        // here. Both fall back to committing locally.
        if (candidate is null || candidate.Value.Equals(context.GrainId))
        {
            return false;
        }

        target = candidate.Value;
        return true;
    }

    /// <summary>
    /// Cheap pre-scan for the batched set path: reports whether any entry falls
    /// outside this leaf's declared range, so the caller can divert the whole
    /// batch to the per-key loop (which forwards through
    /// <c>SetCoreAsync</c>) instead of committing it wholesale through
    /// <c>CommitSetManyAsync</c>, which has no per-key admission step.
    /// </summary>
    private bool ContainsOutOfSpanKey(List<KeyValuePair<string, byte[]>> entries)
    {
        if (!HasDeclaredSpan)
        {
            return false;
        }

        foreach (var entry in entries)
        {
            if (!DeclaresKey(entry.Key))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// The <c>MergeManyAsync</c> counterpart of
    /// <see cref="ContainsOutOfSpanKey(List{KeyValuePair{string, byte[]}})"/>.
    /// </summary>
    private bool ContainsOutOfSpanKey(Dictionary<string, LwwValue<byte[]>> entries)
    {
        if (!HasDeclaredSpan)
        {
            return false;
        }

        foreach (var key in entries.Keys)
        {
            if (!DeclaresKey(key))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Splits a merge batch by declared span, forwards each out-of-span group to
    /// the leaf that declares it, and returns the entries this leaf should still
    /// merge locally. Entries that are out of span but have no resolvable
    /// forward target are retained locally, matching the fail-open rule
    /// documented on this class.
    /// <para>
    /// Grouping by target rather than forwarding per key keeps the batched shape
    /// the caller asked for: a merge that straddles one boundary costs one extra
    /// grain call, not one per row.
    /// </para>
    /// </summary>
    private async Task<Dictionary<string, LwwValue<byte[]>>> ForwardOutOfSpanMergeAsync(
        Dictionary<string, LwwValue<byte[]>> entries, bool isCrossShardMigration)
    {
        var local = new Dictionary<string, LwwValue<byte[]>>(entries.Count);
        Dictionary<GrainId, Dictionary<string, LwwValue<byte[]>>>? buckets = null;

        foreach (var (key, lww) in entries)
        {
            if (!TryResolveSpanForwardTarget(key, out var target))
            {
                local[key] = lww;
                continue;
            }

            buckets ??= new Dictionary<GrainId, Dictionary<string, LwwValue<byte[]>>>();
            if (!buckets.TryGetValue(target, out var bucket))
            {
                buckets[target] = bucket = new Dictionary<string, LwwValue<byte[]>>();
            }

            bucket[key] = lww;
        }

        if (buckets is not null)
        {
            foreach (var (target, bucket) in buckets)
            {
                // The forwarded SplitResult is deliberately discarded. It
                // describes a split of the *sibling*, and the shard root
                // installs a separator against the leaf it called; returning a
                // sibling's result would make it install that separator against
                // the wrong leaf. The sibling's own callers observe its splits.
                // This matches the existing split-recovery forward above it.
                await grainFactory.GetGrain<IBPlusLeafGrain>(target)
                    .MergeManyAsync(bucket, isCrossShardMigration);
            }
        }

        return local;
    }
}
