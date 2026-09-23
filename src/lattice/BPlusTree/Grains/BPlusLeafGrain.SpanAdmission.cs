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
    /// Whether <paramref name="pivot"/> may be used to divide this leaf, via the
    /// shared <see cref="SplitPivotAdmission"/> core the Coyote model also
    /// executes. Admissibility is strictly stronger than
    /// <see cref="DeclaresKey"/>; see that core for why ownership is not a
    /// sufficient test for a pivot. Issue 3117.
    /// </summary>
    private bool IsAdmissibleSplitPivot(string? pivot) =>
        SplitPivotAdmission.IsAdmissible(
            pivot, state.State.LowKeyInclusive, state.State.HighKeyExclusive);

    /// <summary>
    /// Selects the median admissible pivot, or <see langword="null"/> when this
    /// leaf holds no row strictly inside its own declared range and the split
    /// must therefore be declined.
    /// <para>
    /// <c>Cache.Keys</c> is touched only here, on the cold repair path, because
    /// reading it hydrates the whole snapshot. The hot path returns on
    /// <see cref="IsAdmissibleSplitPivot"/> alone, so a healthy split still
    /// bisects without hydrating.
    /// </para>
    /// </summary>
    private string? TryFindAdmissibleSplitPivot() =>
        SplitPivotAdmission.SelectMedianAdmissible(
            Cache.Keys, state.State.LowKeyInclusive, state.State.HighKeyExclusive);

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
    /// outside this leaf's declared range, so the caller can route the batch
    /// through <see cref="SetManyAdmittingSpanAsync"/> (or, for saga and
    /// observer writes, the per-key loop that forwards through
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
                var sibling = grainFactory.GetGrain<IBPlusLeafGrain>(target);

                // Carry the shadow markers across WITH the rows, and before
                // them. A forwarded row keeps its IsMigrated flag, so the
                // destination read gate will consult a marker for it - but the
                // marker lives on THIS leaf, keyed by this leaf's
                // _shadowedSagas and _pendingTx, and the forward would
                // otherwise leave it stranded here. The receiving leaf would
                // then hold a migrated row with no gate and serve the
                // pre-saga value ungated, splitting atomic visibility against
                // a sibling key whose backstop terminal had landed (#3117).
                //
                // Split already does exactly this via the same helper, for the
                // same reason; a span forward moves rows between leaves just
                // as a split does, so it owes the same transfer. Installing
                // the markers FIRST means there is no instant at which the
                // destination holds the row without its gate. Over-installing
                // is harmless and self-healing: once the saga's terminal has
                // been applied on the destination, _recentlyTerminal makes the
                // marker a no-op (see IsShadowedReadSafeAsync).
                //
                // Allocation-free on the steady-state path - the helper
                // returns immediately on two null checks when this leaf holds
                // neither markers nor prepared buckets.
                await TransferShadowMarkersToSiblingAsync(sibling, bucket.Keys);

                // The forwarded SplitResult is deliberately discarded. It
                // describes a split of the *sibling*, and the shard root
                // installs a separator against the leaf it called; returning a
                // sibling's result would make it install that separator against
                // the wrong leaf. The sibling's own callers observe its splits.
                // This matches the existing split-recovery forward above it.
                await sibling.MergeManyAsync(bucket, isCrossShardMigration);
            }
        }

        return local;
    }

    /// <summary>
    /// The conditional-bulk-write counterpart of
    /// <see cref="ForwardOutOfSpanMergeAsync"/>. Splits a conditional batch by
    /// declared span, forwards each out-of-span group to the leaf that declares
    /// it so the guard is evaluated against the key's real committed value,
    /// evaluates the remainder locally, and returns the union of both written
    /// sets. Entries that are out of span but have no resolvable forward target
    /// are retained locally, matching the fail-open rule documented on this
    /// class.
    /// <para>
    /// This exists because the conditional path cannot reuse the unconditional
    /// <c>SetManyAsync</c> forward: the guard has to be evaluated where the
    /// value lives, and <c>SetManyAsync</c> carries no guard. Without it, a key
    /// whose row a split moved to a sibling probes
    /// absent in this leaf's cache and is read as a guard miss, so a matching
    /// key is silently dropped from the written set (issue #2663).
    /// </para>
    /// <para>
    /// Grouping by target rather than forwarding per key keeps the batched
    /// shape the caller asked for: a conditional batch that straddles one
    /// boundary costs one extra grain call, not one per row. Termination is the
    /// same chain argument as the merge forward - a leaf's high bound equals
    /// its successor's low bound, so a forward is strictly monotonic along the
    /// chain and cannot bounce back to the sender.
    /// </para>
    /// </summary>
    private async Task<ConditionalSetManyResult> ForwardOutOfSpanConditionalSetManyAsync(
        List<KeyValuePair<string, byte[]>> entries, LatticePredicateNode predicate)
    {
        var local = new List<KeyValuePair<string, byte[]>>(entries.Count);
        Dictionary<GrainId, List<KeyValuePair<string, byte[]>>>? buckets = null;

        foreach (var entry in entries)
        {
            if (!TryResolveSpanForwardTarget(entry.Key, out var target))
            {
                local.Add(entry);
                continue;
            }

            buckets ??= new Dictionary<GrainId, List<KeyValuePair<string, byte[]>>>();
            if (!buckets.TryGetValue(target, out var bucket))
            {
                buckets[target] = bucket = new List<KeyValuePair<string, byte[]>>();
            }

            bucket.Add(entry);
        }

        if (buckets is null)
        {
            // Every out-of-span entry fell open to a local commit, so the
            // matched set can still straddle the span and the re-scan stands.
            return await SetManyWherePredicateLocalAsync(local, predicate, mayContainOutOfSpanKey: true);
        }

        HashSet<string>? forwardWritten = null;
        foreach (var (target, bucket) in buckets)
        {
            var sibling = grainFactory.GetGrain<IBPlusLeafGrain>(target);

            // Shadow markers are deliberately NOT transferred here. The merge
            // forward above moves rows this leaf currently holds, so their
            // gates must travel with them; a conditional write forwards a
            // caller's proposed value for a row this leaf does not hold, which
            // is the foreground shape SetCoreAsync's span forward takes - and
            // that path transfers no markers either.
            //
            // The forwarded SplitResult is discarded for the same reason the
            // merge forward discards it: it describes a split of the sibling,
            // and the shard root installs the separator it is returned against
            // the leaf it called.
            var forwarded = await sibling.SetManyWherePredicateAsync(bucket, predicate);
            var written = forwarded.WrittenKeys;
            for (var i = 0; i < written.Count; i++)
            {
                (forwardWritten ??= new HashSet<string>(StringComparer.Ordinal)).Add(written[i]);
            }
        }

        var localResult = await SetManyWherePredicateLocalAsync(local, predicate, mayContainOutOfSpanKey: true);
        if (forwardWritten is null)
        {
            return localResult;
        }

        return localResult with
        {
            WrittenKeys = MergeSpanForwardedWrittenKeys(entries, localResult.WrittenKeys, forwardWritten),
        };
    }

    /// <summary>
    /// The unconditional-bulk-write counterpart of
    /// <see cref="ForwardOutOfSpanMergeAsync"/>, used by <c>SetManyAsync</c>
    /// when a foreground batch arrives on a leaf that is mid-split or whose
    /// declared range excludes some of its keys.
    /// <para>
    /// An in-progress split is completed first, once for the whole batch,
    /// through the same gated recovery <c>SetCoreAsync</c> runs per key. That
    /// narrows this leaf's high bound to the split key, and <c>BeginSplit</c>
    /// has already pointed <c>NextSibling</c> at the new sibling, so declared
    /// span admission then routes every key exactly where the per-key recovery
    /// would have: a key at or above the split key goes to the sibling, and the
    /// rest stays here. The batch is then split by declared span, each
    /// out-of-span group is forwarded as one <c>SetManyAsync</c> to the leaf
    /// that declares it, and the remainder commits locally through
    /// <c>CommitSetManyAsync</c>. Entries that are out of span but have no
    /// resolvable forward target are retained locally, matching the fail-open
    /// rule documented on this class.
    /// </para>
    /// <para>
    /// This replaced the per-key <c>SetAsync</c> loop for foreground batches
    /// (issue #3348). That loop paid one full, serial WAL admission and commit
    /// per key, so under WAL saturation a single mid-split or straddling leaf
    /// call ran for minutes. The loop and the fan-out budget above it are
    /// all-or-nothing, so that one slow branch turned an otherwise-written batch
    /// into a refused one. Saga-prepared, atomic-batch, and merge-observer
    /// writes still take the per-key loop: their per-key semantics are what
    /// their own suites prove, and none of them is on the bulk foreground path.
    /// </para>
    /// <para>
    /// The forwards and the local commit run concurrently. They touch disjoint
    /// keys on distinct grains, and <see cref="Task.WhenAll(Task[])"/> observes
    /// every fault, so a failure in one never leaves another unobserved. As with
    /// the per-key loop, a fault after one part has committed leaves the batch
    /// partially applied; a retry is idempotent under LWW.
    /// </para>
    /// </summary>
    private async Task<SplitResult?> SetManyAdmittingSpanAsync(List<KeyValuePair<string, byte[]>> entries)
    {
        SplitResult? recovered = null;
        if (HasInterruptedSplit)
        {
            recovered = await CompleteRecoverySplitUnderGateAsync();
        }

        var local = new List<KeyValuePair<string, byte[]>>(entries.Count);
        Dictionary<GrainId, List<KeyValuePair<string, byte[]>>>? buckets = null;

        foreach (var entry in entries)
        {
            if (!TryResolveSpanForwardTarget(entry.Key, out var target))
            {
                local.Add(entry);
                continue;
            }

            buckets ??= new Dictionary<GrainId, List<KeyValuePair<string, byte[]>>>();
            if (!buckets.TryGetValue(target, out var bucket))
            {
                buckets[target] = bucket = new List<KeyValuePair<string, byte[]>>();
            }

            bucket.Add(entry);
        }

        var localCommit = local.Count > 0
            ? CommitSetManyAsync(local)
            : Task.FromResult<SplitResult?>(null);

        if (buckets is not null)
        {
            var parts = new Task[buckets.Count + 1];
            var i = 0;
            foreach (var (target, bucket) in buckets)
            {
                // Shadow markers are not transferred, for the reason the
                // conditional forward gives: a set forwards a caller's
                // proposed value for a row this leaf does not hold, which is
                // the shape SetCoreAsync's span forward takes. The sibling's
                // SplitResult is discarded for the reason given in
                // ForwardOutOfSpanMergeAsync.
                parts[i++] = grainFactory.GetGrain<IBPlusLeafGrain>(target).SetManyAsync(bucket);
            }

            parts[i] = localCommit;
            await Task.WhenAll(parts);
        }

        // Mirrors the per-key loop, which returned the last non-null result:
        // a split this batch's own commit triggered takes precedence over the
        // recovered one.
        return await localCommit ?? recovered;
    }

    /// <summary>
    /// Re-emits the union of the keys committed locally and the keys a span
    /// forward committed on a sibling, in the caller's original entry order.
    /// <para>
    /// The order is load-bearing, not cosmetic.
    /// <c>ShardRootGrain.ForwardWrittenEntriesToShadowIfNeededAsync</c> pairs a
    /// leaf's written keys back against the slice it dispatched with an
    /// allocation-free two-pointer walk that assumes the written set is an
    /// in-order subsequence of that slice. Appending the forwarded keys after
    /// the local ones would desynchronise that walk and mis-attribute shadow
    /// forwards, so the union is re-derived by walking the original entries.
    /// </para>
    /// </summary>
    private static List<string> MergeSpanForwardedWrittenKeys(
        List<KeyValuePair<string, byte[]>> entries,
        IReadOnlyList<string> localWritten,
        HashSet<string> forwardWritten)
    {
        var merged = new List<string>(localWritten.Count + forwardWritten.Count);
        var next = 0;
        foreach (var entry in entries)
        {
            // The local written set is itself an in-order subsequence of
            // entries, so one forward-only cursor settles local membership
            // without a second lookup structure.
            if (next < localWritten.Count
                && string.Equals(entry.Key, localWritten[next], StringComparison.Ordinal))
            {
                merged.Add(entry.Key);
                next++;
            }
            else if (forwardWritten.Contains(entry.Key))
            {
                merged.Add(entry.Key);
            }
        }

        return merged;
    }
}
