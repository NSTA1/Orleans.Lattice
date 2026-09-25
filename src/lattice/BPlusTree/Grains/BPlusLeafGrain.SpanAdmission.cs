using Microsoft.Extensions.Logging;
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
    /// <para>
    /// While a division of this leaf is in flight the successor pointer is not
    /// used: it already names the new sibling, which may not be initialised
    /// yet, while the high bound is still the pre-split one. A key at or above
    /// that bound goes to <c>OldNextSibling</c>, the real successor, instead
    /// (issue #3583). Termination is unaffected, because that successor's low
    /// bound is the pre-split high bound, so the forward still moves strictly
    /// rightwards.
    /// </para>
    /// </summary>
    private bool TryResolveSpanForwardTarget(string key, out GrainId target, out SpanFailOpenReason failOpen)
    {
        target = default;
        failOpen = SpanFailOpenReason.None;

        var low = state.State.LowKeyInclusive;
        var high = state.State.HighKeyExclusive;
        if (SplitBoundary.Owns(key, low, high))
        {
            return false;
        }

        // While a division is in flight - from the persist of its intent until
        // CompleteSplitAsync narrows this leaf - NextSibling already names the
        // new sibling, but HighKeyExclusive is still the pre-split bound and the
        // new sibling may not be initialised yet. A key at or above that bound
        // belongs to the real successor, which OldNextSibling holds until the
        // narrow. Forwarding it to the new sibling instead lands it on a leaf
        // with no declared span, which accepts and acknowledges it, and the
        // sibling's initialisation then declares [splitKey, preSplitHigh)
        // around it: an acknowledged write no read is routed to (issue #3583).
        var candidate = high is not null && string.CompareOrdinal(key, high) >= 0
            ? HasInterruptedSplit
                ? state.State.OldNextSibling
                : state.State.NextSibling ?? state.State.SplitSiblingId
            : state.State.PrevSibling;

        // A self-reference would spin the forward on this same grain, and a
        // missing pointer means there is nowhere better to put the row than
        // here. Both fall back to committing locally, and both are reported
        // through failOpen so the caller can count the fall-back (issue #2125).
        if (candidate is null)
        {
            failOpen = SpanFailOpenReason.NoSibling;
            return false;
        }

        if (candidate.Value.Equals(context.GrainId))
        {
            failOpen = SpanFailOpenReason.SelfReference;
            return false;
        }

        target = candidate.Value;
        return true;
    }

    /// <summary>
    /// Why <see cref="TryResolveSpanForwardTarget"/> declined to forward an
    /// out-of-span key. <see cref="None"/> covers both the in-span case and a
    /// successful forward, neither of which is a fail-open.
    /// </summary>
    private enum SpanFailOpenReason : byte
    {
        /// <summary>Not a fail-open: the key is in span, or a forward target resolved.</summary>
        None,

        /// <summary>The chain pointer on the key's side is null.</summary>
        NoSibling,

        /// <summary>The chain pointer on the key's side names this leaf.</summary>
        SelfReference,
    }

    /// <summary>
    /// The write origin a fail-open is attributed to on
    /// <see cref="LatticeMetrics.LeafSpanFailOpenCommits"/>.
    /// </summary>
    private enum SpanWriteOrigin : byte
    {
        /// <summary>A foreground set, delete, or batched set.</summary>
        ClientWrite,

        /// <summary>A <c>MergeManyAsync</c> batch that is not a migration import.</summary>
        Merge,

        /// <summary>A <c>MergeManyAsync</c> batch flagged as a cross-shard migration import.</summary>
        CrossShardMigration,
    }

    /// <summary>
    /// Minimum interval, in ticks, between span fail-open warning logs across
    /// the whole silo. Every fail-open is still counted on
    /// <see cref="LatticeMetrics.LeafSpanFailOpenCommits"/>; only the log line
    /// is rate-limited, so a torn chain pointer under a write storm cannot
    /// flood the log.
    /// </summary>
    private static readonly long SpanFailOpenLogIntervalTicks = TimeSpan.FromSeconds(10).Ticks;

    /// <summary>Last UTC tick a span fail-open warning was logged (silo-wide).</summary>
    private static long _lastSpanFailOpenLogTicks;

    /// <summary>
    /// Counts one key this leaf is about to admit locally although its
    /// declared span excludes it, and emits a rate-limited warning naming the
    /// leaf and the tree (issue #2125). Called by every caller of
    /// <see cref="TryResolveSpanForwardTarget"/> when it reports a fail-open,
    /// and never on the in-span path, so a leaf with no declared span cannot
    /// reach it. Observability only: never throws into the write.
    /// </summary>
    private void RecordSpanFailOpenCommit(SpanFailOpenReason reason, SpanWriteOrigin origin)
    {
        var reasonTag = reason == SpanFailOpenReason.SelfReference
            ? LatticeMetrics.SpanFailOpenReasonSelfReference
            : LatticeMetrics.SpanFailOpenReasonNoSibling;
        var originTag = origin switch
        {
            SpanWriteOrigin.Merge => LatticeMetrics.SpanFailOpenOriginMerge,
            SpanWriteOrigin.CrossShardMigration => LatticeMetrics.SpanFailOpenOriginCrossShardMigration,
            _ => LatticeMetrics.SpanFailOpenOriginClientWrite,
        };

        LatticeMetrics.LeafSpanFailOpenCommits.Add(1, LeafTreeTag(), reasonTag, originTag, LeafTenantTag());

        if (!ShouldLogSpanFailOpen())
        {
            return;
        }

        try
        {
            var logger = ResolveLogger();
            if (logger is not null && logger.IsEnabled(LogLevel.Warning))
            {
                logger.LogWarning(
                    "Leaf {GrainId} of tree {TreeId} committed an out-of-span key locally because no neighbouring leaf resolved (reason {Reason}, origin {Origin}). See orleans.lattice.leaf.span_fail_open_commits.",
                    context.GrainId,
                    state.State.TreeId,
                    reasonTag.Value,
                    originTag.Value);
            }
        }
        catch
        {
            // Observability must never fail the write it is describing.
        }
    }

    /// <summary>
    /// Per-silo token check for the span fail-open warning: returns
    /// <see langword="true"/> at most once per
    /// <see cref="SpanFailOpenLogIntervalTicks"/>, via the same interlocked
    /// compare-and-swap gate as the cursor-publish-failure warning.
    /// </summary>
    private static bool ShouldLogSpanFailOpen()
    {
        var now = DateTime.UtcNow.Ticks;
        var last = Volatile.Read(ref _lastSpanFailOpenLogTicks);
        if (now - last < SpanFailOpenLogIntervalTicks)
        {
            return false;
        }

        return Interlocked.CompareExchange(ref _lastSpanFailOpenLogTicks, now, last) == last;
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
    private async Task<(Dictionary<string, LwwValue<byte[]>> Local, SplitResult? ForwardedSplit)> ForwardOutOfSpanMergeAsync(
        Dictionary<string, LwwValue<byte[]>> entries, bool isCrossShardMigration)
    {
        var local = new Dictionary<string, LwwValue<byte[]>>(entries.Count);
        Dictionary<GrainId, Dictionary<string, LwwValue<byte[]>>>? buckets = null;
        var origin = isCrossShardMigration ? SpanWriteOrigin.CrossShardMigration : SpanWriteOrigin.Merge;
        SplitResult? forwardedSplit = null;

        foreach (var (key, lww) in entries)
        {
            if (!TryResolveSpanForwardTarget(key, out var target, out var failOpen))
            {
                if (failOpen != SpanFailOpenReason.None)
                {
                    RecordSpanFailOpenCommit(failOpen, origin);
                }

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

                // The forwarded SplitResult is kept and returned (issue
                // #3523). An earlier revision discarded it on the reasoning
                // that it describes a split of the *sibling* and the shard
                // root installs a separator against the leaf it called. The
                // premise was wrong: a parent inserts a separator by its
                // sorted position, not against a named child, and the shard
                // root is the only party that can link the sibling's new
                // leaf at all. Discarding it orphaned that leaf and every key
                // on it. The shard root links such a split by re-descending
                // on its promoted key; see SplitResult.Additional.
                forwardedSplit = SplitResult.Combine(
                    forwardedSplit,
                    SplitResult.Forward(await sibling.MergeManyAsync(bucket, isCrossShardMigration)));
            }
        }

        return (local, forwardedSplit);
    }

    /// <summary>
    /// Applies a committed value to this leaf's projection, unless a split that
    /// interleaved with the commit has since moved <paramref name="key"/> out of
    /// this leaf's declared span. In that case the value is set aside in
    /// <paramref name="stranded"/> for <see cref="RelocateStrandedAsync"/>.
    /// <para>
    /// A commit checks the span before it awaits the WAL append, and applies
    /// after the append returns. The foreground write methods are
    /// <c>[AlwaysInterleave]</c>, so in between another commit can divide this
    /// leaf. The split moves every row at or above its split key to the new
    /// sibling and then narrows this leaf's high bound. A row stored after that
    /// is outside the range every reader is routed by: the write was
    /// acknowledged, but no read ever finds it (issue #3523). Re-checking here
    /// costs nothing on a leaf with no declared span. The check and the store
    /// run in one synchronous step, so no split can narrow the range between
    /// them.
    /// </para>
    /// <para>
    /// A key with no resolvable forward target is stored here, as it would be
    /// at admission. Such a key was already out of span when it was admitted
    /// and was counted as a fail-open then. Stranding it would count it a
    /// second time, and there is still nowhere better to put it.
    /// </para>
    /// </summary>
    private void StoreAdmittedEntry(
        string key,
        in LwwValue<byte[]> value,
        ref Dictionary<string, LwwValue<byte[]>>? stranded)
    {
        if (HasDeclaredSpan && TryResolveSpanForwardTarget(key, out _, out _))
        {
            (stranded ??= new Dictionary<string, LwwValue<byte[]>>())[key] = value;
            return;
        }

        StoreEntry(key, value);
    }

    /// <summary>
    /// Hands the values <see cref="StoreAdmittedEntry"/> set aside to the leaves
    /// that now declare their keys. The values keep the stamps they were
    /// committed under, so the receiving leaf's last-writer-wins merge orders
    /// them correctly against any newer write to the same key. Keys with no
    /// resolvable forward target are stored here, which is the documented
    /// fail-open rule. Returns every split the receiving leaves report, for the
    /// shard root to link.
    /// <para>
    /// An interrupted split is completed first, for the reason the write
    /// entry points give: forwarding while it is interrupted sends a key at or
    /// above the pre-split bound to <c>OldNextSibling</c>, and once the new
    /// sibling is initialised a reclaim can fold that successor into it and
    /// retire it (issue #3583). When another turn is still running the split,
    /// this waits for it on <c>_splitGate</c> and then routes by the narrowed
    /// span. No caller holds <c>_splitGate</c> here: relocation runs in the
    /// commit path, which takes no gate, and a split never waits on a commit.
    /// </para>
    /// <para>
    /// <paramref name="completeInterruptedSplit"/> is false only for a caller
    /// that cannot report the returned split (the untracked delete). A split
    /// completed there and dropped would leave the new sibling unreachable by
    /// descent, so such a caller leaves the split for a tracked write.
    /// </para>
    /// </summary>
    private async Task<SplitResult?> RelocateStrandedAsync(
        Dictionary<string, LwwValue<byte[]>> stranded, bool isCrossShardMigration, bool completeInterruptedSplit = true)
    {
        SplitResult? recovered = null;
        if (completeInterruptedSplit && HasInterruptedSplit)
        {
            recovered = await CompleteRecoverySplitUnderGateAsync();
        }

        var (local, forwardedSplit) = await ForwardOutOfSpanMergeAsync(stranded, isCrossShardMigration);
        foreach (var (key, lww) in local)
        {
            StoreEntry(key, lww);
        }

        return SplitResult.Combine(recovered, forwardedSplit);
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
            if (!TryResolveSpanForwardTarget(entry.Key, out var target, out var failOpen))
            {
                if (failOpen != SpanFailOpenReason.None)
                {
                    RecordSpanFailOpenCommit(failOpen, SpanWriteOrigin.ClientWrite);
                }

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
        SplitResult? forwardedSplit = null;
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
            // The forwarded SplitResult is kept, for the reason given in
            // ForwardOutOfSpanMergeAsync (issue #3523).
            var forwarded = await sibling.SetManyWherePredicateAsync(bucket, predicate);
            forwardedSplit = SplitResult.Combine(forwardedSplit, SplitResult.Forward(forwarded.Split));
            var written = forwarded.WrittenKeys;
            for (var i = 0; i < written.Count; i++)
            {
                (forwardWritten ??= new HashSet<string>(StringComparer.Ordinal)).Add(written[i]);
            }
        }

        var localResult = await SetManyWherePredicateLocalAsync(local, predicate, mayContainOutOfSpanKey: true);
        if (forwardWritten is null)
        {
            return forwardedSplit is null
                ? localResult
                : localResult with { Split = SplitResult.Combine(localResult.Split, forwardedSplit) };
        }

        return localResult with
        {
            Split = SplitResult.Combine(localResult.Split, forwardedSplit),
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
            if (!TryResolveSpanForwardTarget(entry.Key, out var target, out var failOpen))
            {
                if (failOpen != SpanFailOpenReason.None)
                {
                    RecordSpanFailOpenCommit(failOpen, SpanWriteOrigin.ClientWrite);
                }

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
            var forwards = new Task<SplitResult?>[buckets.Count];
            var i = 0;
            foreach (var (target, bucket) in buckets)
            {
                // Shadow markers are not transferred, for the reason the
                // conditional forward gives: a set forwards a caller's
                // proposed value for a row this leaf does not hold, which is
                // the shape SetCoreAsync's span forward takes. The sibling's
                // SplitResult is kept, for the reason given in
                // ForwardOutOfSpanMergeAsync (issue #3523).
                forwards[i] = grainFactory.GetGrain<IBPlusLeafGrain>(target).SetManyAsync(bucket);
                parts[i] = forwards[i];
                i++;
            }

            parts[i] = localCommit;
            await Task.WhenAll(parts);

            // Every split is returned: the batch's own commit, the recovered
            // one, and each forwarded sibling's. Returning only one - as an
            // earlier revision did, mirroring the per-key loop's "last
            // non-null wins" - left the others' new leaves unlinked and
            // their keys unreachable (issue #3523).
            var splits = SplitResult.Combine(await localCommit, recovered);
            foreach (var forward in forwards)
            {
                splits = SplitResult.Combine(splits, SplitResult.Forward(await forward));
            }

            return splits;
        }

        return SplitResult.Combine(await localCommit, recovered);
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
