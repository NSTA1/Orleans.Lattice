using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Activation-hook partial for <see cref="BPlusLeafGrain"/>. Runs the
/// activation-time WAL materialiser that rebuilds the in-memory
/// projection (the per-activation runtime entry cache and the
/// per-leaf saga pending-tx machinery) from the durable per-shard
/// write-ahead log, then publishes the leaf's projection cursor so the
/// per-shard WAL GC sees the leaf the moment activation completes.
/// <para>
/// The materialiser is the activation-time WAL recovery seam, gated
/// by the persisted <see cref="State.LeafNodeState.ProjectionCheckpointOffset"/>:
/// every WAL entry strictly after the checkpoint is replayed back
/// through <see cref="ILeafProjection.Apply(in LatticeMutation)"/>, the
/// pending-tx map is reconstructed deterministically from prepared
/// mutations whose terminals have not yet replayed, and the persisted
/// checkpoint is advanced under
/// <see cref="ILeafProjection.SetCheckpointOffsetAsync(long, CancellationToken)"/>'s
/// <c>MinUnresolvedPrepareOffset - 1</c> clamp so the next activation
/// never silently advances past a prepare whose terminal is still
/// outstanding.
/// </para>
/// <para>
/// Replay short-circuits to a no-op on two preconditions: the tree id
/// must have been seeded (system-tree leaves and pre-init activations
/// are skipped); and the WAL head must strictly exceed the persisted
/// checkpoint (otherwise there is nothing to replay). The
/// commit-log adapter (<see cref="ICommitLogReader"/>) is registered
/// unconditionally by <c>AddLattice</c> via the in-core
/// <c>WalCommitLogReader</c> default, so the activation hook can
/// always rely on it being resolvable from DI.
/// </para>
/// <para>
/// Before reading any WAL slice the materialiser consults
/// <see cref="ILatticeFallOffLogDetector"/> to classify the gap
/// between the persisted checkpoint and the WAL head/tail. If the
/// detector returns anything other than
/// <see cref="FallOffLogDecision.TailReplay"/> (WAL trimmed past the
/// checkpoint, replay budget exceeded, or projection retention
/// elapsed), the materialiser surfaces
/// <see cref="LeafProjectionStaleException"/> immediately. V1 does
/// not integrate the snapshot-then-WAL or full-rebuild recovery
/// paths; those are tracked as a follow-up so this commit can land
/// the dominant correctness path (tail replay) without taking on
/// snapshot-storage integration in the same change.
/// </para>
/// <para>
/// Replay failures propagate. A leaf that comes online with a stale
/// projection silently violates the saga reader-isolation contract
/// (a continuous reader could observe a half-applied saga across a
/// reactivation), so the activation hook surfaces the exception
/// rather than swallowing it. Cursor-publish errors remain swallowed
/// (the cursor is monotonic and the next foreground flush retries
/// via the lazy-on-flush path) - that contract did not change.
/// </para>
/// <para>
/// V1 single-partition assumption: the materialiser reads WAL
/// partition <c>0</c> only. The existing core test cluster and the
/// single-cluster production deployment configure
/// <c>LatticeReplicationOptions.ReplogPartitions = 1</c>, so every
/// per-key write and every saga terminal-mark for every chain shard
/// lands in partition 0 and the single-partition read recovers the
/// full state. Multi-partition fan-out (i.e. iterating
/// <c>[0, ReplogPartitions)</c> on activation, or hoisting the
/// materialiser into a per-shard driver that dispatches by leaf
/// ownership) is deliberately out of scope for this commit and
/// tracked as a follow-up so the saga reader-isolation promotion
/// can land without taking on the full WAL-routing reconciliation
/// in the same change.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Maximum number of WAL entries the activation-time replay reads
    /// per <see cref="ILeafReplayCoordinatorGrain.ReadSliceAsync"/>
    /// invocation. Bounds the worst-case replay memory footprint for a
    /// long-tailed WAL and lets the activation hook interleave RPC
    /// progress across multiple slice fetches.
    /// </summary>
    private const int ReplaySliceBudget = 256;

    /// <summary>
    /// V1 WAL partition the activation-time replay reads from. Retained
    /// as a name for the legacy single-partition shape (default
    /// <see cref="LatticeOptions.WalPartitions"/> = 1); under multi-
    /// partition replay the activation hook iterates
    /// <c>[0, WalPartitions)</c> and threads each partition through
    /// <see cref="LatticeApplyOffsetContext.BeginScope(int, long)"/>
    /// so the per-partition projection-checkpoint clamp can scope to
    /// the correct partition's offset space.
    /// </summary>
    private const int ReplayWalPartition = 0;

    /// <summary>
    /// Activation hook. Runs the WAL materialiser to bring the
    /// in-memory projection (the per-activation runtime entry cache
    /// plus the per-leaf saga pending-tx map) up to the WAL head, then
    /// publishes the leaf's projection cursor so the per-shard WAL
    /// GC observes the leaf eagerly. No-op when the leaf has not been
    /// seeded with a tree id.
    /// </summary>
    async Task IGrainBase.OnActivateAsync(CancellationToken cancellationToken)
    {
        // Step 0 - try to rehydrate the in-memory entry cache from a
        // persisted leaf snapshot. The snapshot is the safety net for
        // WAL retention fall-off: if a previous maintenance tick wrote
        // a snapshot whose offset exceeds this leaf's persisted
        // ProjectionCheckpointOffset, we hydrate the cache from the
        // snapshot and let the tail replay below cover only the
        // (snapshot, head] suffix. When no snapshot is present (or it
        // is older than the persisted checkpoint), this step is a
        // no-op and the existing WAL-tail-replay path runs unchanged.
        var rehydratedFromSnapshot = await TryRehydrateFromSnapshotAsync(cancellationToken);

        // Step 0.5 - cache/checkpoint coherence reset. The entry
        // cache is per-activation only; it is rebuilt
        // from the WAL on every activation and never persisted. The
        // persisted ProjectionCheckpointOffset, in contrast, survives
        // across activations. When the leaf reactivates after a silo
        // restart (or any cold start) without a snapshot, the cache
        // starts empty but the persisted checkpoint still claims that
        // every offset through N has been applied. Replaying only
        // (N, head] would silently drop offsets 0..N from the rebuilt
        // cache. Compute a local replay-start sentinel (-1) so the WAL
        // replay below covers the entire readable window. We pass this
        // as an override rather than mutating the persisted slot, so
        // every external observer of the checkpoint (digest, snapshot
        // capture guard, materialiser-lag math, fall-off-log detector)
        // continues to see the pre-activation value until the replay's
        // own SetCheckpointOffsetAsync advances it through the normal
        // flush path. The reset is gated on (a) the snapshot rehydrate
        // not having populated the cache (it already advanced the
        // checkpoint to the snapshot offset and that anchor is
        // honoured), and (b) the cache being empty - if some upstream
        // seam (e.g. a future hot-restart path, a sibling-at-birth
        // attach, or a test seeding the cache for unit-test purposes)
        // has already populated the cache, the checkpoint is by
        // definition coherent with it and must not be overridden.
        long? replayCheckpointOverride =
            (!rehydratedFromSnapshot && Cache.Count == 0) ? -1L : null;

        // Step 1 - drive the dormant ILeafProjection.Apply seam over
        // the WAL slice between the persisted checkpoint and the
        // current head. Failures propagate: a leaf that comes online
        // with a stale projection silently violates the saga
        // reader-isolation contract, and the host's grain activation
        // pipeline will retry the activation rather than serve reads
        // from a half-applied state.
        var advanced = await ReplayWalSinceCheckpointAsync(replayCheckpointOverride, cancellationToken);

        // Step 1.5 - if the fall-off-log detector raised the
        // SnapshotPending advisory while classifying the replay path,
        // proactively capture the leaf's projection into the dedicated
        // snapshot grain now. Doing this once per (advisory-firing)
        // activation amortises capture cost over the whole activation
        // cycle and ensures that an active leaf whose checkpoint sits
        // close to the WAL tail is durably snapshotted before any
        // subsequent WAL trim can fall through the gap. Capture errors
        // are best-effort and must not block the leaf coming online -
        // the next periodic recheck (FlushPendingCheckpointAsync) or
        // the next reactivation will retry.
        if (_activationSnapshotPending)
        {
            _activationSnapshotPending = false;
            await TryCaptureSnapshotForAdvisoryAsync();
        }

        // Step 2 - eagerly publish the cursor IFF the materialiser did
        // not already advance the checkpoint. SetCheckpointOffsetAsync
        // routes through FlushPendingCheckpointAsync which already
        // publishes the cursor on every persist; an explicit publish
        // here would be a redundant (idempotent but wasteful) RPC. On
        // the no-replay path (no new entries since checkpoint) we
        // still want to publish so the GC sees the leaf eagerly.
        if (advanced)
            return;

        try
        {
            // Skip leaves whose projection has never advanced
            // (registering at HLC zero would pin the WAL trim point at
            // offset zero forever on a leaf that has never seen a
            // write), and reuse the same gating as the lazy-on-flush
            // path so the consumer-id format and reporter resolution
            // stay in exactly one place.
            var clock = state.State.Clock;
            if (clock <= HybridLogicalClock.Zero)
                return;

            await ReportCursorIfActiveAsync();
        }
        catch (Exception ex)
        {
            // Cursor-publish failures are non-fatal: the cursor is
            // monotonic so the next successful foreground flush
            // catches up via the lazy-on-flush path. Materialiser
            // failures, in contrast, are fatal (they propagate above)
            // because correctness - not progress - is at stake.
            var logger = context.ActivationServices?
                .GetService<ILoggerFactory>()?
                .CreateLogger<BPlusLeafGrain>();
            logger?.LogWarning(
                ex,
                "Eager cursor registration failed during activation for leaf {GrainId}; will retry on next checkpoint flush.",
                context.GrainId);
        }
    }

    /// <summary>
    /// Drives the dormant <see cref="ILeafProjection.Apply(in LatticeMutation)"/>
    /// seam over every WAL entry strictly after
    /// <see cref="State.LeafNodeState.ProjectionCheckpointOffset"/>
    /// and at-or-before the WAL head, then advances the persisted
    /// checkpoint via
    /// <see cref="ILeafProjection.SetCheckpointOffsetAsync(long, CancellationToken)"/>.
    /// The checkpoint advance is clamped behind any unresolved
    /// prepared-saga mutation rebuilt during this replay, so a
    /// subsequent activation re-emits the prepare exactly once when
    /// its terminal mark eventually surfaces.
    /// </summary>
    /// <returns>
    /// <c>true</c> if the materialiser advanced the persisted
    /// checkpoint (and therefore SetCheckpointOffsetAsync already
    /// published the leaf's cursor via FlushPendingCheckpointAsync);
    /// <c>false</c> if the replay was a no-op or every replayed
    /// offset was clamped behind an unresolved prepare. The caller
    /// uses this signal to decide whether the explicit
    /// activation-time cursor publish would be redundant.
    /// </returns>
    /// <remarks>
    /// Per-entry filter: <see cref="ShouldApplyDuringReplay(in LatticeMutation, int?, string?, string?)"/>
    /// drops entries whose <see cref="LatticeMutation.ShardIndex"/>
    /// does not match this leaf's persisted shard, and entries whose
    /// key falls outside this leaf's persisted
    /// [<see cref="State.LeafNodeState.LowKeyInclusive"/>,
    /// <see cref="State.LeafNodeState.HighKeyExclusive"/>) range. The
    /// filter is keyed on persisted ownership identity, not on
    /// authorship - a leaf born from a split must apply WAL entries
    /// that fall in its current range even when those entries were
    /// authored by the donor pre-split (the rebuild-from-WAL
    /// scenario). DeleteRange / TxCommit / TxAbort are applied
    /// unconditionally; unknown <see cref="MutationKind"/> values are
    /// dropped (defensive forward-compat).
    /// </remarks>
    private async Task<bool> ReplayWalSinceCheckpointAsync(long? checkpointOverride, CancellationToken cancellationToken)
    {
        var treeId = state.State.TreeId;
        if (string.IsNullOrEmpty(treeId))
            return false;

        var resolvedOptions = await GetOptionsAsync();
        var partitionCount = Math.Max(1, resolvedOptions.WalPartitions);
        var detector = context.ActivationServices?.GetService<ILatticeFallOffLogDetector>();
        var projection = (ILeafProjection)this;

        var anyAdvanced = false;
        // Pass 1: per-partition tail replay, deferring every saga
        // terminal (TxCommit / TxAbort) into a shared list so the
        // _pendingTx bucket is fully populated across every partition
        // before any terminal drains it. See the DeferredTerminal
        // docstring for the saga atomicity rationale.
        // Per-partition max-applied is tracked so the final reconciled
        // SetCheckpointOffsetAsync (after pass 2's terminals lift the
        // pending-tx clamps) advances each partition's checkpoint to
        // the actual highest offset observed during replay, not the
        // pass-1 clamped value.
        var deferredTerminals = new List<DeferredTerminal>();
        var perPartitionMaxApplied = new long[partitionCount];
        for (var p = 0; p < partitionCount; p++) perPartitionMaxApplied[p] = -1L;

        for (var partition = 0; partition < partitionCount; partition++)
        {
            // Per-partition checkpoint: a leaf whose persisted state
            // pre-dates the per-partition slot falls back to the
            // scalar ProjectionCheckpointOffset for partition 0 only;
            // every other partition starts at the -1 "nothing applied"
            // sentinel. The cold-start cache-empty override (see step
            // 0.5 in OnActivateAsync) drives every partition to -1
            // because the cache rebuild covers the full readable
            // window of every partition.
            long persistedCheckpoint = GetPersistedCheckpointForPartition(partition);
            var checkpoint = checkpointOverride ?? persistedCheckpoint;

#if LATTICE_DIAG
            DiagSink.Write($"[DIAG replay-enter] gid={context.GrainId} treeId={treeId} partition={partition} shardIndex={state.State.ShardIndex} " +
                $"low='{state.State.LowKeyInclusive ?? "<null>"}' high='{state.State.HighKeyExclusive ?? "<null>"}' " +
                $"checkpoint={checkpoint} entryCount={Cache.Count}");
#endif

            if (detector is not null)
            {
                var decision = await detector.ClassifyAsync(
                    treeId,
                    partition,
                    checkpoint,
                    TimeSpan.Zero,
                    resolvedOptions,
                    cancellationToken);

                switch (decision)
                {
                    case FallOffLogDecision.TailReplay:
                        break;
                    case FallOffLogDecision.SnapshotPending:
                        _activationSnapshotPending = true;
                        break;
                    case FallOffLogDecision.SnapshotThenWal:
                    case FallOffLogDecision.FullRebuildFromWal:
                    case FallOffLogDecision.Fail:
                    default:
                        throw new LeafProjectionStaleException(
                            $"Leaf projection for tree '{treeId}' partition {partition} cannot be recovered " +
                            $"from the WAL alone (decision={decision}, persistedCheckpoint={checkpoint}). " +
                            "Snapshot-then-WAL and full-rebuild recovery paths are not yet integrated; " +
                            "operator-driven rebuild is required.");
                }
            }

            var (advanced, maxApplied) = await ReplayPartitionAsync(treeId, partition, checkpoint, projection, deferredTerminals, cancellationToken);
            if (advanced)
                anyAdvanced = true;
            if (maxApplied > perPartitionMaxApplied[partition])
                perPartitionMaxApplied[partition] = maxApplied;
        }

        // Pass 2: drain every deferred saga terminal (and DeleteRange
        // tombstone) in arrival order across partitions. By this
        // point pass 1 has fully populated every saga's pending bucket
        // in _pendingTx and every Set/Delete is in the Cache, so each
        // terminal's ApplyTxCommit / ApplyTxAbort observes the
        // complete prepared-mutation set and each range-tombstone's
        // ApplyDeleteRange iterates the full pre-tombstone Cache.
        // Ordering across partitions does not matter because each
        // terminal's id keys directly into the pending-bucket map;
        // within a single partition's deferred list the arrival order
        // is preserved by the append order.
        foreach (var terminal in deferredTerminals)
        {
            using (LatticeApplyOffsetContext.BeginScope(terminal.Partition, terminal.Offset))
            {
                projection.Apply(terminal.Mutation);
            }
        }

        // Final reconciliation: every pending-tx clamp has lifted now
        // that the terminals have drained, so the per-partition
        // checkpoint can advance to the actual maxApplied observed
        // during pass 1. Without this step, a partition whose
        // pass-1 SetCheckpointOffsetAsync was clamped behind an
        // unresolved prepare (whose terminal would later land in
        // pass 2) would stay at the clamped offset forever, even
        // after the terminal drained its pending bucket - the next
        // activation would needlessly re-replay the prefix that was
        // already absorbed into the Cache.
        for (var partition = 0; partition < partitionCount; partition++)
        {
            var maxApplied = perPartitionMaxApplied[partition];
            if (maxApplied <= GetPersistedCheckpointForPartition(partition))
                continue;
            using (LatticeApplyOffsetContext.BeginScope(partition, maxApplied))
            {
                var current = GetCurrentCheckpointForPartition(partition);
                if (maxApplied > current)
                {
                    await projection.SetCheckpointOffsetAsync(maxApplied, cancellationToken);
                    anyAdvanced = true;
                }
            }
        }

        return anyAdvanced;
    }

    /// <summary>
    /// <summary>
    /// Mutation deferred during pass 1 of the activation-time replay
    /// to be applied in pass 2 once every partition's per-key Set /
    /// Delete entries have been absorbed into the leaf's Cache and
    /// every prepare into <c>_pendingTx</c>. Covers the two mutation
    /// shapes whose apply semantics depend on the global per-shard
    /// state being fully reconstructed:
    /// <list type="bullet">
    ///   <item>
    ///     <see cref="MutationKind.TxCommit"/> /
    ///     <see cref="MutationKind.TxAbort"/> - the saga's per-key
    ///     prepares fan out across multiple WAL partitions while the
    ///     terminal lands in a single (shard-routed) partition. Per-
    ///     partition independent replay would observe the terminal
    ///     before some of its prepares had been absorbed into
    ///     <c>_pendingTx</c>, so the terminal's <c>ApplyTxCommit</c>
    ///     would drain an incomplete bucket and the late-arriving
    ///     prepares would be added to <c>_pendingTx</c> after the
    ///     <c>_recentlyTerminal</c> dedup had already accepted the
    ///     txid - leaving the late prepares stranded and silently
    ///     invisible.
    ///   </item>
    ///   <item>
    ///     <see cref="MutationKind.DeleteRange"/> - the tombstone
    ///     mutation iterates the leaf's Cache at apply time to
    ///     tombstone every in-range key. A range tombstone in
    ///     partition <c>P_t</c> whose target Set entries live in
    ///     partition <c>P_s</c> would see an empty Cache during
    ///     pass 1 (when partition <c>P_t</c> happens to replay
    ///     before partition <c>P_s</c>), tombstone nothing, and let
    ///     the Sets in <c>P_s</c> become visible. Deferring to pass 2
    ///     restores the tombstone-after-its-targets ordering invariant.
    ///   </item>
    /// </list>
    /// </summary>
    private readonly record struct DeferredTerminal(
        int Partition,
        long Offset,
        LatticeMutation Mutation);

    /// <summary>
    /// Per-partition replay inner loop extracted from
    /// <see cref="ReplayWalSinceCheckpointAsync"/>. Reads WAL slices
    /// from <paramref name="partition"/>'s coordinator strictly past
    /// <paramref name="checkpoint"/>, threads
    /// (<paramref name="partition"/>, offset) into
    /// <see cref="LatticeApplyOffsetContext.BeginScope(int, long)"/>
    /// for every Apply, then advances the per-partition projection
    /// checkpoint via the projection seam. Returns <c>true</c> when
    /// the per-partition checkpoint actually advanced.
    /// <para>
    /// <b>Pass 1 of two-pass replay.</b> Saga terminals
    /// (<see cref="MutationKind.TxCommit"/> / <see cref="MutationKind.TxAbort"/>)
    /// are appended to <paramref name="deferredTerminals"/> instead of
    /// being applied inline - see the <see cref="DeferredTerminal"/>
    /// docstring for the saga atomicity rationale. Per-partition
    /// checkpoint advance is also deferred to pass 2: a partition that
    /// emitted a terminal would otherwise advance its checkpoint past
    /// the still-pending prepare offsets in the OTHER partitions'
    /// pending-tx clamp range (the per-partition clamp is scoped to
    /// the partition the prepare landed in, so the terminal's
    /// partition can advance unclamped) - but the terminal itself
    /// hasn't been applied yet, so the visible-state contract requires
    /// us to wait.
    /// </para>
    /// </summary>
    private async Task<(bool Advanced, long MaxApplied)> ReplayPartitionAsync(
        string treeId,
        int partition,
        long checkpoint,
        ILeafProjection projection,
        List<DeferredTerminal> deferredTerminals,
        CancellationToken cancellationToken)
    {
        var coordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
            $"{treeId}/{partition}");

        var head = await coordinator.GetHeadOffsetAsync(cancellationToken);
        if (head <= checkpoint)
            return (false, checkpoint);

        var fromExclusive = checkpoint;
        long maxApplied = checkpoint;

        while (fromExclusive < head)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var slice = await coordinator.ReadSliceAsync(
                fromExclusive,
                head,
                ReplaySliceBudget,
                cancellationToken);

            if (slice.Count == 0)
                break;

            foreach (var entry in slice)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (ShouldApplyDuringReplay(
                    entry.Mutation,
                    state.State.ShardIndex,
                    state.State.LowKeyInclusive,
                    state.State.HighKeyExclusive))
                {
                    // Defer saga terminals AND DeleteRange to pass 2:
                    // - Terminals: see the DeferredTerminal docstring
                    //   for the multi-partition saga atomicity rationale.
                    // - DeleteRange: ApplyDeleteRange iterates the leaf's
                    //   Cache at the moment of apply to tombstone every
                    //   in-range key, but under multi-partition pass 1
                    //   the Cache is still being rebuilt across partitions.
                    //   A DeleteRange that lands in partition 2 but whose
                    //   target Set entries land in partition 5 would
                    //   tombstone nothing in pass 1 (Cache empty for that
                    //   key range) and then the Sets in partition 5 would
                    //   replay AFTER the tombstone, leaving the keys
                    //   visible. Deferring DeleteRange to pass 2 (after
                    //   every Set has populated the Cache) restores the
                    //   tombstone-after-its-targets ordering invariant.
                    if (entry.Mutation.Kind is MutationKind.TxCommit or MutationKind.TxAbort
                        or MutationKind.DeleteRange)
                    {
                        deferredTerminals.Add(new DeferredTerminal(partition, entry.Offset, entry.Mutation));
                    }
                    else
                    {
#if LATTICE_DIAG
                        DiagSink.Write($"[DIAG replay-apply] gid={context.GrainId} partition={partition} offset={entry.Offset} kind={entry.Mutation.Kind} key='{entry.Mutation.Key}' shardIndex={entry.Mutation.ShardIndex}");
#endif
                        using (LatticeApplyOffsetContext.BeginScope(partition, entry.Offset))
                        {
                            projection.Apply(entry.Mutation);
                        }
                    }
                }
#if LATTICE_DIAG
                else
                {
                    DiagSink.Write($"[DIAG replay-skip] gid={context.GrainId} partition={partition} offset={entry.Offset} kind={entry.Mutation.Kind} key='{entry.Mutation.Key}' mutShard={entry.Mutation.ShardIndex} leafShard={state.State.ShardIndex} low='{state.State.LowKeyInclusive ?? "<null>"}' high='{state.State.HighKeyExclusive ?? "<null>"}'");
                }
#endif

                if (entry.Offset > maxApplied)
                    maxApplied = entry.Offset;
            }

            var lastOffset = slice[^1].Offset;
            if (lastOffset <= fromExclusive)
                break;
            fromExclusive = lastOffset;
        }

        // The actual checkpoint advance (SetCheckpointOffsetAsync) is
        // deferred to the post-pass-2 reconciliation step in
        // ReplayWalSinceCheckpointAsync. That step waits until every
        // deferred terminal has applied (lifting pending-tx clamps)
        // and only then advances each partition's persisted checkpoint
        // to the corresponding maxApplied. The pass-1 advance was
        // unsafe because: (a) a partition that observed a prepare in
        // pass 1 would clamp its checkpoint behind the prepare's
        // offset, but the matching terminal would only land in pass
        // 2; (b) a partition with no prepares but the same maxApplied
        // would over-eagerly advance past offsets the deferred
        // terminals must still be observed at by future activations.
        // Returning the per-partition maxApplied here gives the
        // caller the data it needs to do the post-pass-2 advance
        // with full knowledge of every partition's outcome.
        return (Advanced: maxApplied > checkpoint, MaxApplied: maxApplied);
    }

    /// <summary>
    /// Per-WAL-entry filter for the activation-time materialiser.
    /// Decides whether a given WAL entry should be replayed against
    /// this leaf's projection, keyed on the leaf's persisted
    /// <see cref="State.LeafNodeState.ShardIndex"/> and on the leaf's
    /// persisted [<see cref="State.LeafNodeState.LowKeyInclusive"/>,
    /// <see cref="State.LeafNodeState.HighKeyExclusive"/>) ownership
    /// range.
    /// <list type="bullet">
    ///   <item>
    ///     <see cref="MutationKind.Set"/> /
    ///     <see cref="MutationKind.Delete"/> are applied iff the
    ///     entry's <see cref="LatticeMutation.ShardIndex"/> matches
    ///     the leaf's owning shard <em>and</em> the entry's
    ///     <see cref="LatticeMutation.Key"/> falls in the leaf's
    ///     persisted ownership range. The range check is open on
    ///     either side - a <see langword="null"/> bound means "no
    ///     constraint on that side", used for the chain's leftmost
    ///     and rightmost leaves and for legacy state shapes that
    ///     pre-date the slot. Keying on key-range (not on authoring
    ///     leaf grain id) is essential for the rebuild-from-WAL
    ///     scenario: a leaf born from a split has no Entries until
    ///     replay populates them, and the entries that belong to it
    ///     were authored by the donor sibling pre-split. Pre-Option A
    ///     leaves whose <see cref="State.LeafNodeState.ShardIndex"/>
    ///     slot is null apply unconditionally on the shard axis;
    ///     leaves with both range bounds null apply unconditionally
    ///     on the range axis - both axes preserve the legacy V1
    ///     single-leaf-per-shard semantics so a legacy-shaped state
    ///     must not start dropping its own writes after a binary
    ///     upgrade.
    ///   </item>
    ///   <item>
    ///     <see cref="MutationKind.Tombstone"/> reap envelopes
    ///     authored by <c>CompactTombstonesAsync</c> are gated by the
    ///     same shard-and-range filter as <see cref="MutationKind.Set"/>
    ///     / <see cref="MutationKind.Delete"/>: a sibling leaf's reap
    ///     must not unintentionally remove keys from this leaf's
    ///     projection. Reap envelopes that pass the filter route into
    ///     <c>ApplyTombstoneReap</c> which physically removes the
    ///     stamped key iff the existing entry is still a tombstone or
    ///     an expired live entry.
    ///   </item>
    ///   <item>
    ///     <see cref="MutationKind.DeleteRange"/> is applied
    ///     unconditionally. <see cref="BPlusLeafGrain"/>'s replay
    ///     handler iterates this leaf's own entries only, so the call
    ///     is naturally a no-op on leaves that own no keys in the
    ///     range.
    ///   </item>
    ///   <item>
    ///     <see cref="MutationKind.TxCommit"/> /
    ///     <see cref="MutationKind.TxAbort"/> are applied
    ///     unconditionally. The terminal's shard scope is enforced by
    ///     the writer-side partition routing, and the per-leaf
    ///     <c>_recentlyTerminal</c> dedup makes a terminal whose
    ///     pending bucket is empty a trivial no-op.
    ///   </item>
    ///   <item>
    ///     Unknown <see cref="MutationKind"/> values are dropped -
    ///     defensive forward-compat against future kinds whose replay
    ///     semantics the materialiser has not been taught.
    ///   </item>
    /// </list>
    /// </summary>
    internal static bool ShouldApplyDuringReplay(
        in LatticeMutation mutation,
        int? leafShardIndex,
        string? lowKeyInclusive,
        string? highKeyExclusive) => mutation.Kind switch
    {
        MutationKind.Set or MutationKind.Delete or MutationKind.Tombstone =>
            (leafShardIndex is null || mutation.ShardIndex == leafShardIndex.Value)
            && (lowKeyInclusive is null
                || string.CompareOrdinal(mutation.Key, lowKeyInclusive) >= 0)
            && (highKeyExclusive is null
                || string.CompareOrdinal(mutation.Key, highKeyExclusive) < 0),
        MutationKind.DeleteRange => true,
        MutationKind.TxCommit => true,
        MutationKind.TxAbort => true,
        _ => false,
    };
}
