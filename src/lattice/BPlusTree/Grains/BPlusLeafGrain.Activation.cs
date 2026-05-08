using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Activation-hook partial for <see cref="BPlusLeafGrain"/>. Runs the
/// activation-time WAL materialiser that rebuilds the in-memory
/// projection (the visible <c>state.State.Entries</c> map and the
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
/// via the lazy-on-flush path) — that contract did not change.
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
    /// V1 WAL partition the activation-time replay reads from. Pinned
    /// to <c>0</c> until multi-partition fan-out lands; see the type
    /// docstring above for the rationale.
    /// </summary>
    private const int ReplayWalPartition = 0;

    /// <summary>
    /// Activation hook. Runs the WAL materialiser to bring the
    /// in-memory projection (<c>state.State.Entries</c> plus the
    /// per-leaf saga pending-tx map) up to the WAL head, then
    /// publishes the leaf's projection cursor so the per-shard WAL
    /// GC observes the leaf eagerly. No-op when the leaf has not been
    /// seeded with a tree id.
    /// </summary>
    async Task IGrainBase.OnActivateAsync(CancellationToken cancellationToken)
    {
        // Step 1 — drive the dormant ILeafProjection.Apply seam over
        // the WAL slice between the persisted checkpoint and the
        // current head. Failures propagate: a leaf that comes online
        // with a stale projection silently violates the saga
        // reader-isolation contract, and the host's grain activation
        // pipeline will retry the activation rather than serve reads
        // from a half-applied state.
        var advanced = await ReplayWalSinceCheckpointAsync(cancellationToken);

        // Step 2 — eagerly publish the cursor IFF the materialiser did
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
            // because correctness — not progress — is at stake.
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
    /// authorship — a leaf born from a split must apply WAL entries
    /// that fall in its current range even when those entries were
    /// authored by the donor pre-split (the rebuild-from-WAL
    /// scenario). DeleteRange / TxCommit / TxAbort are applied
    /// unconditionally; unknown <see cref="MutationKind"/> values are
    /// dropped (defensive forward-compat).
    /// </remarks>
    private async Task<bool> ReplayWalSinceCheckpointAsync(CancellationToken cancellationToken)
    {
        var treeId = state.State.TreeId;
        if (string.IsNullOrEmpty(treeId))
            return false;

        var checkpoint = state.State.ProjectionCheckpointOffset;

        // Fall-off-log gate: classify the gap before reading any
        // slice. The detector consults the commit log's head and
        // tail offsets, the configured replay budget, and the
        // projection retention. Anything other than TailReplay
        // means the WAL alone cannot recover the projection (either
        // entries below the checkpoint have been trimmed, or the
        // gap exceeds the operator-configured budget). V1 surfaces
        // every non-tail decision as LeafProjectionStaleException
        // because snapshot-driven recovery and full-rebuild are not
        // wired in this commit; the host's grain-activation
        // pipeline will retry, escalate, or surface the failure to
        // the operator according to LatticeOptions.ProjectionRebuildPolicy.
        var detector = context.ActivationServices?.GetService<ILatticeFallOffLogDetector>();
        if (detector is not null)
        {
            var resolvedOptions = await GetOptionsAsync();
            var decision = await detector.ClassifyAsync(
                treeId,
                ReplayWalPartition,
                checkpoint,
                TimeSpan.Zero,
                resolvedOptions,
                cancellationToken);

            switch (decision)
            {
                case FallOffLogDecision.TailReplay:
                    break;
                case FallOffLogDecision.SnapshotThenWal:
                case FallOffLogDecision.FullRebuildFromWal:
                case FallOffLogDecision.Fail:
                default:
                    throw new LeafProjectionStaleException(
                        $"Leaf projection for tree '{treeId}' shard {ReplayWalPartition} cannot be recovered " +
                        $"from the WAL alone (decision={decision}, persistedCheckpoint={checkpoint}). " +
                        "Snapshot-then-WAL and full-rebuild recovery paths are not yet integrated; " +
                        "operator-driven rebuild is required.");
            }
        }

        var coordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
            $"{treeId}/{ReplayWalPartition}");

        var head = await coordinator.GetHeadOffsetAsync(cancellationToken);
        if (head <= checkpoint)
            return false;

        var fromExclusive = checkpoint;
        long maxApplied = checkpoint;
        var projection = (ILeafProjection)this;

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
                // Per-entry cancellation check: the slice budget is
                // 256 entries, but a long-tailed replay may stitch
                // many slices together. Honouring cancellation
                // between Apply calls keeps activation responsive
                // when the host is shutting down or rebalancing.
                cancellationToken.ThrowIfCancellationRequested();

                if (ShouldApplyDuringReplay(
                    entry.Mutation,
                    state.State.ShardIndex,
                    state.State.LowKeyInclusive,
                    state.State.HighKeyExclusive))
                {
                    using (LatticeApplyOffsetContext.BeginScope(entry.Offset))
                    {
                        // Drives the existing ILeafProjection.Apply
                        // dispatch in BPlusLeafGrain.Projection.cs:
                        //
                        //   * Committed Set/Delete -> ApplySet/ApplyDelete
                        //     -> MergeIntoProjection (visible Entries map).
                        //   * Prepared Set/Delete  -> ApplyPreparedSet/Delete
                        //     -> AddPreparedMutation (grain-local
                        //     _pendingTx dictionary in
                        //     BPlusLeafGrain.PendingTx.cs). The ambient
                        //     LatticeApplyOffsetContext scope above
                        //     stamps the WAL offset onto each pending
                        //     record so MinUnresolvedPrepareOffset can
                        //     clamp the checkpoint advance below.
                        //   * TxCommit / TxAbort  -> terminal handler
                        //     flips the pending bucket into Entries
                        //     (commit) or drops it (abort).
                        //   * DeleteRange -> ApplyDeleteRange iterates
                        //     this leaf's own Entries only.
                        //
                        // The materialiser does not duplicate any of
                        // this logic - rebuilding the pending-tx map
                        // is a side effect of replaying through the
                        // same Apply seam that runtime mutations use.
                        projection.Apply(entry.Mutation);
                    }
                }

                if (entry.Offset > maxApplied)
                    maxApplied = entry.Offset;
            }

            // Advance the slice cursor by the highest offset returned;
            // the coordinator may legitimately end the slice short of
            // the requested upper bound when the budget is exhausted
            // or when the WAL has trimmed an interior gap.
            var lastOffset = slice[^1].Offset;
            if (lastOffset <= fromExclusive)
                break; // defensive: never spin if the slice failed to advance.
            fromExclusive = lastOffset;
        }

        if (maxApplied > checkpoint)
        {
            // SetCheckpointOffsetAsync clamps the requested advance
            // behind MinUnresolvedPrepareOffset - 1 (the existing
            // unresolved-prepare clamp in BPlusLeafGrain.Projection.cs)
            // so the persisted checkpoint never overruns an unresolved
            // prepare. The internal FlushPendingCheckpointAsync persist
            // seam fires here - this is the legitimate
            // materialiser-driven checkpoint persist for the projection
            // offset, distinct from the saga pending-tx state, which
            // remains in-memory and is rebuilt deterministically on
            // every reactivation. FlushPendingCheckpointAsync also
            // publishes the cursor as part of its persist sequence,
            // which is why this method returns true here so the
            // activation hook can skip the explicit cursor publish
            // and avoid a redundant RPC.
            await projection.SetCheckpointOffsetAsync(maxApplied, cancellationToken);
            return true;
        }

        return false;
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
    ///     either side — a <see langword="null"/> bound means "no
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
    ///     on the range axis — both axes preserve the legacy V1
    ///     single-leaf-per-shard semantics so a legacy-shaped state
    ///     must not start dropping its own writes after a binary
    ///     upgrade.
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
    ///     Unknown <see cref="MutationKind"/> values are dropped —
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
        MutationKind.Set or MutationKind.Delete =>
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
