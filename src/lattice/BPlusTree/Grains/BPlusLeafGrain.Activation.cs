using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Activation-hook partial for <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/>. Runs the
/// activation-time WAL materialiser that rebuilds the in-memory
/// projection (the per-activation runtime entry cache and the
/// per-leaf saga pending-tx machinery) from the durable per-shard
/// write-ahead log, then publishes the leaf's projection cursor so the
/// per-shard WAL GC sees the leaf the moment activation completes.
/// <para>
/// The materialiser is the activation-time WAL recovery seam, gated
/// by the persisted <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.ProjectionCheckpointOffset"/>:
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
    /// Per-silo (process-wide) ceiling on concurrent activation-time leaf
    /// materialiser replays, lazily sized from
    /// <see cref="LatticeOptions.WalMaterialiserMaxConcurrentReplays"/> on the
    /// first activation that resolves options. A reactivation storm (issue
    /// #1030) would otherwise fan out an unbounded number of WAL replays and
    /// starve the foreground request path; this semaphore bounds the in-flight
    /// replay count so the storm queues instead of stampeding the thread pool.
    /// </summary>
    private static SemaphoreSlim? _replayConcurrencyGate;

    /// <summary>Initialisation guard for <see cref="_replayConcurrencyGate"/>.</summary>
    private static readonly object _replayConcurrencyGateLock = new();

    /// <summary>
    /// Lazily resolves the per-silo replay concurrency gate from
    /// <paramref name="options"/>. A non-positive
    /// <see cref="LatticeOptions.WalMaterialiserMaxConcurrentReplays"/> resolves
    /// to <see cref="Environment.ProcessorCount"/>. The gate is sized once on
    /// first use and is a process-wide structural constant thereafter.
    /// </summary>
    private static SemaphoreSlim ResolveReplayConcurrencyGate(LatticeOptions options)
    {
        var existing = Volatile.Read(ref _replayConcurrencyGate);
        if (existing is not null)
            return existing;

        lock (_replayConcurrencyGateLock)
        {
            if (_replayConcurrencyGate is null)
            {
                var max = options.WalMaterialiserMaxConcurrentReplays;
                if (max <= 0)
                    max = Environment.ProcessorCount;
                _replayConcurrencyGate = new SemaphoreSlim(max, max);
            }

            return _replayConcurrencyGate;
        }
    }

    /// <summary>
    /// Acquires a permit from the per-silo replay concurrency gate, returning
    /// the semaphore so the caller can release it once the replay completes.
    /// Returns <c>null</c> for a leaf with no tree id (a no-op activation that
    /// does no replay and must not consume a permit).
    /// </summary>
    private async Task<SemaphoreSlim?> AcquireReplayPermitAsync(CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(state.State.TreeId))
            return null;

        var options = await GetOptionsAsync();
        var gate = ResolveReplayConcurrencyGate(options);
        await gate.WaitAsync(cancellationToken);
        return gate;
    }

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
        //
        // The replay runs under a per-silo concurrency permit (issue
        // #1030): under a burst that reactivates or splits many leaves
        // at once, an unbounded fan-out of WAL replays saturates every
        // silo thread and starves the foreground request path. The
        // permit caps how many leaf replays run concurrently so a
        // reactivation storm degrades into a bounded queue. A no-op
        // activation (no tree id) takes no permit.
        bool advanced;
        var replayPermit = await AcquireReplayPermitAsync(cancellationToken);
        if (replayPermit is not null)
        {
            // The cold/warm discriminator is precisely the replay-start
            // override computed at step 0.5: a -1 sentinel means neither the
            // snapshot rehydrate nor a pre-populated cache supplied an anchor,
            // so this activation replays the whole readable WAL window (cold);
            // a null override means the activation resumed above an anchor and
            // replays only the tail (warm). Nothing else needs to be computed
            // or plumbed - the value is already in scope, which is why the
            // discriminator belongs on the existing counter rather than on a
            // second one (issue #2148).
            var cold = replayCheckpointOverride == -1L;
            var replayTreeId = state.State.TreeId!;
            LatticeMetrics.LeafActivationReplays.Add(
                1,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, replayTreeId),
                cold ? LatticeMetrics.ActivationTemperatureCold : LatticeMetrics.ActivationTemperatureWarm,
                LatticeTenantLabel.ForTree(replayTreeId));

            // ... and sample the same two totals into the log, because a
            // counter is only readable where something is scraping the
            // process, and the deployed host exposes no metrics endpoint
            // (issue #2148). Observe unconditionally - before any logger or
            // level check - so the totals account for every permitted replay
            // whether or not a line is emitted for it.
            var temperatureSample = ObserveLeafActivationReplay(replayTreeId, cold, Stopwatch.GetTimestamp());
            if (temperatureSample is { } totals)
            {
                // Gate on IsEnabled as the over-budget warning does: the
                // templated call would otherwise allocate a params object[]
                // and box both totals even when the line is filtered out.
                var temperatureLogger = ResolveLogger();
                if (temperatureLogger is not null && temperatureLogger.IsEnabled(LogLevel.Information))
                {
                    temperatureLogger.LogInformation(
                        "Leaf activation replays for tree '{TreeId}' since this silo started: {ColdReplays} cold "
                        + "(no snapshot rehydrate and an empty entry cache, so the whole readable WAL window is "
                        + "replayed) and {WarmReplays} warm (resumed above a snapshot or cache anchor). These are "
                        + "CUMULATIVE process-wide totals, not a count since the previous line: the line is "
                        + "rate-limited to one per tree per {IntervalSeconds}s, so any single line yields the "
                        + "cold:warm ratio and any two yield the rate between them. Activations of a leaf with no "
                        + "tree id bound take no replay permit and are counted on neither arm. Informational: a "
                        + "cold replay is correct, just more expensive than a warm one.",
                        replayTreeId,
                        totals.Cold,
                        totals.Warm,
                        (long)ActivationTemperatureLogInterval.TotalSeconds);
                }
            }
        }
        try
        {
            advanced = await ReplayWalSinceCheckpointAsync(replayCheckpointOverride, cancellationToken);
        }
        finally
        {
            replayPermit?.Release();
        }

        // Step 1.4 - publish this activation's same-silo revision cookie
        // now that the in-memory projection has been rebuilt (issue #2151).
        //
        // The cookie is the mechanism LeafCacheGrain.RefreshAsync uses to
        // decide whether its snapshot is stale, and it is bumped from every
        // state-advancing FOREGROUND site on the leaf. The snapshot
        // rehydrate and WAL replay above are not foreground sites: neither
        // ILeafProjection.Apply nor TryRehydrateFromSnapshotAsync bumps, so
        // before this call a leaf that came back from a projection rebuild
        // (which deactivates, so the next activation replays) had NO
        // registry entry at all. The cache's guard requires an entry to
        // take the revision branch, so a cache still holding a cookie from
        // the previous activation fell through to the TTL gate instead and
        // kept serving its existing snapshot until the TTL elapsed. The
        // removal-on-deactivate comment claims that fall-through reaches
        // "the cross-grain refresh path"; without this publish it reaches a
        // gate that can return early.
        //
        // Publishing unconditionally - rather than only when the replay
        // advanced - keeps the rule simple and the entry's presence tied to
        // the activation rather than to what the WAL happened to contain: a
        // cache cannot tell the two apart, and a leaf whose replay applied
        // nothing may still hold state a cache has drifted from. The cost is
        // one registry entry per live leaf, which is the bound the
        // removal-on-deactivate already maintains.
        //
        // This is only safe because activations are seeded from disjoint
        // cookie ranges (see BumpLocalRevision). Publishing here while
        // activations still restarted from zero would ARM the ABA
        // collision, by making the entry present with a value a previous
        // activation had already published.
        BumpLocalRevision();

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
        //
        // Publishing no durable pin on this path is DELIBERATE, and is
        // load-bearing in the safe direction rather than an oversight -
        // do not "tidy" it by adding a publish call here. A data-free
        // leaf can reach this return with its clock still at Zero,
        // because the replay bumps a partition's max-applied offset for
        // entries it SKIPS as belonging to another leaf's key range. Were
        // it to publish, the frontier half would be Zero again (identical
        // to what stands, so no gain) while the offset half would be
        // strictly HIGHER - and a higher offset raises the WAL GC's
        // offset floor, which REDUCES retention. That is exactly the
        // direction the coverage gate on
        // SeedDurableMaterialiserFrontierAsync now forbids (issue 2150),
        // so a publish here would reintroduce a weaker form of it.
        // Nothing is left unprotected in the meantime: both tree-id birth
        // seams (SetTreeIdAsync and InitializeSiblingAsync) await
        // SeedDurableMaterialiserBlockPinAsync before any routed write
        // makes the leaf's data reachable, and that Zero frontier
        // disables the WAL GC's cursor-trim branch outright until the
        // leaf first checkpoints. The leaf is holding that branch OFF,
        // not holding a weak floor - so there is no window here in which
        // it has no pin at all.
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
            {
                // Never-checkpointed leaf: skip the in-memory registry (a
                // Zero cursor would pin offset zero forever) but still seed
                // a durable Zero "block" pin so the WAL GC retains this
                // leaf's WAL head across a restart until it checkpoints.
                await SeedDurableMaterialiserFrontierAsync();
                return;
            }

            await ReportCursorIfActiveAsync();
        }
        catch (Exception ex)
        {
            // Cursor-publish failures are non-fatal: the cursor is
            // monotonic so the next successful foreground flush
            // catches up via the lazy-on-flush path. Materialiser
            // failures, in contrast, are fatal (they propagate above)
            // because correctness - not progress - is at stake.
            //
            // Always count the failure (issue #1030: keep the true rate
            // observable), but rate-limit the warning log to at most one per
            // silo per CursorFailLogIntervalTicks so a reactivation storm
            // against a saturated silo cannot self-amplify into a log flood.
            LatticeMetrics.LeafActivationCursorPublishFailures.Add(
                1,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, state.State.TreeId),
                LatticeTenantLabel.ForTree(state.State.TreeId));

            if (ShouldLogCursorPublishFailure())
            {
                var logger = context.ActivationServices?
                    .GetService<ILoggerFactory>()?
                    .CreateLogger<BPlusLeafGrain>();
                logger?.LogWarning(
                    ex,
                    "Eager cursor registration failed during activation for leaf {GrainId}; will retry on next checkpoint flush.",
                    context.GrainId);
            }
        }
    }

    /// <summary>
    /// Minimum interval, in ticks, between activation cursor-publish-failure
    /// warning logs across the whole silo. Bounds the log rate during a
    /// reactivation storm (issue #1030) while every failure is still counted by
    /// <see cref="LatticeMetrics.LeafActivationCursorPublishFailures"/>.
    /// </summary>
    private static readonly long CursorFailLogIntervalTicks = TimeSpan.FromSeconds(1).Ticks;

    /// <summary>Last UTC tick a cursor-publish-failure warning was logged (silo-wide).</summary>
    private static long _lastCursorFailLogTicks;

    /// <summary>
    /// Per-silo token check for the cursor-publish-failure warning: returns
    /// <c>true</c> at most once per <see cref="CursorFailLogIntervalTicks"/>.
    /// Uses an interlocked compare-and-swap so concurrent reactivations never
    /// race past the gate together.
    /// </summary>
    private static bool ShouldLogCursorPublishFailure()
    {
        var now = DateTime.UtcNow.Ticks;
        var last = Volatile.Read(ref _lastCursorFailLogTicks);
        if (now - last < CursorFailLogIntervalTicks)
        {
            return false;
        }

        return Interlocked.CompareExchange(ref _lastCursorFailLogTicks, now, last) == last;
    }

    /// <summary>
    /// Drives the dormant <see cref="ILeafProjection.Apply(in LatticeMutation)"/>
    /// seam over every WAL entry strictly after
    /// <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.ProjectionCheckpointOffset"/>
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
    /// Per-entry filter: <see cref="ShouldApplyDuringReplay(in LatticeMutation, int?, string?, string?, ShardMap?)"/>
    /// drops entries whose <see cref="LatticeMutation.ShardIndex"/>
    /// does not match this leaf's persisted shard, and entries whose
    /// key falls outside this leaf's persisted
    /// [<see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.LowKeyInclusive"/>,
    /// <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.HighKeyExclusive"/>) range. The
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

        // Resolve the leaf's current slot-ownership view once for the whole
        // replay. After an adaptive split a post-split write routed to the
        // donor shard for an already-moved slot is shadow-forwarded into the
        // target's WAL but keeps the DONOR's source stamp
        // (mutation.ShardIndex = donor). Gating Set/Delete/Tombstone replay on
        // the stamped ShardIndex alone (issue #909) drops such a record on a
        // cold reactivation from a checkpoint that pre-dates the forward,
        // resurrecting a drained value or losing a tombstone. The fix resolves
        // ownership positively by the key's virtual slot under the current
        // routing map (mirroring the snapshot-leaf fix for issue #907): a
        // record is owned by this leaf iff the current map routes its key's
        // slot to this leaf's shard. The map is fetched best-effort and is
        // only trusted when it actually references this leaf's shard, so a
        // registry hiccup or a foreign physical shard space can never cause a
        // leaf to reject its own writes - in that case replay falls back to
        // the legacy stamp-based axis.
        var replayShardMap = await ResolveReplayShardMapAsync(treeId);

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
        var deferredOffsets = new DeferredOffsetLedger(partitionCount);
        var perPartitionMaxApplied = new long[partitionCount];
        for (var p = 0; p < partitionCount; p++) perPartitionMaxApplied[p] = -1L;

        // Pass-1 absorb frontier. A deferred terminal is only safe to apply in
        // place while pass 1 is still running once every OTHER partition has
        // been fully absorbed, because that is exactly when the terminal's
        // cross-partition dependencies are all present: every saga prepare has
        // landed in _pendingTx and every range-delete target is in the Cache.
        // Within the terminal's own partition the entries below its offset are
        // already applied (the WAL is read in offset order) and the entries
        // above it were appended after it, so they are not dependencies.
        // Partitions are absorbed in index order, so the condition holds for a
        // single-partition tree throughout, and for the last partition of a
        // multi-partition tree; every other terminal stays deferred to pass 2
        // exactly as before.
        var partitionsAbsorbed = 0;

        // Pass-1 sweep order (issue #2089). Pass 1 can only drain a deferred
        // terminal in place for the partition it absorbs LAST, because only
        // then are every other partition's prepares and range-delete targets
        // already in the cache. Every other partition must defer its terminals
        // to pass 2 - so an unresolved saga prepare pins its incremental flush
        // ceiling at (prepare - 1) for the whole of pass 1, and an activation
        // torn down before pass 2 completes banks nothing at all and replays
        // the identical range on the next activation.
        //
        // Sweeping in fixed index order hands that single drain-eligible slot
        // to partition N-1 regardless of where the backlog actually is.
        // Ordering the sweep by backlog ascending gives it instead to the
        // partition with the MOST to replay: the one least likely to finish
        // inside the activation window, and therefore the one that gains most
        // from banking progress incrementally as it scans.
        //
        // This NARROWS the livelock, it does not remove it - the other N-1
        // partitions still cannot drain in pass 1. Removing it needs a durable
        // record of unresolved deferred work so a resumed replay need not
        // re-read it; see issue #2089.
        var sweep = await BuildPassOneSweepOrderAsync(
            treeId, partitionCount, checkpointOverride, cancellationToken);

        // Issue #2165. Reconstruct the deferred work a previous activation
        // recorded durably, BEFORE pass 1 reads anything. This is what makes
        // the checkpoint advance that recorded it safe: the prepares go back
        // into _pendingTx and the terminals into the pass-2 list without the
        // WAL below the checkpoint being re-read. Records at offsets this
        // replay will re-read anyway are dropped rather than restored, so
        // nothing is applied twice.
        RestoreUnresolvedReplayWork(
            projection, partitionCount, checkpointOverride, deferredTerminals);

        foreach (var (partition, probedHead) in sweep)
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

            // Durable-frontier fall-off guard (issue #945: silent durable data
            // loss). The cold-cache-reset override (checkpointOverride = -1, set
            // by OnActivateAsync step 0.5 when the per-activation cache starts
            // empty and no snapshot rehydrated) deliberately drives the replay
            // from the absolute start so the full readable window is rebuilt,
            // and it is also the value handed to the fall-off-log detector
            // below. Feeding the detector -1 intentionally suppresses its
            // shared-WAL replay-budget heuristic (a sibling-populated partition
            // would otherwise trip the budget against this leaf's full range),
            // but it ALSO blinds the detector's WAL-trim trigger
            // (checkpoint > 0 && tail > checkpoint), because -1 is read as
            // "nothing to lose". For a leaf that genuinely has a durable
            // projection checkpoint, that blindness is unsafe: if the WAL has
            // been trimmed past the durable checkpoint and no snapshot covers
            // the gap, a cold replay rebuilds the leaf from only the surviving
            // WAL suffix - dropping every key the trim removed - and then
            // advances the persisted checkpoint and the durable materialiser
            // pin over the lost data. The advanced pin licenses the WAL GC to
            // trim further behind it, laundering the loss across the whole tree.
            // Re-check the trim trigger here against the DURABLE checkpoint and
            // surface the gap as a stale projection rather than silently
            // materialising it away. Gated on the cold-reset override so the
            // warm/snapshot-rehydrated path (where checkpoint == persisted) is
            // unchanged - the detector already covers it.
            //
            // Loss condition is tail > checkpoint + 1, NOT the detector's looser
            // tail > checkpoint. The replay reads strictly past the checkpoint
            // (ReplayPartitionAsync passes fromExclusive = checkpoint), so the
            // FIRST offset this leaf still needs is checkpoint + 1; the entry AT
            // the checkpoint is already applied and harmless to lose. tail is the
            // oldest still-readable offset (ICommitLogReader.GetTailOffsetAsync).
            // When tail == checkpoint + 1 only the already-applied prefix was
            // trimmed and the entire needed (checkpoint, head] window survives -
            // this is the legitimate "durable floor kept the live tail" shape
            // (issue #919), which must replay cleanly, not throw. Loss is real
            // only when the first needed offset itself fell off the log, i.e.
            // tail > checkpoint + 1. This guard and the fall-off-log detector's
            // WAL-trim trigger (LatticeFallOffLogDetector.ClassifyAsync) must use
            // the SAME exact boundary: the detector's SnapshotThenWal decision is
            // NOT a soft rebuild-policy hint - it throws LeafProjectionStaleException
            // below (the SnapshotThenWal/FullRebuild recovery paths are not yet
            // integrated). Before the coverage-gated WAL GC, the detector's looser
            // tail > checkpoint formula never bit because a snapshot-covered leaf
            // could not settle at tail == checkpoint + 1; now it can (the offset
            // floor trims the already-applied checkpoint entry once covered), so
            // the detector was aligned to tail > checkpoint + 1 to match this guard.
            if (checkpointOverride is { } coldReplayStart
                && coldReplayStart < persistedCheckpoint
                && persistedCheckpoint > 0)
            {
                var trimCoordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
                    $"{treeId}/{partition}");
                var tail = await trimCoordinator.GetTailOffsetAsync(cancellationToken);
                if (tail > persistedCheckpoint + 1)
                {
                    throw new LeafProjectionStaleException(
                        $"Leaf projection for tree '{treeId}' partition {partition} cannot be rebuilt " +
                        $"from the WAL: the durable projection checkpoint (offset {persistedCheckpoint}) " +
                        $"has fallen off the log (oldest readable offset {tail}) and no covering snapshot " +
                        "is available, so a cold replay would silently rebuild the leaf over the lost " +
                        "prefix and advance the materialiser pin past unrecoverable data. " +
                        "Operator-driven projection rebuild is required.");
                }

                // Residual liveness signal (#1542). Reaching here means this
                // partition is a genuine cold rebuild over a pre-existing durable
                // checkpoint (persistedCheckpoint > 0) whose full prefix still
                // survives in the readable WAL - the guard above ruled out a
                // fallen-off prefix, and no snapshot rehydrated (step 0.5 chose
                // the -1 override only when the cache started empty and
                // unhydrated). The replay below therefore reconstructs the entire
                // readable window into the cache, so the cache faithfully holds
                // the checkpointed prefix and a graceful-deactivation capture may
                // safely stamp coverage. This closes the gap #1537 leaves for an
                // already-converged, snapshot-less leaf (checkpoint already at
                // head, so no forward advance sets _checkpointAdvancedThisActivation)
                // that would otherwise hold its Zero block pin - and its shared
                // WAL - forever. A brand-new leaf has no pre-existing checkpoint
                // (persistedCheckpoint == 0), never enters this block, and so its
                // foreground writes are never auto-covered on deactivation.
                _cacheRebuiltFromWalStartThisActivation = true;
            }

#if LATTICE_DIAG
            DiagSink.Write($"[DIAG replay-enter] gid={context.GrainId} treeId={treeId} partition={partition} shardIndex={state.State.ShardIndex} " +
                $"low='{state.State.LowKeyInclusive ?? "<null>"}' high='{state.State.HighKeyExclusive ?? "<null>"}' " +
                $"checkpoint={checkpoint} entryCount={Cache.Count}");
#endif

            // Whether the detector elected this partition an OVER-BUDGET
            // CANDIDATE. It is a candidate and not a verdict because the
            // detector compares a partition-wide, pre-filter offset gap
            // against a per-leaf, post-filter budget (issue #2149); the gap
            // is a sound upper bound on this leaf's own work, so it can only
            // over-elect, never under-elect. ReplayPartitionAsync confirms it
            // below against the exact count of entries this leaf actually
            // applies.
            var overBudgetCandidate = false;

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
                    case FallOffLogDecision.TailReplayOverBudget:
                        // A cost trigger fired (replay gap over
                        // MaxLeafReplayEntries, or projection age over
                        // LeafProjectionRetention) but the WAL still covers
                        // every offset this leaf needs, so the replay below
                        // converges to exactly the same projection. Replay
                        // regardless: a long activation is recoverable,
                        // refusing to activate is not (#1738).
                        //
                        // NOTHING IS WARNED OR METERED HERE (issue #2149).
                        // The gap trigger is expressed in partition-wide,
                        // pre-filter offsets while MaxLeafReplayEntries is a
                        // per-leaf, POST-filter budget, so warning here
                        // reported a quantity that was not this leaf's work:
                        // at the measured fan-out of ~1,350 leaves per
                        // partition it fired 19,639 times in 6.26 hours for
                        // leaves whose real work was one to two orders of
                        // magnitude BELOW budget. Both the warning and the
                        // LeafActivationOverBudgetReplays counter now live in
                        // ReplayPartitionAsync, where the entries this leaf
                        // actually applies are counted for free during the
                        // replay that is happening anyway.
                        //
                        // The candidate is still load-bearing: it is a sound
                        // upper bound (applied <= gap), so it can only
                        // over-elect. That is exactly what makes it safe to
                        // use as the gate on the STALL check downstream - the
                        // new fault line can never fire anywhere the old line
                        // did not.
                        overBudgetCandidate = true;
                        break;
                    case FallOffLogDecision.SnapshotThenWal:
                    case FallOffLogDecision.FullRebuildFromWal:
                    case FallOffLogDecision.Fail:
                    default:
                        throw new LeafProjectionStaleException(
                            $"Leaf projection for tree '{treeId}' partition {partition} cannot be recovered " +
                            $"from the WAL alone (decision={decision}, persistedCheckpoint={checkpoint}): the " +
                            "write-ahead log has been trimmed past an offset this leaf still needs and no " +
                            "covering snapshot is available, so replaying the surviving suffix would rebuild " +
                            "the leaf over the lost prefix. Snapshot-then-WAL and full-rebuild recovery paths " +
                            "are not yet integrated; operator-driven rebuild is required.");
                }
            }

            var (advanced, maxApplied) = await ReplayPartitionAsync(treeId, partition, checkpoint, projection, deferredTerminals, deferredOffsets, partitionsAbsorbed == partitionCount - 1, replayShardMap, resolvedOptions.WalReplayMaxRecordsPerTurn, resolvedOptions.MaxDurableUnresolvedReplayWork, probedHead, resolvedOptions.MaxLeafReplayEntries, overBudgetCandidate, cancellationToken);
            partitionsAbsorbed++;
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
        //
        // Each drained terminal is struck off the deferred ledger and its
        // partition's now-recovered ceiling is flushed immediately (issue
        // #1831). The drain is cheap per terminal but the list can be long, so
        // banking the recovered prefix as it shrinks means a teardown during
        // pass 2 keeps the progress the drain has already earned instead of
        // discarding all of it. Both safety clamps still bound every flush:
        // the ceiling stays below the next unresolved deferred offset in that
        // partition and below any unresolved prepare in it.
        foreach (var terminal in deferredTerminals)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using (LatticeApplyOffsetContext.BeginScope(terminal.Partition, terminal.Offset))
            {
                projection.Apply(terminal.Mutation);
            }

            deferredOffsets.Resolve(terminal.Partition, terminal.Offset);
            ResolveUnresolvedReplayWork(terminal.Partition, terminal.Offset);
            if (await TryFlushRecoveredCeilingAsync(
                terminal.Partition,
                perPartitionMaxApplied[terminal.Partition],
                deferredOffsets,
                projection,
                cancellationToken))
            {
                anyAdvanced = true;
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
    /// Mutation deferred during pass 1 of the activation-time replay
    /// to be applied in pass 2 once every partition's per-key Set /
    /// Delete entries have been absorbed into the leaf's Cache and
    /// every prepare into <c>_pendingTx</c>. Covers the two mutation
    /// shapes whose apply semantics depend on the global per-shard
    /// state being fully reconstructed:
    /// <para>
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
    /// </para>
    /// <para>
    ///     <see cref="MutationKind.DeleteRange"/> - the tombstone
    ///     mutation iterates the leaf's Cache at apply time to
    ///     tombstone every in-range key. A range tombstone in
    ///     partition <c>P_t</c> whose target Set entries live in
    ///     partition <c>P_s</c> would see an empty Cache during
    ///     pass 1 (when partition <c>P_t</c> happens to replay
    ///     before partition <c>P_s</c>), tombstone nothing, and let
    ///     the Sets in <c>P_s</c> become visible. Deferring to pass 2
    ///     restores the tombstone-after-its-targets ordering invariant.
    /// </para>
    /// <para>
    ///     Both rationales are about entries in OTHER partitions that pass 1
    ///     has not absorbed yet, so both lapse once every other partition has
    ///     been fully absorbed: every prepare is then in <c>_pendingTx</c> and
    ///     every range-delete target is in the Cache, and the entries above the
    ///     terminal in its own partition were appended after it, so they are
    ///     not dependencies. Pass 1 therefore applies such a mutation in place
    ///     rather than deferring it (issue #1831), which keeps the incremental
    ///     flush ceiling moving for the remainder of the scan. The atomicity
    ///     contract above is unchanged: a terminal is only ever applied when
    ///     its complete prepared-mutation set is already reconstructed.
    /// </para>
    /// </summary>
    private readonly record struct DeferredTerminal(
        int Partition,
        long Offset,
        LatticeMutation Mutation);

    /// <summary>
    /// The minimum interval between over-budget replay warnings for one
    /// (tree, leaf, partition). Chosen so a cold start reports each replaying
    /// leaf partition roughly once a minute rather than once per re-activation
    /// attempt.
    /// </summary>
    private static readonly TimeSpan OverBudgetLogInterval = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Soft cap on <see cref="OverBudgetLogStamps"/>. Reaching it triggers an
    /// opportunistic sweep of entries older than
    /// <see cref="OverBudgetLogInterval"/>, which is free to drop: a stamp that
    /// has already aged past the interval suppresses nothing, so removing it
    /// cannot change what is logged. The cap is a bound on retained keys, not a
    /// hard admission limit - a burst of more than this many distinct leaf
    /// partitions inside one interval is allowed to exceed it rather than
    /// silently losing suppression for the overflow.
    /// </summary>
    private const int OverBudgetLogStampCapacity = 4096;

    /// <summary>
    /// Last-logged timestamps for the over-budget replay warning, keyed by tree,
    /// leaf, and WAL partition. Static because the point is to suppress across
    /// the repeated ACTIVATIONS of the same leaf - per-activation state would
    /// reset every time and suppress nothing.
    /// <para>
    /// The leaf id is part of the key (issue #2023). Without it the key was
    /// (tree, WAL partition), which is <b>not</b> a leaf: the partition ordinal
    /// is iterated <c>[0, WalPartitions)</c> inside every leaf's activation, so
    /// the first leaf to trip the budget suppressed the warning for every other
    /// leaf in that tree and partition for a full minute.
    /// </para>
    /// <para>
    /// Growth is bounded by the number of distinct (tree, leaf, partition)
    /// triples that trip the budget within one
    /// <see cref="OverBudgetLogInterval"/>, not by the leaf count and not by the
    /// attempt count, because <see cref="ShouldLogOverBudgetReplay"/> sweeps
    /// aged-out stamps once the map reaches
    /// <see cref="OverBudgetLogStampCapacity"/>.
    /// </para>
    /// </summary>
    private static readonly ConcurrentDictionary<(string TreeId, string LeafId, int Partition), long> OverBudgetLogStamps = new();

    /// <summary>
    /// True when the over-budget replay warning for this leaf partition is due
    /// again. Suppression is deliberately best-effort under races: two silos may
    /// each emit one line, which is fine - the goal is to stop a re-activation
    /// storm flooding the log, not to guarantee exactly-once logging. The metric
    /// remains the exact count. Internal so the suppression itself is testable
    /// rather than only observable through log output.
    /// </summary>
    /// <param name="treeId">The tree the leaf belongs to.</param>
    /// <param name="leafId">
    /// The leaf's grain id. Load-bearing: it is what makes successive warnings
    /// comparable, so an operator can evaluate the "checkpoint does not advance"
    /// fault criterion the warning states (issue #2023).
    /// </param>
    /// <param name="partition">The WAL partition ordinal being replayed.</param>
    /// <returns><see langword="true"/> when the warning should be emitted.</returns>
    internal static bool ShouldLogOverBudgetReplay(string treeId, string leafId, int partition)
        => ShouldLogThrottled(OverBudgetLogStamps, treeId, leafId, partition);

    /// <summary>
    /// Throttle stamps for the stalled-replay FAULT warning, kept in a map of
    /// their own rather than sharing <see cref="OverBudgetLogStamps"/>.
    /// <para>
    /// Separation is load-bearing (issue #2149). The cost warning and the fault
    /// warning are keyed identically, so a shared map would let a cost line for
    /// a leaf swallow the fault line for the same leaf for a whole
    /// <see cref="OverBudgetLogInterval"/> - silencing the only signal that
    /// found the livelocked leaf of issue #2165, which was 0.25% of warnings in
    /// one measurement window and 59% in the next. Two maps cost one dictionary
    /// and make the fault line unsuppressible by cost noise.
    /// </para>
    /// </summary>
    private static readonly ConcurrentDictionary<(string TreeId, string LeafId, int Partition), long> StalledReplayLogStamps = new();

    /// <summary>
    /// True when the stalled-replay fault warning for this leaf partition is due
    /// again. Throttled on the same interval as the cost warning but through an
    /// independent stamp map, so cost noise can never suppress a fault.
    /// </summary>
    /// <param name="treeId">The tree the leaf belongs to.</param>
    /// <param name="leafId">The leaf's grain id.</param>
    /// <param name="partition">The WAL partition ordinal being replayed.</param>
    /// <returns><see langword="true"/> when the warning should be emitted.</returns>
    internal static bool ShouldLogStalledReplay(string treeId, string leafId, int partition)
        => ShouldLogThrottled(StalledReplayLogStamps, treeId, leafId, partition);

    /// <summary>
    /// Shared body of the per-(tree, leaf, partition) log throttles. Suppression
    /// is deliberately best-effort under races: two silos may each emit one
    /// line, which is fine - the goal is to stop a re-activation storm flooding
    /// the log, not to guarantee exactly-once logging.
    /// </summary>
    /// <param name="stamps">The stamp map to evaluate and update.</param>
    /// <param name="treeId">The tree the leaf belongs to.</param>
    /// <param name="leafId">The leaf's grain id.</param>
    /// <param name="partition">The WAL partition ordinal being replayed.</param>
    /// <returns><see langword="true"/> when the warning should be emitted.</returns>
    private static bool ShouldLogThrottled(
        ConcurrentDictionary<(string TreeId, string LeafId, int Partition), long> stamps,
        string treeId,
        string leafId,
        int partition)
    {
        var now = Stopwatch.GetTimestamp();
        var key = (treeId, leafId, partition);
        if (!stamps.TryGetValue(key, out var last))
        {
            if (stamps.Count >= OverBudgetLogStampCapacity)
            {
                PruneLogStamps(stamps, now);
            }

            return stamps.TryAdd(key, now);
        }

        if (Stopwatch.GetElapsedTime(last, now) < OverBudgetLogInterval)
        {
            return false;
        }

        return stamps.TryUpdate(key, now, last);
    }

    /// <summary>
    /// Drops every stamp that has already aged past
    /// <see cref="OverBudgetLogInterval"/>. Such an entry would permit the
    /// next warning anyway, so removing it is semantically free - it keeps the
    /// map's retained size tracking the leaf partitions that are currently
    /// tripping the condition rather than every leaf that ever did.
    /// </summary>
    /// <param name="stamps">The stamp map to prune.</param>
    /// <param name="now">The timestamp the calling check is evaluated at.</param>
    private static void PruneLogStamps(
        ConcurrentDictionary<(string TreeId, string LeafId, int Partition), long> stamps,
        long now)
    {
        foreach (var stamp in stamps)
        {
            if (Stopwatch.GetElapsedTime(stamp.Value, now) >= OverBudgetLogInterval)
            {
                stamps.TryRemove(stamp);
            }
        }
    }

    /// <summary>
    /// The minimum interval between activation-temperature sample lines for one
    /// tree. Matched to <see cref="OverBudgetLogInterval"/> so the two lines a
    /// replaying tree can emit stay on the same cadence.
    /// </summary>
    private static readonly TimeSpan ActivationTemperatureLogInterval = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Soft cap on <see cref="ActivationTemperatureLogStamps"/>, with the same
    /// contract as <see cref="OverBudgetLogStampCapacity"/>: reaching it
    /// triggers an opportunistic sweep of entries older than
    /// <see cref="ActivationTemperatureLogInterval"/>, which is free to drop
    /// because an aged-out stamp suppresses nothing.
    /// </summary>
    private const int ActivationTemperatureLogStampCapacity = 4096;

    /// <summary>
    /// Last-emitted timestamps for the activation-temperature sample line,
    /// keyed by <b>tree only</b>. Static for the same reason as
    /// <see cref="OverBudgetLogStamps"/> - the point is to suppress across
    /// repeated activations, and per-activation state would reset every time
    /// and suppress nothing.
    /// <para>
    /// The leaf id is deliberately <b>not</b> part of this key, which is the
    /// opposite of the choice <see cref="OverBudgetLogStamps"/> documents for
    /// issue #2023. That reasoning does not transfer: the over-budget warning
    /// reports a per-leaf checkpoint, and comparing checkpoints across lines
    /// naming different leaves is meaningless, so its key must name the leaf.
    /// This line reports a per-tree aggregate that is already summed over every
    /// leaf, so successive lines for one tree are comparable by construction,
    /// and keying by leaf would emit one line per leaf per interval - the flood
    /// the throttle exists to prevent.
    /// </para>
    /// </summary>
    private static readonly ConcurrentDictionary<string, long> ActivationTemperatureLogStamps = new(StringComparer.Ordinal);

    /// <summary>
    /// Cumulative per-tree cold and warm activation-replay totals for the life
    /// of the process. Growth is bounded by the number of distinct trees the
    /// silo activates leaves for - the same bound
    /// <see cref="LatticeMetrics.TagTree"/> already assumes - so, unlike the
    /// stamp maps, this one is never swept: dropping an entry would reset the
    /// totals, and a line whose totals restarted at zero would report a ratio
    /// for an arbitrary sub-window while claiming to be cumulative.
    /// </summary>
    private static readonly ConcurrentDictionary<string, ActivationTemperatureTotals> ActivationTemperatureTotalsByTree = new(StringComparer.Ordinal);

    /// <summary>
    /// One tree's running cold and warm activation-replay totals. Each arm is
    /// exact under concurrency; the returned pair is a sample, so a concurrent
    /// activation on another silo thread may already have advanced the other
    /// arm by the time the pair is read. That is harmless for the ratio, which
    /// is a long-run quantity, and the counter remains the exact count.
    /// </summary>
    private sealed class ActivationTemperatureTotals
    {
        private long _cold;
        private long _warm;

        /// <summary>
        /// Records one replay on the arm selected by <paramref name="cold"/>
        /// and returns the totals as observed immediately afterwards.
        /// </summary>
        public (long Cold, long Warm) Add(bool cold)
        {
            if (cold)
            {
                Interlocked.Increment(ref _cold);
            }
            else
            {
                Interlocked.Increment(ref _warm);
            }

            return (Interlocked.Read(ref _cold), Interlocked.Read(ref _warm));
        }
    }

    /// <summary>
    /// Records one permitted activation replay against its tree's cumulative
    /// cold / warm totals, and returns those totals when the sample line is due
    /// again for that tree - or <see langword="null"/> when the line is
    /// currently suppressed.
    /// <para>
    /// Accumulation is unconditional and happens <b>before</b> the throttle is
    /// consulted, which is the property that makes the design work. A
    /// rate-limited line that announced each activation would undercount by an
    /// unknown factor, because suppression is invisible in the output; a
    /// rate-limited <b>sample of a counter</b> does not, because every
    /// suppressed activation is still in the totals the next emitted line
    /// prints. So any one line yields the cold:warm ratio and any two yield the
    /// rate between them.
    /// </para>
    /// <para>
    /// Suppression is best-effort under races, exactly as
    /// <see cref="ShouldLogOverBudgetReplay"/> is: two threads may each emit a
    /// line, which costs a duplicate sample and nothing else. Internal so the
    /// accumulate-then-throttle composition is testable as one unit rather than
    /// re-assembled by a test in an order production does not use.
    /// </para>
    /// </summary>
    /// <param name="treeId">The tree the activating leaf belongs to.</param>
    /// <param name="cold">
    /// <see langword="true"/> when the activation replays from the <c>-1</c>
    /// sentinel with no snapshot or cache anchor.
    /// </param>
    /// <param name="now">
    /// The <see cref="Stopwatch.GetTimestamp"/> reading to evaluate the
    /// throttle at. Supplied by the caller so a test can advance time
    /// deterministically instead of waiting out the interval.
    /// </param>
    /// <returns>
    /// The tree's cumulative <c>(cold, warm)</c> totals when a line is due,
    /// otherwise <see langword="null"/>.
    /// </returns>
    internal static (long Cold, long Warm)? ObserveLeafActivationReplay(string treeId, bool cold, long now)
    {
        var totals = ActivationTemperatureTotalsByTree
            .GetOrAdd(treeId, static _ => new ActivationTemperatureTotals())
            .Add(cold);

        return ShouldLogActivationTemperature(treeId, now) ? totals : null;
    }

    /// <summary>
    /// True when the activation-temperature sample line is due again for
    /// <paramref name="treeId"/>. Same shape as
    /// <see cref="ShouldLogOverBudgetReplay"/> - a sibling stamp map, its own
    /// interval, and the same aged-out sweep at capacity.
    /// </summary>
    /// <param name="treeId">The tree the activating leaf belongs to.</param>
    /// <param name="now">The timestamp to evaluate the interval against.</param>
    /// <returns><see langword="true"/> when the line should be emitted.</returns>
    private static bool ShouldLogActivationTemperature(string treeId, long now)
    {
        if (!ActivationTemperatureLogStamps.TryGetValue(treeId, out var last))
        {
            if (ActivationTemperatureLogStamps.Count >= ActivationTemperatureLogStampCapacity)
            {
                PruneActivationTemperatureLogStamps(now);
            }

            return ActivationTemperatureLogStamps.TryAdd(treeId, now);
        }

        if (Stopwatch.GetElapsedTime(last, now) < ActivationTemperatureLogInterval)
        {
            return false;
        }

        return ActivationTemperatureLogStamps.TryUpdate(treeId, now, last);
    }

    /// <summary>
    /// Drops every <see cref="ActivationTemperatureLogStamps"/> entry that has
    /// already aged past <see cref="ActivationTemperatureLogInterval"/>. Such an
    /// entry would permit the next line anyway, so removing it is semantically
    /// free. The cumulative totals are a separate map and are never swept.
    /// </summary>
    /// <param name="now">The timestamp the calling check is evaluated at.</param>
    private static void PruneActivationTemperatureLogStamps(long now)
    {
        foreach (var stamp in ActivationTemperatureLogStamps)
        {
            if (Stopwatch.GetElapsedTime(stamp.Value, now) >= ActivationTemperatureLogInterval)
            {
                ActivationTemperatureLogStamps.TryRemove(stamp);
            }
        }
    }

    /// <summary>
    /// Silo-scoped record of the persisted checkpoint each leaf partition was
    /// last seen replaying from, used to tell a slow replay from a STALLED one
    /// (issue #2149, fault shape of issue #2165).
    /// <para>
    /// The old over-budget warning stated the fault criterion in prose - "a
    /// checkpoint that does NOT advance across repeats of this warning for the
    /// SAME leaf and partition is a fault" - and left an operator to evaluate it
    /// by hand across a log. That is exactly the evaluation that found the leaf
    /// of issue #2165 (livelocked 6h52m at an unchanging checkpoint, losing
    /// ~15.8 writes/hour), and exactly the evaluation that could not be made
    /// once 19,604 benign lines were mixed in among the 35 that mattered. This
    /// map performs it in the process instead.
    /// </para>
    /// </summary>
    private static readonly ConcurrentDictionary<(string TreeId, string LeafId, int Partition), long> ReplayCheckpointObservations = new();

    /// <summary>
    /// Soft cap on <see cref="ReplayCheckpointObservations"/>. Unlike the log
    /// stamps there is no age at which an observation is free to drop - a stall
    /// is detected by comparing against an ARBITRARILY old prior observation -
    /// so the map is cleared wholesale when it overflows rather than pruned.
    /// Clearing loses at most one repeat of the fault warning per affected leaf:
    /// a genuinely stuck leaf re-activates continuously and is re-observed on
    /// its next attempt.
    /// </summary>
    private const int ReplayCheckpointObservationCapacity = 8192;

    /// <summary>
    /// Records the checkpoint this leaf partition is replaying from and reports
    /// whether it is UNCHANGED since the previous observation on this silo -
    /// that is, whether the previous activation of this same leaf partition
    /// made no durable forward progress at all.
    /// </summary>
    /// <param name="treeId">The tree the leaf belongs to.</param>
    /// <param name="leafId">The leaf's grain id.</param>
    /// <param name="partition">The WAL partition ordinal being replayed.</param>
    /// <param name="checkpoint">The persisted checkpoint this replay starts from.</param>
    /// <returns>
    /// <see langword="true"/> when a previous observation exists for this leaf
    /// partition and its checkpoint is identical, so the leaf is stalled.
    /// <see langword="false"/> on the first observation (nothing to compare
    /// against) and whenever the checkpoint has advanced.
    /// </returns>
    internal static bool NoteReplayCheckpointObservation(string treeId, string leafId, int partition, long checkpoint)
    {
        var key = (treeId, leafId, partition);
        var stalled = ReplayCheckpointObservations.TryGetValue(key, out var previous) && previous == checkpoint;
        if (!stalled && ReplayCheckpointObservations.Count >= ReplayCheckpointObservationCapacity)
        {
            ReplayCheckpointObservations.Clear();
        }

        ReplayCheckpointObservations[key] = checkpoint;
        return stalled;
    }

    /// <summary>
    /// Clears the throttle and observation state these warnings keep across
    /// activations. Test seam only: the maps are static and silo-scoped, so a
    /// test that asserts on first-observation or first-warning behaviour needs
    /// them empty regardless of what ran before it.
    /// </summary>
    internal static void ResetReplayWarningStateForTests()
    {
        OverBudgetLogStamps.Clear();
        StalledReplayLogStamps.Clear();
        ReplayCheckpointObservations.Clear();
    }

    /// <summary>
    /// Replay-scoped ledger of the WAL offsets whose mutations were deferred
    /// out of pass 1, kept per partition and <b>resolvable</b>: an offset is
    /// struck off the moment its mutation is actually applied, so the
    /// incremental-flush ceiling recovers instead of staying pinned behind the
    /// first deferred mutation for the whole replay (issue #1831).
    /// <para>
    /// Replaces the monotonically non-increasing <c>lowestDeferredOffset</c>
    /// scalar the incremental flush used to clamp against. That scalar was
    /// only ever lowered, so the first <see cref="MutationKind.DeleteRange"/> /
    /// <see cref="MutationKind.TxCommit"/> / <see cref="MutationKind.TxAbort"/>
    /// a partition emitted pinned its ceiling for the entire remainder of the
    /// replay, leaving all durable progress to the post-pass-2 reconciliation -
    /// which only runs when the whole replay fits inside the activation window.
    /// </para>
    /// <para>
    /// <b>Hot-path shape.</b> Offsets arrive in strictly increasing order
    /// within a partition (the WAL is read in offset order), so each
    /// partition's buffer is sorted and the lowest unresolved offset is the
    /// entry at a head cursor - an O(1) array read, allocation-free, evaluated
    /// once per slice. Buffers are allocated lazily per partition and grown by
    /// doubling, so the ledger's cost tracks the number of DEFERRED offsets and
    /// never the number of replayed records.
    /// </para>
    /// </summary>
    internal sealed class DeferredOffsetLedger
    {
        /// <summary>
        /// Marks a struck-off slot. WAL offsets are non-negative, so <c>-1</c>
        /// cannot collide with a real offset - including offset <c>0</c>, which
        /// a cold replay under the checkpoint override genuinely reads.
        /// </summary>
        private const long ResolvedSlot = -1L;

        private const int InitialCapacity = 4;

        private readonly long[]?[] _offsets;
        private readonly int[] _counts;
        private readonly int[] _heads;

        /// <summary>
        /// Creates a ledger covering <paramref name="partitionCount"/> WAL
        /// partitions. No per-partition buffer is allocated until that
        /// partition actually defers something.
        /// </summary>
        internal DeferredOffsetLedger(int partitionCount)
        {
            _offsets = new long[partitionCount][];
            _counts = new int[partitionCount];
            _heads = new int[partitionCount];
        }

        /// <summary>
        /// Records <paramref name="offset"/> as deferred (and therefore
        /// unapplied) under <paramref name="partition"/>. Callers append in
        /// increasing offset order within a partition.
        /// </summary>
        internal void Add(int partition, long offset)
        {
            var buffer = _offsets[partition];
            var count = _counts[partition];
            if (buffer is null)
            {
                buffer = new long[InitialCapacity];
                _offsets[partition] = buffer;
            }
            else if (count == buffer.Length)
            {
                var grown = new long[buffer.Length * 2];
                Array.Copy(buffer, grown, count);
                _offsets[partition] = grown;
                buffer = grown;
            }

            buffer[count] = offset;
            _counts[partition] = count + 1;
        }

        /// <summary>
        /// Strikes <paramref name="offset"/> off <paramref name="partition"/>'s
        /// unresolved set once its mutation has been applied. Resolution
        /// normally arrives in the same order the offsets were added, which is
        /// the O(1) head-advance path; an out-of-order resolution marks its
        /// slot and the head skips it when it gets there. An offset that was
        /// never added, or that was already resolved, is ignored.
        /// </summary>
        internal void Resolve(int partition, long offset)
        {
            var buffer = _offsets[partition];
            if (buffer is null)
                return;

            var count = _counts[partition];
            var head = _heads[partition];
            for (var i = head; i < count; i++)
            {
                if (buffer[i] != offset)
                    continue;
                buffer[i] = ResolvedSlot;
                break;
            }

            while (head < count && buffer[head] == ResolvedSlot)
                head++;
            _heads[partition] = head;
        }

        /// <summary>
        /// The lowest still-unresolved deferred offset in
        /// <paramref name="partition"/>, or <see cref="long.MaxValue"/> when
        /// the partition holds none. The incremental flush clamps strictly
        /// below this value, so a partition with nothing outstanding is free to
        /// advance to its applied frontier.
        /// </summary>
        internal long MinUnresolved(int partition)
        {
            var head = _heads[partition];
            return head < _counts[partition] ? _offsets[partition]![head] : long.MaxValue;
        }
    }

    /// <summary>
    /// Flushes <paramref name="partition"/>'s projection checkpoint up to the
    /// highest offset below which every entry is fully applied, and returns
    /// <c>true</c> when the durable position actually advanced. Shared by the
    /// per-slice flush in pass 1 and the per-terminal flush in pass 2 so both
    /// compute the ceiling from one place and can never drift apart.
    /// <para>
    /// The ceiling is the minimum of three bounds, so it can never license a
    /// checkpoint (or the durable materialiser pin it drives) past an offset
    /// that is not yet applied:
    /// </para>
    /// <list type="number">
    /// <item><paramref name="maxApplied"/> - the highest offset this replay has
    /// reached in the partition.</item>
    /// <item>One below the partition's lowest still-unresolved deferred offset
    /// (<see cref="DeferredOffsetLedger.MinUnresolved"/>), because a deferred
    /// mutation is applied only when it drains.</item>
    /// <item>One below any unresolved saga prepare in the partition
    /// (<see cref="MinUnresolvedPrepareOffsetForPartition"/>), because a
    /// resumed replay must re-read the prepare to rebuild <c>_pendingTx</c>.
    /// <see cref="ILeafProjection.SetCheckpointOffsetAsync"/> applies this
    /// clamp internally too; applying it here as well keeps the cadence from
    /// issuing redundant force-flushes while a prepare sits open below
    /// <paramref name="maxApplied"/>.</item>
    /// </list>
    /// <para>
    /// The flush is skipped unless it strictly advances the partition's current
    /// position, which also keeps
    /// <see cref="ILeafProjection.SetCheckpointOffsetAsync"/>'s monotonic guard
    /// and its idempotent-re-assert force-flush out of the loop. Persisting is
    /// coalesced per <c>MaterialiserCheckpointInterval</c> /
    /// <c>MaterialiserCheckpointEntries</c> inside that seam and drives the
    /// periodic snapshot capture.
    /// </para>
    /// </summary>
    private async Task<bool> TryFlushRecoveredCeilingAsync(
        int partition,
        long maxApplied,
        DeferredOffsetLedger deferredOffsets,
        ILeafProjection projection,
        CancellationToken cancellationToken)
    {
        if (maxApplied < 0)
            return false;

        var ceiling = maxApplied;
        var minDeferred = deferredOffsets.MinUnresolved(partition);
        if (minDeferred != long.MaxValue && minDeferred - 1 < ceiling)
            ceiling = minDeferred - 1;
        if (MinUnresolvedPrepareOffsetForPartition(partition) is long minPrepare && minPrepare - 1 < ceiling)
            ceiling = minPrepare - 1;

        if (ceiling <= GetCurrentCheckpointForPartition(partition))
            return false;

        using (LatticeApplyOffsetContext.BeginScope(partition, ceiling))
        {
            await projection.SetCheckpointOffsetAsync(ceiling, cancellationToken);
        }

        return true;
    }

    /// <summary>
    /// Builds the order in which pass 1 absorbs the WAL partitions, together
    /// with the head offset probed for each.
    /// <para>
    /// Only the partition absorbed LAST is drain-eligible (its cross-partition
    /// dependencies are all present by then), so which partition occupies that
    /// slot decides which one can resolve its own saga prepares during pass 1
    /// and keep banking durable progress. Ordering by backlog ascending awards
    /// the slot to the partition with the most to replay - see issue #2089.
    /// </para>
    /// <para>
    /// The head probe is the same call <see cref="ReplayPartitionAsync"/>
    /// already makes, hoisted so it can inform the ordering and then handed
    /// back down, so ordering costs no additional grain calls. A probe fault
    /// is caught PER PARTITION: the partitions that did probe still inform the
    /// order, the ones that did not keep their natural position and are handed
    /// back with a <c>null</c> head so <see cref="ReplayPartitionAsync"/>
    /// re-probes and surfaces the real fault in its own turn - which is exactly
    /// today's failure behaviour, after the partitions ahead of it have banked.
    /// The fault is logged at Warning naming the partitions, so a degraded
    /// ordering is never silent; see issues #2082 and #2089.
    /// </para>
    /// <para>
    /// A partition sitting at the "nothing applied" sentinel is likewise never
    /// awarded the drain slot: its apparent gap is the whole shard partition's
    /// WAL length rather than this leaf's own pending work, so it is not a
    /// comparable backlog. See the comment on the sentinel test below.
    /// </para>
    /// </summary>
    private async Task<List<(int Partition, long? ProbedHead)>> BuildPassOneSweepOrderAsync(
        string treeId,
        int partitionCount,
        long? checkpointOverride,
        CancellationToken cancellationToken)
    {
        var order = new List<(int Partition, long? ProbedHead)>(partitionCount);

        // A single-partition tree is drain-eligible throughout; there is
        // nothing to order and no reason to spend a probe.
        if (partitionCount <= 1)
        {
            order.Add((0, null));
            return order;
        }

        var heads = new long[partitionCount];
        var backlogs = new long[partitionCount];
        var probed = new bool[partitionCount];
        var comparable = new bool[partitionCount];
        List<int>? unprobed = null;
        Exception? firstProbeFault = null;

        for (var p = 0; p < partitionCount; p++)
        {
            try
            {
                var head = await grainFactory
                    .GetGrain<ILeafReplayCoordinatorGrain>($"{treeId}/{p}")
                    .GetHeadOffsetAsync(cancellationToken);

                var checkpoint = checkpointOverride ?? GetPersistedCheckpointForPartition(p);
                heads[p] = head;
                probed[p] = true;

                // A partition at the "nothing applied" sentinel (-1) has no
                // backlog COMPARABLE WITH one that holds a real checkpoint.
                // Its apparent gap is head + 1, which measures the whole shard
                // partition's WAL rather than this leaf's own pending work:
                // the materialiser's per-leaf range filter drops every entry
                // outside this leaf's key range on iteration, so the real cost
                // is bounded by the leaf's range, not by the head. Ranking a
                // MIXED set on that number awards the single drain-eligible
                // slot to the partition with the LEAST applied - and on legacy
                // state, where every non-zero partition reads -1, it would do
                // so systematically. LatticeFallOffLogDetector guards its own
                // budget trigger with the same `checkpointOffset >= 0` test
                // and for the same reason (the c2-vi split-sibling incident).
                //
                // This is about COMPARABILITY, not about the sentinel being
                // unrankable in itself - see the all-sentinel case below.
                comparable[p] = checkpoint >= 0;
                backlogs[p] = comparable[p] && head > checkpoint ? head - checkpoint : 0L;
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                // Fail soft, per partition. Aborting the whole sweep here would
                // be strictly worse than today: nothing has been banked yet, so
                // a transient probe fault would cost every partition's progress
                // rather than only its own. Swallowing it silently would be the
                // fault-masking shape issue #2082 closed on the trimmed-prefix
                // probe. So: degrade this partition's ordering only, keep its
                // natural position, and log it.
                (unprobed ??= []).Add(p);
                firstProbeFault ??= ex;
            }
        }

        if (unprobed is not null)
        {
            context.ActivationServices?
                .GetService<ILoggerFactory>()?
                .CreateLogger<BPlusLeafGrain>()?
                .LogWarning(
                    firstProbeFault,
                    "Replay sweep-order head probe failed for tree {Tree} partition(s) {Partitions} on leaf {GrainId}; those partitions keep their natural sweep position and will be re-probed during replay. Pass-1 drain eligibility may be awarded to a smaller backlog than intended.",
                    treeId,
                    string.Join(",", unprobed),
                    context.GrainId);
        }

        // ALL-SENTINEL CASE. When no partition holds a real checkpoint, every
        // partition shares the SAME baseline, so head + 1 is a valid relative
        // measure of the work each has to read and the comparability objection
        // above does not apply - it concerns mixing two different baselines,
        // not the sentinel itself. This is the dominant case in practice: the
        // cold-start cache-empty override (step 0.5 of OnActivateAsync) drives
        // checkpointOverride to -1 for every partition, which is precisely the
        // activation with the most to replay and therefore the one this
        // ordering exists to help. Excluding sentinel partitions wholesale
        // would collapse the sweep to index order there and make issue #2089's
        // ordering inert in the only case that matters - while still passing a
        // mixed-baseline test. (Legacy state, where partition 0 reads the
        // scalar slot and the rest read -1, is MIXED, so it keeps the strict
        // guard above and the sentinel partitions never take the drain slot.)
        var anyComparable = false;
        for (var p = 0; p < partitionCount; p++)
            anyComparable |= comparable[p];

        if (!anyComparable)
        {
            for (var p = 0; p < partitionCount; p++)
            {
                if (!probed[p])
                    continue;
                comparable[p] = true;
                backlogs[p] = heads[p];
            }
        }

        var indices = new int[partitionCount];
        for (var p = 0; p < partitionCount; p++)
            indices[p] = p;

        // Ascending by backlog, partition index as the tie-break so the order
        // is deterministic and an evenly spread backlog keeps index order.
        // A partition whose backlog is not KNOWN sorts strictly first - either
        // because its head probe faulted, or because it sits at the "nothing
        // applied" sentinel and has no comparable gap. Neither is ever awarded
        // the single drain-eligible slot on the strength of a backlog we do
        // not actually know.
        Array.Sort(indices, (a, b) =>
        {
            if (comparable[a] != comparable[b])
                return comparable[a] ? 1 : -1;

            var byBacklog = backlogs[a].CompareTo(backlogs[b]);
            return byBacklog != 0 ? byBacklog : a.CompareTo(b);
        });

        foreach (var p in indices)
            order.Add((p, probed[p] ? heads[p] : null));

        return order;
    }

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
    /// and range deletes are appended to <paramref name="deferredTerminals"/>
    /// instead of being applied inline - see the <see cref="DeferredTerminal"/>
    /// docstring for the saga atomicity rationale - unless
    /// <paramref name="drainDeferredInline"/> says every other partition has
    /// already been absorbed, in which case the mutation's cross-partition
    /// dependencies are all present and it is applied in place. Per-partition
    /// checkpoint advance beyond the safe contiguous prefix is deferred to
    /// pass 2: a partition that emitted a terminal would otherwise advance its
    /// checkpoint past the still-pending prepare offsets in the OTHER
    /// partitions' pending-tx clamp range (the per-partition clamp is scoped to
    /// the partition the prepare landed in, so the terminal's partition can
    /// advance unclamped) - but the terminal itself hasn't been applied yet, so
    /// the visible-state contract requires us to wait.
    /// </para>
    /// </summary>
    private async Task<(bool Advanced, long MaxApplied)> ReplayPartitionAsync(
        string treeId,
        int partition,
        long checkpoint,
        ILeafProjection projection,
        List<DeferredTerminal> deferredTerminals,
        DeferredOffsetLedger deferredOffsets,
        bool drainDeferredInline,
        ShardMap? replayShardMap,
        int maxRecordsPerTurn,
        int maxDurableUnresolvedWork,
        long? probedHead,
        int maxLeafReplayEntries,
        bool overBudgetCandidate,
        CancellationToken cancellationToken)
    {
        var coordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
            $"{treeId}/{partition}");

        // Reuse the head the sweep-order pre-pass already probed when it has
        // one, so ordering the sweep costs no extra grain call. A head probed
        // moments ago can only be behind the true head, which simply leaves
        // the newest entries for the materialiser or the next replay.
        var head = probedHead ?? await coordinator.GetHeadOffsetAsync(cancellationToken);
        if (head <= checkpoint)
            return (false, checkpoint);

        // The partition-wide extent this replay scans. It is NOT this leaf's
        // work (issue #2149) - every sibling leaf pinned to this WAL partition
        // contributes to it, ~1,350 of them on the measured deployment - but it
        // IS a sound upper bound on it, and it is the quantity the detector
        // compared against MaxLeafReplayEntries. Both warnings below report it
        // alongside the quantity actually compared so the two can never again
        // be conflated from the log alone.
        var gap = head - checkpoint;

        // STALL (FAULT) CHECK - issue #2149, fault shape of issue #2165.
        //
        // Run BEFORE the scan, so a replay torn down by the activation deadline
        // - which is precisely the fault being detected - still reports it.
        //
        // Gated on the detector's over-budget CANDIDATE. That gate is what
        // makes this line a strict SUBSET of where the old unconditional cost
        // warning fired: this fix can silence noise but can never move the
        // signal to somewhere an operator was not already looking. It is also
        // sound on its own terms - a frozen checkpoint whose partition gap fits
        // inside the budget is an idle leaf, not a livelock - and the #2165
        // leaf's gap was >= 30,639 against a 10,000 budget, three times over.
        //
        // The criterion is the one the old warning stated in prose and left an
        // operator to evaluate by hand: the checkpoint does not advance across
        // repeats for the SAME leaf and partition. n >= 2 by construction, so
        // a single cold activation never trips it.
        if (NoteReplayCheckpointObservation(treeId, ReplicaId, partition, checkpoint) && overBudgetCandidate)
        {
            var stalledLogger = ResolveLogger();
            if (stalledLogger is not null
                && stalledLogger.IsEnabled(LogLevel.Warning)
                && ShouldLogStalledReplay(treeId, ReplicaId, partition))
            {
                stalledLogger.LogWarning(
                    "Leaf projection for tree '{TreeId}' leaf '{Leaf}' WAL partition {Partition} re-entered "
                    + "replay WITHOUT its persisted checkpoint having advanced (persistedCheckpoint "
                    + "{Checkpoint}, unchanged since this leaf partition's previous replay on this silo; WAL "
                    + "partition head {Head}, partition gap {Gap} entries, MaxLeafReplayEntries {Budget}). "
                    + "This is a FAULT, not a slow replay: the previous activation banked no durable forward "
                    + "progress at all, so this leaf is not converging and writes routed to it are being lost "
                    + "for as long as it repeats. Note the gap is the whole PARTITION's extent, shared with "
                    + "every sibling leaf pinned to it, so it is an upper bound on this leaf's work and not a "
                    + "measurement of it.",
                    treeId,
                    ReplicaId,
                    partition,
                    checkpoint,
                    head,
                    gap,
                    maxLeafReplayEntries);
            }
        }

        var fromExclusive = checkpoint;
        long maxApplied = checkpoint;

        // Exact, POST-filter count of the entries THIS leaf takes through the
        // projection rebuild seam - the unit MaxLeafReplayEntries is actually
        // documented in (issue #2149). Deferred terminals count: this same leaf
        // applies them in pass 2, so they are its work too.
        //
        // Counting here costs one increment per applied entry inside a scan
        // that is happening anyway. Establishing the same figure in the
        // detector, as a pre-check, would instead mean reading (checkpoint,
        // head] before the replay and then reading it again to perform the
        // replay - doubling the most expensive part of activation, and doubling
        // exactly the read whose ~30 s overrun IS the livelock of issue #2165.
        // That is why the verdict lives here and not there.
        long appliedEntries = 0;
        var overBudgetWarned = false;

        // Cooperative-yield budget (issue #1030): a long-tailed WAL would let
        // this replay monopolise its activation turn and block the silo
        // scheduler from interleaving other ready work (foreground reads,
        // health probes). Counting processed records and yielding every
        // maxRecordsPerTurn keeps a large replay cooperative. A non-positive
        // budget disables the yield (replay runs to completion uninterrupted).
        var recordsSinceYield = 0;

        // Resumable replay (issue #1513): historically the persisted
        // checkpoint was only advanced by the post-pass-2 reconciliation in
        // ReplayWalSinceCheckpointAsync, so a replay that could not finish
        // within its activation window (a large un-snapshotted WAL prefix
        // relative to the ~30 s RuntimeRequested budget) made no durable
        // progress: the deactivation discarded every applied entry and the
        // next activation restarted from the same offset, so the leaf never
        // converged and the coverage-gated WAL GC could never trim the
        // prefix. Flush the checkpoint incrementally over the strictly
        // contiguous, fully-applied prefix instead, at each slice boundary.
        // A deactivation then loses at most one flush interval, and the
        // checkpoint persist drives the existing periodic snapshot capture
        // (MaybeRunPeriodicSnapshotRecheckAsync) so the next activation can
        // rehydrate from a snapshot and resume from the last durable offset
        // rather than replaying from zero.
        //
        // The advance is bounded so it can NEVER pass an offset that is not
        // yet durably applied (the data-loss class #1492 guards), via two
        // clamps that together hold the checkpoint at the highest offset
        // below which every entry - inline, deferred, and cross-partition
        // saga - is applied:
        //   (a) below the lowest still-UNRESOLVED deferred terminal /
        //       DeleteRange offset in this partition, since those mutations
        //       are applied only when they drain (see the DeferredTerminal
        //       docstring); and
        //   (b) below any unresolved saga prepare in this partition
        //       (MinUnresolvedPrepareOffsetForPartition), because the
        //       matching terminal - even one routed to this same partition -
        //       may itself be deferred, so _pendingTx must be reconstructed
        //       by a resumed replay that re-reads the prepare.
        // TryFlushRecoveredCeilingAsync owns both clamps.
        //
        // Clamp (a) reads a RESOLVABLE ledger rather than a monotonically
        // non-increasing scalar (issue #1831). The old scalar was only ever
        // lowered, so the first deferred mutation a partition emitted pinned
        // its ceiling for the whole remainder of the replay and left every
        // further advance to the post-pass-2 reconciliation - which only runs
        // when the entire replay fits inside the activation window. A backlog
        // large enough to outrun that window therefore banked nothing, the
        // activation was torn down, and the next one replayed the identical
        // range: the #1513 livelock, reopened for any tree that uses range
        // deletes or atomic multi-key writes. With the ledger the ceiling
        // recovers the moment a deferred offset drains, here or in pass 2.

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
                    state.State.HighKeyExclusive,
                    replayShardMap))
                {
                    // This entry is this leaf's own work: it either goes
                    // through ILeafProjection.Apply now, or is deferred to
                    // pass 2 where THIS leaf applies it. Either way it counts
                    // against MaxLeafReplayEntries, which is defined in
                    // exactly those terms.
                    appliedEntries++;

                    // OVER-BUDGET VERDICT (issue #2149). Emitted at the moment
                    // the exact per-leaf count first crosses the budget, and
                    // mid-scan rather than after the replay, so a teardown
                    // later in this activation cannot swallow it.
                    //
                    // This replaces the pre-check warning that compared the
                    // partition-wide gap against this per-leaf budget. At the
                    // measured fan-out of ~1,350 leaves per partition that
                    // comparison fired 19,639 times in 6.26 hours for leaves
                    // whose real work was order 10^2 against a 10^4 budget -
                    // one to two orders of magnitude BELOW budget. The count
                    // below cannot make that error: it is the same quantity
                    // the option documents.
                    //
                    // It also subsumes the detector's -1 sentinel exemption
                    // without needing one. A brand-new leaf is exempted there
                    // because head - (-1) charges it for every sibling's WAL
                    // contribution; here it is charged only for entries in its
                    // own range, so if it really does apply more than the
                    // budget, that is a true positive worth reporting.
                    if (!overBudgetWarned && maxLeafReplayEntries > 0 && appliedEntries > maxLeafReplayEntries)
                    {
                        overBudgetWarned = true;

                        // Tagged with the WAL partition as well as the tree
                        // (issue #2023). Partition is bounded by
                        // LatticeOptions.WalPartitions, so it is safe
                        // cardinality, and without it the counter cannot be
                        // split by the same axis the warning reports - leaving
                        // an operator who sees a rate spike with no way to tell
                        // whether one partition is hot or the whole tree is.
                        // The leaf identity is deliberately NOT a tag: leaf
                        // count is unbounded, so per-leaf detail belongs in the
                        // log line below, not in a time series.
                        LatticeMetrics.LeafActivationOverBudgetReplays.Add(
                            1,
                            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId),
                            new KeyValuePair<string, object?>(LatticeMetrics.TagPartition, partition),
                            LatticeTenantLabel.ForTree(treeId));

                        // Gate on IsEnabled: the templated call would otherwise
                        // allocate a params object[] and box every argument on
                        // every over-budget activation even when warnings are
                        // filtered out.
                        //
                        // Also THROTTLE per (tree, leaf, partition). A cold
                        // start on a large volume re-activates these leaves
                        // continuously, and one warning per attempt buried a
                        // real deployment in 5,752 identical lines in fifteen
                        // minutes - enough to make the log useless for spotting
                        // the faults mixed in among them. The counter above
                        // already records every occurrence, so the log's job is
                        // only to say the condition is happening and let an
                        // operator find the checkpoint; the rate is a metric
                        // concern, not a logging one.
                        //
                        // The leaf id is load-bearing in both the key and the
                        // message (issue #2023). `partition` is the WAL
                        // partition ordinal, iterated [0, WalPartitions) inside
                        // EVERY leaf's activation - it does not identify a leaf.
                        // Keyed on (tree, partition) alone, the first leaf to
                        // trip the budget suppressed the warning for every other
                        // leaf in that tree and partition for a full minute, so
                        // consecutive lines were one-per-minute samples from
                        // arbitrary DIFFERENT leaves. Their checkpoints are not
                        // comparable, which made the "checkpoint that does not
                        // advance" criterion unevaluable and produced a false
                        // livelock report. That criterion is now evaluated in
                        // process by the stall check above; the qualification
                        // stays because it is also what makes successive lines
                        // for one leaf comparable to a human reader.
                        var overBudgetLogger = ResolveLogger();
                        if (overBudgetLogger is not null
                            && overBudgetLogger.IsEnabled(LogLevel.Warning)
                            && ShouldLogOverBudgetReplay(treeId, ReplicaId, partition))
                        {
                            overBudgetLogger.LogWarning(
                                "Leaf projection for tree '{TreeId}' leaf '{Leaf}' WAL partition {Partition} is "
                                + "replaying beyond the configured budget: it has taken {AppliedEntries} entries "
                                + "through its projection rebuild seam, past MaxLeafReplayEntries {Budget} "
                                + "(persistedCheckpoint {Checkpoint}, WAL partition head {Head}, partition gap "
                                + "{Gap} entries). AppliedEntries is this leaf's OWN post-range-filter work and "
                                + "is the quantity compared against the budget; Gap is the whole partition's "
                                + "extent, shared with every sibling leaf pinned to it, and is only an upper "
                                + "bound on it. The write-ahead log still covers the whole needed window, and "
                                + "the replay flushes its checkpoint incrementally, so an activation torn down "
                                + "early USUALLY banks durable forward progress. That is not unconditional: an "
                                + "unresolved saga prepare clamps the incremental flush ceiling strictly below "
                                + "its own offset, and while one is resident the activation banks nothing at "
                                + "all (issue #2183). Activation may take longer than usual. A leaf whose "
                                + "checkpoint does not advance at all is "
                                + "reported separately as a fault, not by this line.",
                                treeId,
                                ReplicaId,
                                partition,
                                appliedEntries,
                                maxLeafReplayEntries,
                                checkpoint,
                                head,
                                gap);
                        }
                    }

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
                    //
                    // Both rationales are about entries in OTHER partitions
                    // that pass 1 has not absorbed yet, so both evaporate once
                    // every other partition is absorbed - which is exactly what
                    // drainDeferredInline reports. Applying in place then costs
                    // nothing in safety and keeps the flush ceiling moving for
                    // the rest of the scan (issue #1831).
                    if (entry.Mutation.Kind is MutationKind.TxCommit or MutationKind.TxAbort
                        or MutationKind.DeleteRange
                        && !drainDeferredInline)
                    {
                        deferredTerminals.Add(new DeferredTerminal(partition, entry.Offset, entry.Mutation));

                        // Issue #2165. Recording the deferred mutation durably
                        // is what lets the ceiling advance past it: a resumed
                        // activation reconstructs it from state instead of
                        // re-reading it, so this partition banks progress even
                        // though it is not the one pass 1 absorbs last. Only
                        // when the ledger is full does the offset go back on
                        // the in-memory clamp, which is the pre-#2165
                        // behaviour.
                        if (!TryRecordUnresolvedReplayWork(
                                partition, entry.Offset, entry.Mutation, maxDurableUnresolvedWork))
                        {
                            deferredOffsets.Add(partition, entry.Offset);
                        }
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

                        // An applied-but-unresolved saga prepare pins the
                        // ceiling for the same reason a deferred terminal does:
                        // it lives only in the activation-scoped _pendingTx
                        // bucket, which no snapshot captures. Recording it
                        // durably lifts that clamp too - see
                        // MinUnresolvedPrepareOffsetForPartition, which skips
                        // every recorded offset.
                        if (entry.Mutation.IsPrepared)
                        {
                            TryRecordUnresolvedReplayWork(
                                partition, entry.Offset, entry.Mutation, maxDurableUnresolvedWork);
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

                if (maxRecordsPerTurn > 0 && ++recordsSinceYield >= maxRecordsPerTurn)
                {
                    recordsSinceYield = 0;
                    await Task.Yield();
                }
            }

            var lastOffset = slice[^1].Offset;
            if (lastOffset <= fromExclusive)
                break;
            fromExclusive = lastOffset;

            // Incremental checkpoint flush over the safe contiguous prefix
            // (issue #1513), clamped by TryFlushRecoveredCeilingAsync so it can
            // never license a checkpoint (or the materialiser pin) past a
            // not-yet-durably-applied offset.
            await TryFlushRecoveredCeilingAsync(
                partition,
                maxApplied,
                deferredOffsets,
                projection,
                cancellationToken);
        }

        // Pass 1 flushes the checkpoint incrementally over the strictly
        // contiguous, fully-applied prefix (the TryFlushRecoveredCeilingAsync
        // clamps above), so partial replay progress is durable across a
        // mid-replay teardown (issue #1513). What it deliberately does NOT do
        // here is advance the checkpoint to the FULL maxApplied: a partition
        // may still hold an undrained deferred terminal or an unresolved
        // prepare below maxApplied, so the remaining advance up to maxApplied
        // is left to the post-pass-2 reconciliation step in
        // ReplayWalSinceCheckpointAsync, which waits until every deferred
        // terminal has applied (lifting the pending-tx clamps) before advancing
        // each partition's persisted checkpoint to its maxApplied. The
        // incremental flush is bounded strictly below those pending offsets by
        // construction, so it never over-advances: (a) a partition that
        // observed a prepare in pass 1 is held behind the prepare's offset, and
        // (b) a partition that deferred a terminal is held behind that
        // terminal's offset until it drains. Returning the per-partition
        // maxApplied here gives the caller the data it needs to do the
        // post-pass-2 advance with full knowledge of every partition's outcome.
        return (Advanced: maxApplied > checkpoint, MaxApplied: maxApplied);
    }

    /// <summary>
    /// Best-effort resolution of the leaf's current slot-ownership map for the
    /// activation-time replay filter. Returns the routing map published for
    /// <paramref name="treeId"/> when (a) this leaf carries a non-null
    /// <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.ShardIndex"/> and (b) that shard index is
    /// actually referenced by the map's physical shard set. In every other
    /// case - a system tree, a legacy slot-less leaf, a registry lookup that
    /// returns no map or throws, or a map drawn from a foreign physical shard
    /// space - this returns <see langword="null"/> so
    /// <see cref="ShouldApplyDuringReplay"/> falls back to the legacy
    /// stamped-<see cref="LatticeMutation.ShardIndex"/> axis. The guard is what
    /// makes the map-based ownership resolution safe to enable unconditionally:
    /// a transient registry failure or a mismatched map can never cause a leaf
    /// to reject its own writes.
    /// <para>
    /// System trees (IDs starting with
    /// <see cref="LatticeConstants.SystemTreePrefix"/>) skip the registry lookup
    /// entirely. The registry is itself backed by the
    /// <see cref="LatticeConstants.RegistryTreeId"/> system tree, so a registry
    /// leaf that called back into the (non-reentrant, singleton) registry grain
    /// during its own activation - which happens inside the registry's own
    /// write turn - would deadlock. System trees never undergo the adaptive
    /// shard split that this slot-ownership resolution guards against, so the
    /// legacy stamp axis is always correct for them. This mirrors the
    /// system-tree guard every other leaf-to-registry call site uses.
    /// </para>
    /// </summary>
    private async Task<ShardMap?> ResolveReplayShardMapAsync(string treeId)
    {
        if (state.State.ShardIndex is not int leafShardIndex)
            return null;

        // The registry is backed by a system tree; a system-tree leaf must
        // never call the registry during activation or it deadlocks the
        // singleton registry grain inside its own write turn.
        if (treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
            return null;

        try
        {
            var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            var map = await registry.GetShardMapAsync(treeId);
            if (map is not null && map.GetPhysicalShardIndices().Contains(leafShardIndex))
                return map;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Best-effort: a registry hiccup must never block leaf recovery.
            // Fall back to the stamp-based filter (pre-#909 behaviour).
        }

        return null;
    }

    /// <summary>
    /// Per-WAL-entry filter for the activation-time materialiser.
    /// Decides whether a given WAL entry should be replayed against
    /// this leaf's projection, keyed on the leaf's slot ownership and on
    /// the leaf's persisted [<see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.LowKeyInclusive"/>,
    /// <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.HighKeyExclusive"/>) ownership
    /// range.
    /// <para>
    ///     <see cref="MutationKind.Set"/> /
    ///     <see cref="MutationKind.Delete"/> are applied iff the entry's
    ///     <see cref="LatticeMutation.Key"/> is owned by this leaf's
    ///     shard <em>and</em> the key falls in the leaf's persisted
    ///     ownership range. Shard ownership is resolved positively by
    ///     the key's virtual slot under
    ///     <paramref name="currentShardMap"/> when one is available
    ///     (<c>currentShardMap.Resolve(key) == leafShardIndex</c>);
    ///     otherwise it falls back to the stamped
    ///     <see cref="LatticeMutation.ShardIndex"/>. Resolving by slot
    ///     rather than by the stamp is what keeps a shadow-forwarded
    ///     record (a post-split write routed to the donor for an
    ///     already-moved slot, forwarded into the target's WAL with the
    ///     donor's stamp) applied on the target leaf that now owns the
    ///     slot, while still dropping genuine sibling-shard data that a
    ///     shared WAL partition multiplexes through (its slot resolves
    ///     to another shard) and donor orphans on the donor leaf (their
    ///     slot has moved away). The range check is open on either side
    ///     - a <see langword="null"/> bound means "no constraint on
    ///     that side", used for the chain's leftmost and rightmost
    ///     leaves and for legacy state shapes that pre-date the slot.
    ///     Keying on key-range (not on authoring leaf grain id) is
    ///     essential for the rebuild-from-WAL scenario: a leaf born from
    ///     a split has no Entries until replay populates them, and the
    ///     entries that belong to it were authored by the donor sibling
    ///     pre-split. Pre-Option A leaves whose
    ///     <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.ShardIndex"/> slot is null
    ///     apply unconditionally on the shard axis; leaves with both
    ///     range bounds null apply unconditionally on the range axis -
    ///     both axes preserve the legacy V1 single-leaf-per-shard
    ///     semantics so a legacy-shaped state must not start dropping
    ///     its own writes after a binary upgrade.
    /// </para>
    /// <para>
    ///     <see cref="MutationKind.Tombstone"/> reap envelopes
    ///     authored by <c>CompactTombstonesAsync</c> are gated by the
    ///     same shard-and-range filter as <see cref="MutationKind.Set"/>
    ///     / <see cref="MutationKind.Delete"/>: a sibling leaf's reap
    ///     must not unintentionally remove keys from this leaf's
    ///     projection. Reap envelopes that pass the filter route into
    ///     <c>ApplyTombstoneReap</c> which physically removes the
    ///     stamped key iff the existing entry is still a tombstone or
    ///     an expired live entry.
    /// </para>
    /// <para>
    ///     <see cref="MutationKind.DeleteRange"/> is applied
    ///     unconditionally. <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/>'s replay
    ///     handler iterates this leaf's own entries only, so the call
    ///     is naturally a no-op on leaves that own no keys in the
    ///     range.
    /// </para>
    /// <para>
    ///     <see cref="MutationKind.TxCommit"/> /
    ///     <see cref="MutationKind.TxAbort"/> are applied
    ///     unconditionally. The terminal's shard scope is enforced by
    ///     the writer-side partition routing, and the per-leaf
    ///     <c>_recentlyTerminal</c> dedup makes a terminal whose
    ///     pending bucket is empty a trivial no-op.
    /// </para>
    /// <para>
    ///     Unknown <see cref="MutationKind"/> values are dropped -
    ///     defensive forward-compat against future kinds whose replay
    ///     semantics the materialiser has not been taught.
    /// </para>
    /// </summary>
    internal static bool ShouldApplyDuringReplay(
        in LatticeMutation mutation,
        int? leafShardIndex,
        string? lowKeyInclusive,
        string? highKeyExclusive,
        ShardMap? currentShardMap) => mutation.Kind switch
    {
        MutationKind.Set or MutationKind.Delete or MutationKind.Tombstone =>
            IsShardOwnedDuringReplay(mutation, leafShardIndex, currentShardMap)
            && SplitBoundary.Owns(mutation.Key, lowKeyInclusive, highKeyExclusive),
        MutationKind.DeleteRange => true,
        MutationKind.TxCommit => true,
        MutationKind.TxAbort => true,
        _ => false,
    };

    /// <summary>
    /// Shard-axis half of <see cref="ShouldApplyDuringReplay"/>. A
    /// slot-less (legacy) leaf owns every shard-axis entry. Otherwise the
    /// entry is owned iff the current routing map resolves its key's slot to
    /// this leaf's shard; when no map is available the legacy stamped
    /// <see cref="LatticeMutation.ShardIndex"/> is used instead.
    /// </summary>
    private static bool IsShardOwnedDuringReplay(
        in LatticeMutation mutation,
        int? leafShardIndex,
        ShardMap? currentShardMap)
    {
        if (leafShardIndex is not int shard)
            return true;

        if (currentShardMap is not null)
            return currentShardMap.Resolve(mutation.Key) == shard;

        return mutation.ShardIndex == shard;
    }
}
