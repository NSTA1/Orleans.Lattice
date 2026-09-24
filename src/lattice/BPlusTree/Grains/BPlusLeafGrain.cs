using System.Buffers;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Leaf node grain implementation. Stores key -> <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/> entries
/// in a sorted dictionary. Splits when the entry count exceeds the leaf-sizing
/// pin in the tree registry.
/// </summary>
// CS9113: 'originClusterIdResolver' is referenced only inside #if LATTICE_DIAG
// blocks (used by DiagSiloTag to disambiguate Site A vs Site B emissions in the
// shared file-based DiagSink log). Suppressed at the parameter list because in
// non-diag builds the parameter is genuinely unread, but removing it would break
// the activation-DI signature and the diag build's site-tagging behaviour.
#pragma warning disable CS9113
internal sealed partial class BPlusLeafGrain(
    IGrainContext context,
    [PersistentState("leaf", LatticeOptions.StorageProviderName)] IPersistentState<LeafNodeState> state,
    IGrainFactory grainFactory,
    LatticeOptionsResolver optionsResolver,
    MutationObserverDispatcher mutationObservers,
    ILatticeOriginClusterIdResolver originClusterIdResolver) : IBPlusLeafGrain, ILeafProjection, IGrainBase
#pragma warning restore CS9113
{
    IGrainContext IGrainBase.GrainContext => context;

#if LATTICE_DIAG
    /// <summary>
    /// Cached cluster id of the silo hosting this leaf activation, used to
    /// disambiguate Site A vs Site B emissions in the shared file-based
    /// <see cref="DiagSink"/> log. Resolved lazily because <c>state.State.TreeId</c>
    /// is null until activation completes; the resolver is keyed by tree id
    /// only because the replication package's per-tree options map may carry
    /// distinct cluster ids per tree (the common case is a single host-wide id).
    /// </summary>
    private string? _diagSiloTag;

    private string DiagSiloTag => _diagSiloTag
        ??= (originClusterIdResolver.Resolve(state.State.TreeId ?? string.Empty) is { Length: > 0 } id ? id : "(local)");
#endif

    /// <summary>
    /// Synchronously flushes any pending projection-checkpoint advance
    /// to durable storage on graceful deactivation so a clean shutdown
    /// does not lose an unflushed checkpoint that the materialiser has
    /// already issued. Crash deactivations bypass this hook by design -
    /// the persisted offset bounds replay cost in that case.
    /// <para>
    /// Also records the once-per-deactivation checkpoint observation
    /// (<see cref="LatticeMetrics.LeafDeactivationCheckpointDelta"/>, issue
    /// #2280). Because crash teardowns bypass this hook, and because Orleans
    /// does not run it at all when <c>OnActivateAsync</c> throws, that
    /// observation is a LOWER BOUND and is documented as one on the
    /// instrument itself.
    /// </para>
    /// </summary>
    async Task IGrainBase.OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        // Sampled BEFORE any flush below so the pair brackets exactly the work
        // this hook performs (issue #2280). Read from the per-partition state
        // array rather than resolved options so the observation adds no await
        // and no new failure mode to the teardown path.
        var checkpointAtEntry = SumPersistedCheckpointsAcrossPartitions();

        // Stop the coverage-lag bound before any teardown work. The
        // deactivation capture below supersedes it for this activation, and a
        // tick landing mid-teardown would race the final capture for the
        // in-flight latch. Disposal is unconditional and needs no null check.
        var coverageLagTimer = System.Threading.Interlocked.Exchange(ref _coverageLagTimer, null);
        coverageLagTimer?.Dispose();

        // Each barrier below is contained INDEPENDENTLY (issue #3366). They
        // previously shared a single try with a single anonymous bare catch
        // that had no logger, ordered most-fragile-first, so a fault in an
        // early barrier silently cancelled every later one: a checkpoint-flush
        // failure also skipped the snapshot capture AND the durable frontier
        // pin, and the whole teardown emitted nothing whatsoever. That made
        // three materially different failures - an early barrier throwing, the
        // capture's uninstrumented early return, and this hook never running -
        // render as byte-identical silence, while each demands a different
        // remedy. Per-barrier containment preserves the original ordering and
        // the original "a storage failure on shutdown must never block
        // deactivation" guarantee, and additionally makes each fault
        // attributable rather than merely survivable.
        //
        // The metric is deliberately NOT zero-primed: a tree that never faults
        // exports no series, so a reader must not read absence as a proven
        // zero. What it buys is the converse - a NON-zero reading names the
        // barrier, which is what no signal previously did.
        async Task RunBarrierAsync(
            KeyValuePair<string, object?> barrier,
            Func<CancellationToken, Task> barrierAction)
        {
            try
            {
                await barrierAction(cancellationToken);
            }
            catch (Exception ex)
            {
                try
                {
                    // Tags are resolved defensively and INDIVIDUALLY, before
                    // the Add. Both helpers read grain state, which throws
                    // "Attempt to access an invalid activation" once the
                    // activation has been invalidated - and a deactivation hook
                    // is precisely where that is reachable. Resolving them as
                    // arguments to Add meant a throwing tag lookup suppressed
                    // the measurement entirely via the outer guard below, so
                    // the barrier failure this catch exists to report was
                    // itself reported as nothing. Telemetry must never decide
                    // whether a fault is observable (the #2312 rule, applied to
                    // this path).
                    var treeTag = TryResolveDeactivationBarrierTag(LeafTreeTag, LatticeMetrics.TagTree);
                    var tenantTag = TryResolveDeactivationBarrierTag(
                        LeafTenantTag, LatticeTenantLabel.ForTree(null).Key);

                    LatticeMetrics.LeafDeactivationBarrierFailures.Add(
                        1,
                        treeTag,
                        barrier,
                        tenantTag);

                    ResolveLogger()?.LogWarning(
                        ex,
                        "Graceful-deactivation barrier '{Barrier}' faulted for leaf '{LeafId}' of tree "
                        + "'{TreeId}'. The barriers ordered after it STILL RUN (issue #3366); before that "
                        + "fix a fault here silently cancelled every remaining durability barrier and "
                        + "emitted no signal of any kind.",
                        barrier.Value,
                        context.GrainId.ToString(),
                        treeTag.Value);
                }
                catch (Exception)
                {
                    // Observability must never fail a deactivation.
                }
            }
        }

        try
        {
            // Durability work runs FIRST (issue #3393). The upward digest
            // publish used to lead this hook, and it is the slow step: in the
            // recorded drain it consumed the deactivation deadline, so by the
            // time the durable barriers below ran the activation had already
            // been torn down and every one of them faulted with "Attempt to
            // access an invalid activation" - the final checkpoint's pin never
            // landed. The order is now: persist the checkpoint and publish its
            // pin, capture the snapshot, republish the pin, and only then
            // publish the digest, which is staleness-tolerant by design.
            //
            // The final persist's own pin is published from INSIDE this barrier,
            // as the first step of the teardown persist's tail (see
            // FlushPendingCheckpointOnDeactivateAsync), so it no longer depends
            // on the trailing frontier-pin barrier surviving to run.
            await RunBarrierAsync(
                LatticeMetrics.DeactivationBarrierCheckpointFlush,
                async ct => await FlushPendingCheckpointOnDeactivateAsync(ct));

            // Liveness barrier (issue #1537): before the durable pin flush,
            // capture a snapshot for any checkpointed-but-uncovered partition
            // so a short-lived bursty activation that never reached the
            // periodic snapshot cadence still leaves durable coverage behind.
            // Without this the leaf's Zero block pin (the safe side of #1535's
            // coverage gate) would retain the shared WAL forever. Ordered
            // before the frontier-pin barrier so the pin resolves to the
            // now-covered frontier and the WAL GC can trim the prefix;
            // best-effort, so a capture failure simply leaves the block pin in
            // place (retained, never trimmed ahead of coverage). Skips, rather
            // than faults, once the deadline has torn the activation down.
            await RunBarrierAsync(
                LatticeMetrics.DeactivationBarrierSnapshotCapture,
                async ct => await TryCaptureSnapshotOnDeactivateAsync(ct));

            // Retention barrier: AWAIT a durable write of this leaf's
            // coverage-gated checkpoint frontier into the cluster-wide pin
            // store so a leaf can never go dormant on a graceful shutdown and
            // then have the shared WAL trimmed past its durable checkpoint
            // across a restart (the "fall off the log" wedge). It runs even when
            // the teardown persist's tail already published (issue #3393):
            // the capture above can raise coverage, and so the pin, after that
            // publish, and the pin store's monotonic-max merge makes the repeat
            // idempotent. It is also the only publisher for a deactivation with
            // no pending advance, where nothing is persisted and no tail runs.
            // Skips, rather than faults, once the deadline has torn the
            // activation down; the tail's publish already stands by then.
            await RunBarrierAsync(
                LatticeMetrics.DeactivationBarrierFrontierPin,
                async ct => await FlushDurableMaterialiserFrontierOnDeactivateAsync(ct));

            if (_deactivationInlineDigestDeferred)
            {
                // The teardown persist's inline upward digest publish, deferred
                // behind the durability barriers above (issue #3393). Attributed
                // to the checkpoint-flush tail exactly as it was when it ran
                // inside the tail, so LeafCheckpointFlushTailFailures keeps its
                // meaning. It supersedes the coalesced drain below: both publish
                // the same dirty digest, and a slow parent must not be hit twice
                // inside one deadline.
                await PublishDeferredDeactivationDigestAsync(cancellationToken);
            }
            else if (_digestCoalescingWindowMs > 0)
            {
                // c2-xxviii: drain any pending coalesced digest publish so a
                // graceful shutdown does not leave the parent's digest table
                // observing a stale snapshot. Crash deactivations bypass this
                // hook by design; the digest is staleness-tolerant and the next
                // mutation on reactivation will republish. Gated on the
                // coalescing window being active because the synchronous-publish
                // path (window=0) already publishes inline on every mutation -
                // running the drain in that case can re-publish a post-publish
                // state that races with materialiser-driven projection rebuilds
                // and changes the parent's observed hash. Note the DEFAULT window
                // is LatticeOptions.DefaultDigestCoalescingWindowMs (5), not 0,
                // so this arm is enabled unless a host opts out. Ordered LAST
                // (issue #3393): it is the slow, staleness-tolerant step.
                await RunBarrierAsync(
                    LatticeMetrics.DeactivationBarrierDigestPublish,
                    async ct => await FlushPendingDigestPublishAsync(ct));
            }
        }
        finally
        {
            // Once per deactivation, before the teardown bookkeeping below, so
            // a throw from either of those cannot suppress the observation.
            RecordDeactivationCheckpointDelta(reason, checkpointAtEntry);

            DisposeProjectionHasher();

            // Remove this activation's same-silo revision cookie so the
            // registry stays bounded by the live-leaf set rather than
            // the lifetime-leaf set, and carry its final counter value
            // into the ticket source so the next activation seeds
            // strictly above it. While the entry is absent, a same-silo
            // LeafCacheGrain still holding _lastSeenPrimaryRevision from
            // this activation falls back to its TTL gate - NOT, as an
            // earlier version of this comment claimed, to the
            // cross-grain refresh path, which the cache reaches only
            // when an entry is present. The next activation republishes
            // a cookie from a strictly higher range during
            // OnActivateAsync (issue #2151), so the cache is forced onto
            // the refresh path as soon as the leaf is back.
            RemoveLeafRevision(context.GrainId);

            // Return this activation's bytes to the per-silo resident working
            // set (issue #2767). In the finally, beside the other teardown
            // bookkeeping, so a storage failure in the try above cannot leak a
            // registration: a leaked registration is permanent, consumes budget
            // no live leaf is using, and drives the silo to shed leaves that are
            // actually in use.
            ReleaseResidentFootprint();

            // Cancel any replay still in flight behind the gate (issue #2871).
            // The replay now outlives the activation hook, so nothing else would
            // stop it: without this it would carry on hydrating the cache of an
            // activation that is being torn down, holding a per-silo replay
            // permit the live leaves are queued for. In the finally with the rest
            // of the teardown bookkeeping, for the same reason as the footprint
            // release above - a storage failure in the try must not leak it.
            CancelReplayBarrier();
        }
    }

    /// <summary>
    /// Set by the teardown persist's tail
    /// (<see cref="CompleteDeactivationCheckpointFlushTailAsync"/>) when it
    /// defers its inline upward digest publish behind the durability barriers,
    /// and consumed by <c>OnDeactivateAsync</c>, which then publishes it last
    /// (issue #3393). Only ever set on the deactivation path.
    /// </summary>
    private bool _deactivationInlineDigestDeferred;

    /// <summary>
    /// Records that a graceful-deactivation barrier SKIPPED because the
    /// activation had already been torn down by the deactivation deadline
    /// (issue #3393), and never throws.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The skip is counted on
    /// <see cref="LatticeMetrics.LeafDeactivationBarrierFailures"/> under the
    /// barrier's EXISTING <c>reason</c> value, exactly as a fault of that
    /// barrier is counted, so the instrument's tag set and value set are
    /// unchanged: <c>reason</c> already names the barrier, and a barrier that
    /// could not do its work did not do its work whether it threw or declined.
    /// What differs is the log line, which says the activation was already torn
    /// down rather than reporting an unexplained fault.
    /// </para>
    /// <para>
    /// Tags are resolved defensively and individually, for the reason the
    /// barrier fault path gives: the very condition being reported makes a
    /// grain-state read throw "Attempt to access an invalid activation".
    /// </para>
    /// </remarks>
    /// <param name="barrier">The barrier's existing reason tag.</param>
    /// <param name="fault">
    /// The invalid-activation fault the barrier caught, or <see langword="null"/>
    /// when it skipped on the cancelled token before reading any state.
    /// </param>
    private void RecordDeactivationBarrierSkip(KeyValuePair<string, object?> barrier, Exception? fault)
    {
        try
        {
            var treeTag = TryResolveDeactivationBarrierTag(LeafTreeTag, LatticeMetrics.TagTree);
            var tenantTag = TryResolveDeactivationBarrierTag(
                LeafTenantTag, LatticeTenantLabel.ForTree(null).Key);

            LatticeMetrics.LeafDeactivationBarrierFailures.Add(1, treeTag, barrier, tenantTag);

            ResolveLogger()?.LogWarning(
                fault,
                "Graceful-deactivation barrier '{Barrier}' skipped for leaf '{LeafId}' of tree '{TreeId}': "
                + "the deactivation deadline had already expired and the activation was already torn down. "
                + "The pin published by this activation's last persist stands (the teardown persist publishes "
                + "its own pin first, issue #3393); a pin that lags only retains more WAL.",
                barrier.Value,
                context.GrainId.ToString(),
                treeTag.Value);
        }
        catch (Exception)
        {
            // Observability must never fail a deactivation.
        }
    }

    /// <summary>
    /// Resolves one deactivation-barrier tag without letting the lookup itself
    /// suppress the measurement it is meant to label. The empty value is what
    /// these instruments already record for a leaf whose tree is unregistered,
    /// so it adds no new tag value and no new cardinality.
    /// </summary>
    private static KeyValuePair<string, object?> TryResolveDeactivationBarrierTag(
        Func<KeyValuePair<string, object?>> resolve,
        string fallbackKey)
    {
        try
        {
            return resolve();
        }
        catch (InvalidOperationException)
        {
            return new KeyValuePair<string, object?>(fallbackKey, string.Empty);
        }
    }

    private static readonly Dictionary<string, LwwValue<byte[]>> EmptyEntries = new();

    /// <summary>
    /// Sums this leaf's <b>persisted</b> per-partition projection checkpoints,
    /// clamping the <c>-1</c> "nothing applied yet" sentinel to zero so the
    /// total is a monotone, non-negative quantity that can be differenced
    /// across the deactivation hook. Partition count comes from the persisted
    /// per-partition array (falling back to the legacy single-partition shape),
    /// so this stays synchronous and cannot fail.
    /// <para>
    /// It reads the <b>persisted</b> mark deliberately, not
    /// <c>GetCurrentCheckpointForPartition</c>. That accessor returns
    /// <c>max(persisted, pending)</c>, so an offset advanced in memory but not
    /// yet flushed is already included on entry to the hook - which is exactly
    /// the state a leaf with unflushed progress is in when it starts
    /// deactivating. Differencing it would report zero for every deactivation
    /// at every rate of occurrence, making the instrument a structural zero
    /// rather than a weak measurement. The durable mark is also the quantity
    /// the field evidence in issue #2280 is stated in.
    /// </para>
    /// </summary>
    private long SumPersistedCheckpointsAcrossPartitions()
    {
        var partitioned = state.State.ProjectionCheckpointOffsetsByPartition;
        var partitionCount = partitioned is { Length: > 0 } ? partitioned.Length : 1;

        long total = 0;
        for (var partition = 0; partition < partitionCount; partition++)
        {
            var checkpoint = GetPersistedCheckpointForPartition(partition);
            if (checkpoint > 0)
                total += checkpoint;
        }

        return total;
    }

    /// <summary>
    /// Records the once-per-deactivation checkpoint observation for issue
    /// #2280: how many projection-checkpoint offsets this activation banked
    /// while tearing down, tagged by tree, deactivation reason and activation
    /// temperature. Per-leaf detail goes to the accompanying debug log line -
    /// never to a tag, because the leaf population is unbounded.
    /// <para>
    /// Never throws. It runs first in the hook's <c>finally</c>, so a fault
    /// here would otherwise suppress the projection-hasher disposal and the
    /// revision-cookie removal that follow it.
    /// </para>
    /// </summary>
    private void RecordDeactivationCheckpointDelta(DeactivationReason reason, long checkpointAtEntry)
    {
        try
        {
            if (state.State.TreeId is not { Length: > 0 } treeId)
                return;

            var checkpointAtExit = SumPersistedCheckpointsAcrossPartitions();
            var delta = checkpointAtExit - checkpointAtEntry;
            if (delta < 0)
                delta = 0;

            var temperature = _activationWasCold
                ? LatticeMetrics.ActivationTemperatureCold
                : LatticeMetrics.ActivationTemperatureWarm;

            LatticeMetrics.LeafDeactivationCheckpointDelta.Record(
                delta,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId),
                new KeyValuePair<string, object?>(LatticeMetrics.TagDeactivationReason, reason.ReasonCode.ToString()),
                temperature,
                LatticeTenantLabel.ForTree(treeId));

            var logger = ResolveLogger();
            if (logger is null || !logger.IsEnabled(LogLevel.Debug))
                return;

            // Debug rather than Information: this fires once per leaf
            // deactivation and the live-leaf population is unbounded, so a
            // reactivation storm would otherwise flood the log with exactly
            // the lines an operator is least able to read.
            logger.LogDebug(
                "Leaf {GrainId} (tree '{TreeId}') deactivating: reason {Reason}, {Temperature} activation, "
                + "checkpoint total {CheckpointAtEntry} -> {CheckpointAtExit} (banked {Delta}), "
                + "{EntriesApplied} entries applied through the projection seam this activation. "
                + "A ZERO delta on a COLD activation is expected and is not by itself a fault: a cold replay "
                + "restarts from the -1 sentinel and the checkpoint is strictly monotonic, so nothing can be "
                + "banked until the applied offset passes the mark this activation started above. Progress "
                + "below that mark is not discarded, it is unrepresentable - read EntriesApplied to tell a "
                + "leaf that did no work from one that did a great deal below its existing mark. This line, "
                + "and the metric beside it, are a LOWER BOUND on deactivations: crash teardowns bypass this "
                + "hook by design, and an activation that THREW never reaches it at all (that population is "
                + "counted by the leaf activation-failure counter instead).",
                context.GrainId,
                treeId,
                reason.ReasonCode,
                _activationWasCold ? "cold" : "warm",
                checkpointAtEntry,
                checkpointAtExit,
                delta,
                _replayEntriesAppliedThisActivation);
        }
        catch
        {
            // Observation must never break deactivation, and must never
            // suppress the teardown steps sequenced after it.
        }
    }

    /// <summary>
    /// Process-wide singleton returned by <see cref="GetDeltaSinceAsync"/>
    /// on the cache-up-to-date fast path (caller's version dominates the
    /// leaf's, no pending split). Sharing the singleton elides three
    /// per-read allocations on the steady-state read path through
    /// <see cref="LeafCacheGrain.RefreshAsync"/>:
    /// <list type="bullet">
    ///   <item>The <see cref="StateDelta"/> record itself (~24 B).</item>
    ///   <item>The <see cref="VersionVector"/> wrapper (~24 B).</item>
    ///   <item>The wrapper's <c>Dictionary&lt;string, HybridLogicalClock&gt;</c>
    ///   (~80 B once the leaf has any writes).</item>
    /// </list>
    /// Safe to share because the only production callers of
    /// <see cref="GetDeltaSinceAsync"/> consume <c>delta.Version</c>
    /// exclusively through the pure static
    /// <see cref="VersionVector.Merge(VersionVector, VersionVector)"/>
    /// (see <see cref="LeafCacheGrain.RefreshAsync"/>) - no caller mutates
    /// the returned vector. The singleton's empty version is also
    /// correctness-equivalent on this branch: the dominate-or-equals
    /// precondition guarantees the caller already saw everything the leaf
    /// has, so merging an empty vector into the caller's vector is a no-op
    /// in observable state. The pre-allocated <see cref="EmptyDeltaTask"/>
    /// elides the <c>Task.FromResult</c> wrapper as well, leaving the
    /// fast-path return as a single static-field load.
    /// </summary>
    private static readonly StateDelta EmptyDelta = new()
    {
        Entries = EmptyEntries,
        Version = new VersionVector(),
        SplitKey = null,
        MovedAwaySlots = null,
        MovedAwayVsc = null,
    };

    private static readonly Task<StateDelta> EmptyDeltaTask = Task.FromResult(EmptyDelta);

    /// <summary>
    /// Cached <see cref="IGrainContext.GrainId"/> rendered as a <see cref="string"/>.
    /// The grain id is immutable for the lifetime of an activation, so this field
    /// is populated lazily on first <see cref="ReplicaId"/> access and reused for
    /// every subsequent read. Eliminates the per-call <see cref="object.ToString"/>
    /// allocation that the previous getter shape paid on every CRUD operation
    /// (8 hot-path call sites: 6 <see cref="VersionVector.Tick(string)"/> calls
    /// across <c>CommitSetAsync</c> / <c>CommitDeleteAsync</c> / <c>MergeAsync</c>
    /// / saga commit + 2 caller-clock reads inside <c>GetDeltaSinceAsync</c>).
    /// </summary>
    private string? _replicaId;

    private string ReplicaId => _replicaId ??= context.GrainId.ToString();
    private ResolvedLatticeOptions? _options;
    private ValueTask<ResolvedLatticeOptions> GetOptionsAsync() =>
        _options is not null
            ? new ValueTask<ResolvedLatticeOptions>(_options)
            : ResolveOptionsSlowAsync();

    private async ValueTask<ResolvedLatticeOptions> ResolveOptionsSlowAsync()
    {
        _options = await optionsResolver.ResolveAsync(state.State.TreeId ?? string.Empty);
        _maintainProjectionDigest = _options.MaintainProjectionDigest;
        // c2-xxviii: cache the coalescing window so the synchronous
        // PublishDigestUpwardAsync hot path can decide whether to
        // schedule the one-shot timer or fall through to inline
        // publish without re-resolving options. The resolver forces
        // the window to 0 when MaintainProjectionDigest is false (no
        // value in coalescing publishes that never happen).
        _digestCoalescingWindowMs = _maintainProjectionDigest
            ? _options.DigestCoalescingWindowMs
            : 0;
        return _options;
    }

    /// <summary>
    /// Cached projection-digest maintenance flag. Mirrors
    /// <see cref="LatticeOptions.MaintainProjectionDigest"/> from the
    /// activation-resolved <see cref="_options"/> so the synchronous
    /// digest funnels (<c>StoreEntry</c>, <c>RemoveEntry</c>,
    /// <c>PublishDigestUpwardAsync</c>) can elide their work without
    /// re-resolving options on every mutation. Defaults to <c>true</c>
    /// to preserve maintenance for unit-test code paths that instantiate
    /// the grain directly and never drive
    /// <see cref="ResolveOptionsSlowAsync"/>; production activations
    /// overwrite this from the resolver before the first mutation lands.
    /// </summary>
    private bool _maintainProjectionDigest = LatticeOptions.DefaultMaintainProjectionDigest;

    /// <summary>
    /// Advances the leaf's local <see cref="HybridLogicalClock"/> for a
    /// commit and returns the value to persist on the freshly-constructed
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/>. When
    /// <see cref="LatticeHlcOverrideContext.Current"/> is <see langword="null"/>
    /// (the foreground-caller default), the local clock advances via
    /// <see cref="HybridLogicalClock.Tick"/> and the same value is
    /// returned. When an override is present (the cross-cluster atomic
    /// apply path), the local clock advances via
    /// <see cref="HybridLogicalClock.Merge"/> so subsequent foreground
    /// ticks remain strictly greater than the override (preserving local
    /// monotonicity), but the <em>override</em> is returned verbatim so
    /// the persisted <see cref="Orleans.Lattice.Primitives.LwwValue{T}.Timestamp"/> matches the
    /// authoring cluster's HLC bit-identically - preserving the
    /// receiver-side LWW resolution invariant.
    /// </summary>
    private HybridLogicalClock AdvanceClockOrOverride()
    {
        var ovr = LatticeHlcOverrideContext.Current;
        if (ovr is { } sourceHlc)
        {
            state.State.Clock = HybridLogicalClock.Merge(state.State.Clock, sourceHlc);
            return sourceHlc;
        }

        state.State.Clock = HybridLogicalClock.Tick(state.State.Clock);
        return state.State.Clock;
    }

    /// <inheritdoc />
    public async Task<HybridLogicalClock> GetClockAsync()
    {
        await AwaitReplayBarrierAsync();
        return state.State.Clock;
    }

    public async Task<byte[]?> GetAsync(string key)
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.Read);
        // Moved-away seal: a slot recorded on this leaf as having
        // migrated to a sibling shard is invisible to every read
        // path, including the LeafCacheGrain pending-key delegation
        // that bypasses the shard front door. See IsKeyMovedAway for the rationale.
        if (IsKeyMovedAway(key))
        {
#if LATTICE_DIAG
            DiagSink.Write($"[DIAG read1-moved-away] gid={context.GrainId} key={key}");
#endif
            return null;
        }

        // Strict atomic-visibility: a key with a pending-tx entry
        // dials back through the per-tree TxRegistry - the
        // registry-recorded saga outcome is the single tree-wide
        // linearization point, so readers never observe a partial
        // commit / abort across leaves. The fast path (no pending
        // entry on this leaf) avoids the RPC entirely.
        if (TryFindPendingForKey(key, out var txid, out var pendingValue))
        {
            return await GetWithPendingAsync(key, txid, pendingValue);
        }

        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        if (Cache.TryGetRow(key, out var lww) && !lww.IsTombstone && !lww.IsExpired(nowTicks))
        {
            // Migration-window shadow guard. A migrated entry on this
            // destination leaf carries the source's pre-saga snapshot
            // (IsMigrated=true). If the split coordinator installed a
            // shadow marker naming a saga that committed at the
            // registry but whose backstop terminal has not yet reached
            // this leaf, serving the migrated value here would split
            // observation against any sibling leaf whose backstop has
            // already landed. The slow path consults the registry and
            // raises StaleShardRoutingException for the Committed-no-
            // backstop case so the LatticeGrain deadline-bounded retry
            // loop re-fans under a fresh snapshot.
            if (lww.IsMigrated && TryGetShadowedSagas(key, out var sagas))
            {
                return await GetWithShadowedMigratedAsync(key, lww.Value, sagas);
            }
#if LATTICE_DIAG
            // DIAG: single-key read-return path.
            DiagSink.Write($"[DIAG read1] gid={context.GrainId} key={key} valRound={DiagDecodeRound(lww.Value)} " +
                $"hlc={lww.Timestamp} isMig={lww.IsMigrated} origin={lww.OriginClusterId ?? "(local)"}");
#endif
            return lww.Value;
        }

#if LATTICE_DIAG
        // DIAG: single-key returning null.
        DiagSink.Write($"[DIAG read1-null] gid={context.GrainId} key={key}");
#endif
        return null;
    }

    /// <summary>
    /// Slow-path completion of <see cref="GetAsync"/> when the key
    /// would surface a migrated entry but carries a destination-side
    /// shadow marker. Resolves every shadowing saga through the
    /// registry and either passes the migrated value through
    /// (InFlight / Aborted, or a decided-or-indeterminate saga whose
    /// backstop terminal has already landed here) or raises
    /// <see cref="StaleShardRoutingException"/> with a sentinel
    /// <c>(-1, -1, -1)</c> tuple so the caller's deadline-bounded
    /// retry loop re-fans under a fresh snapshot.
    /// </summary>
    private async Task<byte[]?> GetWithShadowedMigratedAsync(string key, byte[]? migratedValue, HashSet<Guid> sagas)
    {
        if (await IsShadowedReadSafeAsync(sagas))
        {
#if LATTICE_DIAG
            DiagSink.Write($"[DIAG read1-shadow-pass] gid={context.GrainId} key={key} valRound={DiagDecodeRound(migratedValue)}");
#endif
            return migratedValue;
        }
#if LATTICE_DIAG
        DiagSink.Write($"[DIAG read1-shadow-stale] gid={context.GrainId} key={key} sagas=[{string.Join(',', sagas)}]");
#endif
        throw new StaleShardRoutingException(-1, -1, -1);
    }

    private async Task<byte[]?> GetWithPendingAsync(string key, Guid txid, LwwValue<byte[]> pendingValue)
    {
        var status = await ResolvePendingStatusAsync(txid);
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        // Single-key visibility decision, delegated to the shared, dependency-free
        // AtomicVisibilityGate so the production read path and the Coyote
        // atomic-visibility model execute one identical rule (see #1585). The
        // orphan guard (IsRecentlyTerminal) and the strict-isolation fall-through
        // are encoded in the gate; the historical rationale lives on
        // AtomicVisibilityGate / PendingReadOutcome.
        switch (AtomicVisibilityGate.ResolveKey(status, IsRecentlyTerminal(txid), pendingValue.IsTombstone || pendingValue.IsExpired(nowTicks)))
        {
            case PendingReadOutcome.SurfacePrepared:
                return pendingValue.Value;
            case PendingReadOutcome.Hidden:
                return null;
            default:
                // FallThroughToPreSaga: InFlight, Aborted, or an already-terminal
                // orphan bucket - surface the pre-saga value from Entries.
                if (Cache.TryGetRow(key, out var lww) && !lww.IsTombstone && !lww.IsExpired(nowTicks))
                    return lww.Value;
                return null;
        }
    }

    public async Task<VersionedValue> GetWithVersionAsync(string key)
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.Read);
        // Moved-away seal. See GetAsync for the rationale.
        if (IsKeyMovedAway(key))
        {
            return new VersionedValue();
        }

        if (TryFindPendingForKey(key, out var txid, out var pendingValue))
        {
            return await GetWithVersionWithPendingAsync(key, txid, pendingValue);
        }

        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        if (Cache.TryGetRow(key, out var lww) && !lww.IsTombstone && !lww.IsExpired(nowTicks))
        {
            return new VersionedValue
            {
                Value = lww.Value,
                Version = lww.Timestamp,
                ExpiresAtTicks = lww.ExpiresAtTicks,
                MergeMode = Cache.GetMergeMode(key),
            };
        }

        return new VersionedValue();
    }

    private async Task<VersionedValue> GetWithVersionWithPendingAsync(string key, Guid txid, LwwValue<byte[]> pendingValue)
    {
        var status = await ResolvePendingStatusAsync(txid);
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        // Shared atomic-visibility gate (see GetWithPendingAsync / #1585).
        switch (AtomicVisibilityGate.ResolveKey(status, IsRecentlyTerminal(txid), pendingValue.IsTombstone || pendingValue.IsExpired(nowTicks)))
        {
            case PendingReadOutcome.SurfacePrepared:
                return new VersionedValue
                {
                    Value = pendingValue.Value,
                    Version = pendingValue.Timestamp,
                    ExpiresAtTicks = pendingValue.ExpiresAtTicks,
                    MergeMode = Cache.GetMergeMode(key),
                };
            case PendingReadOutcome.Hidden:
                return new VersionedValue();
            default:
                // FallThroughToPreSaga: InFlight, Aborted, or already-terminal orphan.
                if (Cache.TryGetRow(key, out var lww) && !lww.IsTombstone && !lww.IsExpired(nowTicks))
                    return new VersionedValue
                    {
                        Value = lww.Value,
                        Version = lww.Timestamp,
                        ExpiresAtTicks = lww.ExpiresAtTicks,
                        MergeMode = Cache.GetMergeMode(key),
                    };
                return new VersionedValue();
        }
    }

    public async Task<bool> ExistsAsync(string key)
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.Read);
        // Moved-away seal. See GetAsync for the rationale.
        if (IsKeyMovedAway(key))
        {
            return false;
        }

        if (TryFindPendingForKey(key, out var txid, out var pendingValue))
        {
            return await ExistsWithPendingAsync(key, txid, pendingValue);
        }

        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        return Cache.TryGetRow(key, out var lww) && !lww.IsTombstone && !lww.IsExpired(nowTicks);
    }

    private async Task<bool> ExistsWithPendingAsync(string key, Guid txid, LwwValue<byte[]> pendingValue)
    {
        var status = await ResolvePendingStatusAsync(txid);
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        // Shared atomic-visibility gate (see GetWithPendingAsync / #1585).
        switch (AtomicVisibilityGate.ResolveKey(status, IsRecentlyTerminal(txid), pendingValue.IsTombstone || pendingValue.IsExpired(nowTicks)))
        {
            case PendingReadOutcome.SurfacePrepared:
                return true;
            case PendingReadOutcome.Hidden:
                return false;
            default:
                // FallThroughToPreSaga: InFlight, Aborted, or already-terminal orphan.
                return Cache.TryGetRow(key, out var lww) && !lww.IsTombstone && !lww.IsExpired(nowTicks);
        }
    }

    public async Task<GetOrSetResult> GetOrSetAsync(string key, byte[] value)
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.Write);
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        // Short-circuit: if the key already exists and is live (and not expired)
        // AND has no pending-tx mutation, return its value without writing.
        // A pending mutation makes the key invisible, so we must fall through
        // to the write path to record the caller's intent.
        if (!IsKeyPending(key)
            && Cache.TryGetRow(key, out var existing)
            && !existing.IsTombstone
            && !existing.IsExpired(nowTicks))
        {
            return new GetOrSetResult { ExistingValue = existing.Value };
        }

        // Key is absent, tombstoned, expired, or pending - delegate to the write path and wrap the result.
        return await GetOrSetWriteAsync(key, value);
    }

    private async Task<GetOrSetResult> GetOrSetWriteAsync(string key, byte[] value)
    {
        var splitResult = await SetAsync(key, value);
        return new GetOrSetResult { Split = splitResult };
    }

    public async Task<CasResult> SetIfVersionAsync(string key, byte[] value, HybridLogicalClock expectedVersion)
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.Write);
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        // Pending-tx isolation: a key with an in-flight saga prepare is
        // invisible to CAS - treat it as absent so expectedVersion must
        // be Zero. The CAS write itself races with the saga's terminal
        // mark; LWW resolves the conflict deterministically via HLC.
        var pending = IsKeyPending(key);

        // Check current entry version. Treat expired live entries as absent
        // for CAS purposes (same as tombstones) so a fresh write with
        // expectedVersion == Zero succeeds after expiry.
        if (!pending
            && Cache.TryGetRow(key, out var existing)
            && !existing.IsTombstone
            && !existing.IsExpired(nowTicks))
        {
            if (existing.Timestamp != expectedVersion)
            {
                return new CasResult { Success = false, CurrentVersion = existing.Timestamp };
            }
        }
        else
        {
            // Key is absent, tombstoned, or pending - expectedVersion must be Zero.
            if (expectedVersion != HybridLogicalClock.Zero)
            {
                return new CasResult { Success = false, CurrentVersion = HybridLogicalClock.Zero };
            }
        }

        // Version matches - delegate to the async write path.
        return await SetIfVersionWriteAsync(key, value);
    }

    private async Task<CasResult> SetIfVersionWriteAsync(string key, byte[] value)
    {
        var splitResult = await SetAsync(key, value);
        // After SetAsync, the entry has a new timestamp.
        var newVersion = Cache.TryGetRow(key, out var committed) ? committed.Timestamp : default;
        return new CasResult
        {
            Success = true,
            CurrentVersion = newVersion,
            Split = splitResult
        };
    }

    public async Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys)
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.Read);
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        var predicate = LatticePredicateContext.Current;
        var (outcomes, pendingKeys) = await SnapshotPendingForReadAsync();
        // Resolve every key of this fan-out against a single registry view, so a
        // registry InFlight->Committed transition cannot fall mid-scan and split
        // the observation across keys (see #1584 / TxRegistrySnapshot).
        var registrySnapshot = new TxDecisionView(outcomes);
        var result = new Dictionary<string, byte[]>(keys.Count);
        foreach (var key in keys)
        {
            // Moved-away seal. See GetAsync for the rationale.
            // Hot path: leaves with no moved slots short-circuit on a
            // single nullable read inside IsKeyMovedAway.
            if (IsKeyMovedAway(key))
            {
#if LATTICE_DIAG
                DiagSink.Write($"[DIAG read-moved-away] gid={context.GrainId} key={key}");
#endif
                continue;
            }

            if (pendingKeys.TryGetValue(key, out var pending))
            {
                var status = registrySnapshot.Resolve(pending.txid);
                if (AtomicVisibilityGate.ResolveKey(status, IsRecentlyTerminal(pending.txid), pending.value.IsTombstone || pending.value.IsExpired(nowTicks)) != PendingReadOutcome.FallThroughToPreSaga)
                {
                    if (!pending.value.IsTombstone && !pending.value.IsExpired(nowTicks))
                    {
                        if (predicate is null || LatticePredicateEvaluator.Matches(pending.value.Value, predicate.Value))
                            result[key] = pending.value.Value!;
#if LATTICE_DIAG
                        // DIAG: pending-bucket-committed read path.
                        DiagSink.Write($"[DIAG read-pending-committed] silo={DiagSiloTag} gid={context.GrainId} key={key} tx={pending.txid} valRound={DiagDecodeRound(pending.value.Value)} hlc={pending.value.Timestamp}");
#endif
                    }
                    else
                    {
#if LATTICE_DIAG
                        DiagSink.Write($"[DIAG read-pending-committed-tomb] silo={DiagSiloTag} gid={context.GrainId} key={key} tx={pending.txid}");
#endif
                    }
                    continue;
                }
#if LATTICE_DIAG
                // DIAG: pending-bucket-fallthrough (InFlight, Aborted, or already-terminal'd).
                DiagSink.Write($"[DIAG read-pending-fallthrough] silo={DiagSiloTag} gid={context.GrainId} key={key} tx={pending.txid} status={status} alreadyTerminal={(_recentlyTerminal is not null && _recentlyTerminal.Contains(pending.txid))}");
#endif
                // InFlight, Aborted, or orphan-pending (committed bucket whose
                // saga terminal has already landed on this leaf) - fall through
                // to Entries. See GetWithPendingAsync for the orphan-pending
                // rationale: a late-arriving shadow-forward of a prepare can
                // bucket a saga whose terminal has already drained into Entries,
                // and surfacing the orphan would shadow the authoritative
                // Entries value (or a strictly-later saga's value).
            }

            if (Cache.TryGetRow(key, out var lww) && !lww.IsTombstone && !lww.IsExpired(nowTicks))
            {
                // Migration-window shadow guard. See GetAsync for the
                // full rationale: when the surfacing entry is a
                // destination-side migration (IsMigrated=true) and
                // the split coordinator installed a shadow marker
                // naming a saga that is not known to be undecided,
                // and whose backstop terminal has not landed here,
                // as the owner of this key, raise
                // StaleShardRoutingException so the LatticeGrain retry loop re-fans under a fresh
                // snapshot. Cheap on the steady-state path: a single
                // null check plus a dictionary miss when no marker
                // is installed.
                if (lww.IsMigrated && TryGetShadowedSagas(key, out var shadowSagas))
                {
                    if (!await IsShadowedReadSafeAsync(shadowSagas))
                    {
#if LATTICE_DIAG
                        DiagSink.Write($"[DIAG read-shadow-stale] silo={DiagSiloTag} gid={context.GrainId} key={key} sagas=[{string.Join(',', shadowSagas)}]");
#endif
                        throw new StaleShardRoutingException(-1, -1, -1);
                    }
                }
                if (predicate is not null && !LatticePredicateEvaluator.Matches(lww.Value, predicate.Value))
                    continue;
                result[key] = lww.Value!;
#if LATTICE_DIAG
                // DIAG: read-return path - capture what each leaf returns per key.
                DiagSink.Write($"[DIAG read] silo={DiagSiloTag} gid={context.GrainId} key={key} valRound={DiagDecodeRound(lww.Value)} " +
                    $"hlc={lww.Timestamp} isMig={lww.IsMigrated} origin={lww.OriginClusterId ?? "(local)"}");
#endif
            }
        }
        return result;
    }

    public async Task<SplitResult?> SetAsync(string key, byte[] value)
    {
        await AwaitReplayBarrierAsync();
        return await SetCoreAsync(key, value, 0L);
    }

    /// <inheritdoc />
    public async Task<SplitResult?> SetAsync(string key, byte[] value, long expiresAtTicks)
    {
        await AwaitReplayBarrierAsync();
        return await SetCoreAsync(key, value, expiresAtTicks);
    }

    public async Task<LwwEntry?> GetRawEntryAsync(string key)
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.Read);
        if (Cache.TryGetRow(key, out var lww))
            return new LwwEntry(key, lww, Cache.GetMergeMode(key));
        return null;
    }

    /// <inheritdoc />
    public async Task<List<LwwEntry?>> GetRawEntriesAsync(List<string> keys)
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.Read);
        // Pure in-memory dictionary lookup loop; no I/O, no allocation
        // beyond the result list itself. The Orleans grain-call boundary
        // wraps this in a single async state machine even though the
        // method body is synchronous, so the cost per batch is one
        // Task allocation regardless of key count - which is exactly
        // the win the saga's PrepareAsync capture loop targets.
        var cache = Cache;
        var result = new List<LwwEntry?>(keys.Count);
        foreach (var key in keys)
        {
            if (cache.TryGetRow(key, out var lww))
                result.Add(new LwwEntry(key, lww, cache.GetMergeMode(key)));
            else
                result.Add(null);
        }
        return result;
    }

    private async Task<SplitResult?> SetCoreAsync(string key, byte[] value, long expiresAtTicks)
    {
        EnsureInternalOrigin(LatticeOperation.Write);
        using var _mutationScope = EnterMutationScope();
        // Recovery: if a previous split was interrupted, complete it first.
        if (HasInterruptedSplit)
        {
            var recovered = await CompleteRecoverySplitUnderGateAsync();

            // Apply the caller's write to the correct leaf so it isn't silently dropped.
            if (string.Compare(key, state.State.SplitKey!, StringComparison.Ordinal) >= 0)
            {
                // The key belongs to the new sibling - forward it there.
                // The sibling publishes its own mutation notification after persist,
                // so we do not publish one here to avoid a duplicate for the same key.
                var sibling = grainFactory.GetGrain<IBPlusLeafGrain>(state.State.SplitSiblingId!.Value);
                await sibling.SetAsync(key, value, expiresAtTicks);
            }
            else
            {
                // The key belongs to this leaf - write it via the
                // WAL-first commit path so the WAL append and the
                // in-memory projection update remain consistent with the
                // main path below.
                await CommitSetAsync(key, value, expiresAtTicks);
            }

            return recovered;
        }

        // Declared-span admission (see BPlusLeafGrain.SpanAdmission.cs).
        // Routing can still name this leaf for a key its declared range no
        // longer covers - most sharply between a split narrowing this leaf's
        // high bound and the shard root installing the separator that redirects
        // the key. Committing here would produce a row this leaf's own replay
        // filter refuses to reinstate, leaving durability to depend on where
        // the declaring leaf's checkpoint happens to sit. Forward instead.
        if (TryResolveSpanForwardTarget(key, out var spanTarget))
        {
            // The sibling publishes its own mutation notification after
            // persist, so none is published here. Its SplitResult is discarded
            // for the reason given in ForwardOutOfSpanMergeAsync.
            await grainFactory.GetGrain<IBPlusLeafGrain>(spanTarget)
                .SetAsync(key, value, expiresAtTicks);
            return null;
        }

        return await CommitSetAsync(key, value, expiresAtTicks);
    }

    /// <summary>
    /// Commit path for <see cref="MutationKind.Set"/>.
    /// Steps in order:
    /// <list type="number">
    ///   <item><b>build</b> - tick HLC + version vector and construct
    ///   the LWW value plus its observer-bound mutation envelope;</item>
    ///   <item><b>wal</b> - append the mutation to the per-shard WAL via
    ///   the resolved <see cref="ICommitLogWriter"/> (no-op when the
    ///   adapter is absent);</item>
    ///   <item><b>apply</b> - merge the LWW value into the in-memory
    ///   projection and check the leaf-split predicate;</item>
    ///   <item><b>observer</b> - publish the post-commit mutation to
    ///   any registered <see cref="IMutationObserver"/> inside a
    ///   <see cref="LatticeCommitLogContext"/> scope so a downstream
    ///   replication-aware observer can detect the commit-log source
    ///   and short-circuit its loop-prevention.</item>
    /// </list>
    /// </summary>
    private async Task<SplitResult?> CommitSetAsync(string key, byte[] value, long expiresAtTicks)
    {
        using var _commitScope = EnterCommitScope();
        // step 0 (build) - HLC tick (or override), build LwwValue. Version
        // vector is foreground-only; ILeafProjection.Apply does not advance it.
        // Prepared writes (saga prepare phase) skip the Version publication
        // because they route into the pending-tx map, not visible Entries;
        // publishing on prepare would advance the cache's saved callerClock
        // past prepare time and the cache's per-entry HLC delta filter would
        // then exclude the drained value when the saga's terminal mark
        // re-stamps and surfaces it. The terminal handler publishes Version
        // itself so the cache observes a single linearization-point
        // advance covering the whole saga's drained set.
        //
        // PublishVersionAdvance: lift Version[ReplicaId] to the entry's
        // own stamp. The stamp equals Entries[key].Timestamp by
        // construction, so the cache filter `lww.Timestamp > callerClock`
        // delivers this entry on any refresh where the caller's saved
        // callerClock is strictly less than the stamp (i.e. every fresh
        // LeafCacheGrain activation, and every refresh that has not yet
        // observed this write). VersionVector.Tick(ReplicaId) would call
        // HLC.Tick against DateTimeOffset.UtcNow.Ticks and could land
        // strictly above stamp, causing the filter to silently drop this
        // entry on its next refresh. Passing stamp directly avoids that.
        // See PublishVersionAdvance's XML doc for the full invariant.
        var stamp = AdvanceClockOrOverride();
        var isPrepared = LatticePreparedContext.Current;
        if (!isPrepared)
            PublishVersionAdvance(stamp);
        BumpLocalRevision();

        // Post-merge observer (LWW). A fresh foreground Set wins the row-level
        // LWW by strict HLC-tick monotonicity, so `value` is the canonical
        // merged result for this key. When an observer is registered it may
        // normalise / re-encode the merged bytes (AcceptTransformed is permitted
        // for LwwRegister records; the durable WAL record carries the full
        // winning value, so a transform stays replay-deterministic). Zero-cost
        // when inactive (cached flag): `value` is used verbatim and no observer
        // is consulted. The prior value is read here, before StoreEntry
        // overwrites it, so the observer sees the true local input.
        var mergedValue = value;
        if (MergeObserverActive)
        {
            byte[]? priorValue = Cache.TryGetRow(key, out var priorRow) && !priorRow.IsTombstone
                ? priorRow.Value
                : null;
            mergedValue = await ApplyMergeObserverAsync(
                key, LatticeMergeMode.LwwRegister, priorValue, value, value, CancellationToken.None);
        }

        var newEntry = LwwValue<byte[]>.CreateWithExpiry(mergedValue, stamp, expiresAtTicks)
            with
            {
                OriginClusterId = LatticeOriginContext.Current,
                VectorClock = LatticeVectorClockContext.Current,
            };

        var options = await GetOptionsAsync();

        // step 1 (wal) - propagate exceptions: pre-Apply failure leaves
        // state untouched and the foreground caller observes the WAL error.
        var walStartTicks = Stopwatch.GetTimestamp();
        var writer = ResolveCommitLogWriter();
        if (writer is not null)
        {
            var entry = WalRecordBuilder.ForSet(
                state.State.TreeId ?? string.Empty,
                state.State.ShardIndex ?? 0,
                key,
                newEntry,
                isPrepared);
            await writer.AppendAsync(entry);
        }
        RecordCommitStep("wal", walStartTicks);

        // step 2 (apply) - LWW-merge into the in-memory projection, or
        // into the per-leaf pending-tx map when the mutation is a saga
        // prepare-phase write. Prepared writes never trigger a leaf
        // split because they are not yet visible in Entries.
        var applyStartTicks = Stopwatch.GetTimestamp();
        SplitResult? splitResult = null;
        if (isPrepared)
        {
            // Carry the typed CRDT delta (when present) and the tree's merge
            // mode into the pending-tx delta side-map so the saga's terminal
            // commit folds the delta into the receiver's current visible state
            // rather than installing this prepared LWW value verbatim. The
            // delta rides the ambient LatticeDeltaContext (the same source the
            // WAL record's Delta slot reads above), and the mode is resolved
            // through the per-tree resolver that also stamps WalRecord.Mode -
            // so the foreground commit and the activation-time WAL replay
            // reconstruct the side-map identically. Plain LWW prepared writes
            // leave the ambient delta null and the side-map untouched.
            var preparedDelta = LatticeDeltaContext.Current;
            var preparedMode = preparedDelta is null
                ? LatticeMergeMode.LwwRegister
                : ResolveMergeMode();
            AddPreparedMutation(
                LatticeTransactionContext.Current,
                key,
                newEntry,
                delta: preparedDelta,
                mode: preparedMode);
        }
        else
        {
            StoreEntry(key, newEntry);
            // Foreground commit constructs a fresh LwwValue with the
            // default IsMigrated=false, so StoreEntry's merge clears
            // any stale migration provenance from a prior migrated
            // entry on the same key automatically - the flag rides
            // with the value, not in a side-channel map.
            if (IsLeafOverCapacity(options.MaxLeafKeys, options.MaxLeafBytes))
            {
                splitResult = await SplitIfNeededUnderGateAsync(options.MaxLeafKeys, options.MaxLeafBytes);
            }
        }
        RecordCommitStep("apply", applyStartTicks);

        // step 3 (observer) - publish under a commit-log scope so a
        // replication-aware observer can detect the source and avoid
        // re-appending its own input back into the WAL.
        var observerStartTicks = Stopwatch.GetTimestamp();
        if (mutationObservers.HasObservers)
        {
            // For non-prepared writes, the key may have migrated to the
            // new sibling on a split - fall back to newEntry, which is
            // guaranteed by strict-HLC-tick monotonicity to be the
            // committed LWW winner. For prepared writes the entry is in
            // the pending-tx map (not Entries), so always use newEntry
            // verbatim; the observer payload's IsPrepared flag tells
            // downstream consumers the entry is not yet visible.
            LwwValue<byte[]> published;
            if (isPrepared)
            {
                published = newEntry;
            }
            else
            {
                published = Cache.TryGetRow(key, out var committed) ? committed : newEntry;
            }
            using (LatticeCommitLogContext.BeginScope())
            {
                await PublishSetAsync(key, published);
            }
        }
        RecordCommitStep("observer", observerStartTicks);

        // Forward the projection-hash delta (if any) to the parent
        // internal node so the chained subtree fold stays current.
        // No-op when the running hash did not change (dominated
        // re-application) or when this leaf has no parent (flat-tree
        // root-is-leaf shape). Wrapped in the commit-step recorder so
        // the per-write parent-digest RPC is attributable on
        // LeafCommitDuration alongside the wal / apply / observer
        // stages.
        var digestStartTicks = Stopwatch.GetTimestamp();
        await PublishDigestUpwardAsync();
        RecordCommitStep("digest", digestStartTicks);

        return splitResult;
    }

    public async Task<SplitResult?> SetManyAsync(List<KeyValuePair<string, byte[]>> entries)
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.Write);
        using var _mutationScope = EnterMutationScope();
        ArgumentNullException.ThrowIfNull(entries);
        if (entries.Count == 0)
        {
            return null;
        }

        // Fast path: in the common foreground bulk-write case we can
        // collapse the per-key WAL round-trip into a single batched
        // commit-log dispatch via ICommitLogWriter.AppendManyAsync. The
        // fast path is gated by the conditions that distinguish the
        // simple commit path inside CommitSetAsync - if any per-call
        // context flag would otherwise alter the per-key semantics, we
        // fall through to the per-key loop and pay the original cost.
        // Saga prepare-phase writes were historically routed through
        // the per-key fallback loop below because the batched
        // CommitSetManyAsync path was originally written for visible
        // writes only (StoreEntry into Entries) and could not bucket
        // entries into the leaf's pending-tx map. Phase D1b (c2-ix
        // memo): CommitSetManyAsync now handles the `isPrepared`
        // branch internally - prepared entries route to
        // AddPreparedMutation via the same one-batched-WAL-append
        // shape foreground SetManyAsync uses, removing the saga's
        // size-1-WAL-batch cost identified by D1's phase-A
        // attribution (wal.append.in_flight=0 throughout the D1 run).
        //
        // Saga callers stamp the atomic-batch ambient with
        // (Size, BaseIndex) AND an optional `key -> globalIndex` map.
        // The map exists so the leaf can recover each entry's true
        // saga-global index after LatticeGrain.SetManyAsync's
        // shard-bucketing fan-out re-groups the saga's flat entry list
        // into per-shard buckets (bucket-local position no longer
        // equals saga-global position). When the map is absent the
        // batched commit path falls back to BaseIndex + bucketLocal,
        // matching the foreground non-saga shape.
        //
        // An in-progress split, an active post-merge observer, or a key
        // outside this leaf's declared range each rule out the wholesale
        // CommitSetManyAsync commit: the split must complete first so each
        // entry lands on the leaf that will own it, the batched commit does
        // not invoke the observer, and CommitSetManyAsync has no per-key
        // admission step. A foreground batch then goes through
        // SetManyAdmittingSpanAsync, which completes the split once, forwards
        // each out-of-span group as one batch, and commits the rest in one
        // append (#3348). Saga-prepared, atomic-batch, and observer writes keep
        // the per-key SetAsync loop, whose per-key semantics their suites
        // prove. The observer flag is cached and the span scan is skipped on a
        // leaf with no declared bounds, so the fast path below is unchanged on
        // the common shape. See BPlusLeafGrain.SpanAdmission.cs.
        if (HasInterruptedSplit || MergeObserverActive || ContainsOutOfSpanKey(entries))
        {
            // (#3348) A foreground batch keeps its batched shape through a
            // split or a span straddle: one WAL append for the in-span entries
            // plus one forwarded SetMany per sibling, instead of a full serial
            // WAL round trip per key. See SetManyAdmittingSpanAsync.
            if (!MergeObserverActive && !LatticePreparedContext.Current && LatticeAtomicBatchContext.Current is null)
            {
                return await SetManyAdmittingSpanAsync(entries);
            }

            SplitResult? lastSplit = null;
            foreach (var entry in entries)
            {
                var split = await SetAsync(entry.Key, entry.Value);
                if (split is not null)
                    lastSplit = split;
            }
            return lastSplit;
        }

        return await CommitSetManyAsync(entries);
    }

    /// <summary>
    /// Conditional bulk write: commits only the entries whose <b>current</b>
    /// stored value satisfies <paramref name="predicate"/>, evaluated once
    /// here at write time against each key's committed JSON document view. A
    /// key with no live committed value is treated as non-matching and is
    /// skipped. The matched entries are committed through the same batched
    /// commit path as <see cref="SetManyAsync"/> (so replication ships an
    /// ordinary per-key Set for each written entry and no predicate is
    /// re-evaluated downstream); the returned <see cref="ConditionalSetManyResult.WrittenKeys"/>
    /// reports exactly the committed subset.
    /// <para>
    /// Declared-span admission runs <b>before</b> the guard pass, and the order
    /// matters. "No live committed value" is only sound evidence of a guard
    /// miss for a key this leaf declares. For a key whose row a split moved to
    /// a sibling, the absence says nothing about the guard - the real value
    /// still lives on the declaring leaf and may well satisfy it - yet the two
    /// cases were previously indistinguishable, so a matching key was dropped
    /// from the written set with no error and no metric while the caller was
    /// told the batch completed. Such entries are therefore forwarded to the
    /// leaf that declares them and the guard is evaluated there. See
    /// <c>BPlusLeafGrain.SpanAdmission.cs</c> and issue #2663.
    /// </para>
    /// </summary>
    public async Task<ConditionalSetManyResult> SetManyWherePredicateAsync(
        List<KeyValuePair<string, byte[]>> entries, LatticePredicateNode predicate)
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.Write);
        using var _mutationScope = EnterMutationScope();
        ArgumentNullException.ThrowIfNull(entries);
        if (entries.Count == 0)
        {
            return new ConditionalSetManyResult { WrittenKeys = Array.Empty<string>() };
        }

        // Admission precedes the guard: see the remarks above. On a leaf with
        // no declared span - the steady state - this returns on its first line,
        // so the common path pays nothing.
        if (ContainsOutOfSpanKey(entries))
        {
            return await ForwardOutOfSpanConditionalSetManyAsync(entries, predicate);
        }

        // Every entry is in span, so the guard's "absent means non-matching"
        // inference is sound for all of them and the matched set cannot
        // straddle the span either.
        return await SetManyWherePredicateLocalAsync(entries, predicate, mayContainOutOfSpanKey: false);
    }

    /// <summary>
    /// Evaluates the guard against this leaf's own committed rows and commits
    /// the matching subset. Every caller must have established that the
    /// entries belong here, either because
    /// <see cref="ContainsOutOfSpanKey(List{KeyValuePair{string, byte[]}})"/>
    /// cleared them or because
    /// <see cref="ForwardOutOfSpanConditionalSetManyAsync"/> retained them
    /// under the fail-open rule.
    /// </summary>
    /// <param name="entries">The candidate entries, already span-admitted.</param>
    /// <param name="predicate">The guard evaluated against each key's current committed value.</param>
    /// <param name="mayContainOutOfSpanKey">
    /// <see langword="true"/> when the caller retained entries it could not
    /// route, so the matched set must be re-scanned for out-of-span keys before
    /// the batched commit; <see langword="false"/> when admission already
    /// cleared the whole batch and that scan would be redundant.
    /// </param>
    private async Task<ConditionalSetManyResult> SetManyWherePredicateLocalAsync(
        List<KeyValuePair<string, byte[]>> entries,
        LatticePredicateNode predicate,
        bool mayContainOutOfSpanKey)
    {
        if (entries.Count == 0)
        {
            return new ConditionalSetManyResult { WrittenKeys = Array.Empty<string>() };
        }

        // Guard pass: keep only entries whose current committed value matches
        // the predicate. We allocate the matched/written lists lazily on the
        // first hit so an all-guarded-out batch stays allocation-free past the
        // single dictionary probe per key.
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        List<KeyValuePair<string, byte[]>>? matched = null;
        List<string>? writtenKeys = null;
        foreach (var entry in entries)
        {
            if (Cache.TryGetRow(entry.Key, out var lww)
                && !lww.IsTombstone
                && !lww.IsExpired(nowTicks)
                && LatticePredicateEvaluator.Matches(lww.Value, predicate))
            {
                (matched ??= new List<KeyValuePair<string, byte[]>>(entries.Count)).Add(entry);
                (writtenKeys ??= new List<string>(entries.Count)).Add(entry.Key);
            }
        }

        if (matched is null)
        {
            return new ConditionalSetManyResult { WrittenKeys = Array.Empty<string>() };
        }

        SplitResult? split;
        var splitInProgress = HasInterruptedSplit;
        // An out-of-span entry can only reach the matched set when the caller
        // retained it here under the fail-open forward rule, or when a row was
        // orphaned before this rule existed. Routing it per key keeps the guard
        // uniform across every batched write path and stops such a row being
        // re-committed out of span. See BPlusLeafGrain.SpanAdmission.cs.
        if (splitInProgress || MergeObserverActive
            || (mayContainOutOfSpanKey && ContainsOutOfSpanKey(matched)))
        {
            // Mirror SetManyAsync's split-in-progress fallback: the split
            // recovery in SetCoreAsync forwards mid-batch entries across two
            // grains, so the matched entries are committed one at a time. An
            // active post-merge observer also routes per key so every LWW write
            // is observed (the batched CommitSetManyAsync path does not invoke
            // the observer); zero-cost on the default null-observer path.
            SplitResult? lastSplit = null;
            foreach (var entry in matched)
            {
                var s = await SetAsync(entry.Key, entry.Value);
                if (s is not null)
                    lastSplit = s;
            }
            split = lastSplit;
        }
        else
        {
            split = await CommitSetManyAsync(matched);
        }

        return new ConditionalSetManyResult { Split = split, WrittenKeys = writtenKeys! };
    }

    /// <summary>
    /// Per-call threshold above which <see cref="CommitSetManyAsync"/>
    /// rents the <see cref="WalRecord"/> buffer from
    /// <see cref="ArrayPool{T}.Shared"/>. Below this threshold the
    /// method allocates a fresh <c>WalRecord[count]</c> directly: the
    /// pool's small-bucket Clear-on-return cost (proportional to the
    /// bucket size, not to the caller's requested <c>count</c>) and
    /// the pool's reuse-keeps-arrays-live working-set effect together
    /// dominate the saving on small batches. Empirically, on the
    /// 2026-06-06 cohort that motivated this threshold:
    /// <list type="bullet">
    /// <item>An unconditional-pool first attempt regressed
    /// <c>BulkLoad_DeeperTree</c> (<c>MaxLeafKeys=4</c>, per-call
    /// <c>count=32</c>) +16% on <c>mean_ns</c> and widened its
    /// deterministic <c>alloc_b</c> IQR 13.85x.</item>
    /// <item>A threshold of 32 also regressed <c>BulkLoad_DeeperTree</c>
    /// (+4.5% <c>alloc_b</c>, IQR ratio 12.30x) because
    /// <c>count == 32</c> still selected the pool path and
    /// <c>Rent(32)</c> produced bimodal per-run allocation depending
    /// on which pool bucket the rent landed on.</item>
    /// <item>A threshold of 128 (this value) routes every workload
    /// with per-call <c>count &lt;= 64</c> through the direct path,
    /// preserving baseline behaviour on every small-leaf bench while
    /// pooling for the large-batch foreground commit shape -
    /// <c>SetMany_4Shards</c> (per-shard <c>count ~ 250</c>) and
    /// <c>BulkLoad</c> (single-shard <c>count = 1000</c>) - both of
    /// which took the full -36% to -38% <c>alloc_b</c> win.</item>
    /// </list>
    /// The pool-vs-allocation choice is hidden from the rest of the
    /// method by the <c>rentedFromPool</c> bool that the finally
    /// block branches on. clearArray=true is required on the pool
    /// path so the rented array does not pin <see cref="string"/> /
    /// <see cref="byte"/>[] / <see cref="VersionVector"/> references
    /// in the pool slot between rents.
    /// </summary>
    private const int CommitSetManyPoolThreshold = 128;

    /// <summary>
    /// Foreground bulk-write commit path. Collapses the per-key WAL
    /// round-trip into a single <see cref="Orleans.Lattice.BPlusTree.Grains.ICommitLogWriter.AppendManyAsync"/>
    /// call so a N-key batch routed to the same WAL partition pays one
    /// grain RPC instead of N. The per-key in-memory apply, observer
    /// publication, and digest publication still happen, but the
    /// digest funnel only fires once at the end of the batch (it is
    /// idempotent against <c>_digestDirty</c>) and the split predicate
    /// is checked once after every per-key apply has landed - if any
    /// per-key apply pushed the leaf above <see cref="ResolvedLatticeOptions.MaxLeafKeys"/>,
    /// a single <see cref="SplitAsync"/> runs at the end and produces
    /// the same <see cref="SplitResult"/> shape the per-key loop would
    /// have produced for its final overflowing entry.
    /// </summary>
    private async Task<SplitResult?> CommitSetManyAsync(List<KeyValuePair<string, byte[]>> entries)
    {
        using var _commitScope = EnterCommitScope();
        var count = entries.Count;
        // walEntries is a transient, single-use buffer: built once
        // below, passed once to ICommitLogWriter.AppendManyAsync via an
        // ArraySegment slice. On large batches the buffer is rented
        // from the shared pool and returned with clearArray=true so the
        // rented array does not pin string / byte[] / VersionVector
        // references in the pool slot. On small batches the pool
        // overhead (smallest bucket = 16 slots, Array.Clear cost is
        // proportional to bucket size not to `count`) dominates the
        // saving, so we allocate a fresh WalRecord[count] directly -
        // see CommitSetManyPoolThreshold for the empirical motivation.
        // The pool path saves a per-call WalRecord[count] heap
        // allocation (~130 B/slot * count) on the dominant foreground
        // bulk-write shape - 49.5% of the SetMany_4Shards -Profile
        // alloc top-N before this change, sourced from this method's
        // List<WalRecord>..ctor(int32) frame.
        var rentedFromPool = count >= CommitSetManyPoolThreshold;
        var walEntries = rentedFromPool
            ? ArrayPool<WalRecord>.Shared.Rent(count)
            : new WalRecord[count];
        try
        {
        var stamps = new HybridLogicalClock[count];
        var values = new LwwValue<byte[]>[count];

        // step 0 (build) - HLC tick per entry, WAL record per entry. We
        // do not call PublishVersionAdvance per entry; the version
        // vector tracks the highest local clock, so a single publish of
        // the last-assigned stamp at the end of the batch dominates
        // every individual entry's stamp by monotonic-tick construction.
        // BumpLocalRevision is also folded to a single call at the end
        // - the registry observes one revision bump per batched commit,
        // which is the correct shape for the LeafCacheGrain refresh
        // protocol (every refresh sees the whole batch as one logical
        // delta).
        var origin = LatticeOriginContext.Current;
        var vectorClock = LatticeVectorClockContext.Current;
        var delta = LatticeDeltaContext.Current;
        var transactionId = LatticeTransactionContext.Current;
        var category = LatticeMaintenanceContext.Current;
        var treeId = state.State.TreeId ?? string.Empty;
        var shardIndex = state.State.ShardIndex ?? 0;

        // Saga-flag pickup: isPrepared routes entries into the leaf's
        // pending-tx map via AddPreparedMutation (step 2 below); the
        // atomicBatch (Size, BaseIndex) pair plus optional
        // key->globalIndex map together stamp the wire-level
        // AtomicBatchSize / AtomicBatchIndex slots on every per-entry
        // WAL record so the receiver-side cross-cluster
        // atomic-visibility gate can reassemble the saga's siblings.
        // When the key->globalIndex map is present (the saga path
        // post-D1b), we prefer it because LatticeGrain.SetManyAsync's
        // shard-bucketing fan-out re-groups entries into per-shard
        // buckets and bucket-local position no longer equals
        // saga-global position. When absent (the foreground path, or
        // a saga that happens to land entirely on a single shard with
        // a contiguous slice), we fall back to BaseIndex + bucketLocal.
        var isPrepared = LatticePreparedContext.Current;
        var atomicBatch = LatticeAtomicBatchContext.Current;
        var atomicBatchSize = atomicBatch?.Size ?? 0;
        var atomicBatchBaseIndex = atomicBatch?.Index ?? 0;
        var atomicBatchIndexMap = LatticeAtomicBatchContext.CurrentIndexMap;
        // Per-entry author-delta carry. A cross-tree atomic write that
        // stages a distinct typed CRDT delta per entry (public staged
        // CRDT writes / flag-CRDT membership rows) supplies them through the
        // atomic-batch delta map keyed by entry key. The durable WAL
        // record is the source the replication shipper ships from, so the
        // per-entry delta MUST be stamped here - falling back to the
        // saga-wide / single-write carry when no per-key delta is present
        // keeps every value-only and saga-wide-only write byte-identical.
        var atomicBatchDeltaMap = LatticeAtomicBatchContext.CurrentDeltaMap;
        // Per-entry delete set. A mixed set+delete atomic batch supplies the
        // keys that must stage as prepared tombstones (MutationKind.Delete)
        // rather than value writes; the saga's terminal flips the whole mixed
        // batch visible (or drops it on abort) atomically. Null for every
        // upsert-only batch (saga or foreground), keeping the value path
        // byte-identical.
        var atomicBatchDeleteSet = LatticeAtomicBatchContext.CurrentDeleteSet;

        for (var i = 0; i < count; i++)
        {
            var key = entries[i].Key;
            var value = entries[i].Value;
            var stamp = AdvanceClockOrOverride();
            stamps[i] = stamp;
            var isDelete = atomicBatchDeleteSet is not null && atomicBatchDeleteSet.Contains(key);
            var lww = isDelete
                ? LwwValue<byte[]>.Tombstone(stamp)
                    with
                    {
                        OriginClusterId = origin,
                        VectorClock = vectorClock,
                    }
                : LwwValue<byte[]>.CreateWithExpiry(value, stamp, 0L)
                    with
                    {
                        OriginClusterId = origin,
                        VectorClock = vectorClock,
                    };
            values[i] = lww;
            int atomicBatchIndexForEntry;
            if (atomicBatchSize > 0)
            {
                if (atomicBatchIndexMap is not null
                    && atomicBatchIndexMap.TryGetValue(key, out var globalIndex))
                {
                    atomicBatchIndexForEntry = globalIndex;
                }
                else
                {
                    atomicBatchIndexForEntry = atomicBatchBaseIndex + i;
                }
            }
            else
            {
                atomicBatchIndexForEntry = 0;
            }
            walEntries[i] = new WalRecord
            {
                TreeId = treeId,
                Op = lww.IsTombstone ? MutationKind.Delete : MutationKind.Set,
                Key = key,
                Value = lww.IsTombstone ? null : lww.Value,
                Timestamp = lww.Timestamp,
                IsTombstone = lww.IsTombstone,
                ExpiresAtTicks = lww.ExpiresAtTicks,
                OriginClusterId = lww.OriginClusterId,
                VectorClock = lww.VectorClock,
                TransactionId = transactionId,
                Category = category,
                Delta = atomicBatchDeltaMap is not null
                    && atomicBatchDeltaMap.TryGetValue(key, out var perEntryWalDelta)
                    ? perEntryWalDelta
                    : delta,
                AtomicBatchSize = atomicBatchSize,
                AtomicBatchIndex = atomicBatchIndexForEntry,
                IsPrepared = isPrepared,
                ShardIndex = shardIndex,
            };
        }

        // Publish the highest assigned stamp once - the version vector
        // for this replica jumps directly to the batch's end. Prepared
        // saga writes are NOT visible in Entries (they live in the
        // pending-tx map until the terminal Commit broadcast), so
        // skipping the version-advance publish for prepared writes
        // matches the per-key CommitSetAsync prepared branch and
        // avoids advertising not-yet-visible writes to readers. The
        // local-revision bump still fires for prepared writes to
        // match CommitSetAsync (the registry's per-leaf revision
        // counter advances on every committed leaf RPC regardless of
        // visibility, so the LeafCacheGrain refresh protocol sees
        // every prepare-phase RPC as a single logical delta even
        // though the entries are invisible until the terminal flips).
        var highStamp = stamps[count - 1];
        if (!isPrepared)
        {
            PublishVersionAdvance(highStamp);
        }
        BumpLocalRevision();

        var options = await GetOptionsAsync();

        // step 1 (wal) - one batched dispatch. Pre-Apply failure here
        // leaves projection state untouched and surfaces the WAL error
        // to the caller, exactly matching the per-key path's contract.
        var walStartTicks = Stopwatch.GetTimestamp();
        var writer = ResolveCommitLogWriter();
        if (writer is not null)
        {
            // ArraySegment<T> implements IReadOnlyList<T>, so the
            // segment slice bounds AppendManyAsync's view to the first
            // `count` slots of the buffer (which may be oversized on
            // the pool path, exactly `count` on the direct-allocation
            // path). The boxing on the IReadOnlyList<WalRecord>
            // parameter is a single ~24 B allocation per call, dwarfed
            // by the per-call WalRecord[count] array allocation the
            // pool path replaces.
            await writer.AppendManyAsync(new ArraySegment<WalRecord>(walEntries, 0, count));
        }
        RecordCommitStep("wal", walStartTicks);

        // step 2 (apply) - per-key apply into the projection. Branches
        // on `isPrepared`: visible writes call StoreEntry (which merges
        // into Entries and runs the split predicate); prepared saga
        // writes call AddPreparedMutation (which buckets into the
        // pending-tx map and is invisible to readers until the saga's
        // terminal mark fires). The split predicate is checked once at
        // the end and only for the visible path; prepared writes never
        // trigger a leaf split because they are not yet visible in
        // Entries.
        var applyStartTicks = Stopwatch.GetTimestamp();
        SplitResult? splitResult = null;
        if (isPrepared)
        {
            // Per-entry CRDT-delta carry for the FOREGROUND saga. When the
            // staged write supplies a per-entry typed delta, record it in the
            // pending-tx side-map so the saga's terminal drain folds the delta
            // into this leaf's current visible state instead of installing the
            // pre-computed merged-state value last-writer-wins. Folding makes
            // the foreground contribution commute with any concurrent
            // replicated fold of a remote site's staged delta, so the two
            // clusters converge on the per-replica union regardless of the
            // order in which the local and replicated terminals drain.
            var preparedMode = atomicBatchDeltaMap is not null
                ? ResolveMergeMode()
                : LatticeMergeMode.LwwRegister;
            for (var i = 0; i < count; i++)
            {
                var key = entries[i].Key;
                var perEntryDelta = atomicBatchDeltaMap is not null
                    && atomicBatchDeltaMap.TryGetValue(key, out var d)
                    ? d
                    : null;
                AddPreparedMutation(
                    transactionId,
                    key,
                    values[i],
                    count,
                    delta: perEntryDelta,
                    mode: perEntryDelta is not null ? preparedMode : LatticeMergeMode.LwwRegister);
            }
        }
        else
        {
            for (var i = 0; i < count; i++)
            {
                StoreEntry(entries[i].Key, values[i]);
            }
            if (IsLeafOverCapacity(options.MaxLeafKeys, options.MaxLeafBytes))
            {
                splitResult = await SplitIfNeededUnderGateAsync(options.MaxLeafKeys, options.MaxLeafBytes);
            }
        }
        RecordCommitStep("apply", applyStartTicks);

        // step 3 (observer) - publish each committed entry under a
        // single commit-log scope. The fan-out shape is per-key (one
        // notification per mutation), matching the per-key loop's
        // contract; the saving in this method is in the WAL round-trip
        // and the digest publication, not the observer fan-out.
        // Prepared writes publish values[i] verbatim (the entry is in
        // the pending-tx map, not Cache), matching CommitSetAsync's
        // per-key observer branch; the observer payload's IsPrepared
        // flag tells downstream consumers the entry is not yet visible.
        // Per-entry atomic-batch index stamping: PublishSetAsync reads
        // LatticeAtomicBatchContext.Current to stamp the outbound
        // LatticeMutation's AtomicBatchSize / AtomicBatchIndex slots
        // (the wire-level metadata replication consumers use to
        // reconstruct saga sibling membership). When the saga's
        // key->globalIndex map is present we override the ambient
        // per-entry so each LatticeMutation carries its true
        // saga-global index regardless of how LatticeGrain.SetManyAsync
        // bucketed the entries; otherwise we fall back to BaseIndex + i
        // (matching the WAL-record stamping a few lines above).
        var observerStartTicks = Stopwatch.GetTimestamp();
        if (mutationObservers.HasObservers)
        {
            using (LatticeCommitLogContext.BeginScope())
            {
                var previousBatch = LatticeAtomicBatchContext.Current;
                try
                {
                    for (var i = 0; i < count; i++)
                    {
                        var key = entries[i].Key;
                        LwwValue<byte[]> published;
                        if (isPrepared)
                        {
                            published = values[i];
                        }
                        else
                        {
                            published = Cache.TryGetRow(key, out var committed) ? committed : values[i];
                        }
                        if (atomicBatchSize > 0)
                        {
                            int globalIndex;
                            if (atomicBatchIndexMap is not null
                                && atomicBatchIndexMap.TryGetValue(key, out var fromMap))
                            {
                                globalIndex = fromMap;
                            }
                            else
                            {
                                globalIndex = atomicBatchBaseIndex + i;
                            }
                            LatticeAtomicBatchContext.Current = (atomicBatchSize, globalIndex);
                        }
                        if (published.IsTombstone)
                        {
                            await PublishDeleteAsync(key, published);
                        }
                        else
                        {
                            await PublishSetAsync(key, published);
                        }
                    }
                }
                finally
                {
                    LatticeAtomicBatchContext.Current = previousBatch;
                }
            }
        }
        RecordCommitStep("observer", observerStartTicks);

        // step 4 (digest) - one digest fold for the whole batch.
        // Prepared writes route into the pending-tx map (not visible
        // Entries), so the running projection hash does NOT change
        // and PublishDigestUpwardAsync is a no-op via _digestDirty=
        // false. Foreground (non-prepared) writes flip _digestDirty
        // via StoreEntry's MarkDigestDirty call; the funnel coalesces
        // every per-key hash contribution into a single parent
        // notification.
        var digestStartTicks = Stopwatch.GetTimestamp();
        await PublishDigestUpwardAsync();
        RecordCommitStep("digest", digestStartTicks);

        return splitResult;
        }
        finally
        {
            // Return only the buffers we rented; direct-allocation
            // small-batch buffers are normal heap arrays and will be
            // collected by the GC. clearArray: true on the pool path
            // so the rented array does not pin string / byte[] /
            // VersionVector references in the pool slot between rents.
            if (rentedFromPool)
            {
                ArrayPool<WalRecord>.Shared.Return(walEntries, clearArray: true);
            }
        }
    }

    public async Task<bool> DeleteAsync(string key)
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.Delete);
        using var _mutationScope = EnterMutationScope();
        var isPrepared = LatticePreparedContext.Current;

        // Declared-span admission (see BPlusLeafGrain.SpanAdmission.cs). This
        // runs ahead of the absent-row short-circuit below: the row this delete
        // targets lives on the leaf that declares the key, so short-circuiting
        // here would report "nothing to delete" while leaving the row live, and
        // committing here would append a tombstone this leaf's own replay filter
        // drops. Forwarding is the only answer that makes the returned bool true.
        if (TryResolveSpanForwardTarget(key, out var spanTarget))
        {
            return await grainFactory.GetGrain<IBPlusLeafGrain>(spanTarget).DeleteAsync(key);
        }

        // For non-prepared deletes, the absent / tombstoned short-circuit
        // saves an HLC tick and a WAL append. For prepared deletes the
        // saga still expects a pending-tx entry, so we always emit a
        // tombstone into the pending bucket - committing the saga must
        // make the absence durable (the caller's pre-saga value is
        // captured separately by the saga coordinator).
        if (!isPrepared && (!Cache.TryGetRow(key, out var existing) || existing.IsTombstone))
        {
            return false;
        }

        // step 0 (build) - HLC tick (or override), build tombstone, build mutation envelope.
        // PublishVersionAdvance lifts Version[ReplicaId] to the tombstone's
        // own Timestamp; the cache filter `lww.Timestamp > callerClock`
        // then delivers the tombstone on its next refresh. See
        // CommitSetAsync for the full invariant.
        var stamp = AdvanceClockOrOverride();
        // Prepared deletes route to the pending-tx map and skip the
        // Version publication for the same reason as CommitSetAsync (see
        // the build-step comment there for the cache-callerClock argument).
        if (!isPrepared)
            PublishVersionAdvance(stamp);
        BumpLocalRevision();
        var tombstone = LwwValue<byte[]>.Tombstone(stamp)
            with
            {
                OriginClusterId = LatticeOriginContext.Current,
                VectorClock = LatticeVectorClockContext.Current,
            };
        var delta = LatticeDeltaContext.Current;
        var batch = LatticeAtomicBatchContext.Current;
        var transactionId = LatticeTransactionContext.Current;

        // step 1 (wal)
        var walStartTicks = Stopwatch.GetTimestamp();
        var writer = ResolveCommitLogWriter();
        if (writer is not null)
        {
            // Historical wire shape: the Delete path does not stamp
            // ShardIndex on the WAL record (see CommitSetAsync for the
            // Set path, which does). Preserved verbatim through the
            // direct-WalRecord builder migration so the apply-side
            // replay filter on the receiving leaf sees the same
            // default-0 shard slot it always saw on Delete entries.
            var entry = new WalRecord
            {
                TreeId = state.State.TreeId ?? string.Empty,
                Op = MutationKind.Delete,
                Key = key,
                Timestamp = tombstone.Timestamp,
                IsTombstone = true,
                OriginClusterId = tombstone.OriginClusterId,
                VectorClock = tombstone.VectorClock,
                TransactionId = transactionId,
                Category = LatticeMaintenanceContext.Current,
                Delta = delta,
                AtomicBatchSize = batch?.Size ?? 0,
                AtomicBatchIndex = batch?.Index ?? 0,
                IsPrepared = isPrepared,
            };
            await writer.AppendAsync(entry);
        }
        RecordCommitStep("wal", walStartTicks);

        // step 2 (apply) - LWW-merge into the in-memory projection, or
        // into the per-leaf pending-tx map when the mutation is a saga
        // prepare-phase write.
        var applyStartTicks = Stopwatch.GetTimestamp();
        if (isPrepared)
        {
            AddPreparedMutation(transactionId, key, tombstone);
        }
        else
        {
            StoreEntry(key, tombstone);
            // Tombstone has IsMigrated=false (default), so the merge
            // result inside StoreEntry clears any stale migration
            // marker for the same key naturally - no explicit cleanup
            // call required.
        }
        RecordCommitStep("apply", applyStartTicks);

        LatticeMetrics.LeafTombstonesCreated.Add(1, LeafTreeTag(), LeafTenantTag());

        // step 3 (observer) - inside a commit-log scope.
        var observerStartTicks = Stopwatch.GetTimestamp();
        if (mutationObservers.HasObservers)
        {
            using (LatticeCommitLogContext.BeginScope())
            {
                await PublishDeleteAsync(key, tombstone);
            }
        }
        RecordCommitStep("observer", observerStartTicks);

        // Forward the projection-hash delta to the parent internal
        // node. See CommitSetAsync for the no-op semantics. Wrapped in
        // the commit-step recorder so the per-write parent-digest RPC
        // is attributable on LeafCommitDuration alongside the wal /
        // apply / observer stages.
        var digestStartTicks = Stopwatch.GetTimestamp();
        await PublishDigestUpwardAsync();
        RecordCommitStep("digest", digestStartTicks);

        // Best-effort policy trigger: a fresh tombstone may push the
        // leaf past the configured ratio / size thresholds. No-op when
        // both knobs hold their defaults.
        EvaluateCompactionTrigger();

        return true;
    }

    public async Task<RangeDeleteResult> DeleteRangeAsync(string startInclusive, string endExclusive, LatticePredicateNode? predicate = null)
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.RangeDelete);
        using var _mutationScope = EnterMutationScope();
        // Collect matching keys. Entries is a SortedDictionary so we can
        // break early once we pass endExclusive - but we must still report
        // whether we observed a key >= endExclusive so the shard
        // coordinator can terminate the chain walk deterministically.
        //
        // NOT converted to a bounded Cache.EnumerateRange(...) walk, unlike the
        // sibling read seams on this surface (issue #2368). The work itself is
        // ranged and retains only keys, so it looks like the easiest
        // conversion here - but `pastRange` is derived from observing a key at
        // or above endExclusive, and a ranged walk yields no such key by
        // construction. A conversion that simply drops the observation reports
        // PastRange=false forever, and ShardRootGrain's range-delete chain walk
        // then visits every remaining leaf in the shard instead of stopping.
        //
        // A frame-index lower-bound probe is the obvious substitute and is
        // UNSOUND: Cache.Remove hydrates and pins the key's block before
        // removing the row, so a removed key stays in the frame's ordinal index
        // while being absent from the projection. Such a probe therefore
        // over-reports, and an over-reported PastRange truncates the walk and
        // silently leaves part of the range undeleted - trading a performance
        // fault for a correctness one. A sound probe has to ask for a resident
        // key at or above the bound, or an UNHYDRATED frame block at or above
        // it, which is new cache surface rather than a call-site change.
        //
        // BEWARE THE COMMIT RECORD HERE, WHICH OVERSTATES WHAT WAS CONVERTED.
        // 563681c99 carries the subject "stop the baseline freeze and range
        // delete detaching the leaf frame (#2835)". The "range delete" in that
        // subject is ApplyDeleteRange in BPlusLeafGrain.Projection.cs - the
        // REPLAYED range delete - and that commit does not touch this method at
        // all. The foreground DeleteRangeAsync you are reading is still a
        // whole-cache walk, deliberately, for the reasons above.
        //
        // That discrepancy is recorded here rather than quietly reconciled. A
        // merged commit subject cannot be rewritten, so the only place a reader
        // can discover the overstatement is from the source side, and an
        // epic-level reader reconciling subjects against the definition of done
        // would otherwise score this seam as converted when it is not. The
        // source is the honest record; the subject is the overstated one
        // (issue #2864).
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        List<string>? keysToDelete = null;
        var pastRange = false;
        foreach (var (key, lww) in Cache.EnumerateRows())
        {
            if (string.Compare(key, endExclusive, StringComparison.Ordinal) >= 0)
            {
                pastRange = true;
                break;
            }

            if (string.Compare(key, startInclusive, StringComparison.Ordinal) >= 0
                && !lww.IsTombstone && !lww.IsExpired(nowTicks))
            {
                // Predicate-filtered delete evaluates the predicate once,
                // here at write time, against the live value. The matched
                // keys are the only rows tombstoned and are recorded in the
                // WAL record / result so replay and replication reproduce
                // exactly this set without re-evaluating the predicate.
                if (predicate is { } pred && !LatticePredicateEvaluator.Matches(lww.Value, pred))
                    continue;
                (keysToDelete ??= []).Add(key);
            }
        }

        if (keysToDelete is null)
        {
            // Nothing to delete on this leaf - skip the WAL append, the
            // HLC tick, and every other step. The shard-level publish
            // helper still emits a per-shard DeleteRange mutation with
            // HybridLogicalClock.Zero so replication consumers propagate
            // the range unconditionally.
            return new RangeDeleteResult { Deleted = 0, PastRange = pastRange };
        }

        // step 0 (build) - HLC tick (or override), build tombstone, build
        // mutation envelope covering the whole range. The leaf does not
        // publish the per-range mutation - that's a shard-level concern -
        // but it still appends the range tombstone to the WAL so a future
        // replay applies the same set-of-keys closure rather than each
        // individual key. PublishVersionAdvance lifts Version[ReplicaId]
        // to the range tombstone's own stamp so the cache delta filter
        // delivers every fresh tombstone (see CommitSetAsync for the
        // full invariant).
        var stamp = AdvanceClockOrOverride();
        PublishVersionAdvance(stamp);
        BumpLocalRevision();
        var tombstone = LwwValue<byte[]>.Tombstone(stamp)
            with
            {
                OriginClusterId = LatticeOriginContext.Current,
                VectorClock = LatticeVectorClockContext.Current,
            };

        // step 1 (wal)
        var walStartTicks = Stopwatch.GetTimestamp();
        var writer = ResolveCommitLogWriter();
        if (writer is not null)
        {
            // Stamp the leaf's owning chain-shard index; see SetAsync
            // for the rationale. DeleteRange replay on the receiving
            // leaf iterates that leaf's own Entries only, so the
            // filter is not strictly required for correctness on
            // DeleteRange - but stamping consistently keeps every
            // mutation kind on the same wire shape so receivers and
            // operator tooling can rely on the slot being populated.
            var entry = WalRecordBuilder.ForDeleteRange(
                state.State.TreeId ?? string.Empty,
                state.State.ShardIndex ?? 0,
                startInclusive,
                endExclusive,
                tombstone,
                predicate is null ? null : keysToDelete);
            await writer.AppendAsync(entry);
        }
        RecordCommitStep("wal", walStartTicks);

        // step 2 (apply) - tombstone every matched key with the same HLC.
        var applyStartTicks = Stopwatch.GetTimestamp();
        foreach (var key in keysToDelete)
        {
            StoreEntry(key, tombstone);
            // Range tombstone has IsMigrated=false (default); merge
            // inside StoreEntry naturally clears any stale migration
            // marker - the flag rides with the value, not in a
            // side-channel map.
        }
        RecordCommitStep("apply", applyStartTicks);

        LatticeMetrics.LeafTombstonesCreated.Add(keysToDelete.Count, LeafTreeTag(), LeafTenantTag());

        // No leaf-level observer publish for DeleteRange - the shard
        // coordinator publishes one per-shard mutation after the
        // chain walk completes. RecordCommitStep("observer", ...) is
        // skipped to avoid recording a zero-duration measurement that
        // would skew the histogram for the legitimate per-key emit
        // step on Set / Delete.

        // Forward the projection-hash delta (an XOR-fold over every
        // tombstoned key's contribution swap) to the parent internal
        // node. A single publication covers the whole range. Wrapped
        // in the commit-step recorder so the per-range parent-digest
        // RPC is attributable on LeafCommitDuration alongside the wal
        // / apply stages (DeleteRange has no per-leaf observer step;
        // see the comment above).
        var digestStartTicks = Stopwatch.GetTimestamp();
        await PublishDigestUpwardAsync();
        RecordCommitStep("digest", digestStartTicks);

        // Best-effort policy trigger: range deletes can swing the
        // tombstone ratio sharply on a single leaf.
        EvaluateCompactionTrigger();

        return new RangeDeleteResult
        {
            Deleted = keysToDelete.Count,
            PastRange = pastRange,
            MatchedKeys = predicate is null ? null : keysToDelete,
        };
    }

    public async Task<int> CountAsync()
    {
        await AwaitReplayBarrierAsync();
        return await CountAsync(null, null);
    }

    public async Task<int> CountAsync(string? startInclusive, string? endExclusive)
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.RangeRead);
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        var (outcomes, pendingKeys) = await SnapshotPendingForReadAsync();

        // Honor the in-progress split boundary exactly as GetKeysAsync
        // does. During CompleteSplitAsync the donor's NextSibling already
        // points at the new sibling (which holds the right half) while the
        // donor's own cache still holds those same rows; the right half is
        // not removed until later in the same activation. A secondary-silo
        // restart that interrupts the split persists this in-between state,
        // so a chain-walk counter (ShardRootGrain.CountAsync ->
        // SimpleSumCountAsync) would visit the right half on both leaves and
        // over-count. Skipping rows >= SplitKey while a split is in progress
        // makes the donor report only the keys it still owns.
        // The [startInclusive, endExclusive) bounds use the same Ordinal
        // comparison semantics as GetKeysAsync so a ranged count (the
        // aggregation-view group-value count over [ReservedFloor, null))
        // matches an equivalent ranged key enumeration exactly, without
        // materialising any keys across the wire.
        //
        // Bounded, windowed hydration. Two things are needed and only the pair
        // works (issue #2368):
        //
        //   * WINDOWS. The whole-cache view calls HydrateAll, which ends in
        //     DetachSnapshot and leaves every row resident for the life of the
        //     activation, forfeiting the cheap frame-only division for good.
        //
        //   * A CLIP. A single Cache.EnumerateRange over the caller's bounds is
        //     NOT enough, and for the dominant caller it does nothing at all.
        //     CountAsync() passes (null, null), so that range spans every block;
        //     HydrateRange then protects the whole span in its TrimToBudget
        //     call, so nothing is evictable and every block ends up resident at
        //     once. Since issue #2843 that no longer detaches the frame - a
        //     completed ranged hydration retains it so a division can still
        //     bisect - but a whole-span single window still pins the entire leaf
        //     resident for the life of the operation, defeating the residency
        //     bound exactly as before (the transient whole-leaf buffer of issue
        //     #2842). Converted in form, unchanged in resident cost. Walking
        //     budget-sized windows keeps the trim able to evict behind us, which
        //     is what stops the whole leaf ever being resident at once.
        //
        // Clipping each window to the caller's [start, end) is what keeps a
        // genuinely ranged count cheap rather than whole-leaf. Windows ascend
        // and are disjoint, and the fold is a counter, so the total is identical
        // to the one-pass walk this replaced.
        //
        // This matters here more than anywhere else on this surface, because
        // CountAsync is reached from ShardRootGrain's warm-up probe on the root
        // leaf and from the no-split-yet fast path in LatticeGrain.CountAsync -
        // both of which describe it in comments as "cheap", and both of which
        // therefore ran before any division could.
        var splitInProgress = HasInterruptedSplit;
        var splitKey = state.State.SplitKey;
        var scanStart = startInclusive;
        var scanEnd = MinOrdinal(endExclusive, splitInProgress ? splitKey : null);
        var count = 0;
        foreach (var (windowStart, windowEnd) in Cache.GetFullScanWindowsWithoutHydrating())
        {
            if (scanEnd is not null && windowStart is not null &&
                string.CompareOrdinal(windowStart, scanEnd) >= 0)
                break;

            var from = MaxOrdinal(windowStart, scanStart);
            var to = MinOrdinal(windowEnd, scanEnd);
            if (from is not null && to is not null &&
                string.CompareOrdinal(from, to) >= 0)
                continue;

            foreach (var (key, lww) in Cache.EnumerateRange(from, to))
            {
                if (endExclusive is not null &&
                    string.Compare(key, endExclusive, StringComparison.Ordinal) >= 0)
                    break;

                if (splitInProgress && splitKey is not null &&
                    string.Compare(key, splitKey, StringComparison.Ordinal) >= 0)
                    break;

                if (startInclusive is not null &&
                    string.Compare(key, startInclusive, StringComparison.Ordinal) < 0)
                    continue;

                if (pendingKeys.TryGetValue(key, out var pending))
                {
                    var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
                    if (status == TxStatus.Committed)
                    {
                        if (!pending.value.IsTombstone && !pending.value.IsExpired(nowTicks)) count++;
                        continue;
                    }
                    // InFlight or Aborted - fall through to Entries
                    // (pre-saga visibility). See GetWithPendingAsync.
                }
                if (lww.IsTombstone || lww.IsExpired(nowTicks)) continue;
                count++;
            }
        }

        // Fresh committed pending keys that are NOT in Entries
        // (saga inserted a brand-new key) must also be counted.
        foreach (var (key, pending) in pendingKeys)
        {
            if (Cache.ContainsKey(key)) continue;
            if (endExclusive is not null &&
                string.Compare(key, endExclusive, StringComparison.Ordinal) >= 0)
                continue;
            if (splitInProgress && splitKey is not null &&
                string.Compare(key, splitKey, StringComparison.Ordinal) >= 0)
                continue;
            if (startInclusive is not null &&
                string.Compare(key, startInclusive, StringComparison.Ordinal) < 0)
                continue;
            var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
            if (status != TxStatus.Committed) continue;
            if (pending.value.IsTombstone || pending.value.IsExpired(nowTicks)) continue;
            count++;
        }

        return count;
    }

    public async Task<LeafStats> GetStatsAsync()
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.RangeRead);
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        var (outcomes, pendingKeys) = await SnapshotPendingForReadAsync();

        // Honor the in-progress split boundary as CountAsync / GetKeysAsync
        // do, so a donor mid-split (or durably stuck mid-split after a
        // secondary-silo restart) reports stats for only the keys it still
        // owns rather than double-counting the right half that already lives
        // on the new sibling.
        //
        // Walks in bounded key windows rather than over the whole-cache view,
        // for the reason ComputeFullProjectionHashFromState does: all three
        // accumulators are order-independent counters and the windows are
        // disjoint and exhaustive, so the totals are identical to the one-pass
        // walk this replaced. What changes is peak footprint - the whole-cache
        // view calls HydrateAll, which ends in DetachSnapshot and makes every
        // row resident for the life of the activation (issue #2368).
        var splitInProgress = HasInterruptedSplit;
        var splitKey = state.State.SplitKey;
        var scanEnd = splitInProgress ? splitKey : null;
        var live = 0;
        var tombstones = 0;
        var stateBytes = 0L;
        foreach (var (windowStart, windowEnd) in Cache.GetFullScanWindowsWithoutHydrating())
        {
            if (scanEnd is not null && windowStart is not null &&
                string.CompareOrdinal(windowStart, scanEnd) >= 0)
                break;

            foreach (var (key, lww) in Cache.EnumerateRange(windowStart, MinOrdinal(windowEnd, scanEnd)))
            {
                if (splitInProgress && splitKey is not null &&
                    string.Compare(key, splitKey, StringComparison.Ordinal) >= 0)
                    break;

                if (pendingKeys.TryGetValue(key, out var pending))
                {
                    var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
                    if (status == TxStatus.Committed)
                    {
                        if (pending.value.IsTombstone || pending.value.IsExpired(nowTicks)) tombstones++;
                        else live++;
                        stateBytes += EntryStateBytes(key, pending.value.Value);
                        continue;
                    }
                    // InFlight or Aborted - fall through to Entries
                    // (pre-saga visibility). See GetWithPendingAsync.
                }
                if (lww.IsTombstone || lww.IsExpired(nowTicks)) tombstones++;
                else live++;
                stateBytes += EntryStateBytes(key, lww.Value);
            }
        }

        // Fresh committed pending keys not yet in Entries.
        foreach (var (key, pending) in pendingKeys)
        {
            if (Cache.ContainsKey(key)) continue;
            if (splitInProgress && splitKey is not null &&
                string.Compare(key, splitKey, StringComparison.Ordinal) >= 0)
                continue;
            var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
            if (status != TxStatus.Committed) continue;
            if (pending.value.IsTombstone || pending.value.IsExpired(nowTicks)) tombstones++;
            else live++;
            stateBytes += EntryStateBytes(key, pending.value.Value);
        }

        return new LeafStats { LiveKeys = live, Tombstones = tombstones, StateBytes = stateBytes };
    }

    /// <summary>
    /// Approximate retained state byte footprint of a single leaf entry:
    /// the UTF-8 byte length of <paramref name="key"/> plus the stored
    /// value byte length. Feeds <see cref="LeafStats.StateBytes"/>;
    /// counts the logical key/value payload only and excludes per-entry
    /// CRDT metadata and persistence framing.
    /// </summary>
    private static long EntryStateBytes(string key, byte[]? value) =>
        System.Text.Encoding.UTF8.GetByteCount(key) + (value?.Length ?? 0);

    public Task<GrainId?> GetNextSiblingAsync() =>
        Task.FromResult(state.State.NextSibling);

    public async Task SetNextSiblingAsync(GrainId? siblingId)
    {
        // U9p step c2-iv-redux: serialise every public PersistAsync
        // site through the per-activation _splitGate. With
        // SetAsync / SetManyAsync / DeleteAsync marked
        // [AlwaysInterleave], the foreground split flow (which
        // already holds the gate via SplitIfNeededUnderGateAsync)
        // can release the activation turn at any of its cross-grain
        // awaits; without serialisation here, a sibling-pointer
        // update RPC from another leaf's split flow lands a
        // concurrent PersistAsync against the same row and the
        // loser of the etag CAS throws InconsistentStateException.
        // Mirrors c2-vi-followup's fix on BPlusInternalGrain.
        await _splitGate.WaitAsync().ConfigureAwait(true);
        try
        {
            state.State.NextSibling = siblingId;
            await PersistAsync();
        }
        finally
        {
            _splitGate.Release();
        }
    }

    public Task<GrainId?> GetPrevSiblingAsync() =>
        Task.FromResult(state.State.PrevSibling);

    public async Task SetPrevSiblingAsync(GrainId? siblingId)
    {
        // See SetNextSiblingAsync above for the gate rationale.
        await _splitGate.WaitAsync().ConfigureAwait(true);
        try
        {
            state.State.PrevSibling = siblingId;
            await PersistAsync();
        }
        finally
        {
            _splitGate.Release();
        }
    }

    public async Task SetTreeIdAsync(string treeId)
    {
        await AwaitReplayBarrierAsync();

        // See SetNextSiblingAsync above for the gate rationale.
        var treeIdJustSet = false;
        await _splitGate.WaitAsync().ConfigureAwait(true);
        try
        {
            if (state.State.TreeId is not null) return;
            var prevTreeId = state.State.TreeId;
            state.State.TreeId = treeId;
            try
            {
                await PersistAsync();
                treeIdJustSet = true;
            }
            catch
            {
                // Class B revert: a thrown WriteStateAsync leaves the
                // in-memory TreeId set while storage stays null. The
                // idempotency guard above would then short-circuit every
                // retry from this activation, permanently divorcing the
                // leaf's in-memory tree id from storage. Roll back the
                // in-memory assignment so the next call retries the persist.
                state.State.TreeId = prevTreeId;
                throw;
            }
        }
        finally
        {
            _splitGate.Release();
        }

        // Birth seam (root / bulk-load leaf): durably seed a Zero "block" pin
        // BEFORE the shard-root's follow-up MergeEntriesAsync makes this leaf's
        // data reachable in the WAL, so a forward trim driver cannot trim past
        // the leaf's un-materialised frontier in the pre-first-checkpoint
        // window. Idempotent and non-throwing; runs outside the split gate so
        // the durable pin RPC does not extend the gate hold.
        if (treeIdJustSet)
        {
            await SeedDurableMaterialiserBlockPinAsync();
        }
    }

    /// <summary>
    /// Returns this leaf's tree id. <b>Metadata, deliberately NOT gated on the
    /// replay</b> (issue #2871 acceptance criterion 2): the tree id is persisted
    /// grain state that the replay neither reads nor writes, so waiting on the
    /// replay would buy no correctness and would recreate the wedge this issue
    /// removes - this is the probe the WAL GC blocked-leaf reactivation sweep
    /// (issues #2768 / #2870) uses to reach a leaf whose replay cannot complete.
    /// </summary>
    /// <remarks>
    /// The <see cref="EnsureReplayStarted"/> call is a deliberate side effect on a
    /// getter and is the point of the call from the sweep's perspective. The
    /// sweep's remedy is that the leaf REPAIRS ITSELF; the probe merely causes it.
    /// Rearming here is what makes a touch work on a leaf whose previous replay
    /// faulted or was cancelled, without which the sweep would deliver its probe
    /// successfully - flipping <c>undelivered</c> to <c>completed</c> - while
    /// <c>healed</c> stayed at zero forever. It is non-blocking and non-throwing,
    /// so it cannot make this getter slow or fail.
    /// </remarks>
    public Task<string?> GetTreeIdAsync()
    {
        EnsureReplayStarted();
        return Task.FromResult(state.State.TreeId);
    }

    public async Task SetShardIndexAsync(int shardIndex)
    {
        await AwaitReplayBarrierAsync();

        // See SetNextSiblingAsync above for the gate rationale.
        await _splitGate.WaitAsync().ConfigureAwait(true);
        try
        {
            // Idempotent: skip the persist if the slot is already seeded.
            // The shard-root coordinator calls this once per leaf-create
            // alongside SetTreeIdAsync; a re-call (e.g. from a defensive
            // re-seed in a future code path) must not silently overwrite
            // the persisted value, both because the value is immutable
            // for a leaf's lifetime and because the writer would
            // otherwise pay an extra WriteStateAsync round-trip on every
            // shard-root activation that walks its leaves.
            if (state.State.ShardIndex is not null) return;
            var prevShardIndex = state.State.ShardIndex;
            state.State.ShardIndex = shardIndex;
            try
            {
                await PersistAsync();
            }
            catch
            {
                // Class B revert: see SetTreeIdAsync above. Without this,
                // the activation stamps ShardIndex on every foreground
                // commit while every peer (or a future reactivation) still
                // sees a null slot, and the replay-time ownership filter on
                // the cross-shard fanout regression gate silently drops the
                // legitimate records.
                state.State.ShardIndex = prevShardIndex;
                throw;
            }
        }
        finally
        {
            _splitGate.Release();
        }
    }

    public async Task SetKeyRangeAsync(string? lowKeyInclusive, string? highKeyExclusive)
    {
        await AwaitReplayBarrierAsync();

        // See SetNextSiblingAsync above for the gate rationale.
        await _splitGate.WaitAsync().ConfigureAwait(true);
        try
        {
            // Idempotent on the low bound: every legitimate caller
            // (CompleteSplitAsync stamping a freshly-created sibling)
            // passes a non-null splitKey as the low bound, so a non-null
            // persisted LowKeyInclusive is the unambiguous "already
            // seeded" sentinel. Donors never call this - they update
            // their own HighKeyExclusive directly inside CompleteSplitAsync
            // when narrowing their own range to the split key.
            if (state.State.LowKeyInclusive is not null) return;
            var prevLowKey = state.State.LowKeyInclusive;
            var prevHighKey = state.State.HighKeyExclusive;
            state.State.LowKeyInclusive = lowKeyInclusive;
            state.State.HighKeyExclusive = highKeyExclusive;
            try
            {
                await PersistAsync();
            }
            catch
            {
                // Class B revert: see SetTreeIdAsync above. Both fields
                // are restored together because the guard short-circuits
                // on LowKeyInclusive alone - leaving the in-memory range
                // even partially seeded would re-route range scans against
                // a topology storage never accepted.
                state.State.LowKeyInclusive = prevLowKey;
                state.State.HighKeyExclusive = prevHighKey;
                throw;
            }
        }
        finally
        {
            _splitGate.Release();
        }
    }

    public Task<LeafKeyRange> GetKeyRangeAsync() =>
        Task.FromResult(new LeafKeyRange
        {
            LowKeyInclusive = state.State.LowKeyInclusive,
            HighKeyExclusive = state.State.HighKeyExclusive,
        });

    public async Task SetCheckpointOffsetHintsAsync(long[] offsetsByPartition)
    {
        await AwaitReplayBarrierAsync();

        ArgumentNullException.ThrowIfNull(offsetsByPartition);

        // Apply one hint per WAL partition under that partition's apply-offset
        // scope so the per-partition clamp targets the right offset space.
        //
        // The scope is opened HERE, inside the callee, and that is the whole
        // point of the signature. The removed singular form took only an offset
        // and let the callee resolve its partition from
        // LatticeApplyOffsetContext.CurrentPartition ?? 0; that context is an
        // AsyncLocal and does not flow across an Orleans grain call, so the
        // scope was always absent at the callee and every hint silently landed
        // on partition 0 (issue #2699). Carrying the partition in the argument
        // is what makes the scoping something the caller cannot fail to supply.
        //
        // Routes through ApplyCheckpointHintAsync, the shared hint seam, so the
        // unresolved-prepare clamp is honoured AND a hint that no longer moves
        // this leaf forward is dropped before it reaches
        // ILeafProjection.SetCheckpointOffsetAsync. That seam REJECTS a
        // backward move by throwing rather than absorbing it, and this callee
        // is not always fresh: a split retry re-sends the heads captured at the
        // original split to a sibling that has since applied past them (issue
        // #3360).
        for (var p = 0; p < offsetsByPartition.Length; p++)
        {
            await ApplyCheckpointHintAsync(p, offsetsByPartition[p]);
        }
    }

    public async Task InitializeSiblingAsync(SiblingInitialization init)
    {
        await AwaitReplayBarrierAsync();

        // Batched birth-time seeding for a freshly created split sibling.
        // Collapses the five separate gated setter RPCs (tree id, shard
        // index, key range, next/prev sibling pointers) the donor used to
        // issue serially into one gate acquire and one PersistAsync, and
        // additionally seeds the donor's moved-away seal, which never had a
        // setter of its own (issue 3121).
        // Preserves each setter's idempotent semantics: the write-once
        // slots (tree id, shard index, key-range low bound) are skipped
        // when already seeded, so a recovery-path re-call against a
        // partially-seeded sibling is safe. The seal is unioned rather than
        // write-once for the same reason, stated at its own site below.
        await _splitGate.WaitAsync().ConfigureAwait(true);
        try
        {
            // Snapshot the pre-seed values so a thrown PersistAsync can
            // revert the whole batch together (Class B revert; see the
            // individual setters for the per-slot rationale). Reverting
            // all-or-nothing matches the persist being all-or-nothing.
            var prevTreeId = state.State.TreeId;
            var prevShardIndex = state.State.ShardIndex;
            var prevLowKey = state.State.LowKeyInclusive;
            var prevHighKey = state.State.HighKeyExclusive;
            var prevNext = state.State.NextSibling;
            var prevPrev = state.State.PrevSibling;
            var prevMovedSlots = state.State.MovedAwaySlots;
            var prevMovedVsc = state.State.MovedAwayVirtualShardCount;

            var changed = false;

            // Tree id: write-once.
            if (state.State.TreeId is null && init.TreeId is not null)
            {
                state.State.TreeId = init.TreeId;
                changed = true;
            }

            // Shard index: write-once.
            if (state.State.ShardIndex is null && init.ShardIndex is { } shardIndex)
            {
                state.State.ShardIndex = shardIndex;
                changed = true;
            }

            // Key range: the low bound is the seeded sentinel (every
            // legitimate sibling-birth caller passes a non-null low
            // bound). Both bounds move together so a range scan never
            // observes a half-seeded range.
            if (state.State.LowKeyInclusive is null)
            {
                state.State.LowKeyInclusive = init.LowKeyInclusive;
                state.State.HighKeyExclusive = init.HighKeyExclusive;
                changed = true;
            }

            // Sibling pointers: overwrite unconditionally (the split flow
            // is the sole authority for the freshly created sibling's
            // links at birth).
            if (!Equals(state.State.NextSibling, init.NextSibling))
            {
                state.State.NextSibling = init.NextSibling;
                changed = true;
            }
            if (!Equals(state.State.PrevSibling, init.PrevSibling))
            {
                state.State.PrevSibling = init.PrevSibling;
                changed = true;
            }

            // The donor's moved-away seal. Unioned rather than assigned, so seeding
            // is monotonic: a seal is sticky by design, and a re-call against a
            // partially seeded sibling must never drop a slot. The donor keeps its
            // own seal - this is a copy, not a move.
            //
            // Deliberately NOT gated on the write-once test the range uses. A
            // freshly minted sibling has no seal, so the two agree in the universal
            // case; but where they differ - a sibling that has somehow already been
            // sealed - the write-once rule would silently discard the donor's seal,
            // which is the very defect this seeding exists to close. Issue 3121.
            if (MovedAwaySealInheritance.TryInherit(
                    state.State.MovedAwaySlots,
                    state.State.MovedAwayVirtualShardCount,
                    init.MovedAwaySlots,
                    init.MovedAwayVirtualShardCount,
                    out var inheritedSlots,
                    out var inheritedVsc))
            {
                // Copy when the adopted array is the sender's own instance. The core
                // is a pure function and hands the donor's array straight back on the
                // universal path, which is right for a core but wrong to retain here:
                // this array becomes durable state, and a co-located donor's
                // [Immutable] payload is handed over without a deep copy, so
                // retaining it would alias two leaves' persisted state to one array.
                // That is the same ingress rule TrackedCrdtCarrierExemptions records,
                // and ImmutableGrainBoundaryContractTests enforces it.
                //
                // The union path already built a fresh array, so it is not copied
                // again, and an unsealed donor never reaches here at all. The cost is
                // therefore one small copy per split of a sealed leaf.
                state.State.MovedAwaySlots = ReferenceEquals(inheritedSlots, init.MovedAwaySlots)
                    ? inheritedSlots!.AsSpan().ToArray()
                    : inheritedSlots;
                state.State.MovedAwayVirtualShardCount = inheritedVsc;
                changed = true;
            }

            if (changed)
            {
                try
                {
                    await PersistAsync();
                }
                catch
                {
                    state.State.TreeId = prevTreeId;
                    state.State.ShardIndex = prevShardIndex;
                    state.State.LowKeyInclusive = prevLowKey;
                    state.State.HighKeyExclusive = prevHighKey;
                    state.State.NextSibling = prevNext;
                    state.State.PrevSibling = prevPrev;
                    state.State.MovedAwaySlots = prevMovedSlots;
                    state.State.MovedAwayVirtualShardCount = prevMovedVsc;
                    throw;
                }
            }
        }
        finally
        {
            _splitGate.Release();
        }

        // Birth seam (split sibling): durably seed a Zero "block" pin BEFORE the
        // donor's CompleteSplitAsync proceeds to MergeEntriesAsync, which is
        // what appends this sibling's inherited entries to the WAL. Awaiting the
        // seed here guarantees the WAL GC sees a floor for the sibling the
        // moment its data becomes reachable, so a forward trim driver
        // (replication shipper, materialised view, or the wall-clock TTL
        // ceiling) cannot trim past the sibling's un-materialised frontier in
        // the window before its first checkpoint. Idempotent (the pin store's
        // monotonic-max merge coalesces a Zero re-seed once a real frontier has
        // landed) and non-throwing, so a recovery-path re-call is safe. Runs
        // outside the split gate so the durable pin RPC does not extend the gate
        // hold. Guarded on a set tree id so a malformed init (no tree id) is a
        // no-op rather than seeding against an empty consumer id.
        if (state.State.TreeId is not null)
        {
            await SeedDurableMaterialiserBlockPinAsync(init.WalHeadsAtBirth);
        }
    }

    public async Task<int> CompactTombstonesAsync(TimeSpan gracePeriod)
    {
        await AwaitReplayBarrierAsync();

        // Skip scan if nothing has changed since last compaction.
        if (state.State.LastCompactionVersion.DominatesOrEquals(state.State.Version))
        {
            // Sample the current tombstone ratio on the per-leaf
            // histogram and notify the pass that this leaf was a
            // no-op so the visited-counter stays accurate.
            SampleLeafTombstoneRatio();
            var noopTreeTag = LeafTreeTag();
            var noopTenantTag = LeafTenantTag();
            var noopTriggerTag = CompactionTriggerTag();
            var noopPathTag = CompactionPathTag();
            if (noopTriggerTag is { } noopTrig)
            {
                if (noopPathTag is { } noopPath)
                    LatticeMetrics.CompactionLeavesVisited.Add(
                        1,
                        new System.Diagnostics.TagList
                        {
                            noopTreeTag,
                            LatticeMetrics.OutcomeNoop,
                            noopTrig,
                            noopPath,
                            noopTenantTag,
                        });
                else
                    LatticeMetrics.CompactionLeavesVisited.Add(
                        1,
                        new System.Diagnostics.TagList
                        {
                            noopTreeTag,
                            LatticeMetrics.OutcomeNoop,
                            noopTrig,
                            noopTenantTag,
                        });
            }
            else if (noopPathTag is { } noopPath2)
            {
                LatticeMetrics.CompactionLeavesVisited.Add(
                    1,
                    new System.Diagnostics.TagList
                    {
                        noopTreeTag,
                        LatticeMetrics.OutcomeNoop,
                        noopPath2,
                        noopTenantTag,
                    });
            }
            else
            {
                LatticeMetrics.CompactionLeavesVisited.Add(1, noopTreeTag, LatticeMetrics.OutcomeNoop, noopTenantTag);
            }
            return 0;
        }

        // Pre-scan ratio sample so dashboards see space-amplification
        // hot spots even on passes that ultimately reap nothing in
        // this grace window (every tombstone still inside the grace).
        SampleLeafTombstoneRatio();

        // Tombstone-reap is a structural rewrite of state already authored
        // by user writes - not a semantic causal event. Wrap the entire
        // body in a maintenance scope so every emitted WAL envelope is
        // stamped `Category = MutationCategory.Maintenance` independently
        // of whether the caller (e.g. `TombstoneCompactionGrain` vs. a
        // direct test invocation) supplied an outer scope. The
        // `LatticeMaintenanceContext` is `RequestContext`-backed, so a
        // pre-existing maintenance bit set by the coordinator is preserved
        // by the disposal sequence (the inner scope restores the prior
        // value, not the absence of one).
        using var maintenanceScope = LatticeMaintenanceContext.BeginScope();

        var startTicks = Stopwatch.GetTimestamp();
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        var cutoff = nowTicks - gracePeriod.Ticks;
        var toRemove = new List<(string Key, HybridLogicalClock ReapAt)>();
        var anyInGraceRemaining = false;
        var tombstonesRemoved = 0;
        var expiredRemoved = 0;

        foreach (var (windowStart, windowEnd) in Cache.GetFullScanWindowsWithoutHydrating())
        {
            // Bounded windows rather than the whole-cache view: the scan
            // retains only the key and reap stamp of each condemned row, never
            // its payload, so it needs to VISIT every row and not to hold one.
            // The whole-cache view calls HydrateAll, which ends in
            // DetachSnapshot and holds all of them for the life of the
            // activation - on precisely the oversized, tombstone-heavy leaf a
            // reap is trying to shrink (issue #2368). The windows are disjoint,
            // exhaustive and ascending, so the condemned set and its order are
            // identical to the one-pass walk this replaced.
            foreach (var (key, lww) in Cache.EnumerateRange(windowStart, windowEnd))
            {
                if (lww.IsTombstone)
                {
                    if (lww.Timestamp.WallClockTicks <= cutoff)
                    {
                        toRemove.Add((key, lww.Timestamp));
                        tombstonesRemoved++;
                    }
                    else
                    {
                        // Tombstone is still within the grace window - a future pass
                        // must re-scan it once the grace has elapsed.
                        anyInGraceRemaining = true;
                    }
                    continue;
                }

                // Reap expired live entries past the same grace period.
                // Reads already hide them; a short retention after expiry protects
                // against a stale merge resurrecting the entry (another replica
                // whose clock is behind could re-send the pre-expiry LwwValue).
                if (lww.ExpiresAtTicks != 0 && lww.ExpiresAtTicks <= nowTicks)
                {
                    if (lww.ExpiresAtTicks <= cutoff)
                    {
                        toRemove.Add((key, lww.Timestamp));
                        expiredRemoved++;
                    }
                    else
                    {
                        anyInGraceRemaining = true;
                    }
                }
            }
        }

        // WAL-as-sole-commit-point: every reaped key is durably committed
        // by appending one `LatticeMutation { Kind = Tombstone, IsMerge = true }`
        // envelope per removed entry to the per-shard WAL before the
        // in-memory removal. Activation-time replay routes the envelope
        // through `ApplyTombstoneReap(...)` which physically removes
        // the entry iff the existing local entry is still a tombstone
        // or expired live entry. The replay handler does its own HLC
        // dominance check so a Tombstone envelope cannot resurrect a
        // freshly-rewritten live entry that landed after the reap
        // envelope was written.
        //
        // The reap envelope's HLC is the existing tombstone / expired
        // entry's own timestamp - reusing it (instead of Tick()ing) is
        // intentional: the envelope's intent is "remove the entry whose
        // current HLC is X", and stamping X verbatim makes the replay
        // dominance check `existing.Timestamp <= mutation.Timestamp`
        // trivially satisfied for the entry the compactor saw, while
        // any later live rewrite (with HLC > X) is automatically
        // protected from accidental reap by the same comparison.
        if (toRemove.Count > 0)
        {
            var writer = ResolveCommitLogWriter();
            var treeId = state.State.TreeId ?? string.Empty;
            var shardIndex = state.State.ShardIndex ?? 0;
            var origin = LatticeOriginContext.Current;
            var vc = LatticeVectorClockContext.Current;
            var transactionId = LatticeTransactionContext.Current;
            var maintenance = LatticeMaintenanceContext.Current;

            foreach (var (key, reapAt) in toRemove)
            {
                if (writer is not null)
                {
                    var entry = new WalRecord
                    {
                        TreeId = treeId,
                        Op = MutationKind.Tombstone,
                        Key = key,
                        Timestamp = reapAt,
                        IsTombstone = true,
                        OriginClusterId = origin,
                        VectorClock = vc,
                        TransactionId = transactionId,
                        Category = maintenance,
                        IsPrepared = false,
                        IsMerge = true,
                        ShardIndex = shardIndex,
                    };

                    var walStartTicks = Stopwatch.GetTimestamp();
                    try
                    {
                        await writer.AppendAsync(entry);
                    }
                    finally
                    {
                        var elapsedMs = (Stopwatch.GetTimestamp() - walStartTicks) * 1000.0 / Stopwatch.Frequency;
                        LatticeMetrics.LeafWriteDuration.Record(elapsedMs,
                            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId),
                            LatticeMetrics.KindCompact,
                            LatticeTenantLabel.ForTree(treeId));
                    }
                }

                RemoveEntry(key);
            }
        }

        // Only mark this version as "fully compacted" when no tombstones were
        // left in the grace window. Stamping while tombstones remain would
        // dead-end every subsequent pass until a new write ticks the version
        // vector (audit bug #2). The advance lives in-memory only; the next
        // projection-checkpoint flush snapshots it alongside Entries, and a
        // missed flush before deactivation simply causes the next activation
        // to re-scan once (no data loss).
        if (!anyInGraceRemaining)
            state.State.LastCompactionVersion = state.State.Version.Clone();

        var elapsedTotalMs = (Stopwatch.GetTimestamp() - startTicks) * 1000.0 / Stopwatch.Frequency;
        var treeTag = LeafTreeTag();
        var tenantTag = LeafTenantTag();
        var triggerTag = CompactionTriggerTag();
        var pathTag = CompactionPathTag();
        var outcomeTag = toRemove.Count > 0 ? LatticeMetrics.OutcomeReaped : LatticeMetrics.OutcomeNoop;
        if (triggerTag is { } trig)
        {
            LatticeMetrics.LeafCompactionDuration.Record(elapsedTotalMs, treeTag, trig, tenantTag);
            if (tombstonesRemoved > 0)
                LatticeMetrics.LeafTombstonesReaped.Add(tombstonesRemoved, treeTag, trig, tenantTag);
            if (expiredRemoved > 0)
                LatticeMetrics.LeafTombstonesExpired.Add(expiredRemoved, treeTag, trig, tenantTag);

            // Pass-level per-leaf outcome: reaped if at least one entry
            // was physically removed, otherwise noop (the leaf had work
            // pending but every tombstone was still in the grace window).
            if (pathTag is { } path)
                LatticeMetrics.CompactionLeavesVisited.Add(
                    1,
                    new System.Diagnostics.TagList
                    {
                        treeTag,
                        outcomeTag,
                        trig,
                        path,
                        tenantTag,
                    });
            else
                LatticeMetrics.CompactionLeavesVisited.Add(
                    1,
                    new System.Diagnostics.TagList
                    {
                        treeTag,
                        outcomeTag,
                        trig,
                        tenantTag,
                    });
        }
        else
        {
            LatticeMetrics.LeafCompactionDuration.Record(elapsedTotalMs, treeTag, tenantTag);
            if (tombstonesRemoved > 0)
                LatticeMetrics.LeafTombstonesReaped.Add(tombstonesRemoved, treeTag, tenantTag);
            if (expiredRemoved > 0)
                LatticeMetrics.LeafTombstonesExpired.Add(expiredRemoved, treeTag, tenantTag);

            if (pathTag is { } path)
                LatticeMetrics.CompactionLeavesVisited.Add(
                    1,
                    new System.Diagnostics.TagList
                    {
                        treeTag,
                        outcomeTag,
                        path,
                        tenantTag,
                    });
            else
                LatticeMetrics.CompactionLeavesVisited.Add(1, treeTag, outcomeTag, tenantTag);
        }

        // Forward the projection-hash delta from the reaped tombstones
        // (and the entry-count shrinkage) to the parent internal node.
        // No-op when no entries were actually removed. Structural
        // event - bypass the c2-xxviii coalescing window.
        await PublishDigestUpwardInlineAsync();

        return toRemove.Count;
    }

    public async Task<StateDelta> GetDeltaSinceAsync(VersionVector sinceVersion)
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.RangeRead);
        // NOTE: Replication paths intentionally propagate expired entries.
        // Readers filter them via LwwValue.IsExpired; shipping them to peers
        // preserves CRDT convergence so LWW can resolve by timestamp on
        // replicas whose wall clocks are drifted. CompactTombstonesAsync
        // reaps them after the configured grace period on each replica.
        // If the caller's version dominates ours, they already have everything.
        if (sinceVersion.DominatesOrEquals(state.State.Version))
        {
            // Steady-state fast path: no pending split, no moved-away
            // slots to advertise. Return the process-wide empty-delta
            // singleton so the receiver's VersionVector.Merge folds in
            // nothing (the caller already dominates) and we elide three
            // heap allocations per read. See EmptyDelta XML doc above
            // for the safety argument.
            if (state.State.SplitKey is null
                && (state.State.MovedAwaySlots is null || state.State.MovedAwaySlots.Length == 0))
            {
                return await EmptyDeltaTask;
            }

            // SplitKey or MovedAwaySlots is set: the caller needs the
            // prune signal even though Entries is empty. Allocate a
            // per-call envelope so the signal is observed; this branch
            // is rare (only fires between a split / moved-away commit
            // and the next compaction sweep).
            return new StateDelta
            {
                Entries = EmptyEntries,
                Version = state.State.Version.Clone(),
                SplitKey = state.State.SplitKey,
                MovedAwaySlots = state.State.MovedAwaySlots is { Length: > 0 } ms ? ms : null,
                MovedAwayVsc = state.State.MovedAwayVirtualShardCount,
            };
        }

        // Return all entries whose timestamp is newer than what the caller has seen.
        // We compare each entry's timestamp against the caller's clock for our replica.
        var callerClock = sinceVersion.GetClock(ReplicaId);
        var changed = new Dictionary<string, LwwValue<byte[]>>();

        // Bounded windows rather than the whole-cache view. The delta retains
        // only the rows that beat the caller's clock, which on a caught-up
        // caller is none at all - yet the whole-cache view calls HydrateAll
        // regardless, making every row resident for the life of the activation
        // and forfeiting the cheap frame-only division (issue #2368). The
        // result is a key-addressed map built from disjoint, exhaustive
        // windows, so it is identical to the one-pass walk this replaced.
        foreach (var (windowStart, windowEnd) in Cache.GetFullScanWindowsWithoutHydrating())
        {
            foreach (var (key, lww) in Cache.EnumerateRange(windowStart, windowEnd))
            {
                if (lww.Timestamp > callerClock)
                {
                    changed[key] = lww;
                }
            }
        }

        return new StateDelta
        {
            Entries = changed,
            Version = state.State.Version.Clone(),
            SplitKey = state.State.SplitKey,
            MovedAwaySlots = state.State.MovedAwaySlots is { Length: > 0 } ms2 ? ms2 : null,
            MovedAwayVsc = state.State.MovedAwayVirtualShardCount,
        };
    }

    public async Task<StateDelta> GetDeltaSinceForSlotsAsync(VersionVector sinceVersion, int[] sortedMovedSlots, int virtualShardCount)
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.RangeRead);
        ArgumentNullException.ThrowIfNull(sinceVersion);
        ArgumentNullException.ThrowIfNull(sortedMovedSlots);

        if (sortedMovedSlots.Length == 0 || sinceVersion.DominatesOrEquals(state.State.Version))
        {
            return await EmptyDeltaTask;
        }

        var callerClock = sinceVersion.GetClock(ReplicaId);
        var changed = new Dictionary<string, LwwValue<byte[]>>();

        // Bounded windows, as GetDeltaSinceAsync uses. The slot filter makes
        // the retained set narrower still - only rows hashing into a moved
        // slot survive it - so paying HydrateAll for the whole leaf to answer
        // it was the worst ratio on this surface.
        foreach (var (windowStart, windowEnd) in Cache.GetFullScanWindowsWithoutHydrating())
        {
            foreach (var (key, lww) in Cache.EnumerateRange(windowStart, windowEnd))
            {
                if (lww.Timestamp <= callerClock) continue;
                var slot = ShardMap.GetVirtualSlot(key, virtualShardCount);
                if (Array.BinarySearch(sortedMovedSlots, slot) < 0) continue;
                changed[key] = lww;
            }
        }

        return new StateDelta
        {
            Entries = changed,
            Version = state.State.Version.Clone(),
            SplitKey = state.State.SplitKey,
            MovedAwaySlots = state.State.MovedAwaySlots is { Length: > 0 } ms3 ? ms3 : null,
            MovedAwayVsc = state.State.MovedAwayVirtualShardCount,
        };
    }

    public async Task MergeEntriesAsync(Dictionary<string, LwwValue<byte[]>> entries)
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.Write);
        using var _mutationScope = EnterMutationScope();
#if LATTICE_DIAG
        // DIAG leaf-cross-leaf-merge: fires when a sibling leaf or
        // a split-source leaf hands a batch of LWW values into this
        // leaf. This path stamps IsMigrated=true on every incoming
        // entry (see StoreEntry call below) but does NOT update
        // MovedAwaySlots on the SOURCE leaf - that mask is driven only
        // by ShardRootGrain.MarkLeavesMovedAwayAsync during a shard-
        // wide split. The V_{N-2} regression in Section 14 hinges on
        // whether a slot migration arrives via this path (no source-
        // side mask) or via the shard-split path (source-side mask
        // present). The DIAG event records the merge size and a key
        // sample so the trace can attribute each post-merge
        // commit-key event to the correct upstream channel.
        var diagKeySample = entries.Count == 0
            ? string.Empty
            : string.Join(",", entries.Keys.Take(8));
        DiagSink.Write($"[DIAG leaf-cross-leaf-merge] gid={context.GrainId} entriesCount={entries.Count} keySample=[{diagKeySample}] currentMovedSlots=[{(state.State.MovedAwaySlots is null ? "" : string.Join(',', state.State.MovedAwaySlots))}] currentClock={state.State.Clock}");
#endif
        // NOTE: Expired entries are merged as-is and not filtered here.
        // Replication must preserve them so CRDT LWW convergence is resolved
        // by timestamp, not by the wall clock of whichever replica happens to
        // see a write first. Readers filter expired entries; compaction reaps
        // them after the grace period.
        //
        // Track the high-water timestamp of the incoming batch so we can
        // (a) advance state.State.Clock past it (audit bug #3), and (b)
        // publish it as Version[ReplicaId] so LeafCacheGrain delta checks
        // detect the new entries. Using VersionVector.Tick(ReplicaId) here
        // would key the advance off DateTimeOffset.UtcNow.Ticks and could
        // land strictly above the merged entries' Timestamps - causing the
        // cache's `lww.Timestamp > callerClock` delta filter to silently
        // drop the freshly-merged values on its next refresh. Publishing
        // maxIncoming keeps Version[ReplicaId] equal to the latest stamp
        // any merged entry actually carries.
        //
        // WAL-as-sole-commit-point: every incoming entry is durably
        // committed by appending a LatticeMutation { Kind = Set | Delete,
        // IsMerge = true, ... } to the per-shard WAL via ICommitLogWriter
        // before the in-memory projection mutation (StoreEntry). The WAL
        // append is the durability point; crash recovery rebuilds Entries
        // from the WAL via the activation-time replay path. The legacy
        // standalone state-row persist that used to follow this loop is
        // gone - every leaf foreground commit now obeys the
        // WAL-as-sole-commit-point invariant.
        var writer = ResolveCommitLogWriter();
        var treeId = state.State.TreeId ?? string.Empty;
        var shardIndex = state.State.ShardIndex ?? 0;
        var maintenance = LatticeMaintenanceContext.Current;
        var transactionId = LatticeTransactionContext.Current;
        var maxIncoming = HybridLogicalClock.Zero;

        // step 0 (build) - one mutation envelope per incoming entry. The
        // per-key WAL grain hop that this loop used to do has been
        // collapsed into a single batched ICommitLogWriter.AppendManyAsync
        // dispatch below, matching the foreground SetManyAsync fast path.
        // Bulk-load / snapshot-restore / sibling-redistribute all reach
        // this method with batches sized at MaxLeafKeys, so the per-call
        // grain-hop count drops from O(MaxLeafKeys) to O(1) on the
        // merge channel as well.
        List<WalRecord>? walEntries = null;
        if (writer is not null && entries.Count > 0)
            walEntries = new List<WalRecord>(entries.Count);

        foreach (var (key, incoming) in entries)
        {
            if (incoming.Timestamp > maxIncoming)
                maxIncoming = incoming.Timestamp;

            walEntries?.Add(new WalRecord
            {
                TreeId = treeId,
                Op = incoming.IsTombstone ? MutationKind.Delete : MutationKind.Set,
                Key = key,
                Value = incoming.IsTombstone ? null : incoming.Value,
                Timestamp = incoming.Timestamp,
                IsTombstone = incoming.IsTombstone,
                ExpiresAtTicks = incoming.IsTombstone ? 0 : incoming.ExpiresAtTicks,
                OriginClusterId = incoming.OriginClusterId,
                VectorClock = incoming.VectorClock,
                TransactionId = transactionId,
                Category = maintenance,
                IsPrepared = false,
                IsMerge = true,
                ShardIndex = shardIndex,
            });
        }

        // step 1 (wal) - one batched dispatch for the whole merge batch.
        // The LeafWriteDuration histogram is recorded once per batch with
        // tag `kind=merge` so operators can size sibling-redistribute /
        // replication-apply / snapshot-restore traffic against ordinary
        // writes on the same instrument. (Per-entry recording would
        // inflate the histogram count and bias percentile reads of the
        // single-write path.)
        if (walEntries is { Count: > 0 })
        {
            var walStartTicks = Stopwatch.GetTimestamp();
            try
            {
                await writer!.AppendManyAsync(walEntries);
            }
            finally
            {
                var elapsedMs = (Stopwatch.GetTimestamp() - walStartTicks) * 1000.0 / Stopwatch.Frequency;
                LatticeMetrics.LeafWriteDuration.Record(elapsedMs,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId),
                    new KeyValuePair<string, object?>(LatticeMetrics.TagKind, "merge"),
                    LatticeTenantLabel.ForTree(treeId));
            }
        }

        // step 2 (apply) - per-key LWW merge into the projection.
        // Cross-leaf migration provenance: stamp IsMigrated=true on
        // each incoming value before merging. If the imported HLC
        // wins the LWW merge, the resulting Entries[K] carries the
        // flag (StoreEntry returns the merged winner). If a
        // pre-existing dominator wins, its own IsMigrated is
        // preserved by Merge - either way the value's provenance
        // travels with the value, and no out-of-band map is
        // required. The foreground orphan-drain guard in
        // BPlusLeafGrain.ApplyTxCommit reads existing.IsMigrated
        // to distinguish a migrated dominator (drain proceeds)
        // from a sibling-saga drain (drain skips).
        foreach (var (key, incoming) in entries)
        {
            StoreEntry(key, incoming with { IsMigrated = true });
        }

        // Publish a Version advance so LeafCacheGrain delta checks detect
        // the new entries. Without this, a freshly-split sibling has an
        // empty version vector and the cache short-circuits (empty dominates
        // empty), never populating its local cache.
        if (entries.Count > 0)
        {
            // Advance the local HLC past the highest incoming timestamp so a
            // subsequent local write produces a stamp that dominates the just-
            // merged values (audit bug #3). Without this, a merged future-dated
            // entry silently wins LWW against every local write until wall clock
            // catches up.
            if (maxIncoming > state.State.Clock)
                state.State.Clock = maxIncoming;
            PublishVersionAdvance(maxIncoming);
            BumpLocalRevision();
        }

        // Forward the projection-hash delta to the parent internal
        // node. See CommitSetAsync for the no-op semantics. CRDT
        // merge is a structural apply (replication-driven, not the
        // per-write hot path) - bypass the c2-xxviii coalescing window.
        await PublishDigestUpwardInlineAsync();
    }

    public async Task<List<string>> GetKeysAsync(string? startInclusive = null, string? endExclusive = null, string? afterExclusive = null, string? beforeExclusive = null, LatticePredicateNode? predicate = null)
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.RangeRead);
        var startTicks = Stopwatch.GetTimestamp();
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        // Issue #2786: the earliest instant at which this answer could change
        // with nothing written. See PublishLeafExpiryHorizon.
        var earliestExpiry = long.MaxValue;
        var splitInProgress = HasInterruptedSplit;
        var splitKey = state.State.SplitKey;
        var (outcomes, pendingKeys) = await SnapshotPendingForReadAsync();

        // Pre-size the result list to bound the small-end resize chain
        // (0 -> 4 -> 8 -> 16 -> ... -> 256 = 7 resizes for the common
        // ~250-entry-per-leaf shape). Capped at 256: for small leaves the
        // cap collapses to Entries.Count (no waste); for large leaves the
        // cap prevents the cycle-27 trap where pre-sizing to Entries.Count
        // over-allocates by ~10x when the range filter or split-key bound
        // truncates iteration well below the leaf's total entry count.
        // 256 is just above the typical page-size shape (KeysPageSize
        // default 512 / typical fanout 2-4 cursors = ~128-256 keys
        // per leaf per page) so it sized the initial array to the
        // expected emission, not the worst-case.
        var keys = new List<string>(capacity: Math.Min(Cache.Count, 256));
        // Windowed and clipped, for the reason CountAsync documents at length
        // (issue #2368). A single Cache.EnumerateRange over the caller's bounds
        // reads as already-bounded, but GetKeysAsync's parameters all default to
        // null: an unbounded call resolves to EnumerateRange(null, null), whose
        // HydrateRange protects the entire span in its own TrimToBudget call, so
        // every block ends up resident at once. Since issue #2843 that no longer
        // detaches the frame (a completed ranged hydration retains it so a
        // division can still bisect), but it still pins the whole leaf resident
        // for the life of the operation, defeating the residency bound. That
        // makes this a residency-pinning site that the HydrateAll / Keys /
        // EnumerateRows / UnderlyingRows signature does not match.
        var scanStart = MaxOrdinal(startInclusive, afterExclusive);
        var scanEnd = MinOrdinal(MinOrdinal(endExclusive, beforeExclusive), splitInProgress ? splitKey : null);
        foreach (var (windowStart, windowEnd) in Cache.GetFullScanWindowsWithoutHydrating())
        {
            if (scanEnd is not null && windowStart is not null &&
                string.CompareOrdinal(windowStart, scanEnd) >= 0)
                break;

            var from = MaxOrdinal(windowStart, scanStart);
            var to = MinOrdinal(windowEnd, scanEnd);
            if (from is not null && to is not null &&
                string.CompareOrdinal(from, to) >= 0)
                continue;

            foreach (var (key, lww) in Cache.EnumerateRange(from, to))
            {
                if (endExclusive is not null && string.Compare(key, endExclusive, StringComparison.Ordinal) >= 0)
                    break;

                if (beforeExclusive is not null && string.Compare(key, beforeExclusive, StringComparison.Ordinal) >= 0)
                    break;

                if (splitInProgress && splitKey is not null &&
                    string.Compare(key, splitKey, StringComparison.Ordinal) >= 0)
                    break;

                if (startInclusive is not null && string.Compare(key, startInclusive, StringComparison.Ordinal) < 0)
                    continue;

                if (afterExclusive is not null && string.Compare(key, afterExclusive, StringComparison.Ordinal) <= 0)
                    continue;

                if (pendingKeys.TryGetValue(key, out var pending))
                {
                    var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
                    if (status == TxStatus.Committed)
                    {
                        if (!pending.value.IsTombstone && !pending.value.IsExpired(nowTicks)
                            && (predicate is null || LatticePredicateEvaluator.Matches(pending.value.Value, predicate.Value)))
                        {
                            TrackEarliestExpiry(ref earliestExpiry, pending.value.ExpiresAtTicks);
                            keys.Add(key);
                        }
                        continue;
                    }
                    // InFlight, Aborted, or orphan-pending (committed bucket whose
                    // saga terminal has already landed on this leaf) - fall through
                    // to Entries. See GetWithPendingAsync for the orphan-pending
                    // rationale: a late-arriving shadow-forward of a prepare can
                    // bucket a saga whose terminal has already drained into Entries,
                    // and surfacing the orphan would shadow the authoritative
                    // Entries value (or a strictly-later saga's value).
                }

                if (lww.IsTombstone || lww.IsExpired(nowTicks))
                    continue;

                if (predicate is not null && !LatticePredicateEvaluator.Matches(lww.Value, predicate.Value))
                    continue;

                TrackEarliestExpiry(ref earliestExpiry, lww.ExpiresAtTicks);
                keys.Add(key);
            }
        }

        // Fresh committed pending keys not yet in Entries, respecting range filters.
        foreach (var (key, pending) in pendingKeys)
        {
            if (Cache.ContainsKey(key)) continue;
            if (endExclusive is not null && string.Compare(key, endExclusive, StringComparison.Ordinal) >= 0) continue;
            if (beforeExclusive is not null && string.Compare(key, beforeExclusive, StringComparison.Ordinal) >= 0) continue;
            if (splitInProgress && splitKey is not null && string.Compare(key, splitKey, StringComparison.Ordinal) >= 0) continue;
            if (startInclusive is not null && string.Compare(key, startInclusive, StringComparison.Ordinal) < 0) continue;
            if (afterExclusive is not null && string.Compare(key, afterExclusive, StringComparison.Ordinal) <= 0) continue;
            var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
            if (status != TxStatus.Committed) continue;
            if (pending.value.IsTombstone || pending.value.IsExpired(nowTicks)) continue;
            if (predicate is not null && !LatticePredicateEvaluator.Matches(pending.value.Value, predicate.Value)) continue;
            TrackEarliestExpiry(ref earliestExpiry, pending.value.ExpiresAtTicks);
            keys.Add(key);
        }
        keys.Sort(StringComparer.Ordinal);
        PublishLeafExpiryHorizon(context.GrainId, earliestExpiry);

        var elapsedMs = (Stopwatch.GetTimestamp() - startTicks) * 1000.0 / Stopwatch.Frequency;
        LatticeMetrics.LeafScanDuration.Record(elapsedMs,
            LeafTreeTag(),
            new KeyValuePair<string, object?>(LatticeMetrics.TagOperation, "keys"),
            LeafTenantTag());
        return keys;
    }

    public async Task<List<KeyValuePair<string, byte[]>>> GetEntriesAsync(string? startInclusive = null, string? endExclusive = null, string? afterExclusive = null, string? beforeExclusive = null, LatticePredicateNode? predicate = null)
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.RangeRead);
        var startTicks = Stopwatch.GetTimestamp();
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        // Issue #2786: the earliest instant at which this answer could change
        // with nothing written. See PublishLeafExpiryHorizon.
        var earliestExpiry = long.MaxValue;
        var splitInProgress = HasInterruptedSplit;
        var splitKey = state.State.SplitKey;
        var (outcomes, pendingKeys) = await SnapshotPendingForReadAsync();

        // Pre-size the result list to bound the small-end resize chain, mirroring
        // the sibling GetKeysAsync path above: capped at 256 so small leaves
        // collapse to Cache.Count (no waste) while large leaves avoid the ~10x
        // over-allocation a bare Cache.Count would cause when the range filter or
        // split-key bound truncates iteration well below the leaf's entry count.
        var entries = new List<KeyValuePair<string, byte[]>>(capacity: Math.Min(Cache.Count, 256));
        // Windowed and clipped, exactly as the sibling GetKeysAsync above; an
        // unbounded call here resolves to EnumerateRange(null, null) and
        // detaches the frame (issue #2368).
        var scanStart = MaxOrdinal(startInclusive, afterExclusive);
        var scanEnd = MinOrdinal(MinOrdinal(endExclusive, beforeExclusive), splitInProgress ? splitKey : null);
        foreach (var (windowStart, windowEnd) in Cache.GetFullScanWindowsWithoutHydrating())
        {
            if (scanEnd is not null && windowStart is not null &&
                string.CompareOrdinal(windowStart, scanEnd) >= 0)
                break;

            var from = MaxOrdinal(windowStart, scanStart);
            var to = MinOrdinal(windowEnd, scanEnd);
            if (from is not null && to is not null &&
                string.CompareOrdinal(from, to) >= 0)
                continue;

            foreach (var (key, lww) in Cache.EnumerateRange(from, to))
            {
                if (endExclusive is not null && string.Compare(key, endExclusive, StringComparison.Ordinal) >= 0)
                    break;

                if (beforeExclusive is not null && string.Compare(key, beforeExclusive, StringComparison.Ordinal) >= 0)
                    break;

                if (splitInProgress && splitKey is not null &&
                    string.Compare(key, splitKey, StringComparison.Ordinal) >= 0)
                    break;

                if (startInclusive is not null && string.Compare(key, startInclusive, StringComparison.Ordinal) < 0)
                    continue;

                if (afterExclusive is not null && string.Compare(key, afterExclusive, StringComparison.Ordinal) <= 0)
                    continue;

                if (pendingKeys.TryGetValue(key, out var pending))
                {
                    var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
                    if (status == TxStatus.Committed)
                    {
                        if (!pending.value.IsTombstone && !pending.value.IsExpired(nowTicks)
                            && (predicate is null || LatticePredicateEvaluator.Matches(pending.value.Value, predicate.Value)))
                        {
                            TrackEarliestExpiry(ref earliestExpiry, pending.value.ExpiresAtTicks);
                            entries.Add(new KeyValuePair<string, byte[]>(key, pending.value.Value!));
                        }
                        continue;
                    }
                    // InFlight or Aborted - fall through to Entries
                    // (pre-saga visibility). See GetWithPendingAsync.
                }

                if (lww.IsTombstone || lww.IsExpired(nowTicks))
                    continue;

                if (predicate is not null && !LatticePredicateEvaluator.Matches(lww.Value, predicate.Value))
                    continue;

                TrackEarliestExpiry(ref earliestExpiry, lww.ExpiresAtTicks);
                entries.Add(new KeyValuePair<string, byte[]>(key, lww.Value!));
            }
        }

        // Fresh committed pending keys not yet in Entries, respecting range filters.
        foreach (var (key, pending) in pendingKeys)
        {
            if (Cache.ContainsKey(key)) continue;
            if (endExclusive is not null && string.Compare(key, endExclusive, StringComparison.Ordinal) >= 0) continue;
            if (beforeExclusive is not null && string.Compare(key, beforeExclusive, StringComparison.Ordinal) >= 0) continue;
            if (splitInProgress && splitKey is not null && string.Compare(key, splitKey, StringComparison.Ordinal) >= 0) continue;
            if (startInclusive is not null && string.Compare(key, startInclusive, StringComparison.Ordinal) < 0) continue;
            if (afterExclusive is not null && string.Compare(key, afterExclusive, StringComparison.Ordinal) <= 0) continue;
            var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
            if (status != TxStatus.Committed) continue;
            if (pending.value.IsTombstone || pending.value.IsExpired(nowTicks)) continue;
            if (predicate is not null && !LatticePredicateEvaluator.Matches(pending.value.Value, predicate.Value)) continue;
            TrackEarliestExpiry(ref earliestExpiry, pending.value.ExpiresAtTicks);
            entries.Add(new KeyValuePair<string, byte[]>(key, pending.value.Value!));
        }
        entries.Sort(static (a, b) => StringComparer.Ordinal.Compare(a.Key, b.Key));
        PublishLeafExpiryHorizon(context.GrainId, earliestExpiry);

        var elapsedMs = (Stopwatch.GetTimestamp() - startTicks) * 1000.0 / Stopwatch.Frequency;
        LatticeMetrics.LeafScanDuration.Record(elapsedMs,
            LeafTreeTag(),
            new KeyValuePair<string, object?>(LatticeMetrics.TagOperation, "entries"),
            LeafTenantTag());
        return entries;
    }

    public async Task<Dictionary<string, byte[]>> GetLiveEntriesAsync()
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.RangeRead);
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        var (outcomes, pendingKeys) = await SnapshotPendingForReadAsync();
        // Presize to the cached row count (the first loop's upper bound), mirroring
        // the sibling GetLiveRawEntriesAsync; the prior grow-from-empty map rehashed
        // its bucket and entry arrays repeatedly on a full-leaf read.
        var result = new Dictionary<string, byte[]>(Cache.Count);
        // Bounded windows rather than the whole-cache view. This read does
        // retain every live value, so its peak is inherently the live set -
        // but that set is released when the call returns, whereas HydrateAll
        // ends in DetachSnapshot and keeps the whole leaf resident and
        // unsheddable for the rest of the activation, which is what forfeits
        // the cheap frame-only division (issue #2368). The result is a
        // key-addressed map built from disjoint, exhaustive windows, so it is
        // identical to the one-pass walk this replaced.
        foreach (var (windowStart, windowEnd) in Cache.GetFullScanWindowsWithoutHydrating())
        {
            foreach (var (key, lww) in Cache.EnumerateRange(windowStart, windowEnd))
            {
                if (pendingKeys.TryGetValue(key, out var pending))
                {
                    var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
                    if (status == TxStatus.Committed)
                    {
                        if (!pending.value.IsTombstone && !pending.value.IsExpired(nowTicks))
                            result[key] = pending.value.Value!;
                        continue;
                    }
                    // InFlight or Aborted - fall through to Entries
                    // (pre-saga visibility). See GetWithPendingAsync.
                }
                if (lww.IsTombstone || lww.IsExpired(nowTicks)) continue;
                result[key] = lww.Value!;
            }
        }
        foreach (var (key, pending) in pendingKeys)
        {
            if (Cache.ContainsKey(key)) continue;
            var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
            if (status != TxStatus.Committed) continue;
            if (pending.value.IsTombstone || pending.value.IsExpired(nowTicks)) continue;
            result[key] = pending.value.Value!;
        }
        return result;
    }

    /// <inheritdoc />
    public async Task<List<LwwEntry>> GetLiveRawEntriesAsync()
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.RangeRead);
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        var (outcomes, pendingKeys) = await SnapshotPendingForReadAsync();
        var result = new List<LwwEntry>(Cache.Count);
        // Converted to a bounded windowed walk (issue #2834), matching the
        // sibling GetLiveEntriesAsync directly above. This was the last
        // whole-cache read seam on the leaf that could be converted at the call
        // site; the residue that remains is documented on the seams themselves.
        //
        // The conversion turns on WHICH merge-mode accessor the loop body uses,
        // and getting that wrong reintroduces two distinct faults:
        //
        //   1. Cache.GetMergeMode(key) is a key-addressed accessor and ends in
        //      TrimToBudget, which protects only the ONE block it touched.
        //      Under a windowed walk that block is not the window, so the trim
        //      can evict a block of the window currently being enumerated -
        //      structurally modifying the dictionary under the enumerator.
        //   2. Buffering the window and looking the modes up afterwards does
        //      not rescue it either: EvictBlock drops the evicted rows' entries
        //      from the merge-mode map, so a post-eviction lookup returns null
        //      where a mode exists and the ANSWER changes rather than merely
        //      the cost.
        //
        // Cache.GetMergeModeWithoutHydrating (added by #2835) avoids both: it
        // reads the merge-mode side-map directly and never hydrates, touches or
        // trims, and it is called inline on a key the walk has just yielded, so
        // the row is resident by construction. This is the same pattern
        // BPlusLeafGrain.FrozenBaseline.cs uses, for the same reason.
        //
        // What the conversion buys: the whole-cache view called HydrateAll,
        // which ends in DetachSnapshot and is irreversible for the activation.
        // A leaf divides cheaply only while its frame is attached, so reading
        // every live raw entry - an ordinary read, nothing to do with splitting
        // - permanently forfeited the bounded division for that activation.
        //
        // What it does NOT buy, and must not be read as buying: a bound on peak
        // residency when the hydration budget is not smaller than the leaf. The
        // windows are only as sheddable as TrimToBudget makes them, and an
        // operator who raises LeafHydrationResidentBytes towards MaxLeafBytes
        // gets a walk that is windowed in shape and whole-leaf in cost
        // (issue #2836). LatticeOptionsResolver warns about that configuration;
        // the frame itself is retained either way since #2843.
        foreach (var (windowStart, windowEnd) in Cache.GetFullScanWindowsWithoutHydrating())
        {
            foreach (var (key, lww) in Cache.EnumerateRange(windowStart, windowEnd))
            {
                if (pendingKeys.TryGetValue(key, out var pending))
                {
                    var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
                    if (status == TxStatus.Committed)
                    {
                        if (!pending.value.IsTombstone && !pending.value.IsExpired(nowTicks))
                            result.Add(new LwwEntry(key, pending.value));
                        continue;
                    }
                    // InFlight or Aborted - fall through to Entries
                    // (pre-saga visibility). See GetWithPendingAsync.
                }
                if (lww.IsTombstone || lww.IsExpired(nowTicks)) continue;
                result.Add(new LwwEntry(key, lww, Cache.GetMergeModeWithoutHydrating(key)));
            }
        }
        foreach (var (key, pending) in pendingKeys)
        {
            if (Cache.ContainsKey(key)) continue;
            var status = outcomes.TryGetValue(pending.txid, out var s) ? s : TxStatus.InFlight;
            if (status != TxStatus.Committed) continue;
            if (pending.value.IsTombstone || pending.value.IsExpired(nowTicks)) continue;
            result.Add(new LwwEntry(key, pending.value));
        }
        return result;
    }

    /// <summary>
    /// Returns all key-value entries in this leaf including tombstones,
    /// preserving the original <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/> timestamps.
    /// Internal method for unit testing - not exposed on the grain interface
    /// to avoid Orleans generic type serialization issues.
    /// <para>
    /// Walks budget-sized windows rather than the whole-cache view (issue
    /// #2368). It does hand back a copy of every row, so its own peak is the
    /// whole leaf either way - but that copy is the caller's and is released
    /// with it, whereas the whole-cache view additionally ends in
    /// DetachSnapshot and leaves the CACHE fully resident for the rest of the
    /// activation. Residency is incidental to this operation, not required by
    /// it, so there is nothing here to keep resident afterwards.
    /// </para>
    /// </summary>
    internal Task<Dictionary<string, LwwValue<byte[]>>> GetAllRawEntriesAsync()
    {
        var result = new Dictionary<string, LwwValue<byte[]>>(Cache.Count);
        foreach (var (windowStart, windowEnd) in Cache.GetFullScanWindowsWithoutHydrating())
        {
            foreach (var (key, lww) in Cache.EnumerateRange(windowStart, windowEnd))
            {
                result[key] = lww;
            }
        }
        return Task.FromResult(result);
    }

    public async Task<SplitResult?> MergeManyAsync(Dictionary<string, LwwValue<byte[]>> entries, bool isCrossShardMigration = false)
    {
        await AwaitReplayBarrierAsync();

        EnsureInternalOrigin(LatticeOperation.Write);
        using var _mutationScope = EnterMutationScope();
        // Recovery: if a previous split was interrupted, complete it first.
        if (HasInterruptedSplit)
        {
            var recovered = await CompleteRecoverySplitUnderGateAsync();

            // Re-merge entries that belong to the new sibling.
            var siblingEntries = new Dictionary<string, LwwValue<byte[]>>();
            var localEntries = new Dictionary<string, LwwValue<byte[]>>();
            foreach (var (key, lww) in entries)
            {
                if (string.Compare(key, state.State.SplitKey!, StringComparison.Ordinal) >= 0)
                    siblingEntries[key] = lww;
                else
                    localEntries[key] = lww;
            }

            if (siblingEntries.Count > 0)
            {
                var sibling = grainFactory.GetGrain<IBPlusLeafGrain>(state.State.SplitSiblingId!.Value);
                // Carry this leaf's shadow markers for the re-routed keys
                // across before the rows themselves, for the same reason the
                // span forward does (see ForwardOutOfSpanMergeAsync): a
                // forwarded row keeps its IsMigrated flag and so will be
                // gated on the sibling, but the marker that gates it lives
                // here and would otherwise be stranded, leaving the sibling
                // serving a pre-saga value ungated (#3117). Markers first, so
                // the sibling never holds the row without its gate.
                await TransferShadowMarkersToSiblingAsync(sibling, siblingEntries.Keys);
                // Forward the caller's migration intent verbatim - a cross-shard migration
                // import that arrives during split recovery is still a migration on the sibling.
                await sibling.MergeManyAsync(siblingEntries, isCrossShardMigration);
            }

            // Merge remaining local entries via the WAL-routed path so the
            // surviving foreground commit invariant holds across split
            // recovery too. The topology-only `PersistAsync()` above
            // captures the split-complete state row; the local merge
            // entries themselves flow through `ICommitLogWriter` inside
            // `MergeIntoStateAsync` rather than re-using the legacy
            // state-row persist.
            if (localEntries.Count > 0)
            {
                await MergeIntoStateAsync(localEntries, isCrossShardMigration);
            }

            return recovered;
        }

        if (entries.Count == 0)
        {
            return null;
        }

        // Declared-span admission (see BPlusLeafGrain.SpanAdmission.cs).
        //
        // This deliberately applies to cross-shard migration imports as well.
        // An earlier revision exempted them, reasoning that migration is a
        // topology-seeding operation whose coordinator places rows deliberately
        // and sets the destination's range as a separate step, so its keys are
        // legitimately outside the range at the moment they arrive. The premise
        // is sound but the exemption is not needed to honour it, because the
        // seeding shape is already admitted by two independent properties of
        // the admission path itself:
        //
        //   * a destination whose range has not been set yet has two null
        //     bounds, so HasDeclaredSpan is false and ContainsOutOfSpanKey
        //     returns false without a single comparison; and
        //   * a freshly-seeded destination has no chain pointers yet, so
        //     TryResolveSpanForwardTarget resolves nothing and
        //     ForwardOutOfSpanMergeAsync falls open to the local commit.
        //
        // On the seeding shape the exemption was therefore already a no-op, and
        // the only shape it actually changed was the one it was never meant to
        // cover: a leaf whose span was narrowed by its OWN split, receiving a
        // late import for a key that split moved to its sibling. There the
        // exemption re-created the key locally as an IsMigrated=true pre-saga
        // row on a leaf that no longer owns it. The asymmetric
        // migration-vs-foreground guard in MergeIntoStateAsync cannot suppress
        // that row, because that guard fires only when the destination already
        // holds a non-migrated entry for the key and split had removed the row
        // entirely. The next split then handed the stale row forward into the
        // live topology, where it was served as a torn read (issue #3117).
        //
        // Routing the import to the leaf that actually declares the key fixes
        // that at the seam where ownership is decided, and leaves the saga
        // machinery untouched.
        if (ContainsOutOfSpanKey(entries))
        {
            entries = await ForwardOutOfSpanMergeAsync(entries, isCrossShardMigration);
            if (entries.Count == 0)
            {
                return null;
            }
        }

        await MergeIntoStateAsync(entries, isCrossShardMigration);

        SplitResult? splitResult = null;
        var mergeOptions = await GetOptionsAsync();
        if (IsLeafOverCapacity(mergeOptions.MaxLeafKeys, mergeOptions.MaxLeafBytes))
        {
            splitResult = await SplitIfNeededUnderGateAsync(mergeOptions.MaxLeafKeys, mergeOptions.MaxLeafBytes);
        }

        return splitResult;
    }

    private async Task MergeIntoStateAsync(Dictionary<string, LwwValue<byte[]>> entries, bool isCrossShardMigration)
    {
        // Track the high-water timestamp of the incoming batch so we can
        // (a) advance state.State.Clock past it (audit bug #3), and (b)
        // publish it as Version[ReplicaId] so LeafCacheGrain delta checks
        // detect the new entries. See MergeEntriesAsync for the full
        // invariant.
        //
        // WAL-as-sole-commit-point: every accepted entry is durably
        // committed by appending a LatticeMutation { Kind = Set | Delete,
        // IsMerge = true, ... } to the per-shard WAL via ICommitLogWriter
        // before the in-memory projection mutation. The asymmetric
        // migration-vs-foreground guard runs first and silently drops
        // imports that lose to an authoritative foreground commit on
        // the destination - those skipped entries are not appended to
        // the WAL because they are not committed.
        var writer = ResolveCommitLogWriter();
        var treeId = state.State.TreeId ?? string.Empty;
        var shardIndex = state.State.ShardIndex ?? 0;
        var maintenance = LatticeMaintenanceContext.Current;
        var transactionId = LatticeTransactionContext.Current;
        var maxIncoming = HybridLogicalClock.Zero;
        var appliedAny = false;

        // step 0 (filter + build) - first pass classifies each incoming
        // entry under the asymmetric migration-vs-foreground rule
        // (described in detail below), and builds the per-entry mutation
        // envelope and post-stamping LwwValue used by both the WAL
        // append and the projection apply. The per-key WAL grain hop
        // that used to run inside this loop has been collapsed into a
        // single batched ICommitLogWriter.AppendManyAsync dispatch
        // below, matching the foreground SetManyAsync fast path and
        // the MergeEntriesAsync sibling fast path. Cross-shard migration
        // (the dominant caller, via online-reshard and shard splits)
        // gets the same O(1) grain-hop savings as the foreground path.
        //
        // Allocation note: the `accepted` work list is only materialised
        // on the cross-shard migration path, where (a) the asymmetric
        // guard may filter entries so step 2 cannot iterate `entries`
        // directly, and (b) each surviving entry's `toStore` value
        // differs from `incoming` (it carries IsMigrated=true). On the
        // non-migration path every incoming entry survives unchanged,
        // so step 2 iterates `entries` directly and the per-batch
        // `KeyValuePair<string, LwwValue<byte[]>>[]` backing array
        // (~64 B per entry, ~16 KiB on a default-size leaf) is not
        // allocated.
        List<WalRecord>? walEntries = null;
        List<KeyValuePair<string, LwwValue<byte[]>>>? accepted = null;
        if (writer is not null && entries.Count > 0)
            walEntries = new List<WalRecord>(entries.Count);
        if (isCrossShardMigration && entries.Count > 0)
            accepted = new List<KeyValuePair<string, LwwValue<byte[]>>>(entries.Count);

        foreach (var (key, incoming) in entries)
        {
            // Asymmetric migration-vs-foreground rule. Only fires on the
            // cross-shard migration callsites (TreeShardSplitGrain
            // -> ForwardMovedSlotEntriesAsync, and ShardRootGrain.Split.cs
            // shadow-forward). When the destination already has a
            // non-migration entry for the key, that entry is an
            // authoritative post-split foreground commit (a direct Set,
            // a saga's prepared-bucket drain, or a saga's cross-migration
            // LWW backstop) and MUST NOT be overwritten by a migration
            // import regardless of LWW HLC comparison: the migrated
            // record's HLC reflects the SOURCE leaf's accumulated clock
            // at migration time, which can dominate the freshly-created
            // destination's Clock at the time the foreground write landed
            // - even though the foreground write is logically newer
            // (post-split-routing-update) and the destination is the new
            // owner of the key.
            //
            // Non-migration callers (cross-cluster replication, tree-merge,
            // snapshot restore, intra-shard sibling-merge) use the
            // symmetric LWW-by-HLC contract: the incoming entry wins iff
            // its HLC dominates, regardless of the existing entry's
            // IsMigrated flag. For those callers the asymmetric guard
            // would silently drop legitimate higher-HLC imports and
            // tombstones (see the cross-cluster LWW contract tests).
            //
            // The Fix-M backstop pre-advance (BPlusLeafGrain.PendingTx.cs)
            // already handles the migration-FIRST, terminal-SECOND
            // ordering by Ticking the stamp past existing migrated
            // Entries' HLCs. This guard handles the reverse ordering
            // (terminal-FIRST on a fresh leaf, migration-SECOND with
            // an inverted HLC).
            if (isCrossShardMigration
                && Cache.TryGetRow(key, out var existing)
                && !existing.IsMigrated)
            {
                continue;
            }

            if (incoming.Timestamp > maxIncoming)
                maxIncoming = incoming.Timestamp;

            // Stamp IsMigrated=true ONLY on the cross-shard migration
            // callsite. Non-migration callers preserve the incoming entry's
            // own IsMigrated flag verbatim - that flag is normally `false`
            // for foreground writes on the source and `true` only when the
            // source-side entry was itself a migration import being
            // re-replicated / re-merged forward.
            var toStore = isCrossShardMigration ? (incoming with { IsMigrated = true }) : incoming;
            accepted?.Add(new KeyValuePair<string, LwwValue<byte[]>>(key, toStore));

            walEntries?.Add(new WalRecord
            {
                TreeId = treeId,
                Op = toStore.IsTombstone ? MutationKind.Delete : MutationKind.Set,
                Key = key,
                Value = toStore.IsTombstone ? null : toStore.Value,
                Timestamp = toStore.Timestamp,
                IsTombstone = toStore.IsTombstone,
                ExpiresAtTicks = toStore.IsTombstone ? 0 : toStore.ExpiresAtTicks,
                OriginClusterId = toStore.OriginClusterId,
                VectorClock = toStore.VectorClock,
                TransactionId = transactionId,
                Category = maintenance,
                IsPrepared = false,
                IsMerge = true,
                ShardIndex = shardIndex,
            });
        }

        // step 1 (wal) - one batched dispatch for the whole accepted
        // batch. Recorded once on LeafWriteDuration tagged kind=merge;
        // see MergeEntriesAsync for the per-batch-recording rationale.
        if (walEntries is { Count: > 0 })
        {
            var walStartTicks = Stopwatch.GetTimestamp();
            try
            {
                await writer!.AppendManyAsync(walEntries);
            }
            finally
            {
                var elapsedMs = (Stopwatch.GetTimestamp() - walStartTicks) * 1000.0 / Stopwatch.Frequency;
                LatticeMetrics.LeafWriteDuration.Record(elapsedMs,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId),
                    new KeyValuePair<string, object?>(LatticeMetrics.TagKind, "merge"),
                    LatticeTenantLabel.ForTree(treeId));
            }
        }

        // step 2 (apply) - per-key LWW merge into the projection. On
        // the cross-shard migration path we iterate the post-filter
        // `accepted` list so guard-rejected entries are not applied;
        // on the non-migration path every entry survives unchanged and
        // we iterate `entries` directly to avoid the per-batch work
        // list allocation.
        if (isCrossShardMigration)
        {
            if (accepted is { Count: > 0 })
            {
                for (var i = 0; i < accepted.Count; i++)
                {
                    StoreEntry(accepted[i].Key, accepted[i].Value);
                    appliedAny = true;
                }
            }
        }
        else
        {
            foreach (var (key, incoming) in entries)
            {
                StoreEntry(key, incoming);
                appliedAny = true;
            }
        }

        if (appliedAny)
        {
            // Advance the local HLC past the highest incoming timestamp so
            // subsequent local writes dominate the merged values (audit bug #3).
            if (maxIncoming > state.State.Clock)
                state.State.Clock = maxIncoming;
            PublishVersionAdvance(maxIncoming);
            BumpLocalRevision();
        }

        // step 3 (observer) - publish each applied entry to the silo-wide
        // IMutationObserver seam so downstream consumers (the compiled
        // authorization-policy snapshot maintainer, the membership cache,
        // the replication sink nudge) react to a replication-apply /
        // tree-merge / snapshot-restore write exactly as they do to a
        // foreground commit. Without this the receiver of a replicated
        // reserved-tree write converges in state but never rebuilds the
        // per-silo snapshot the enforcement path reads, so a cross-cluster
        // grant or revoke lands durably yet is never enforced on the peer
        // until an unrelated event happens to rebuild it.
        //
        // The cross-shard migration path is intentionally excluded: a
        // migration is an internal topology move of an already-authored
        // value, not a new logical mutation, so it must stay observationally
        // silent (matching MergeEntriesAsync). The HasObservers guard keeps
        // the common single-cluster case (no auth, no replication - no
        // observer registered) at a single branch check with no added cost,
        // and local foreground writes never reach this method, so no write
        // is double-published. The publish runs under a commit-log scope so
        // a replication-aware observer recognises the source and does not
        // treat the apply as a fresh local authored write.
        if (!isCrossShardMigration && appliedAny && mutationObservers.HasObservers)
        {
            using (LatticeCommitLogContext.BeginScope())
            {
                foreach (var (key, incoming) in entries)
                {
                    var published = Cache.TryGetRow(key, out var committed) ? committed : incoming;
                    if (published.IsTombstone)
                        await PublishDeleteAsync(key, published);
                    else
                        await PublishSetAsync(key, published);
                }
            }
        }

        // Forward the projection-hash delta to the parent internal
        // node. See MergeEntriesAsync for the no-op semantics. CRDT
        // merge is a structural apply - bypass the c2-xxviii
        // coalescing window.
        await PublishDigestUpwardInlineAsync();
    }

    public async Task ClearGrainStateAsync()
    {
        // Retire the replay BEFORE the clear (issue #2871). The replay now runs
        // concurrently with requests, so an in-flight one would otherwise
        // re-hydrate the cache from the WAL immediately after this clear -
        // resurrecting, in memory, exactly the state the purge was asked to
        // remove, in the window before the deactivation below takes effect.
        RetireReplayBarrier();

        // Retire the materialiser pins BEFORE the clear too, and for a stricter
        // reason than ordering hygiene (issue #3101). The pins' consumer ids are
        // derived from state.State.TreeId, which the clear nulls, so after it
        // they cannot be computed at all. A pin left behind here is a permanent
        // WAL retention floor: the GC resolves the leaf, activates it, finds no
        // tree id bound, and gets NotDriven for the life of the deployment.
        await UnregisterMaterialiserPinsAsync();

        await state.ClearStateAsync();
        context.Deactivate(new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "Tree purged"));
    }
}
