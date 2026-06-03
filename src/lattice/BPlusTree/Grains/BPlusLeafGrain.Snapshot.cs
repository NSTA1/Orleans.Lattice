using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Snapshot-capture partial for <see cref="BPlusLeafGrain"/>. Adds the
/// <see cref="IBPlusLeafGrain.CaptureSnapshotAsync"/> seam that copies
/// the per-activation entry cache into a canonical byte-row
/// <see cref="LeafSnapshotBlob"/> and persists it through the dedicated
/// <see cref="ILeafSnapshotStorageGrain"/> keyed by this leaf's grain
/// id. The capture is read-only on the leaf side - it stamps the blob
/// with the already-persisted <c>ProjectionCheckpointOffset</c> and
/// does not mutate any leaf state.
/// <para>
/// Capture is driven by the leaf itself (not by the maintenance
/// grain): when the fall-off-log detector raises the
/// <see cref="FallOffLogDecision.SnapshotPending"/> advisory at
/// activation time, the leaf latches <see cref="_activationSnapshotPending"/>
/// and captures once the tail replay has completed. While the leaf
/// stays active, every
/// <see cref="LatticeOptions.LeafSnapshotReClassifyEveryNCheckpoints"/>
/// successful checkpoint persist re-classifies and (on advisory) drives
/// another capture. A single-flight guard
/// (<see cref="_snapshotCaptureInFlight"/>) suppresses overlapping
/// captures so a slow <c>SaveAsync</c> cannot pin a follow-on capture
/// behind it - the follow-on is dropped and the next cadence tick
/// re-evaluates.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Latched at activation when the fall-off-log detector returns
    /// <see cref="FallOffLogDecision.SnapshotPending"/>. The activation
    /// hook reads-and-clears the flag after the tail replay so a
    /// proactive capture fires exactly once per advisory-firing
    /// activation.
    /// </summary>
    private bool _activationSnapshotPending;

    /// <summary>
    /// Single-flight guard for the snapshot-capture seam. Set on
    /// entry to <see cref="CaptureSnapshotAsync"/> and the periodic
    /// recheck path, cleared on completion. Concurrent capture
    /// invocations observe a <c>true</c> value and return immediately;
    /// the next cadence tick re-evaluates.
    /// </summary>
    private bool _snapshotCaptureInFlight;

    /// <summary>
    /// Number of successful checkpoint persists since this
    /// activation last ran the periodic snapshot recheck.
    /// <see cref="FlushPendingCheckpointAsync"/> increments this on
    /// every successful persist; when it reaches
    /// <see cref="LatticeOptions.LeafSnapshotReClassifyEveryNCheckpoints"/>
    /// the leaf re-runs the fall-off-log detector and, on advisory,
    /// drives a capture.
    /// </summary>
    private int _checkpointPersistCountSinceRecheck;

    /// <summary>
    /// Checkpoint offset at the moment the most recent proactive
    /// capture landed. The periodic recheck uses this as a cheap
    /// "we already snapshotted this projection" filter: when the
    /// post-flush checkpoint equals the last-captured checkpoint, the
    /// recheck short-circuits before the classifier RPCs (which would
    /// re-fetch the WAL head and tail, then re-classify, and at best
    /// drive a redundant capture of an identical projection). Set to
    /// the snapshot's offset on a successful capture; never reset
    /// across the activation lifetime.
    /// </summary>
    private long _lastCapturedCheckpointOffset = long.MinValue;

    /// <summary>
    /// Byte-accurate footprint of the most recently persisted snapshot
    /// for this leaf, or <c>0</c> when no snapshot has been captured this
    /// activation. Mirrors the value written into
    /// <see cref="LeafSnapshotBlob.SnapshotBytes"/> at capture time and
    /// is consumed by the per-persist byte-footprint publish path in
    /// <c>BPlusLeafGrain.Metrics.PersistAsync</c> so the shard root's
    /// running snapshot-bytes total stays current without a snapshot
    /// storage read on the persist hot path.
    /// </summary>
    private long _lastCapturedSnapshotBytes;
    /// <inheritdoc />
    public async Task CaptureSnapshotAsync()
    {
        // No-op for an uninitialised leaf. TreeId is assigned during
        // SetTreeIdAsync (called by the shard root on first attach);
        // without it the snapshot grain key would be meaningless and
        // there is no cache content worth persisting anyway.
        if (state.State.TreeId is null)
        {
            return;
        }

        // The "nothing applied" sentinel (-1) means the leaf has not
        // yet absorbed any WAL entry into its projection; capturing
        // an empty cache at offset -1 would create a snapshot the
        // activation path is required to ignore (every checkpoint
        // >= -1), so the work is pure overhead. Skip.
        var checkpoint = state.State.ProjectionCheckpointOffset;
        if (checkpoint < 0)
        {
            return;
        }

        // Single-flight guard. A second capture invocation that arrives
        // while a previous SaveAsync is still in flight is dropped on
        // the floor: the in-flight capture will land soon and any
        // subsequent advisory (activation re-entry or the periodic
        // recheck) will re-evaluate. This prevents an unbounded queue
        // of capture awaits when the snapshot storage provider is
        // slow.
        if (_snapshotCaptureInFlight)
        {
            return;
        }
        _snapshotCaptureInFlight = true;
        try
        {
            // Single-threaded copy of the cache rows under the grain
            // turn. EnumerateRows yields the SortedDictionary's
            // key-ordered KeyValuePair sequence; the resulting list is
            // a self-contained value snapshot that survives subsequent
            // foreground mutations on this activation.
            var rows = new List<LeafSnapshotRow>(Cache.Count);
            foreach (var kv in Cache.EnumerateRows())
            {
                rows.Add(new LeafSnapshotRow(kv.Key, kv.Value));
            }

            var blob = new LeafSnapshotBlob
            {
                SnapshotOffset = checkpoint,
                Rows = rows,
                CapturedAtTicks = DateTime.UtcNow.Ticks,
                // Snapshot row footprint matches the leaf-state byte formula
                // by construction (the snapshot is a copy of the cache), so
                // use the cache's incrementally-maintained running total
                // rather than re-walking every row at capture time.
                SnapshotBytes = Cache.StateBytes,
            };

            var snapshotGrain = grainFactory.GetGrain<ILeafSnapshotStorageGrain>(
                context.GrainId.GetGuidKey());
            await snapshotGrain.SaveAsync(blob, CancellationToken.None);
            _lastCapturedCheckpointOffset = checkpoint;
            _lastCapturedSnapshotBytes = blob.SnapshotBytes;
        }
        finally
        {
            _snapshotCaptureInFlight = false;
        }
    }

    /// <summary>
    /// Activation-side advisory handler. Wraps
    /// <see cref="CaptureSnapshotAsync"/> in a best-effort try/catch so
    /// a transient snapshot-storage failure does not block the leaf
    /// coming online. The next periodic recheck (or the next
    /// reactivation's advisory) re-attempts the capture.
    /// </summary>
    private async Task TryCaptureSnapshotForAdvisoryAsync()
    {
        try
        {
            await CaptureSnapshotAsync();
        }
        catch (Exception ex)
        {
            var logger = context.ActivationServices?
                .GetService<ILoggerFactory>()?
                .CreateLogger<BPlusLeafGrain>();
            logger?.LogWarning(
                ex,
                "Proactive snapshot capture for leaf {GrainId} failed; will retry on next periodic recheck or reactivation.",
                context.GrainId);
        }
    }

    /// <summary>
    /// Periodic snapshot-recheck hook, called by
    /// <see cref="FlushPendingCheckpointAsync"/> after every successful
    /// checkpoint persist. Increments the per-activation persist
    /// counter; when the counter reaches
    /// <see cref="LatticeOptions.LeafSnapshotReClassifyEveryNCheckpoints"/>
    /// it resets, re-classifies the leaf's WAL gap, and (on
    /// <see cref="FallOffLogDecision.SnapshotPending"/>) drives a
    /// capture. Returns synchronously when the option is <c>0</c>
    /// (disabled) or the threshold has not yet been reached.
    /// </summary>
    private async Task MaybeRunPeriodicSnapshotRecheckAsync()
    {
        if (state.State.TreeId is null)
        {
            return;
        }

        var resolved = await GetOptionsAsync();
        var threshold = resolved.LeafSnapshotReClassifyEveryNCheckpoints;
        if (threshold <= 0)
        {
            // Periodic recheck disabled. The activation-time advisory
            // path is the only proactive-capture driver.
            return;
        }

        _checkpointPersistCountSinceRecheck++;
        if (_checkpointPersistCountSinceRecheck < threshold)
        {
            return;
        }
        _checkpointPersistCountSinceRecheck = 0;

        if (_snapshotCaptureInFlight)
        {
            // A previous capture has not yet completed; skip this
            // recheck. The next post-threshold persist will retry.
            return;
        }

        // Cheap short-circuit: if the persisted checkpoint has not
        // advanced since the most recent successful capture, the
        // classifier would just re-derive an identical (or strictly
        // weaker) decision and any follow-on capture would write a
        // byte-identical blob. Skip the classifier RPCs and the
        // potential redundant SaveAsync.
        if (state.State.ProjectionCheckpointOffset == _lastCapturedCheckpointOffset)
        {
            return;
        }

        var detector = context.ActivationServices?.GetService<ILatticeFallOffLogDetector>();
        if (detector is null)
        {
            return;
        }

        try
        {
            // Per-partition classification: under multi-partition WAL
            // any partition raising SnapshotPending warrants a
            // proactive whole-leaf capture (the capture is partition-
            // independent - it serialises the full entry cache).
            var partitionCount = Math.Max(1, resolved.WalPartitions);
            for (var partition = 0; partition < partitionCount; partition++)
            {
                var decision = await detector.ClassifyAsync(
                    state.State.TreeId,
                    partition,
                    GetPersistedCheckpointForPartition(partition),
                    TimeSpan.Zero,
                    resolved,
                    CancellationToken.None);
                if (decision == FallOffLogDecision.SnapshotPending)
                {
                    await TryCaptureSnapshotForAdvisoryAsync();
                    return;
                }
            }
        }
        catch (Exception ex)
        {
            var logger = context.ActivationServices?
                .GetService<ILoggerFactory>()?
                .CreateLogger<BPlusLeafGrain>();
            logger?.LogWarning(
                ex,
                "Periodic snapshot recheck for leaf {GrainId} failed; will retry on next cadence.",
                context.GrainId);
        }
    }

    /// <summary>
    /// Activation-time rehydration seam. Consults the dedicated
    /// snapshot storage grain for a persisted blob and, when the blob
    /// is newer than the leaf's persisted
    /// <see cref="State.LeafNodeState.ProjectionCheckpointOffset"/>,
    /// repopulates the in-memory entry cache from the canonical byte
    /// rows and advances the persisted checkpoint to the snapshot's
    /// offset. The projection digest is invalidated (set to <c>null</c>)
    /// so the next read or fold lazily rebuilds it via the existing
    /// <c>EnsureProjectionHashInitialized</c> path; this preserves the
    /// canonical-full-walk hash invariant the chained internal-node
    /// fold depends on.
    /// <para>
    /// No-op preconditions: tree id unset (uninitialised leaf); no
    /// snapshot present; snapshot offset not strictly greater than the
    /// persisted checkpoint (a stale snapshot whose offset the leaf
    /// has already run past). After a successful rehydrate the caller
    /// (the activation hook) drives the WAL tail-replay from the new
    /// checkpoint forward, so a snapshot that covers a prefix of the
    /// WAL plus tail-replayed suffix produces a projection identical
    /// to a from-zero replay.
    /// </para>
    /// </summary>
    internal async Task<bool> TryRehydrateFromSnapshotAsync(CancellationToken cancellationToken)
    {
        if (state.State.TreeId is null)
        {
            return false;
        }

        cancellationToken.ThrowIfCancellationRequested();

        LeafSnapshotBlob? blob;
        try
        {
            var snapshotGrain = grainFactory.GetGrain<ILeafSnapshotStorageGrain>(
                context.GrainId.GetGuidKey());
            blob = await snapshotGrain.LoadAsync(cancellationToken);
        }
        catch
        {
            // Snapshot load is best-effort: a transient storage failure
            // must not block the leaf coming online. The activation
            // path falls through to the existing WAL-tail replay,
            // which can still recover the projection as long as the
            // WAL has not trimmed past the checkpoint.
            return false;
        }

        if (blob is null)
        {
            return false;
        }

        var checkpoint = state.State.ProjectionCheckpointOffset;
        if (blob.SnapshotOffset <= checkpoint)
        {
            // Snapshot is older than the persisted checkpoint; the
            // leaf has already applied past the snapshot via the
            // foreground path. Ignore the blob.
            return false;
        }

        // Bulk-load the canonical byte rows. We bypass StoreEntry
        // (the per-mutation LWW funnel) because the snapshot rows
        // are themselves a point-in-time projection; running them
        // through LWW would be a no-op against an empty cache but
        // would also re-fold the digest incrementally on every row.
        // We instead invalidate the digest below and let the lazy
        // full-walk recompute it.
        Cache.Clear();
        // Defensive: LeafSnapshotBlob.Rows is documented "never null"
        // and defaults to Array.Empty, but the setter is public and a
        // partially-deserialised blob could surface null. Treat null
        // as an empty row set rather than NPE-ing on the foreach.
        var rows = blob.Rows;
        if (rows is not null)
        {
            foreach (var row in rows)
            {
                Cache.StoreRow(row.Key, row.Value);
            }
        }

        // Advance the persisted checkpoint to match the snapshot.
        // The WAL tail replay below picks up at (SnapshotOffset, head].
        state.State.ProjectionCheckpointOffset = blob.SnapshotOffset;

        // Invalidate the digest so EnsureProjectionHashInitialized's
        // lazy backfill path recomputes the canonical full-walk hash
        // over the rehydrated cache. The chained internal-node fold
        // depends on this hash matching the canonical full-walk hash
        // bit-for-bit; recomputing from scratch is the only way to
        // guarantee equivalence with a from-zero replay.
        state.State.ProjectionHash = null;

        // Drop the cached XxHash128 hasher so the next contribution
        // allocates a fresh instance. Mirrors the rebuild seam in
        // BPlusLeafGrain.ProjectionAdmin.cs - keeps the rehydrated
        // activation indistinguishable from a fresh activation.
        DisposeProjectionHasher();

        // Carry the snapshot's persisted byte total forward so the next
        // per-persist byte-footprint publish reflects the rehydrated
        // snapshot footprint without re-reading the snapshot grain.
        _lastCapturedSnapshotBytes = blob.SnapshotBytes;

        return true;
    }

    /// <inheritdoc />
    public Task ForceDeactivateAsync()
    {
        // Test-only deactivation seam. Wraps the protected
        // Grain.DeactivateOnIdle() extension so integration tests can
        // drive activation-time rehydration end-to-end without relying
        // on the silo's idle-collection scheduler. The runtime
        // schedules the deactivation after the current grain turn
        // completes; the caller must briefly wait (e.g. a short delay
        // or a poll loop on a fresh activation) before observing the
        // post-rehydrate activation. We cannot block here without
        // deadlocking: OnDeactivateAsync can only run once this turn
        // ends.
        this.DeactivateOnIdle();
        return Task.CompletedTask;
    }
}