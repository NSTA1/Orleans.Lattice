using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Snapshot-capture partial for <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/>. Adds the
/// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.CaptureSnapshotAsync"/> seam that copies
/// the per-activation entry cache into a canonical byte-row
/// <see cref="LeafSnapshotBlob"/> and persists it through the dedicated
/// <see cref="ILeafSnapshotStorageGrain"/> keyed by this leaf's grain
/// id. The capture is read-only on the leaf side - it stamps the blob
/// with the already-persisted <c>ProjectionCheckpointOffset</c> and
/// does not mutate any leaf state.
/// <para>
/// Capture is driven by the leaf itself (not by the maintenance
/// grain): when the fall-off-log detector raises the
/// <see cref="Orleans.Lattice.BPlusTree.Grains.FallOffLogDecision.SnapshotPending"/> advisory at
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
    /// <see cref="Orleans.Lattice.BPlusTree.Grains.FallOffLogDecision.SnapshotPending"/>. The activation
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
    /// Set once this activation genuinely advances a per-partition projection
    /// checkpoint over cache-resident applies - i.e. the pending-advance branch
    /// of <see cref="FlushPendingCheckpointAsync"/> runs, which only happens
    /// after foreground writes or a WAL tail replay have folded new entries
    /// into the in-memory cache and moved the checkpoint forward. It is the
    /// safety precondition for the graceful-deactivation snapshot capture
    /// (<see cref="TryCaptureSnapshotOnDeactivateAsync"/>): a checkpoint that
    /// advanced this way is, by construction, backed by data the cache holds,
    /// so capturing that cache and stamping the new checkpoint truthfully
    /// records coverage. A leaf that merely reactivated cold - its persisted
    /// checkpoint restored from state or a rehydrate that reset uncovered
    /// partitions to <c>-1</c>, with no forward apply this activation - never
    /// sets this, so the deactivation hook does not capture an empty/partial
    /// cache and falsely claim coverage of a prefix the WAL alone still holds
    /// (the #1535 no-loss invariant). Reset to <c>false</c> on every fresh
    /// activation because it is a plain instance field, never persisted.
    /// </summary>
    private bool _checkpointAdvancedThisActivation;

    /// <summary>
    /// Set once this activation cold-rebuilt the in-memory cache from the start
    /// of the readable WAL over a <b>pre-existing durable checkpoint</b> - the
    /// residual-liveness path in <c>ReplayWalSinceCheckpointAsync</c>, latched only
    /// for a partition whose persisted <c>ProjectionCheckpointOffset</c> was
    /// already greater than zero, whose cold-cache override (<c>-1</c>) drove the
    /// replay from the absolute WAL start, and whose durable-frontier fall-off
    /// guard (#945) passed - so the checkpointed prefix provably still survives in
    /// the readable WAL and the rebuild reconstructs the entire readable window.
    /// Like <see cref="_checkpointAdvancedThisActivation"/> this is a safety
    /// precondition for the graceful-deactivation snapshot capture
    /// (<see cref="TryCaptureSnapshotOnDeactivateAsync"/>): a faithful full rebuild
    /// leaves the cache holding a superset of every checkpointed prefix, so
    /// capturing that cache and stamping each partition's checkpoint truthfully
    /// records coverage. It closes the residual liveness gap #1537 leaves open -
    /// an already-converged leaf (its checkpoint already at head) cold-reactivates,
    /// rebuilds its full cache, but advances no checkpoint, so
    /// <see cref="_checkpointAdvancedThisActivation"/> alone stays <c>false</c> and
    /// the block pin would never lift. A brand-new leaf has no pre-existing
    /// checkpoint (persisted offset <c>0</c>), never satisfies the guard condition,
    /// and so its foreground writes are never auto-covered - preserving the #1535
    /// no-loss invariant and leaving an uncovered checkpoint pinned at the Zero
    /// block. Reset to <c>false</c> on every fresh activation because it is a plain
    /// instance field, never persisted.
    /// </summary>
    private bool _cacheRebuiltFromWalStartThisActivation;

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

    /// <summary>
    /// Per-partition WAL offset that a durable leaf snapshot is known to
    /// cover for this activation, or <see langword="null"/> when no snapshot
    /// coverage is known. Slot <c>p</c> holds the highest offset a durable
    /// snapshot covers for partition <c>p</c>; <c>-1</c> (or an absent slot)
    /// means "no durable snapshot covers this partition", so the WAL prefix
    /// is the only durable copy and must not be trimmed.
    /// <para>
    /// Set from a loaded <see cref="LeafSnapshotBlob"/> at activation
    /// (rehydrate, both accept and decline paths - a loaded blob is durable
    /// regardless of whether it repopulates the cache) and advanced after a
    /// successful <see cref="CaptureSnapshotAsync"/>. Read by
    /// <c>ResolveDurablePinForPartition</c> to gate the durable materialiser
    /// pin at <c>min(checkpoint, coveredOffset)</c> so the WAL GC never
    /// authorises trimming a checkpointed prefix that no snapshot covers.
    /// </para>
    /// </summary>
    private long[]? _durableSnapshotOffsetsByPartition;

    /// <summary>
    /// Returns the highest WAL offset a durable snapshot is known to cover
    /// for <paramref name="partition"/> this activation, or <c>-1</c> when no
    /// durable snapshot covers it. Consumed by the coverage-gated durable-pin
    /// resolution.
    /// </summary>
    internal long DurableSnapshotCoverageForPartition(int partition)
    {
        var arr = _durableSnapshotOffsetsByPartition;
        if (arr is null || partition < 0 || partition >= arr.Length)
            return -1L;
        return arr[partition];
    }

    /// <summary>
    /// Records that a durable snapshot covers each partition through the
    /// offsets in <paramref name="blob"/>. A blob predating the
    /// per-partition field (legacy) is treated as covering partition 0 only,
    /// through its scalar <see cref="LeafSnapshotBlob.SnapshotOffset"/>.
    /// Coverage only ever advances (per-partition max) so an out-of-order or
    /// stale load can never lower a known covered offset.
    /// </summary>
    private void RecordDurableSnapshotCoverage(LeafSnapshotBlob blob)
    {
        var perPartition = blob.SnapshotOffsetsByPartition;
        if (perPartition is null || perPartition.Length == 0)
        {
            // Legacy blob: only partition 0 coverage is known.
            perPartition = new[] { blob.ScalarOffsetOrSentinel() };
        }

        var current = _durableSnapshotOffsetsByPartition;
        if (current is null || current.Length < perPartition.Length)
        {
            var grown = new long[perPartition.Length];
            for (var i = 0; i < grown.Length; i++)
            {
                var existing = current is not null && i < current.Length ? current[i] : -1L;
                grown[i] = Math.Max(existing, perPartition[i]);
            }
            _durableSnapshotOffsetsByPartition = grown;
            return;
        }

        for (var i = 0; i < perPartition.Length; i++)
            current[i] = Math.Max(current[i], perPartition[i]);
    }
    /// <inheritdoc />
    public Task CaptureSnapshotAsync() => CaptureSnapshotCoreAsync(CancellationToken.None);

    /// <summary>
    /// Cancellable core of the snapshot-capture seam. The grain-interface
    /// entrypoint keeps its parameterless wire signature and delegates here
    /// with <see cref="CancellationToken.None"/>; the graceful-deactivation
    /// path (issue #1965) supplies Orleans' deactivation token instead, so a
    /// leaf that overruns the deactivation deadline abandons its blob write
    /// rather than being cancelled inside the runtime's own frame.
    /// </summary>
    private async Task CaptureSnapshotCoreAsync(CancellationToken cancellationToken)
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
        // an empty cache would create a snapshot the activation path
        // is required to ignore, so the work is pure overhead. But the
        // check MUST be per-partition: gating on partition 0's scalar
        // checkpoint alone (the historical behaviour) starves every
        // leaf whose live keys hash only to non-zero partitions -
        // partition 0 stays at -1 forever while a non-zero partition
        // holds committed, block-pinned, un-trimmable WAL, so coverage
        // never advances and that partition's WAL grows unbounded
        // (reopening the #1489/#1490 growth class). Proceed when ANY
        // partition has absorbed at least one entry.
        var resolved = await GetOptionsAsync();
        var partitionCount = Math.Max(1, resolved.WalPartitions);
        var checkpoint = state.State.ProjectionCheckpointOffset;
        var anyPartitionCheckpointed = checkpoint >= 0;
        for (var p = 1; p < partitionCount && !anyPartitionCheckpointed; p++)
        {
            if (GetCurrentCheckpointForPartition(p) >= 0)
                anyPartitionCheckpointed = true;
        }
        if (!anyPartitionCheckpointed)
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
            // key-ordered KeyValuePair sequence; the resulting buffer is
            // a self-contained value snapshot that survives subsequent
            // foreground mutations on this activation.
            //
            // The buffer is rented rather than allocated: it exists only to
            // hand an ordered span to the encoder, so it must not outlive the
            // capture. Draining the cache before reading Count keeps the two
            // consistent (EnumerateRows materialises any deferred rows first).
            var cacheRows = Cache.EnumerateRows();
            var rowCount = Cache.Count;
            var buffer = ArrayPool<LeafSnapshotRow>.Shared.Rent(rowCount);
            IReadOnlyList<LeafSnapshotRow> legacyRows = Array.Empty<LeafSnapshotRow>();
            byte[]? encodedRows;
            try
            {
                var written = 0;
                foreach (var kv in cacheRows)
                {
                    if (written == rowCount)
                    {
                        break;
                    }

                    buffer[written++] = new LeafSnapshotRow(kv.Key, kv.Value, Cache.GetMergeMode(kv.Key));
                }

                var rows = new ReadOnlySpan<LeafSnapshotRow>(buffer, 0, written);
                if (resolved.LeafSnapshotBinaryEncodingEnabled)
                {
                    // Compact binary frame: one allocation, raw value bytes, no
                    // per-row serializer envelope. A blob captured this way
                    // leaves the legacy Rows slot empty, which is how the lazy
                    // rewrite happens - the next natural capture of a leaf
                    // whose durable blob is still legacy simply persists the
                    // frame instead, with no migration pass anywhere.
                    encodedRows = LeafSnapshotCodec.Encode(rows);
                }
                else
                {
                    encodedRows = null;
                    var copy = new LeafSnapshotRow[written];
                    rows.CopyTo(copy);
                    legacyRows = copy;
                }
            }
            finally
            {
                // Rows hold references (key strings, value arrays); clear on
                // return so a pooled buffer cannot pin a released snapshot.
                ArrayPool<LeafSnapshotRow>.Shared.Return(buffer, clearArray: true);
            }

            // Per-partition coverage. Under the default WalPartitions = 8
            // the scalar SnapshotOffset only describes partition 0, but the
            // snapshot rows are the full entry cache and therefore cover the
            // checkpointed prefix of EVERY partition. Stamp each partition's
            // current checkpoint so the coverage-gated trim floor can
            // authorise trimming each partition's prefix independently. Slot
            // 0 mirrors the scalar SnapshotOffset for wire-compat.
            var perPartitionOffsets = new long[partitionCount];
            perPartitionOffsets[0] = checkpoint;
            for (var p = 1; p < partitionCount; p++)
                perPartitionOffsets[p] = GetCurrentCheckpointForPartition(p);

            var blob = new LeafSnapshotBlob
            {
                SnapshotOffset = LeafSnapshotBlob.NormalizeScalarOffset(checkpoint),
                Rows = legacyRows,
                EncodedRows = encodedRows,
                CapturedAtTicks = DateTime.UtcNow.Ticks,
                // Snapshot row footprint matches the leaf-state byte formula
                // by construction (the snapshot is a copy of the cache), so
                // use the cache's incrementally-maintained running total
                // rather than re-walking every row at capture time.
                SnapshotBytes = Cache.StateBytes,
                SnapshotOffsetsByPartition = perPartitionOffsets,
            };

            var snapshotGrain = grainFactory.GetGrain<ILeafSnapshotStorageGrain>(
                context.GrainId.GetGuidKey());
            await snapshotGrain.SaveAsync(blob, cancellationToken);
            _lastCapturedSnapshotBytes = blob.SnapshotBytes;
            // The blob is now durable, so the checkpointed prefix it covers
            // is recoverable independently of the WAL. Advance the coverage
            // view; the NEXT durable-pin flush will then authorise trimming
            // up to min(checkpoint, coveredOffset) per partition. Advancing
            // coverage only AFTER a confirmed SaveAsync (and the pin lagging
            // by design - the cursor report precedes this capture in
            // FlushPendingCheckpointAsync) keeps the pin conservative: it can
            // never license a trim ahead of durable coverage.
            RecordDurableSnapshotCoverage(blob);
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
    /// <para>
    /// A cancellation raised by the caller's token is <b>not</b> a failure
    /// and is swallowed without logging: the graceful-deactivation caller
    /// (issue #1965) passes Orleans' deactivation token, and thousands of
    /// leaves going idle together would otherwise turn one log flood into
    /// another. Coverage is only ever advanced after a confirmed save, so an
    /// abandoned capture leaves the pin conservative and the WAL retained.
    /// </para>
    /// </summary>
    private async Task TryCaptureSnapshotForAdvisoryAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await CaptureSnapshotCoreAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Deliberate abandonment on a deactivation deadline; see above.
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
    /// <see cref="Orleans.Lattice.BPlusTree.Grains.FallOffLogDecision.SnapshotPending"/>) drives a
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

        // Per-partition "already covered" debounce. A capture is worth
        // running only when SOME partition's current checkpoint has advanced
        // beyond the offset a durable snapshot already covers for it. Gating
        // on partition 0's scalar checkpoint alone (the historical behaviour)
        // froze coverage whenever partition 0 idled while other partitions
        // took writes past the threshold: the busy partition's durable pin
        // stayed pinned at its stale covered offset (min(checkpoint, covered)
        // == covered) and its retained WAL grew unbounded. Compare each
        // partition's current checkpoint against its recorded durable coverage
        // so no partition can be starved of capture, and so a projection that
        // is already fully covered still short-circuits without a redundant
        // byte-identical blob write.
        var recheckPartitionCount = Math.Max(1, resolved.WalPartitions);
        var anyPartitionNeedsCapture = false;
        for (var p = 0; p < recheckPartitionCount; p++)
        {
            if (GetCurrentCheckpointForPartition(p) > DurableSnapshotCoverageForPartition(p))
            {
                anyPartitionNeedsCapture = true;
                break;
            }
        }
        if (!anyPartitionNeedsCapture)
        {
            return;
        }

        // Unconditional cadence capture (issue: cold-restart residual
        // prefix loss). Historically this path re-ran the fall-off-log
        // classifier and captured ONLY when it raised the SnapshotPending
        // advisory. That gate is unsafe under the coverage-gated trim floor:
        // the durable pin now BLOCKS trimming a checkpointed prefix that no
        // snapshot covers, so the WAL tail stays low and the classifier's
        // proximity heuristic (tail near checkpoint) never fires - the block
        // would then be held forever and the WAL would grow unbounded,
        // reintroducing the #1489/#1490 growth class. Capturing on the fixed
        // checkpoint cadence instead guarantees every blocked prefix is
        // covered by a durable snapshot within at most
        // LeafSnapshotReClassifyEveryNCheckpoints checkpoints, after which
        // the pin advances to min(checkpoint, coveredOffset) and the WAL GC
        // trims the now-covered prefix. This is what keeps retention bounded
        // (invariant b) while the coverage gate keeps it lossless
        // (invariant a). The single-flight guard above and the SaveAsync
        // best-effort try/catch bound the cost of a slow snapshot store.
        await TryCaptureSnapshotForAdvisoryAsync();
    }

    /// <summary>
    /// Graceful-deactivation snapshot-capture hook (issue #1537), invoked
    /// from <c>OnDeactivateAsync</c> after the final checkpoint flush and
    /// immediately before the durable materialiser-pin flush.
    /// <para>
    /// The two standing proactive-capture drivers - the activation-time
    /// advisory (<see cref="_activationSnapshotPending"/>) and the periodic
    /// recheck (<see cref="MaybeRunPeriodicSnapshotRecheckAsync"/>) - both
    /// require the leaf to stay activated long enough to either cross the
    /// activation-time WAL-tail margin or accumulate
    /// <see cref="LatticeOptions.LeafSnapshotReClassifyEveryNCheckpoints"/>
    /// checkpoint persists. A short-lived bursty activation (activate, take a
    /// few writes and checkpoints, then deactivate before the cadence
    /// threshold) fires neither, so a data-bearing leaf can checkpoint, go
    /// dormant, and leave its <see cref="HybridLogicalClock.Zero"/> block pin
    /// held forever - the shared-shard WAL is then retained without bound.
    /// This is the liveness gap on the safe side of the #1535 coverage gate:
    /// the block pin never loses data, but nothing lifts it.
    /// </para>
    /// <para>
    /// Capturing here closes the gap. Any checkpointed-but-uncovered partition
    /// gets a durable snapshot before the leaf goes dormant, so the durable
    /// pin flush that follows resolves the pin to
    /// <c>min(checkpoint, coveredOffset) == checkpoint</c>
    /// (<see cref="ResolveDurablePinForPartition"/>) and the WAL GC can trim
    /// the now-covered prefix. The capture is best-effort by construction
    /// (<see cref="TryCaptureSnapshotForAdvisoryAsync"/> swallows storage
    /// faults, and <see cref="CaptureSnapshotAsync"/> advances coverage only
    /// after a confirmed <c>SaveAsync</c>): if it fails, coverage does not
    /// advance, the pin stays a Zero block pin, and the WAL is retained rather
    /// than trimmed ahead of durable coverage - so the #1535 no-loss
    /// invariant is preserved. Crash deactivations bypass
    /// <c>OnDeactivateAsync</c> (and thus this hook) by design; the persisted
    /// checkpoint still bounds the next activation's replay cost.
    /// </para>
    /// </summary>
    private async Task TryCaptureSnapshotOnDeactivateAsync(CancellationToken cancellationToken)
    {
        if (state.State.TreeId is null)
        {
            return;
        }

        // Safety gate (the #1535 no-loss invariant). Capture only when the
        // in-memory cache faithfully holds every checkpointed prefix it would
        // stamp as covered. Two independent activations satisfy that:
        //   (a) this activation advanced a checkpoint over cache-resident applies
        //       (foreground writes or a completed tail replay) -
        //       _checkpointAdvancedThisActivation; or
        //   (b) this activation cold-rebuilt the cache from the WAL start over a
        //       pre-existing durable checkpoint (persisted offset > 0) whose
        //       prefix provably survives in the readable WAL (the #945 fall-off
        //       guard passed), so the cache holds the entire readable window, a
        //       superset of the checkpointed prefix -
        //       _cacheRebuiltFromWalStartThisActivation.
        // Signal (b) is what closes the residual #1537 gap: an already-converged
        // leaf (checkpoint already at head) cold-reactivates and rebuilds its
        // full cache but advances no checkpoint, so (a) alone stays false and the
        // Zero block pin would never lift. A brand-new leaf (no pre-existing
        // checkpoint) never satisfies (b), and a leaf that merely reactivated cold
        // WITHOUT a full rebuild (its persisted checkpoint restored from state
        // with a non-empty cache, or a rehydrate that reset uncovered partitions
        // to -1 with no forward apply) satisfies neither and does not capture, so
        // it can never write a snapshot claiming coverage of data the cache never
        // held. The one shape that could make a -1 rebuild unfaithful - a trimmed
        // WAL prefix with no covering snapshot - throws at activation (the #945
        // fall-off guard) before signal (b) is ever latched.
        if (!_checkpointAdvancedThisActivation && !_cacheRebuiltFromWalStartThisActivation)
        {
            return;
        }

        // "Already covered" debounce, mirroring the periodic recheck's
        // per-partition check: only pay for a cache copy plus blob write when
        // some partition's checkpoint has actually advanced beyond the offset
        // a durable snapshot already covers for it. A leaf that already reached
        // full coverage on the periodic cadence (the long-lived case that does
        // not hit this gap) short-circuits here with no allocation.
        var resolved = await GetOptionsAsync();
        var partitionCount = Math.Max(1, resolved.WalPartitions);
        var anyPartitionNeedsCapture = false;
        for (var p = 0; p < partitionCount; p++)
        {
            if (GetCurrentCheckpointForPartition(p) > DurableSnapshotCoverageForPartition(p))
            {
                anyPartitionNeedsCapture = true;
                break;
            }
        }
        if (!anyPartitionNeedsCapture)
        {
            return;
        }

        await TryCaptureSnapshotForAdvisoryAsync(cancellationToken);
    }

    /// <summary>
    /// Activation-time rehydration seam. Consults the dedicated
    /// snapshot storage grain for a persisted blob and, when the blob
    /// is newer than the leaf's persisted
    /// <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.ProjectionCheckpointOffset"/>,
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

        // Reject a blob whose row payload cannot be read in full - a truncated
        // or corrupt binary frame, or a legacy row list with a null key -
        // BEFORE anything treats it as durable. This must happen ahead of
        // RecordDurableSnapshotCoverage: reporting coverage for a prefix the
        // blob cannot actually reproduce is precisely what would authorise the
        // coverage-gated WAL GC to trim the last durable copy of that prefix.
        // An unreadable blob is "no snapshot", so the activation falls through
        // to the ordinary WAL replay with its own fall-off guards intact.
        if (!blob.ValidateRowPayload())
        {
            return false;
        }

        // A loaded blob is durable regardless of whether it repopulates the
        // cache below. Record its coverage NOW - on both the accept and the
        // decline path - so the coverage-gated durable pin
        // (ResolveDurablePinForPartition) knows which checkpointed prefixes a
        // snapshot already protects and can authorise the WAL GC to trim
        // them. Missing this on the decline path is exactly what would keep a
        // block pin from ever lifting (unbounded WAL).
        RecordDurableSnapshotCoverage(blob);

        var checkpoint = state.State.ProjectionCheckpointOffset;
        if (blob.ScalarOffsetOrSentinel() <= checkpoint)
        {
            // The snapshot is at or behind the persisted partition-0
            // checkpoint. Historically this always declined ("we already
            // absorbed everything the snapshot contains via the WAL"). That
            // is only true when the WAL prefix the snapshot covers is still
            // readable. Under the coverage-gated trim floor the WAL GC trims a
            // checkpointed prefix precisely BECAUSE a snapshot covers it, so
            // in the cold-restart steady state the snapshot can be the ONLY
            // durable copy of [0, checkpoint]. Probe the WAL tail per
            // partition: if any partition's oldest readable offset has
            // advanced past 0 the covered prefix has been trimmed and this
            // snapshot MUST rehydrate it; if every tail is still 0 the WAL is
            // intact and the snapshot is redundant, so decline to avoid a
            // pointless cache replace (preserving issue #919's
            // Activation_ignores_snapshot_at_equal_offset intent for the
            // WAL-intact case). See the residual cold-restart prefix-loss
            // finding.
            if (!await AnyPartitionWalPrefixTrimmedAsync(cancellationToken))
            {
                return false;
            }
        }

        // Bulk-load the canonical byte rows. We bypass StoreEntry
        // (the per-mutation LWW funnel) because the snapshot rows
        // are themselves a point-in-time projection; running them
        // through LWW would be a no-op against an empty cache but
        // would also re-fold the digest incrementally on every row.
        // We instead invalidate the digest below and let the lazy
        // full-walk recompute it.
        //
        // Bounded hydration (issue #1839). When the blob carries a binary
        // frame and partial hydration is enabled, attach the frame as the
        // cache's lazily hydrated backing instead of decoding it: the cache
        // then reports the whole snapshot's row count, footprint and live
        // count immediately, and materialises entry ranges out of the frame
        // only as reads require them. Activation cost stops being a function
        // of blob size. The attach can decline (a legacy row list, or a frame
        // whose rows are not strictly ascending and therefore not safely
        // seekable), and declining simply falls through to the full decode
        // below - the pre-#1839 behaviour, byte for byte.
        //
        // This changes nothing about coverage. RecordDurableSnapshotCoverage
        // ran above off the blob's own offsets, and a capture materialises
        // every row before it writes, so a partially hydrated leaf can never
        // stamp coverage for rows it does not hold.
        var hydrationOptions = await GetOptionsAsync();
        var attached = false;
        if (hydrationOptions.LeafPartialHydrationEnabled
            && blob.HasBinaryRowPayload()
            && blob.EncodedRows is { Length: > 0 } frame)
        {
            attached = Cache.TryAttachSnapshot(frame, hydrationOptions.LeafHydrationResidentBytes);
        }

        if (!attached)
        {
            Cache.Clear();
            // Encoding-agnostic streaming decode. A binary blob decodes each row
            // straight into the cache with no intermediate row collection; a
            // legacy blob walks its row list exactly as before. The payload was
            // validated above, so a mid-stream parse failure is impossible here
            // and would throw rather than silently truncate the snapshot.
            foreach (var row in blob.EnumerateRows())
            {
                Cache.StoreRow(row.Key, row.Value);
                if (row.MergeMode is { } mode)
                {
                    // Recover the durable per-key merge-mode discriminator from
                    // the checkpoint so a freeze/capture after a rehydrate-from-
                    // checkpoint (without a full WAL replay) stays mode-faithful.
                    Cache.SetMergeMode(row.Key, mode);
                }
            }
        }

        // Advance the persisted checkpoint per partition to match the
        // snapshot EXACTLY - including resetting a partition to -1 when the
        // snapshot predates that partition's checkpoint. After Cache.Clear()
        // above the in-memory cache holds ONLY the snapshot's rows, so every
        // partition's checkpoint must equal what the reloaded cache actually
        // contains for it. The old `if (perPartition[p] >= 0)` skip left an
        // uncovered partition's checkpoint AHEAD of the reloaded cache: the
        // tail replay then resumed at (checkpoint_p, head] and silently
        // skipped [0, checkpoint_p], dropping every entry the snapshot did not
        // carry. Resetting to -1 is loss-free precisely because the coverage
        // gate never trims an uncovered partition: perPartition[p] == -1 on
        // the latest blob means partition p was never snapshot-covered, so
        // ResolveDurablePinForPartition held its block pin and its full WAL
        // [0, checkpoint_p] survives - the from-zero replay rebuilds it
        // intact. (Coverage is monotonic and we always load the latest blob,
        // so an ever-covered partition would carry perPartition[p] >= 0.)
        var perPartition = blob.SnapshotOffsetsByPartition;
        if (perPartition is not null && perPartition.Length > 0)
        {
            for (var p = 0; p < perPartition.Length; p++)
                SetPersistedCheckpointForPartition(p, perPartition[p]);
        }
        else
        {
            state.State.ProjectionCheckpointOffset = blob.ScalarOffsetOrSentinel();
        }

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

    /// <summary>
    /// Best-effort probe: has any WAL partition's oldest still-readable
    /// offset advanced past <c>0</c> (i.e. has the WAL GC trimmed a prefix)?
    /// Used by <see cref="TryRehydrateFromSnapshotAsync"/> to decide whether
    /// a snapshot at or behind the persisted checkpoint is the sole durable
    /// copy of a trimmed prefix (rehydrate) or redundant against an intact
    /// WAL (decline). Mirrors the #945 fall-off guard's coordinator
    /// resolution (<c>{treeId}/{partition}</c>, <c>GetTailOffsetAsync</c>).
    /// A coordinator failure is swallowed and treated as "not trimmed" for
    /// that partition: the caller then declines the at/behind snapshot and
    /// the normal WAL replay (plus the #945 guard) still protects against
    /// loss.
    /// <para>
    /// Each partition is probed independently (issue #2082). A single
    /// faulting coordinator previously aborted the whole probe, so one slow
    /// partition reported the entire tree as untrimmed even when later
    /// partitions had advanced tails - and because this probe is itself a
    /// grain call into the tree being replayed, it is most likely to fault
    /// on exactly the saturated tree where declining costs a full replay
    /// from offset zero. That made the decline self-reinforcing. Faults are
    /// now confined to the partition that raised them.
    /// </para>
    /// <para>
    /// The decline itself is deliberately unchanged: "could not probe" and
    /// "probed everything, all genuinely zero" both return
    /// <see langword="false"/>, because a probe that could not prove a
    /// prefix was trimmed must never be read as proof that it was. The
    /// distinction is surfaced in the log rather than in the return value,
    /// so durability semantics are identical and only the diagnosis
    /// improves.
    /// </para>
    /// </summary>
    private async Task<bool> AnyPartitionWalPrefixTrimmedAsync(CancellationToken cancellationToken)
    {
        var treeId = state.State.TreeId;
        if (string.IsNullOrEmpty(treeId))
            return false;

        int partitionCount;
        try
        {
            var resolved = await GetOptionsAsync();
            partitionCount = Math.Max(1, resolved.WalPartitions);
        }
        catch (Exception ex)
        {
            // Without the partition count there is nothing to probe, so this
            // one genuinely ends the probe rather than a single partition.
            ResolveLogger()?.LogWarning(
                ex,
                "Leaf {GrainId}: could not resolve the WAL partition count for tree '{TreeId}', so the "
                + "prefix-trimmed probe could not run and the snapshot rehydrate declines. If this repeats, "
                + "the leaf is replaying its whole WAL window on every activation.",
                context.GrainId,
                treeId);
            return false;
        }

        var unprobed = 0;
        Exception? firstFault = null;
        for (var partition = 0; partition < partitionCount; partition++)
        {
            try
            {
                var coordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
                    $"{treeId}/{partition}");
                var tail = await coordinator.GetTailOffsetAsync(cancellationToken);
                if (tail > 0)
                    return true;
            }
            catch (OperationCanceledException)
            {
                // The activation is going away; further probes cannot help.
                return false;
            }
            catch (Exception ex)
            {
                // Confine the fault to this partition and keep probing: a
                // later partition may well have a trimmed prefix, and missing
                // it costs a full replay.
                unprobed++;
                firstFault ??= ex;
            }
        }

        if (unprobed > 0)
        {
            ResolveLogger()?.LogWarning(
                firstFault,
                "Leaf {GrainId}: {Unprobed} of {Partitions} WAL partition(s) of tree '{TreeId}' could not be "
                + "probed for a trimmed prefix, and no probed partition reported one, so the snapshot "
                + "rehydrate declines and this activation replays from the oldest readable offset. This is a "
                + "safe outcome but an expensive one, and it is NOT evidence that no prefix was trimmed - the "
                + "unprobed partitions are simply unknown. Repeats on a saturated tree are self-reinforcing, "
                + "because the probe is itself a call into the tree being replayed.",
                context.GrainId,
                unprobed,
                partitionCount,
                treeId);
        }

        return false;
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