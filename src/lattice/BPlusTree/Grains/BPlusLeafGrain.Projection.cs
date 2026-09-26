using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// <see cref="ILeafProjection"/> implementation for <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/>.
/// Replays a single durably-committed mutation against the leaf's
/// in-memory state using LWW semantics; persists the projection
/// checkpoint offset alongside the leaf's existing storage row.
/// <para>
/// This seam is live in production: the activation-time cold-rebuild
/// path (<c>OnActivateAsync</c> -&gt; <c>ReplayWalSinceCheckpointAsync</c>
/// -&gt; <c>ReplayPartitionAsync</c>) drives
/// <see cref="ILeafProjection.Apply"/> over every WAL entry after the
/// persisted checkpoint. Because CRDT-mode Set records are delta-only on
/// the WAL (the encoder strips the post-merge <c>Value</c>), a
/// non-prepared CRDT replay must fold the typed delta into the prior
/// visible state rather than install <c>Value</c> verbatim - see
/// <see cref="ApplySet"/>.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Pending in-memory checkpoint offset that has been requested via
    /// <see cref="ILeafProjection.SetCheckpointOffsetAsync"/> but not
    /// yet durably persisted. <c>null</c> when no advance is pending
    /// (the persisted offset on <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState"/> is the
    /// source of truth).
    /// <para>
    /// Under <see cref="LatticeOptions.WalPartitions"/> greater than 1
    /// this map is keyed by partition; partition <c>0</c>'s entry is
    /// also mirrored into the scalar <c>ProjectionCheckpointOffset</c>
    /// on flush so a downgrade to a legacy silo still reads a valid
    /// single-partition shape.
    /// </para>
    /// </summary>
    private Dictionary<int, long>? _pendingCheckpointOffsetsByPartition;

    /// <summary>
    /// <see cref="Stopwatch.GetTimestamp"/> reading at the last durable
    /// checkpoint persist. Compared against
    /// <c>MaterialiserCheckpointInterval</c> on each advance to decide
    /// whether the time-driven flush should fire.
    /// </summary>
    private long _lastCheckpointPersistTimestamp = Stopwatch.GetTimestamp();

    void ILeafProjection.Apply(in LatticeMutation mutation)
    {
        switch (mutation.Kind)
        {
            case MutationKind.Set:
                if (mutation.IsPrepared)
                    ApplyPreparedSet(mutation);
                else
                    ApplySet(mutation);
                break;
            case MutationKind.Delete:
                if (mutation.IsPrepared)
                    ApplyPreparedDelete(mutation);
                else
                    ApplyDelete(mutation);
                break;
            case MutationKind.DeleteRange:
                ApplyDeleteRange(mutation);
                break;
            case MutationKind.TxCommit:
                ApplyTxCommit(mutation.TransactionId);
                AdvanceProjectionClock(mutation.Timestamp);
                break;
            case MutationKind.TxAbort:
                ApplyTxAbort(mutation.TransactionId);
                AdvanceProjectionClock(mutation.Timestamp);
                break;
            case MutationKind.Tombstone:
                ApplyTombstoneReap(mutation);
                AdvanceProjectionClock(mutation.Timestamp);
                break;
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(mutation),
                    mutation.Kind,
                    $"Unknown {nameof(MutationKind)} '{mutation.Kind}'.");
        }
    }

    Task<long> ILeafProjection.GetCheckpointOffsetAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        // Return the most recent offset the caller has communicated
        // (pending or persisted, whichever is higher) for the partition
        // the caller is currently scoped under. The "durably committed"
        // notion is preserved via FlushCheckpointAsync; this accessor
        // reports the materialiser's current view so a read-modify-
        // write caller observes its own most-recent advance.
        var partition = LatticeApplyOffsetContext.CurrentPartition ?? 0;
        return Task.FromResult(GetCurrentCheckpointForPartition(partition));
    }

    /// <summary>
    /// Returns the materialiser's current per-partition view of the
    /// projection checkpoint (max of pending and persisted) for
    /// <paramref name="partition"/>. Partition <c>0</c> always reflects
    /// the scalar <c>ProjectionCheckpointOffset</c> slot for wire-compat
    /// with legacy single-partition state.
    /// </summary>
    internal long GetCurrentCheckpointForPartition(int partition)
    {
        var persisted = GetPersistedCheckpointForPartition(partition);
        if (_pendingCheckpointOffsetsByPartition is not null
            && _pendingCheckpointOffsetsByPartition.TryGetValue(partition, out var pending))
        {
            return Math.Max(persisted, pending);
        }
        return persisted;
    }

    /// <summary>
    /// The single seam through which a <em>hinted</em> projection checkpoint is
    /// stamped: advances partition <paramref name="partition"/> to
    /// <paramref name="offset"/> only when that would move the checkpoint
    /// forward, and does nothing otherwise.
    /// <para>
    /// A hint is a WAL head captured at some earlier moment, so by the time it
    /// is stamped the target may already have applied past it - a split retry
    /// replays the heads captured at the original split, and both the donor and
    /// the sibling keep applying while the split is in flight.
    /// <c>ILeafProjection.SetCheckpointOffsetAsync</c> <em>rejects</em> a
    /// backward move by throwing <see cref="ArgumentOutOfRangeException"/>
    /// (which is load-bearing for its non-hint callers), so a stale hint that
    /// reached the seam would fault the enclosing batch. Every hint is
    /// therefore filtered here rather than at each call site: guarding one of
    /// two call sites and leaving the other is exactly how issue 905's fix left
    /// issue #3360 behind.
    /// </para>
    /// <para>
    /// The scope is opened inside this method, so a caller cannot stamp a hint
    /// against the wrong offset space, and the skip path opens no scope and
    /// starts no state machine - the split path is hot.
    /// </para>
    /// <para>
    /// A hint is also refused outright on a partition a starvation drive has
    /// latched as stale (issue #3477). That partition's WAL was trimmed past an
    /// offset its persisted checkpoint still needs, so the range between the
    /// checkpoint and the hinted head was never applied here. Stamping the hint
    /// would persist a checkpoint past rows the leaf does not hold - silent loss
    /// - and, because the latch is keyed on the persisted checkpoints, would
    /// clear the latch with nothing repaired. Only an apply or an operator reset
    /// may move a latched partition.
    /// </para>
    /// </summary>
    /// <param name="partition">The WAL partition ordinal the hint targets.</param>
    /// <param name="offset">The hinted WAL head offset. Non-positive offsets are ignored.</param>
    private ValueTask ApplyCheckpointHintAsync(int partition, long offset)
    {
        if (offset <= 0 || GetCurrentCheckpointForPartition(partition) >= offset)
        {
            return ValueTask.CompletedTask;
        }

        if (IsPartitionStaleLatched(partition))
        {
            LogCheckpointHintRefused(partition, offset);
            return ValueTask.CompletedTask;
        }

        return new ValueTask(StampCheckpointHintAsync(partition, offset));
    }

    /// <summary>
    /// Logs, once per latch, that a checkpoint hint was refused on a stale
    /// partition (issue #3477).
    /// </summary>
    private void LogCheckpointHintRefused(int partition, long offset)
    {
        var latch = _projectionStaleDriveLatch;
        if (ReferenceEquals(_checkpointHintRefusalLoggedFor, latch))
        {
            return;
        }

        _checkpointHintRefusalLoggedFor = latch;
        ResolveLogger()?.LogWarning(
            "Leaf {Leaf} on tree {Tree} refused a checkpoint hint to offset {Offset} on WAL partition {Partition}: a starvation drive found this partition stale, so the range between its persisted checkpoint ({Checkpoint}) and the hint was trimmed before this leaf applied it. Stamping the hint would persist a checkpoint past rows the leaf does not hold. The persisted checkpoint is left where it is; see docs/lattice/projection-rebuild.md.",
            context.GrainId,
            state.State.TreeId,
            offset,
            partition,
            GetPersistedCheckpointForPartition(partition));
    }

    /// <summary>
    /// The slow half of <see cref="ApplyCheckpointHintAsync"/>: opens the
    /// partition's apply-offset scope and drives the projection seam.
    /// </summary>
    private async Task StampCheckpointHintAsync(int partition, long offset)
    {
        using (LatticeApplyOffsetContext.BeginScope(partition, offset))
        {
            await ((ILeafProjection)this).SetCheckpointOffsetAsync(offset, CancellationToken.None);
        }
    }

    private long GetPersistedCheckpointForPartition(int partition)
    {
        if (partition == 0)
        {
            // Partition 0 lives in the scalar slot, which has no initializer and
            // is therefore born 0 rather than at the -1 "nothing applied"
            // sentinel every other partition uses (issue #2703). An unassigned 0
            // is genuinely ambiguous - it means either "checkpointed at offset
            // 0" or "never checkpointed" - so resolve it the conservative way
            // and report the sentinel. Under-reporting progress costs at most a
            // re-read of WAL offset 0, whose apply is idempotent; over-reporting
            // it skips the replay advance that would have recorded the
            // checkpoint, which is the defect itself. The first assignment sets
            // ProjectionCheckpointOffsetAssigned, after which a persisted 0 is
            // read at face value and the deferral disappears for good.
            //
            // This is the ONLY place the ambiguity is resolved. Every consumer
            // of a per-partition checkpoint reads through this accessor, so all
            // of them are honest by construction rather than by each carrying
            // its own guard.
            if (state.State.ProjectionCheckpointOffset == 0
                && state.State.ProjectionCheckpointOffsetAssigned != true)
            {
                return -1L;
            }
            return state.State.ProjectionCheckpointOffset;
        }
        var arr = state.State.ProjectionCheckpointOffsetsByPartition;
        if (arr is null || partition >= arr.Length)
            return -1L; // "nothing applied" sentinel - legacy state has no per-partition value.
        return arr[partition];
    }

    private void SetPersistedCheckpointForPartition(int partition, long value)
    {
        if (partition == 0)
        {
            state.State.ProjectionCheckpointOffset = value;
            // Records that the scalar now holds an assigned value, so a
            // persisted 0 stops reading as the ambiguous type default
            // (issue #2703).
            state.State.ProjectionCheckpointOffsetAssigned = true;
        }
        // Mirror partition 0 into the array slot (when present) so a
        // host that later reads ProjectionCheckpointOffsetsByPartition
        // observes a consistent picture; mirror non-zero partitions
        // into the array slot, growing it on first write. We never
        // shrink: the array's length is the maximum partition count
        // ever observed on this leaf.
        var arr = state.State.ProjectionCheckpointOffsetsByPartition;
        if (arr is null)
        {
            if (partition == 0)
                return; // legacy single-partition state, scalar slot suffices.
            arr = new long[partition + 1];
            // Seed every slot to the -1 "nothing applied" sentinel
            // except partition 0, which mirrors the scalar slot.
            for (var i = 0; i < arr.Length; i++)
                arr[i] = -1L;
            arr[0] = GetPersistedCheckpointForPartition(0);
            arr[partition] = value;
            state.State.ProjectionCheckpointOffsetsByPartition = arr;
            return;
        }
        if (partition >= arr.Length)
        {
            var grown = new long[partition + 1];
            arr.CopyTo(grown, 0);
            for (var i = arr.Length; i < grown.Length; i++)
                grown[i] = -1L;
            grown[partition] = value;
            state.State.ProjectionCheckpointOffsetsByPartition = grown;
            return;
        }
        arr[partition] = value;
    }

    async Task ILeafProjection.SetCheckpointOffsetAsync(long offset, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var partition = LatticeApplyOffsetContext.CurrentPartition ?? 0;
        var persisted = GetPersistedCheckpointForPartition(partition);
        long current = persisted;
        if (_pendingCheckpointOffsetsByPartition is not null
            && _pendingCheckpointOffsetsByPartition.TryGetValue(partition, out var p))
        {
            current = Math.Max(persisted, p);
        }

        if (offset < current)
        {
            throw new ArgumentOutOfRangeException(
                nameof(offset),
                offset,
                $"Projection checkpoint must be monotonically non-decreasing; current offset for partition {partition} is {current}.");
        }

        // Clamp the requested advance back behind any unresolved saga
        // prepare for this partition. See the multi-partition note in
        // RemovePendingTxOffsetsForTransaction: the clamp is partition-
        // scoped because cross-partition offsets are disjoint.
        if (MinUnresolvedPrepareOffsetForPartition(partition) is long minPrepare)
        {
            var clampFloor = minPrepare - 1;
            if (offset > clampFloor)
            {
                offset = clampFloor;
            }
        }

        if (offset < current)
        {
            // Clamp drove the requested offset back behind the current
            // materialised position. Silent no-op.
            return;
        }

        if (offset == current)
        {
            // Idempotent re-assert is a force-flush signal.
            await FlushPendingCheckpointAsync(persistEvenWithoutPendingAdvance: true);
            return;
        }

        (_pendingCheckpointOffsetsByPartition ??= new Dictionary<int, long>())[partition] = offset;

        // Coalescing predicate: persist if either threshold has been
        // exceeded. Zero interval means every-entry mode.
        var options = await GetOptionsAsync();
        var pendingEntries = offset - persisted;

        if (options.MaterialiserCheckpointInterval == TimeSpan.Zero
            || pendingEntries >= options.MaterialiserCheckpointEntries
            || HasIntervalElapsed(options.MaterialiserCheckpointInterval))
        {
            await FlushPendingCheckpointAsync(persistEvenWithoutPendingAdvance: false);
        }
    }

    async Task ILeafProjection.FlushCheckpointAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await FlushPendingCheckpointAsync(persistEvenWithoutPendingAdvance: false);
    }

    /// <summary>
    /// The <c>checkpoint_flush</c> graceful-deactivation barrier: the
    /// teardown persist. Commits the pending advance exactly as
    /// <see cref="FlushPendingCheckpointAsync"/> does (with
    /// <c>persistEvenWithoutPendingAdvance: false</c>, which is what
    /// <see cref="ILeafProjection.FlushCheckpointAsync"/> passes), and then runs
    /// the deactivation shape of the post-persist tail,
    /// <see cref="CompleteDeactivationCheckpointFlushTailAsync"/>, which publishes
    /// the final advance's durable pin once its snapshot recheck has run
    /// (issues #3393, #3599).
    /// </summary>
    /// <remarks>
    /// The commit is restated here rather than routed through
    /// <see cref="FlushPendingCheckpointAsync"/> so the persist path shared
    /// with every other caller - the coalescing fast path, the idempotent
    /// re-assert, the interface flush - and its tail
    /// <see cref="CompleteCheckpointFlushTailAsync"/> stay byte-identical: no
    /// new argument, no new branch, no change to the steady-state allocation
    /// profile. Keep the two commit sequences in step; the deactivation
    /// fixtures pin this one's observable effects (persisted checkpoint,
    /// digest dirtied and published, the cache-backed-coverage latch).
    /// </remarks>
    /// <param name="cancellationToken">The deactivation deadline.</param>
    private async Task FlushPendingCheckpointOnDeactivateAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_pendingCheckpointOffsetsByPartition is not { Count: > 0 } pending)
        {
            return;
        }

        foreach (var (partition, offset) in pending)
        {
            SetPersistedCheckpointForPartition(partition, offset);
        }

        _pendingCheckpointOffsetsByPartition = null;
        _checkpointAdvancedThisActivation = true;
        MarkDigestDirty();
        await PersistAsync();

        // Durable write committed; the tail is contained exactly as the
        // ordinary tail is (#2220).
        _lastCheckpointPersistTimestamp = Stopwatch.GetTimestamp();
        await CompleteDeactivationCheckpointFlushTailAsync(cancellationToken);
    }

    /// <summary>
    /// Synchronously flushes any pending checkpoint advance to durable
    /// storage. Called from <see cref="ILeafProjection.FlushCheckpointAsync"/>,
    /// from idempotent re-assert in <see cref="ILeafProjection.SetCheckpointOffsetAsync"/>, 
    /// from the coalescing fast-path when a threshold is met, and from
    /// the grain's graceful-deactivation hook so an unflushed advance
    /// is not lost on a clean shutdown.
    /// </summary>
    /// <param name="persistEvenWithoutPendingAdvance">
    /// When <c>true</c>, persist the leaf state even if no checkpoint
    /// advance is pending. Used by idempotent re-assert to commit
    /// in-memory Apply work that has accumulated since the previous
    /// persist.
    /// </param>
    private async Task FlushPendingCheckpointAsync(bool persistEvenWithoutPendingAdvance)
    {
        if (_pendingCheckpointOffsetsByPartition is { Count: > 0 } pending)
        {
            foreach (var (partition, offset) in pending)
            {
                SetPersistedCheckpointForPartition(partition, offset);
            }
            _pendingCheckpointOffsetsByPartition = null;
            // A forward checkpoint advance here is driven by cache-resident
            // applies (foreground writes or WAL tail replay folded into the
            // in-memory cache before SetCheckpointOffsetAsync queued the
            // advance). Latch that so the graceful-deactivation snapshot
            // capture knows this activation produced cache-backed coverage it
            // may safely persist - as opposed to a cold reactivation whose
            // checkpoint was merely restored from state (see
            // TryCaptureSnapshotOnDeactivateAsync and the #1535 no-loss gate).
            _checkpointAdvancedThisActivation = true;
            // The checkpoint offset is a field of the published
            // ChildDigestSnapshot, so an advance must propagate upward
            // even when the projection hash itself is unchanged - the
            // parent's SubtreeHighestCheckpointOffset aggregate
            // depends on it.
            MarkDigestDirty();
            await PersistAsync();
            // The durable advance has now committed. Per the #2220
            // invariant, no failure in the post-persist notification tail
            // (cursor report, inline upward digest publish, snapshot
            // recheck) may propagate out of this flush and destroy the
            // activation once the durable write has landed.
            await CompleteCheckpointFlushTailAsync();
            return;
        }

        if (persistEvenWithoutPendingAdvance)
        {
            await PersistAsync();
            // Durable write committed; contain the post-persist tail so a
            // notification failure cannot destroy the activation (#2220).
            await CompleteCheckpointFlushTailAsync();
        }
    }

    /// <summary>
    /// Runs the post-persist notification tail of a checkpoint flush -
    /// the cursor report, the inline upward digest publish, and the
    /// periodic snapshot recheck - with each step contained so a failure
    /// cannot propagate out of <see cref="FlushPendingCheckpointAsync"/>
    /// once the durable write has already committed.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The invariant (issue #2220): after <c>PersistAsync</c> commits the
    /// advanced checkpoint, this activation is durably correct. The
    /// remaining steps are notifications, not part of the durability
    /// contract <c>PersistAsync</c> has already satisfied, so none of them
    /// may tear the activation down. The field mechanism was the inline
    /// upward digest publish: a synchronous parent-chain publish whose
    /// latency consumed the activation budget during cold replay, so the
    /// flush faulted, the activation was destroyed, and the reactivation
    /// re-drove the same replay - a loop. Containing the tail breaks that
    /// loop.
    /// </para>
    /// <para>
    /// Each failure is recorded at warning with its exception, never
    /// swallowed silently, so a genuine upward cascade (for example an
    /// issue #2218-class fault) stays observable. Each is ALSO counted on
    /// <see cref="LatticeMetrics.LeafCheckpointFlushTailFailures"/>, tagged
    /// with the step, because a log line was the only signal these steps
    /// produced: the enclosing <c>checkpoint_flush</c> deactivation barrier
    /// reports success when the durable write lands and only the tail
    /// faults, so the barrier counter read zero while every leaf in a silo
    /// faulted here (issue #3393). The digest stays dirty
    /// on a failed publish (<c>PublishCurrentDigestAndClearDirtyAsync</c>
    /// clears the flag only on success) so the coalescing timer or the
    /// next mutation re-drives it; the cursor report and snapshot recheck
    /// are idempotent and re-run on the next flush. Steps are contained
    /// independently so a failure in one does not skip the others.
    /// </para>
    /// </remarks>
    private async Task CompleteCheckpointFlushTailAsync()
    {
        _lastCheckpointPersistTimestamp = Stopwatch.GetTimestamp();

        try
        {
            await ReportCursorIfActiveAsync();
        }
        catch (Exception ex)
        {
            RecordCheckpointFlushTailFailure(LatticeMetrics.CheckpointFlushTailCursorReport);
            ResolveLogger()?.LogWarning(
                ex,
                "Leaf {GrainId}: cursor report failed after a durable checkpoint flush; the checkpoint is "
                + "persisted and the report re-drives on the next flush. The activation is retained (#2220).",
                context.GrainId);
        }

        try
        {
            // Structural callers bypass the c2-xxviii coalescing window so
            // the parent's chained-fold observes the new checkpoint offset
            // before this method returns. A publish fault or its bounded
            // latency (see PublishCurrentDigestAsync) must not propagate:
            // the durable write has committed and the digest stays dirty
            // for out-of-band re-drive.
            await PublishDigestUpwardInlineAsync();
        }
        catch (Exception ex)
        {
            RecordCheckpointFlushTailFailure(LatticeMetrics.CheckpointFlushTailInlineDigestPublish);
            ResolveLogger()?.LogWarning(
                ex,
                "Leaf {GrainId}: inline upward digest publish failed after a durable checkpoint flush; the "
                + "checkpoint is persisted, the digest stays dirty and the publish re-drives out of band. "
                + "The activation is retained (#2220).",
                context.GrainId);
        }

        var keptRevisionBeforeRecheck = _snapshotKeptRevision;
        try
        {
            // Arm the coverage-lag bound here as well as at activation. A leaf
            // BORN in this activation has no tree id when the replay samples it
            // (the birth seam seeds it afterwards), so the activation site
            // declines - correctly, since arming there would resolve options for
            // an empty id and can deadlock the silo. This site runs on a leaf
            // that has provably been seeded and has provably persisted a
            // checkpoint, which is exactly the population the bound is for, and
            // the call is idempotent so it costs one field read thereafter.
            await EnsureCoverageLagTimerAsync();
            await MaybeRunPeriodicSnapshotRecheckAsync(fromCheckpointPersist: true);
        }
        catch (Exception ex)
        {
            RecordCheckpointFlushTailFailure(LatticeMetrics.CheckpointFlushTailSnapshotRecheck);
            ResolveLogger()?.LogWarning(
                ex,
                "Leaf {GrainId}: periodic snapshot recheck failed after a durable checkpoint flush; the "
                + "checkpoint is persisted and the recheck re-runs on the next flush. The activation is "
                + "retained (#2220).",
                context.GrainId);
        }

        // A starvation drive or warm rescue persists through this tail and then
        // publishes after its own recheck, so publishing here too would only
        // send the same pin twice.
        if (_snapshotKeptRevision == keptRevisionBeforeRecheck
            || _starvationDriveInFlight
            || _warmRescueInFlight)
        {
            return;
        }

        try
        {
            // Issue #3599. The cursor report above published the pin BEFORE the
            // recheck, so it was clamped at the pre-capture coverage, and a
            // capture the store just kept restamped coverage. Without this
            // publish nothing banks the higher pin until the next checkpoint
            // persist, which a write-idle leaf never produces. Published only
            // when a capture actually landed, so the steady-state tail keeps its
            // single debounced mirror and no extra round trip.
            await FlushDurableMaterialiserFrontierAsync();
            _durableFrontierBarriered = true;
        }
        catch (Exception ex)
        {
            RecordCheckpointFlushTailFailure(LatticeMetrics.CheckpointFlushTailCursorReport);
            ResolveLogger()?.LogWarning(
                ex,
                "Leaf {GrainId}: publishing the durable pin after a snapshot capture failed; the checkpoint "
                + "and coverage are persisted and the pin republishes on the next flush or coverage-lag "
                + "tick. The activation is retained (#2220, #3599).",
                context.GrainId);
        }
    }

    /// <summary>
    /// The deactivation shape of <see cref="CompleteCheckpointFlushTailAsync"/>,
    /// run for the teardown persist only (issue #3393).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>The final advance's pin is published here, and awaited.</b> On the
    /// ordinary tail the pin rides the cursor report as a debounced
    /// fire-and-forget mirror, which is right for the steady state and wrong
    /// for the last persist of an activation: nothing guarantees a debounced
    /// write lands before the activation is gone, and the dedicated
    /// <c>frontier_pin</c> barrier that was meant to cover it runs after the
    /// snapshot capture, by which point the recorded drain had already torn
    /// every activation down. So the durable write of the advance this persist
    /// just committed happens in this tail, through the batched, awaited
    /// <see cref="FlushDurableMaterialiserFrontierAsync"/>, ahead of every later
    /// barrier the teardown runs. The pin it publishes is unchanged in shape:
    /// resolved per partition by <c>ResolveDurablePinForPartition</c> from the
    /// PERSISTED checkpoint (never the pending one, issue #3476) and capped by
    /// durable snapshot coverage. This is not a write-through: it runs once per
    /// deactivation, and the pin grain's own batching is untouched.
    /// </para>
    /// <para>
    /// <b>The pin is published before the snapshot recheck and again after it
    /// when a capture landed</b> (issue #3599). The first publish is #3393's:
    /// it precedes the recheck so a recheck that overruns the drain deadline or
    /// throws can never cost the teardown its pin. Because the pin is clamped
    /// by durable coverage and coverage is restamped only by a capture the store
    /// kept, that first publish carries the pre-capture coverage; when the
    /// recheck's capture is kept (the kept revision moved, the same gate the
    /// ordinary tail uses) the pin is republished at the post-capture coverage
    /// rather than left to the <c>frontier_pin</c> barrier, which a drain
    /// deadline most often skips. The clamp itself is unchanged: each publish
    /// carries <c>min(persisted, coverage at publish time)</c> and never more.
    /// </para>
    /// <para>
    /// <b>The inline upward digest publish is deferred</b>, not dropped: it is
    /// flagged and published by <c>OnDeactivateAsync</c> after the durability
    /// barriers, still attributed to this tail's inline-digest step. It is the
    /// slow step - a synchronous parent-chain hop - and running it here put
    /// its latency in front of the snapshot capture and the pin, which is the
    /// mechanism #3393 records.
    /// </para>
    /// <para>
    /// The coverage-lag timer is NOT re-armed here, unlike the ordinary tail:
    /// <c>OnDeactivateAsync</c> disposed it before the first barrier precisely so
    /// no tick races the final capture.
    /// </para>
    /// <para>
    /// Each step is contained and counted on
    /// <see cref="LatticeMetrics.LeafCheckpointFlushTailFailures"/> exactly as on
    /// the ordinary tail, the pin publish under the cursor-report step it
    /// previously rode on, so the durable write that has already committed is
    /// never undone by a notification failure (#2220).
    /// </para>
    /// </remarks>
    /// <param name="cancellationToken">The deactivation deadline.</param>
    private async Task CompleteDeactivationCheckpointFlushTailAsync(CancellationToken cancellationToken)
    {
        _deactivationInlineDigestDeferred = true;

        // Issue #3393: the final advance's pin is published FIRST, ahead of the
        // recheck, so a recheck that overruns the drain deadline or throws can
        // never cost the teardown its pin. It resolves per partition to
        // min(persisted, existing coverage).
        await PublishDeactivationDurablePinAsync(afterRecheck: false, cancellationToken);


        var keptRevisionBeforeRecheck = _snapshotKeptRevision;
        try
        {
            await MaybeRunPeriodicSnapshotRecheckAsync(fromCheckpointPersist: true);
        }
        catch (Exception ex)
        {
            RecordCheckpointFlushTailFailure(LatticeMetrics.CheckpointFlushTailSnapshotRecheck);
            ResolveLogger()?.LogWarning(
                ex,
                "Leaf {GrainId}: periodic snapshot recheck failed after the final checkpoint flush of a "
                + "graceful deactivation; the checkpoint is persisted, its durable pin was already published at "
                + "the existing coverage, and the deactivation snapshot capture still runs (#2220, #3599).",
                context.GrainId);
        }

        // Issue #3599: a capture the store kept during the recheck restamped
        // coverage, so the pin published above is clamped at the PRE-capture
        // coverage. Republish now rather than leave the post-capture value to
        // the frontier_pin barrier, the step a drain deadline most often skips.
        // Gated on the kept revision exactly as the ordinary tail is, so a
        // recheck that captured nothing, failed, or had its save declined costs
        // no second round trip, and the clamp is never loosened.
        if (_snapshotKeptRevision != keptRevisionBeforeRecheck)
        {
            await PublishDeactivationDurablePinAsync(afterRecheck: true, cancellationToken);

        }

        try
        {
            await ReportCursorIfActiveAsync(publishDurablePin: false);
        }
        catch (Exception ex)
        {
            RecordCheckpointFlushTailFailure(LatticeMetrics.CheckpointFlushTailCursorReport);
            ResolveLogger()?.LogWarning(
                ex,
                "Leaf {GrainId}: cursor report failed after the final checkpoint flush of a graceful "
                + "deactivation; the checkpoint is persisted and its durable pin was already published (#3393).",
                context.GrainId);
        }
    }

    /// <summary>
    /// Publishes the durable pin from <see cref="CompleteDeactivationCheckpointFlushTailAsync"/>
    /// through the batched, awaited <see cref="FlushDurableMaterialiserFrontierAsync"/>,
    /// and never throws.
    /// </summary>
    /// <param name="afterRecheck">
    /// <see langword="false"/> for the #3393 publish that precedes the recheck;
    /// <see langword="true"/> for the #3599 republish after a capture the store kept.
    /// </param>
    /// <param name="cancellationToken">The deactivation deadline.</param>
    private async Task PublishDeactivationDurablePinAsync(bool afterRecheck, CancellationToken cancellationToken)
    {
        try
        {
            await FlushDurableMaterialiserFrontierAsync(cancellationToken);

            // The first-real-frontier batched flush the cursor report would
            // otherwise perform has just happened.
            _durableFrontierBarriered = true;
        }
        catch (Exception ex)
        {
            RecordCheckpointFlushTailFailure(LatticeMetrics.CheckpointFlushTailCursorReport);
            if (afterRecheck)
            {
                ResolveLogger()?.LogWarning(
                    ex,
                    "Leaf {GrainId}: republishing the durable pin after the final snapshot recheck of a "
                    + "graceful deactivation failed; the pre-capture pin is already published and the "
                    + "frontier-pin barrier retries the publish (#3599).",
                    context.GrainId);
            }
            else
            {
                ResolveLogger()?.LogWarning(
                    ex,
                    "Leaf {GrainId}: publishing the durable pin for the final checkpoint of a graceful "
                    + "deactivation failed; the checkpoint is persisted and the frontier-pin barrier retries the "
                    + "publish (#3393).",
                    context.GrainId);
            }
        }
    }

    /// <summary>
    /// Publishes the teardown persist's inline upward digest, deferred by
    /// <see cref="CompleteDeactivationCheckpointFlushTailAsync"/> behind the
    /// durability barriers (issue #3393), and never throws.
    /// </summary>
    /// <remarks>
    /// The publish is the one the tail used to run inline - the same
    /// <see cref="PublishDigestUpwardInlineAsync"/>, which also retires any
    /// pending coalesced publish - and its failure is counted on the tail's
    /// existing inline-digest step, so moving it changes when it runs and
    /// nothing about what it publishes or how its failure is reported. A
    /// failed publish leaves the digest dirty; it is staleness-tolerant and the
    /// next mutation after reactivation republishes it.
    /// </remarks>
    /// <param name="drainsPendingCoalescedPublish">
    /// Whether a coalesced digest publish was pending when the hook started.
    /// Before the reorder the leading <c>digest_publish</c> barrier drained it
    /// and recorded <c>deactivation_flush</c>, after which the tail's inline
    /// publish had nothing left to send; this one publish now carries both, so
    /// it is recorded as that drain to keep the outcome's meaning.
    /// </param>
    /// <param name="cancellationToken">
    /// The deactivation deadline. The publish is not started once it has
    /// expired: the activation is torn down by then and the hop could only
    /// fault, so it is counted as a failed inline-digest step instead.
    /// </param>
    private async Task PublishDeferredDeactivationDigestAsync(
        bool drainsPendingCoalescedPublish,
        CancellationToken cancellationToken)
    {
        _deactivationInlineDigestDeferred = false;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (drainsPendingCoalescedPublish && TryBeginPendingDigestDrain(cancellationToken, out var parentId))
            {
                await PublishDrainedDigestAsync(parentId, cancellationToken);
            }
            else
            {
                await PublishDigestUpwardInlineAsync();
            }
        }
        catch (Exception ex)
        {
            RecordCheckpointFlushTailFailure(LatticeMetrics.CheckpointFlushTailInlineDigestPublish);
            try
            {
                ResolveLogger()?.LogWarning(
                    ex,
                    "Leaf {GrainId}: the inline upward digest publish of a graceful deactivation's final "
                    + "checkpoint, deferred behind the durability barriers (#3393), failed; the digest stays "
                    + "dirty and is republished on the next mutation after reactivation.",
                    context.GrainId);
            }
            catch (Exception)
            {
                // Observability must never fail a deactivation.
            }
        }
    }

    /// <summary>
    /// Records one checkpoint-flush TAIL failure against
    /// <see cref="LatticeMetrics.LeafCheckpointFlushTailFailures"/> (issue
    /// #3393), and never throws.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The tail's containment is deliberate (#2220) and unchanged by this
    /// method: the durable write has already committed and a failed
    /// notification must not tear the activation down. What was missing was any
    /// signal other than a log line - the enclosing <c>checkpoint_flush</c>
    /// deactivation barrier reports SUCCESS when only the tail faults, so
    /// <see cref="LatticeMetrics.LeafDeactivationBarrierFailures"/> stayed at
    /// zero while a production drain took 1,699 inline-digest-publish faults.
    /// </para>
    /// <para>
    /// Tags are resolved defensively and INDIVIDUALLY, exactly as the barrier
    /// path does. Both helpers read grain state, which throws "Attempt to
    /// access an invalid activation" once the activation has been invalidated -
    /// and this tail is reachable from the deactivation hook, where that is not
    /// merely possible but the observed norm. Resolving them as arguments to
    /// <c>Add</c> would let a throwing tag lookup suppress the measurement
    /// entirely, so the fault this method exists to report would itself report
    /// as nothing. Telemetry must never decide whether a fault is observable
    /// (the #2312 rule).
    /// </para>
    /// </remarks>
    private void RecordCheckpointFlushTailFailure(KeyValuePair<string, object?> step)
    {
        try
        {
            var treeTag = TryResolveTailTag(LeafTreeTag, LatticeMetrics.TagTree);
            var tenantTag = TryResolveTailTag(
                LeafTenantTag, LatticeTenantLabel.ForTree(null).Key);

            LatticeMetrics.LeafCheckpointFlushTailFailures.Add(1, treeTag, step, tenantTag);
        }
        catch (Exception)
        {
            // Observability must never fail a checkpoint flush.
        }

        // Resolves one tag without letting the lookup itself suppress the
        // measurement it is meant to label. The empty value is what these
        // instruments already record for a leaf whose tree is unregistered, so
        // it adds no new tag value and no new cardinality.
        static KeyValuePair<string, object?> TryResolveTailTag(
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
    }

    private bool HasIntervalElapsed(TimeSpan interval)
    {
        if (interval == Timeout.InfiniteTimeSpan)
            return false;
        var elapsedTicks = Stopwatch.GetTimestamp() - _lastCheckpointPersistTimestamp;
        var elapsedMs = elapsedTicks * 1000.0 / Stopwatch.Frequency;
        return elapsedMs >= interval.TotalMilliseconds;
    }

    private void ApplySet(in LatticeMutation mutation)
    {
        // Non-prepared CRDT-mode Set records are delta-only on the WAL:
        // the producer never materialises the post-merge state into
        // WalRecord.Value and the canonical encoder strips the slot on
        // encode, so a durable replay observes Value == null with the
        // typed delta carried in mutation.Delta and the convergence rule
        // in mutation.Mode. Fold the delta into this leaf's current
        // visible state instead of installing the (null) Value verbatim.
        // Folds compose incrementally across multiple replayed deltas for
        // the same key because each fold reads the prior post-fold state
        // back out of the cache and the WAL replays entries in offset
        // order. LWW Sets (Mode == LwwRegister, Delta == null) skip the
        // fold and replay byte-for-byte as before.
        if (mutation.Mode != LatticeMergeMode.LwwRegister
            && mutation.Delta is not null
            && !mutation.IsPrepared)
        {
            // FoldPreparedCrdtDelta resolves the registered CrdtShape,
            // deserialises the typed delta, loads the prior visible state
            // (or an empty primitive when the key is absent / tombstoned),
            // applies MergeDelta, and re-serialises the post-fold bytes -
            // exactly the reconstruction this replay path needs.
            var foldedBytes = FoldPreparedCrdtDelta(mutation.Key, mutation.Delta, mutation.Mode);
            // Capture the prior row's expiry before the merge so the expiry
            // can be re-forced to the running max afterwards (see below).
            var priorExpiry = Cache.TryPeekRow(mutation.Key, out var priorRow, out _)
                ? priorRow.ExpiresAtTicks
                : 0L;
            var folded = new LwwValue<byte[]>
            {
                Value = foldedBytes,
                Timestamp = mutation.Timestamp,
                IsTombstone = false,
                ExpiresAtTicks = mutation.ExpiresAtTicks,
                OriginClusterId = mutation.OriginClusterId,
                VectorClock = mutation.VectorClock,
            };
            MergeIntoProjection(mutation.Key, folded);
            // Re-force the per-entry expiry to the max-absolute-ticks join of
            // the prior row's expiry and this record's expiry. MergeIntoProjection
            // resolves the row by HLC (StoreEntry), so when the prior row's HLC
            // dominates this record's it keeps the prior (possibly smaller)
            // expiry, discarding the join result. Each WAL record carries the
            // running max expiry stamped at foreground apply time and replay is
            // offset-ordered, so folding max(priorExpiry, record.expiry) here
            // reconstructs exactly the foreground cumulative-max result,
            // independent of the HLC ordering of the interleaved records.
            ForceEntryExpiry(mutation.Key, Math.Max(priorExpiry, mutation.ExpiresAtTicks));
            // Record the per-key CRDT merge mode so a snapshot capture built
            // from this replayed projection labels the key faithfully. Set
            // after MergeIntoProjection because its StoreRow write evicts any
            // prior recorded mode.
            Cache.SetMergeMode(mutation.Key, mutation.Mode);
            AdvanceProjectionClock(mutation.Timestamp);
            return;
        }

        // Defensive guard for a delta-only CRDT record whose authored
        // merge mode could not be recovered (mutation.Mode == LwwRegister
        // yet a typed Delta is present and the record is neither prepared
        // nor a tombstone). This is the signature of a legacy WAL record
        // authored before the mode was made durable (wire id 26) on a
        // tree the resolver does not know - its mode is unrecoverable
        // from every durable source, so the fold above could not run.
        // Installing the stripped null Value verbatim would empty the key
        // via LWW (a non-tombstone null at the record's recent timestamp
        // beats the prior folded value) - the exact data-loss symptom of
        // issue #926. Skip the record instead so the last folded value
        // (loaded from the leaf checkpoint) is preserved; the lost
        // increment is unrecoverable here, but the key is not destroyed.
        if (mutation.Delta is not null
            && !mutation.IsPrepared
            && !mutation.IsTombstone
            && mutation.Value is null)
        {
            return;
        }

        var incoming = new LwwValue<byte[]>
        {
            Value = mutation.IsTombstone ? null : mutation.Value,
            Timestamp = mutation.Timestamp,
            IsTombstone = mutation.IsTombstone,
            ExpiresAtTicks = mutation.ExpiresAtTicks,
            OriginClusterId = mutation.OriginClusterId,
            VectorClock = mutation.VectorClock,
        };
        MergeIntoProjection(mutation.Key, incoming);
        AdvanceProjectionClock(mutation.Timestamp);
    }

    /// <summary>
    /// Replay path for a prepared-phase Set mutation. Routes the entry
    /// into the per-leaf pending-tx map rather than the visible
    /// projection so concurrent readers see pre-saga state until the
    /// saga's terminal mark surfaces.
    /// </summary>
    private void ApplyPreparedSet(in LatticeMutation mutation)
    {
        var incoming = new LwwValue<byte[]>
        {
            Value = mutation.IsTombstone ? null : mutation.Value,
            Timestamp = mutation.Timestamp,
            IsTombstone = mutation.IsTombstone,
            ExpiresAtTicks = mutation.ExpiresAtTicks,
            OriginClusterId = mutation.OriginClusterId,
            VectorClock = mutation.VectorClock,
        };
        // Carry the WAL-stamped typed CRDT delta and merge mode into the
        // pending-tx delta side-map so the activation-time replay
        // reconstructs the same fold state the foreground commit recorded.
        // The prepared WAL record carries both Delta and Mode (see
        // WalRecordConverter), and WalRecordConverter.FromWalRecord copies
        // them onto the mutation, so an LWW prepared write (Mode ==
        // LwwRegister, Delta == null) reconstructs no side-map entry and
        // replays byte-for-byte as before.
        AddPreparedMutation(
            mutation.TransactionId,
            mutation.Key,
            incoming,
            delta: mutation.Delta,
            mode: mutation.Mode);
        AdvanceProjectionClock(mutation.Timestamp);
    }

    private void ApplyDelete(in LatticeMutation mutation)
    {
        var tombstone = new LwwValue<byte[]>
        {
            Value = null,
            Timestamp = mutation.Timestamp,
            IsTombstone = true,
            ExpiresAtTicks = 0,
            OriginClusterId = mutation.OriginClusterId,
            VectorClock = mutation.VectorClock,
        };
        MergeIntoProjection(mutation.Key, tombstone);
        AdvanceProjectionClock(mutation.Timestamp);
    }

    /// <summary>
    /// Replay path for a prepared-phase Delete mutation. Routes the
    /// tombstone into the per-leaf pending-tx map rather than the
    /// visible projection.
    /// </summary>
    private void ApplyPreparedDelete(in LatticeMutation mutation)
    {
        var tombstone = new LwwValue<byte[]>
        {
            Value = null,
            Timestamp = mutation.Timestamp,
            IsTombstone = true,
            ExpiresAtTicks = 0,
            OriginClusterId = mutation.OriginClusterId,
            VectorClock = mutation.VectorClock,
        };
        AddPreparedMutation(mutation.TransactionId, mutation.Key, tombstone);
        AdvanceProjectionClock(mutation.Timestamp);
    }

    private void ApplyDeleteRange(in LatticeMutation mutation)
    {
        var endExclusive = mutation.EndExclusiveKey;
        if (endExclusive is null)
            return;

        var startInclusive = mutation.Key;
        if (string.CompareOrdinal(startInclusive, endExclusive) >= 0)
            return;

        // Tombstone every existing entry inside the range. The mutation
        // carries one HLC for the whole batch; replays converge under LWW
        // because the tombstone's timestamp dominates any earlier write
        // and is dominated by any later write.
        //
        // A predicate-filtered range delete carries the explicit set of
        // matched keys (evaluated once at the authoring leaf). Replay
        // tombstones exactly that set - never re-deriving membership from a
        // predicate - so recovery is deterministic and independent of the
        // value bytes this projection currently holds.
        List<string>? toRewrite = null;
        var matchedKeys = mutation.MatchedKeys;
        if (matchedKeys is not null)
        {
            foreach (var key in matchedKeys)
            {
                if (string.CompareOrdinal(key, startInclusive) < 0
                    || string.CompareOrdinal(key, endExclusive) >= 0)
                    continue;
                if (Cache.TryGetRow(key, out _))
                    (toRewrite ??= []).Add(key);
            }
        }
        else
        {
            // Walked in bounded key windows clipped to the delete range, rather
            // than over the whole-cache view. Only keys are retained here, so
            // nothing on this path needs the rows to stay resident - and the
            // whole-cache view calls HydrateAll, which ends in DetachSnapshot
            // and leaves every row resident for the life of the activation,
            // forfeiting the leaf's cheap division (issue #2771). A replayed
            // unpredicated range delete is a routine mutation, so before this
            // change any leaf replaying one lost its lazily hydrated frame.
            //
            // Clipping matters as much as windowing. An unpredicated range
            // delete may span the whole leaf, so a single EnumerateRange over
            // [startInclusive, endExclusive) would materialise all of it at
            // once and peak exactly where HydrateAll did - bounded only in the
            // sense that it could be evicted afterwards, which is not the
            // property this needs.
            //
            // The windows are disjoint, exhaustive and ascending, so
            // intersecting each with the delete range visits every key in the
            // range exactly once and in the same order the whole-cache walk
            // produced. The explicit lower/upper guards the one-pass walk
            // needed are now carried by the range bounds themselves.
            foreach (var (windowStart, windowEnd) in Cache.GetFullScanWindowsWithoutHydrating())
            {
                var from = windowStart is null
                    || string.CompareOrdinal(windowStart, startInclusive) < 0
                        ? startInclusive
                        : windowStart;
                var to = windowEnd is null
                    || string.CompareOrdinal(windowEnd, endExclusive) > 0
                        ? endExclusive
                        : windowEnd;
                if (string.CompareOrdinal(from, to) >= 0)
                    continue;

                foreach (var (key, _) in Cache.EnumerateRange(from, to))
                {
                    (toRewrite ??= []).Add(key);
                }
            }
        }

        if (toRewrite is null)
            return;

        var tombstone = new LwwValue<byte[]>
        {
            Value = null,
            Timestamp = mutation.Timestamp,
            IsTombstone = true,
            ExpiresAtTicks = 0,
            OriginClusterId = mutation.OriginClusterId,
            VectorClock = mutation.VectorClock,
        };

        foreach (var key in toRewrite)
        {
            MergeIntoProjection(key, tombstone);
        }

        AdvanceProjectionClock(mutation.Timestamp);
    }

    private void MergeIntoProjection(string key, LwwValue<byte[]> incoming)
    {
        // Replay-path projection writes are non-migration: they
        // mirror the foreground commit / drain / backstop semantics
        // (cross-shard migrations are persisted via PersistAsync, not
        // through the WAL, so they never re-emerge here at replay
        // time). The incoming value carries IsMigrated=false by default
        // (every WAL-authored LwwValue is non-migration), so when it
        // wins the merge inside StoreEntry it naturally clears any
        // stale migration marker, keeping replay's post-state bit-
        // identical to foreground's.
        StoreEntry(key, incoming);
    }

    /// <summary>
    /// Replay path for a <see cref="MutationKind.Tombstone"/> reap
    /// envelope authored by <c>CompactTombstonesAsync</c>. Physically
    /// removes the stamped key from the visible projection if the
    /// existing entry is a tombstone (or an already-expired live
    /// entry) whose timestamp does not dominate the reap envelope's
    /// timestamp. The HLC guard preserves LWW convergence under
    /// replay reordering - a reap envelope from an earlier compaction
    /// pass cannot resurrect-and-then-remove a freshly-written live
    /// entry that the same WAL slice already replayed.
    /// </summary>
    private void ApplyTombstoneReap(in LatticeMutation mutation)
    {
        if (!Cache.TryGetRow(mutation.Key, out var existing))
            return;
        if (existing.Timestamp > mutation.Timestamp)
            return;
        // Reap is well-formed only against tombstones or expired live
        // entries - any other shape indicates a stale envelope whose
        // counterpart Set replay landed later, and the live entry must
        // stay. The compactor only emits Tombstone envelopes for entries
        // that already met the tombstone-or-expired predicate at
        // compaction time, so this guard is a defence-in-depth check.
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        if (!existing.IsTombstone && !existing.IsExpired(nowTicks))
            return;
        RemoveEntry(mutation.Key);
    }

    private void AdvanceProjectionClock(HybridLogicalClock incoming)
    {
        if (incoming > state.State.Clock)
            state.State.Clock = incoming;
    }
}
