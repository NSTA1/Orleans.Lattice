using Orleans.Lattice.Primitives;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

// Compaction dirty-leaf fast-path tracking on the shard root.
//
// Background. The legacy compaction coordinator activates every leaf in the
// shard's chain to ask whether it has tombstones to reap. On a tree where
// most leaves have nothing to do this is a large activation-cost spike for
// no work. The architecturally correct fix is to push the "is this leaf
// dirty?" signal up to the shard root, which already routes every Delete.
//
// Design (U9h-B coalesced).
//   * Per-shard persisted state holds a Dictionary<leafGrainId, markHlc>
//     of leaves that have observed a routed Delete since the last drain.
//   * On Delete routing the shard root max-merges the destination leaf
//     into the dictionary IN MEMORY with a monotonically-advancing HLC,
//     sets a dirty flag, and arms a coalescing flush timer. The Delete
//     hot path returns immediately - no storage write is performed.
//   * The flush timer drains the dirty flag and persists the state in
//     one WriteStateAsync per coalescing window
//     (`LatticeOptions.DirtyLeafFlushIntervalMs`, default 50 ms),
//     regardless of how many distinct leaves were dirtied. This removes
//     the only hot-path shard-root storage write that the U9h audit
//     classified as racing concurrent SetManyAsync turns.
//   * `ClearDirtyLeavesUpToAsync` always writes (it is admin path) and
//     therefore implicitly flushes pending in-memory marks in the same
//     persist call.
//   * `OnDeactivateAsync` performs a final flush so clean shutdown does
//     not lose pending marks. A silo crash that loses an in-memory mark
//     causes that leaf to be visited by the legacy chain-walk fallback
//     on the next compaction pass (the snapshot will appear empty on
//     re-activation), so the loss bound is one missed leaf per crashed
//     activation per dirty-window - bounded and self-healing.
//
// Wire compatibility.
//   * Legacy ShardRootState rounds-trips with an empty dictionary and
//     a zero HybridLogicalClock for LastDirtyAdvance. The coordinator
//     interprets that combination as "no signal yet, fall back to the
//     legacy chain walk for this pass" and the dirty set is populated
//     from that pass forward.
internal sealed partial class ShardRootGrain
{
    /// <summary>
    /// Most-recent HLC stamped onto a dirty-leaf entry. Seeded lazily
    /// from <see cref="State.ShardRootState.LastDirtyAdvance"/> on first
    /// use so that marks in the post-restart window strictly monotonically
    /// advance past the persisted watermark.
    /// </summary>
    private HybridLogicalClock _dirtyMarkClock;
    private bool _dirtyMarkClockInitialized;

    /// <summary>
    /// In-memory flag toggled by <see cref="MarkLeafDirtyAsync"/> and
    /// cleared by <see cref="FlushPendingDirtyMarksAsync"/>. When
    /// <see langword="true"/>, <see cref="state"/>.State has accumulated
    /// dirty-leaf marks that have not yet been persisted by the flush
    /// timer. The flag itself is the only synchronisation primitive
    /// the hot path needs - Orleans' single-threaded reentrancy model
    /// guarantees serial access to it from grain turns, and the timer
    /// callback runs on the same activation scheduler.
    /// </summary>
    private bool _dirtyMarksPendingFlush;

    /// <summary>
    /// Set while a <see cref="WriteShardStateAsync"/> call from
    /// <see cref="FlushPendingDirtyMarksAsync"/> is awaiting storage,
    /// so a re-entrant timer tick or admin-path flush request observes
    /// the in-flight write and skips issuing a redundant one. Marks
    /// committed during an in-flight flush re-arm the dirty flag for
    /// the next flush cycle.
    /// </summary>
    private bool _dirtyFlushInFlight;

    /// <summary>
    /// Coalescing flush timer registered on first dirty mark. Disposed
    /// in <c>OnDeactivateAsync</c>. The timer's callback is a no-op
    /// when <see cref="_dirtyMarksPendingFlush"/> is <see langword="false"/>,
    /// so a long-quiescent shard pays only the timer-tick scheduler cost.
    /// </summary>
    private IDisposable? _dirtyFlushTimer;

    /// <summary>
    /// Records <paramref name="leafId"/> as dirty by max-merging a
    /// freshly-ticked HLC into <see cref="state"/>.State and arming the
    /// coalescing flush timer. Returns synchronously - the storage
    /// write is performed off-path by the timer callback.
    /// <para>
    /// Best-effort: a transient WriteStateAsync failure inside the timer
    /// callback is logged and swallowed. The dirty mark is a
    /// maintenance-cadence hint, not a correctness signal - on persist
    /// failure the dirty flag remains set and the next timer tick
    /// retries.
    /// </para>
    /// </summary>
    private Task MarkLeafDirtyAsync(GrainId leafId)
    {
        if (!_dirtyMarkClockInitialized)
        {
            _dirtyMarkClock = state.State.LastDirtyAdvance;
            _dirtyMarkClockInitialized = true;
        }
        _dirtyMarkClock = HybridLogicalClock.Tick(_dirtyMarkClock);

        // LWW max-merge into the in-memory state. The compaction
        // coordinator reads this dictionary directly via
        // GetDirtyLeavesSinceLastCompactionAsync so an unpersisted mark
        // is still routable; it is only at-risk of being lost on an
        // unclean silo shutdown before the timer fires.
        var key = leafId.ToString();
        if (state.State.DirtyLeavesSinceLastCompaction.TryGetValue(key, out var existing))
        {
            if (_dirtyMarkClock > existing)
                state.State.DirtyLeavesSinceLastCompaction[key] = _dirtyMarkClock;
        }
        else
        {
            state.State.DirtyLeavesSinceLastCompaction[key] = _dirtyMarkClock;
        }

        _dirtyMarksPendingFlush = true;
        EnsureDirtyFlushTimerArmed();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Registers the coalescing flush timer on first use. Idempotent:
    /// subsequent calls return immediately once the timer is alive. The
    /// timer is disposed in <c>OnDeactivateAsync</c> as part of the
    /// final flush sequence.
    /// <para>
    /// Wrapped in a try/catch so unit-test harnesses that supply a
    /// substituted <see cref="IGrainContext"/> (which does not implement
    /// <c>RegisterGrainTimer</c>) still exercise the in-memory mark path
    /// without throwing. The deactivation flush path remains the
    /// fallback that drains pending marks.
    /// </para>
    /// </summary>
    private void EnsureDirtyFlushTimerArmed()
    {
        if (_dirtyFlushTimer is not null) return;

        var intervalMs = _cachedOptions?.DirtyLeafFlushIntervalMs
            ?? LatticeOptions.DefaultDirtyLeafFlushIntervalMs;
        if (intervalMs <= 0)
        {
            // Coalescing disabled: fall back to a synchronous best-effort
            // flush on every mark. Cheap when the dirty flag is already
            // false (no-op); otherwise costs one WriteStateAsync per
            // first-call-per-leaf-per-window, matching pre-U9h-B
            // behaviour.
            _ = FlushPendingDirtyMarksAsync();
            return;
        }

        try
        {
            var period = TimeSpan.FromMilliseconds(intervalMs);
            _dirtyFlushTimer = this.RegisterGrainTimer(
                OnDirtyFlushTimerTickAsync,
                new GrainTimerCreationOptions(dueTime: period, period: period));
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex,
                "Could not register dirty-leaf flush timer on shard {ShardKey} (likely a test harness without a grain runtime); falling back to drain-on-clear / drain-on-deactivate.",
                context.GrainId.Key.ToString());
        }
    }

    private async Task OnDirtyFlushTimerTickAsync(CancellationToken cancellationToken)
    {
        if (!_dirtyMarksPendingFlush) return;
        try
        {
            await FlushPendingDirtyMarksAsync();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Coalesced dirty-leaf flush failed for shard {ShardKey}; will retry on next tick.",
                context.GrainId.Key.ToString());
        }
    }

    /// <summary>
    /// Drains <see cref="_dirtyMarksPendingFlush"/> and persists the
    /// shard-root state in one <see cref="WriteShardStateAsync"/> call.
    /// No-op when no marks are pending. Safe to call from any grain turn
    /// or admin path; re-entrant calls observe the in-flight flush via
    /// <see cref="_dirtyFlushInFlight"/> and skip.
    /// </summary>
    private async Task FlushPendingDirtyMarksAsync()
    {
        if (!_dirtyMarksPendingFlush || _dirtyFlushInFlight) return;

        _dirtyFlushInFlight = true;
        _dirtyMarksPendingFlush = false;
        try
        {
            await WriteShardStateAsync();
        }
        catch
        {
            // Re-arm so the next tick retries. We don't restore an
            // older snapshot of DirtyLeavesSinceLastCompaction because
            // the in-memory dictionary is already the source of truth
            // for the coordinator's reads.
            _dirtyMarksPendingFlush = true;
            throw;
        }
        finally
        {
            _dirtyFlushInFlight = false;
        }
    }

    /// <summary>
    /// Final flush invoked from <c>OnDeactivateAsync</c>. Disposes the
    /// timer first so it cannot fire after deactivation begins, then
    /// drains pending marks. Best-effort - failures are logged and
    /// swallowed so a transient storage outage does not block grain
    /// deactivation.
    /// </summary>
    private async Task FlushPendingDirtyMarksOnDeactivateAsync(CancellationToken cancellationToken)
    {
        _dirtyFlushTimer?.Dispose();
        _dirtyFlushTimer = null;

        if (!_dirtyMarksPendingFlush) return;
        try
        {
            await FlushPendingDirtyMarksAsync();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Final dirty-leaf flush failed for shard {ShardKey} during deactivation; pending marks will be re-discovered by chain-walk fallback on next pass.",
                context.GrainId.Key.ToString());
        }
    }

    /// <inheritdoc />
    public async Task<DirtyLeavesSnapshot> GetDirtyLeavesSinceLastCompactionAsync()
    {
        await PrepareForOperationAsync();

        var dict = state.State.DirtyLeavesSinceLastCompaction;
        if (dict.Count == 0)
        {
            return new DirtyLeavesSnapshot
            {
                DirtyLeaves = [],
                ObservedAdvance = state.State.LastDirtyAdvance,
            };
        }

        var leaves = new List<GrainId>(dict.Count);
        var max = state.State.LastDirtyAdvance;
        foreach (var (key, hlc) in dict)
        {
            leaves.Add(GrainId.Parse(key));
            if (hlc > max) max = hlc;
        }

        return new DirtyLeavesSnapshot
        {
            DirtyLeaves = leaves,
            ObservedAdvance = max,
        };
    }

    /// <inheritdoc />
    public async Task ClearDirtyLeavesUpToAsync(HybridLogicalClock advance)
    {
        await PrepareForOperationAsync();

        var dict = state.State.DirtyLeavesSinceLastCompaction;
        if (dict.Count == 0 && advance <= state.State.LastDirtyAdvance)
            return;

        var prevDict = dict.Count > 0 ? new Dictionary<string, HybridLogicalClock>(dict) : null;
        var prevAdvance = state.State.LastDirtyAdvance;
        var prevPendingFlush = _dirtyMarksPendingFlush;

        if (dict.Count > 0)
        {
            // Trim entries whose mark HLC is at-or-before the watermark;
            // preserve strictly-greater entries so a delete that arrived
            // during the in-flight pass is picked up next pass.
            List<string>? toRemove = null;
            foreach (var (key, hlc) in dict)
            {
                if (hlc <= advance)
                {
                    toRemove ??= [];
                    toRemove.Add(key);
                }
            }
            if (toRemove is not null)
            {
                foreach (var key in toRemove)
                    dict.Remove(key);
            }
        }

        if (advance > state.State.LastDirtyAdvance)
            state.State.LastDirtyAdvance = advance;

        // The write below persists the trimmed dictionary AND any
        // pending in-memory marks the flush timer has not yet drained
        // (because both live in the same state object). Pre-clear the
        // pending-flush flag so a successful write does not leave the
        // timer thinking it still owes work; restore on failure.
        _dirtyMarksPendingFlush = false;
        try
        {
            await WriteShardStateAsync();
        }
        catch
        {
            // Restore in-memory state on persist failure.
            if (prevDict is not null)
            {
                state.State.DirtyLeavesSinceLastCompaction = prevDict;
            }
            state.State.LastDirtyAdvance = prevAdvance;
            _dirtyMarksPendingFlush = prevPendingFlush;
            throw;
        }
    }
}

