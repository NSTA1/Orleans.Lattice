using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Leaf-side digest <i>publication</i> wiring (distinct from the
/// digest <i>computation</i> in <c>BPlusLeafGrain.Digest.cs</c>):
/// records this leaf's parent internal node id via
/// <see cref="SetParentAsync"/> and publishes a fresh
/// <see cref="ChildDigestSnapshot"/> to that parent whenever the
/// leaf's projection digest changes. Together with the matching
/// internal-node aggregator (<c>BPlusInternalGrain.Digest.cs</c>),
/// this maintains an O(1)-per-shard root-side fold so a whole-tree
/// digest poll no longer fans out into per-leaf grain calls.
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Per-activation flag that is flipped <see langword="true"/> by
    /// <c>StoreEntry</c> / <c>RemoveEntry</c> whenever the running
    /// projection hash changes, and consulted by
    /// <see cref="PublishDigestUpwardAsync"/> so a mutation funnel that
    /// makes no observable change (e.g. a re-application of an
    /// already-applied LWW value at or below the persisted Timestamp)
    /// pays zero cross-grain publication cost. Reset on every
    /// successful publish.
    /// </summary>
    private bool _digestDirty;

    /// <summary>
    /// c2-xxviii: handle of the one-shot grain timer scheduled by
    /// <see cref="PublishDigestUpwardAsync"/> when digest coalescing
    /// is enabled (<c>DigestCoalescingWindowMs &gt; 0</c>). Non-null
    /// when a coalesced publish is pending; the
    /// <see cref="OnDigestCoalesceTimerTickAsync"/> handler clears the
    /// reference after firing the cross-grain publish. Subsequent
    /// dirtying calls observe the non-null reference and skip
    /// rescheduling, so N mutations within the window share one
    /// cross-grain hop. Cleared on graceful deactivation after a
    /// synchronous drain.
    /// </summary>
    private IDisposable? _digestPublishTimer;

    /// <summary>
    /// c2-xxviii: cached coalescing window (ms) sourced from
    /// <see cref="LatticeOptions.DigestCoalescingWindowMs"/> via the
    /// activation-resolved options. <c>0</c> means coalescing is
    /// disabled (the pre-c2-xxviii synchronous publish shape).
    /// Initialised to <c>0</c> for unit-test activations that bypass
    /// <c>ResolveOptionsSlowAsync</c>; production activations
    /// overwrite this from the resolver before the first mutation.
    /// </summary>
    private int _digestCoalescingWindowMs;

    /// <summary>
    /// Per-activation guard preventing repeated one-way latch stamping
    /// on every trimmed-path mutation. Set on the first observed
    /// disabled-digest mutation after activation; the latch itself is
    /// idempotent on the registry side, but skipping the cross-grain
    /// hop after the first stamp avoids unnecessary registry churn for
    /// every subsequent write on the same activation.
    /// </summary>
    private bool _latchAttempted;

    /// <summary>
    /// Marks the running projection hash as dirty. Called from the
    /// digest funnels (<c>StoreEntry</c>, <c>RemoveEntry</c>) so the
    /// per-mutation publication path can elide no-op publishes.
    /// </summary>
    internal void MarkDigestDirty() => _digestDirty = true;

    /// <inheritdoc />
    public async Task SetParentAsync(GrainId? parentId)
    {
        // U9p step c2-iv-redux: serialise the public state-write
        // surface through the per-activation _splitGate. The internal
        // grain's seeding path calls this RPC concurrently with the
        // donor leaf's split flow (which already holds the gate via
        // SplitIfNeededUnderGateAsync) and with other topology RPCs
        // (SetNextSiblingAsync / SetPrevSiblingAsync / ...); without
        // the gate, two pending state.WriteStateAsync calls race the
        // underlying grain-storage etag CAS and the loser throws
        // InconsistentStateException. Mirrors c2-vi-followup's fix on
        // BPlusInternalGrain.SetParentAsync. The gate is non-reentrant
        // and SetParentAsync is only ever called as a cross-grain RPC
        // (targeting a different activation than the caller's), so no
        // self-recursive acquisition is possible.
        await _splitGate.WaitAsync().ConfigureAwait(true);
        try
        {
            if (state.State.ParentId == parentId)
            {
                // Idempotent re-call with the same parent: no persist, no
                // callback. The internal-node seeding path consults the
                // child via GetChildDigestSnapshotAsync directly after a
                // SetParentAsync call so we never callback into a parent
                // that may still be inside its own AcceptSplitAsync /
                // InitializeAsync mutation frame (which would deadlock the
                // non-reentrant internal grain).
                return;
            }

            var prev = state.State.ParentId;
            state.State.ParentId = parentId;
            try
            {
                await PersistAsync();
            }
            catch
            {
                state.State.ParentId = prev;
                throw;
            }

            // Mark the digest dirty so the next mutation triggers an upward
            // publish to the new parent. The parent itself is expected to
            // pull our current snapshot via GetChildDigestSnapshotAsync
            // immediately after this call (driven by the internal-grain
            // seeding path), which keeps the parent's ChildDigests table
            // consistent without a reentrant callback.
            if (parentId is not null) _digestDirty = true;
        }
        finally
        {
            _splitGate.Release();
        }
    }

    /// <inheritdoc />
    public Task<ChildDigestSnapshot> GetChildDigestSnapshotAsync()
    {
        EnsureProjectionHashInitialized();
        return Task.FromResult(new ChildDigestSnapshot
        {
            // Clone so an in-place XOR on state.State.ProjectionHash
            // cannot retroactively mutate the bytes the caller has
            // captured.
            Hash = (byte[])state.State.ProjectionHash!.Clone(),
            EntryCount = Cache.Count,
            CheckpointOffset = state.State.ProjectionCheckpointOffset,
        });
    }

    /// <summary>
    /// Publishes this leaf's current <see cref="ChildDigestSnapshot"/>
    /// to its parent internal node, if a parent is registered and the
    /// running projection hash has been marked dirty since the previous
    /// publish. Called from every mutation funnel after the digest
    /// funnels (<c>StoreEntry</c> / <c>RemoveEntry</c>) have flipped
    /// <see cref="_digestDirty"/>. A <see langword="null"/> parent
    /// (flat-tree case where the root is itself a leaf) makes this a
    /// no-op; the shard-root digest read path falls back to reading
    /// the leaf directly in that shape.
    /// <para>
    /// c2-xxviii coalescing: when
    /// <see cref="LatticeOptions.DigestCoalescingWindowMs"/> is &gt;0,
    /// this method does <em>not</em> perform the cross-grain publish
    /// inline. Instead it registers a one-shot grain timer that fires
    /// after the configured window; mutations arriving within the
    /// window observe <see cref="_digestPublishTimer"/> already
    /// non-null and skip rescheduling. The handler
    /// (<see cref="OnDigestCoalesceTimerTickAsync"/>) does the actual
    /// cross-grain hop, collapsing N per-call publishes into one. The
    /// hot path returns <see cref="Task.CompletedTask"/> and allocates
    /// nothing once the timer is scheduled. Setting the window to 0
    /// restores the pre-c2-xxviii synchronous-publish shape.
    /// </para>
    /// </summary>
    private Task PublishDigestUpwardAsync()
    {
        if (!_maintainProjectionDigest)
        {
            // Disabled-mode mutation: we have just landed a write while
            // the resolved opt-out is false, which means the persisted
            // projection-hash aggregate is now permanently stale (the
            // running XOR fold has skipped contributions). Stamp the
            // one-way registry latch so any later configuration flip
            // back to MaintainProjectionDigest = true cannot silently
            // re-expose the digest API on top of a hash that no longer
            // describes the leaf contents. The stamp is best-effort
            // (a registry failure during the trimmed path must not fail
            // the user-visible mutation), idempotent (LatchProjection...
            // is a no-op once already true), and gated by an activation
            // guard so we pay at most one registry hop per activation.
            //
            // Disabled-digest mutations still need to publish the byte
            // footprint so the shard root's running totals stay current
            // even when projection-hash maintenance is off.
            var latchTask = _latchAttempted ? Task.CompletedTask : TryStampDigestLatchAsync();
            if (!_latchAttempted) _latchAttempted = true;
            return AwaitBoth(latchTask, TryPublishByteFootprintAsync());
        }
        if (!_digestDirty)
        {
            // No digest delta this turn, but a structural / topology
            // mutation may still have changed the byte footprint (e.g.
            // a tombstone-compaction reap that shrinks the cache
            // without altering the projection hash). Publish the byte
            // footprint anyway; the helper coalesces no-op publishes.
            return TryPublishByteFootprintAsync();
        }
        if (state.State.ParentId is not { } parentId)
        {
            // Clear the dirty flag even without a parent so we do not
            // accumulate dirt across mutations on a flat-tree leaf. A
            // future re-parent re-publishes via SetParentAsync. Still
            // publish the byte footprint so the shard root's running
            // totals reflect this leaf even in the flat-tree case.
            _digestDirty = false;
            return TryPublishByteFootprintAsync();
        }

        // c2-xxviii: coalescing path. When the window is positive and
        // no publish is already scheduled, register the one-shot timer
        // and let it drive the cross-grain hop. Subsequent dirtying
        // mutations within the window see _digestPublishTimer non-null
        // and skip rescheduling, so we pay one Orleans grain hop per
        // window rather than one per write. _digestDirty stays set
        // until the handler completes the publish.
        //
        // Structural callers (leaf split, rebuild, snapshot capture,
        // saga terminal apply, tombstone-reap compaction, cross-shard
        // merge) opt out of coalescing by calling
        // <see cref="PublishDigestUpwardInlineAsync"/> instead - those
        // paths need the parent's chained-fold to observe the new
        // aggregate before the caller returns so operator-tooling
        // oracles (e.g. RebuildLeafProjectionAsync's post-rebuild
        // digest read) see the post-publish state synchronously.
        if (_digestCoalescingWindowMs > 0)
        {
            if (_digestPublishTimer is null)
            {
                try
                {
                    var window = TimeSpan.FromMilliseconds(_digestCoalescingWindowMs);
                    _digestPublishTimer = this.RegisterGrainTimer(
                        OnDigestCoalesceTimerTickAsync,
                        new GrainTimerCreationOptions(dueTime: window, period: Timeout.InfiniteTimeSpan));
                    LatticeMetrics.LeafDigestPublishes.Add(1, LeafTreeTag(), LatticeMetrics.PathCoalescedScheduledTag);
                }
                catch
                {
                    // Test harnesses without a grain runtime can throw
                    // here. Fall back to synchronous publish so the
                    // digest still reaches the parent on the same call.
                    LatticeMetrics.LeafDigestPublishes.Add(1, LeafTreeTag(), LatticeMetrics.PathInlineTag);
                    return PublishCurrentDigestAndClearDirtyAsync(parentId);
                }
            }
            else
            {
                // A coalesced publish is already pending; this dirty
                // mutation rides on the existing window. This is the
                // "publish saved" surface the coalescing default exists
                // for.
                LatticeMetrics.LeafDigestPublishes.Add(1, LeafTreeTag(), LatticeMetrics.PathCoalescedSkippedTag);
            }
            return Task.CompletedTask;
        }

        // Coalescing disabled - pre-c2-xxviii synchronous publish.
        LatticeMetrics.LeafDigestPublishes.Add(1, LeafTreeTag(), LatticeMetrics.PathInlineTag);
        return PublishCurrentDigestAndClearDirtyAsync(parentId);
    }

    /// <summary>
    /// Synchronous variant of <see cref="PublishDigestUpwardAsync"/>
    /// that bypasses the c2-xxviii coalescing window and publishes
    /// the current digest snapshot to the parent before returning.
    /// <para>
    /// Use from structural callers (leaf split, rebuild, snapshot
    /// capture, saga terminal apply, tombstone-reap compaction,
    /// cross-shard merge) whose contract requires the parent's
    /// chained-fold to observe the new aggregate by the time the
    /// caller returns - per the c2-xxviii memo, coalescing is
    /// deliberately scoped to the per-write hot path and structural
    /// events are excluded so operator-tooling oracles
    /// (e.g. <c>RebuildLeafProjectionAsync</c> followed by
    /// <c>GetLeafProjectionDigestAsync</c>) see post-publish state
    /// without a settle delay.
    /// </para>
    /// <para>
    /// Cancels any pending coalesced publish timer before issuing the
    /// inline publish so we do not double-publish the same delta.
    /// </para>
    /// </summary>
    private Task PublishDigestUpwardInlineAsync()
    {
        if (!_maintainProjectionDigest)
        {
            if (!_latchAttempted)
            {
                _latchAttempted = true;
                return TryStampDigestLatchAsync();
            }
            return Task.CompletedTask;
        }
        if (!_digestDirty) return Task.CompletedTask;
        if (state.State.ParentId is not { } parentId)
        {
            _digestDirty = false;
            return Task.CompletedTask;
        }

        // Cancel any pending coalesced publish so the inline call is
        // the authoritative one. The timer handler is idempotent on
        // _digestDirty so leaving a fired tick in flight would be
        // harmless, but cancelling avoids a redundant cross-grain hop.
        var pending = System.Threading.Interlocked.Exchange(ref _digestPublishTimer, null);
        pending?.Dispose();

        LatticeMetrics.LeafDigestPublishes.Add(1, LeafTreeTag(), LatticeMetrics.PathInlineTag);
        return PublishCurrentDigestAndClearDirtyAsync(parentId);
    }

    /// <summary>
    /// Async-state-machine helper that awaits one cross-grain digest
    /// publish and clears <see cref="_digestDirty"/> on success. Split
    /// out of <see cref="PublishDigestUpwardAsync"/> so the hot path
    /// remains a non-async <c>Task</c>-returning method whose
    /// "nothing to do" branches return <see cref="Task.CompletedTask"/>
    /// without allocating an async state machine.
    /// </summary>
    private async Task PublishCurrentDigestAndClearDirtyAsync(GrainId parentId)
    {
        await PublishCurrentDigestAsync(parentId);
        _digestDirty = false;
    }

    /// <summary>
    /// c2-xxviii: one-shot timer handler that fires the coalesced
    /// digest publish after the configured window elapses. Disposes
    /// the timer reference before awaiting the cross-grain hop so a
    /// concurrent mutation arriving during the publish can schedule a
    /// fresh window for the next round. Any exception is logged and
    /// swallowed - the next mutation reschedules; the running digest
    /// state on the parent is staleness-tolerant by design (consumers
    /// re-poll).
    /// </summary>
    private async Task OnDigestCoalesceTimerTickAsync(CancellationToken cancellationToken)
    {
        var timer = System.Threading.Interlocked.Exchange(ref _digestPublishTimer, null);
        timer?.Dispose();

        if (!_maintainProjectionDigest) return;
        if (!_digestDirty) return;
        if (state.State.ParentId is not { } parentId) return;

        try
        {
            await PublishCurrentDigestAsync(parentId);
            _digestDirty = false;
            LatticeMetrics.LeafDigestPublishes.Add(1, LeafTreeTag(), LatticeMetrics.PathCoalescedFiredTag);
        }
        catch
        {
            // Swallow: digest is staleness-tolerant; the next mutation
            // reschedules. A logger is not in scope on this partial,
            // and an unobserved exception here would otherwise crash
            // the timer continuation.
        }
    }

    /// <summary>
    /// c2-xxviii: synchronously drain any pending coalesced digest
    /// publish. Called from the leaf's graceful
    /// <c>OnDeactivateAsync</c> so a clean shutdown does not leave the
    /// parent's digest table observing a stale snapshot. Crash
    /// deactivations bypass this hook by design - the digest is
    /// staleness-tolerant and the next mutation on reactivation will
    /// republish.
    /// </summary>
    internal async Task FlushPendingDigestPublishAsync()
    {
        var timer = System.Threading.Interlocked.Exchange(ref _digestPublishTimer, null);
        timer?.Dispose();

        if (!_maintainProjectionDigest) return;
        if (!_digestDirty) return;
        if (state.State.ParentId is not { } parentId) return;

        try
        {
            await PublishCurrentDigestAsync(parentId);
            _digestDirty = false;
            LatticeMetrics.LeafDigestPublishes.Add(1, LeafTreeTag(), LatticeMetrics.PathDeactivationFlushTag);
        }
        catch
        {
            // Match OnDeactivateAsync's swallow-on-shutdown contract.
        }
    }

    /// <summary>
    /// Best-effort stamp of the one-way registry latch that records
    /// "this tree has accepted writes while digest maintenance was
    /// disabled". Skipped for system trees because they bypass the
    /// registry entirely (and never participate in digest maintenance
    /// by design); failures are swallowed because the mutation has
    /// already succeeded and re-attempting the stamp on the next
    /// mutation is acceptable.
    /// </summary>
    private async Task TryStampDigestLatchAsync()
    {
        var treeId = state.State.TreeId;
        if (string.IsNullOrEmpty(treeId)) return;
        if (treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            // System trees bypass the registry; the resolver already
            // forces MaintainProjectionDigest = false for them and the
            // latch is meaningless because there is no per-tree row to
            // stamp.
            return;
        }
        try
        {
            var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            await registry.LatchProjectionDigestPermanentlyDisabledAsync(treeId);
        }
        catch
        {
            // Latch stamping is best-effort. A registry failure must
            // not propagate into the user-visible mutation. The guard
            // flag is reset so a subsequent mutation gets another
            // chance to stamp.
            _latchAttempted = false;
        }
    }

    private async Task PublishCurrentDigestAsync(GrainId parentId)
    {
        EnsureProjectionHashInitialized();
        var snapshot = new ChildDigestSnapshot
        {
            // Clone so a subsequent in-place XOR on
            // state.State.ProjectionHash does not retroactively mutate
            // the bytes the parent's table has captured.
            Hash = (byte[])state.State.ProjectionHash!.Clone(),
            EntryCount = Cache.Count,
            CheckpointOffset = state.State.ProjectionCheckpointOffset,
        };
        var parent = grainFactory.GetGrain<IBPlusInternalGrain>(parentId);
        await parent.OnChildDigestPublishedAsync(context.GrainId, snapshot);

        // Piggyback the per-leaf byte-footprint publish on the digest
        // commit boundary. Skipped when the values are unchanged since
        // the last publish (see TryPublishByteFootprintAsync). Failures
        // are swallowed: the shard root's running totals are best-effort
        // and re-anchored by the operator-driven
        // ILatticeAdmin.RefreshStorageUsageAsync seam.
        await TryPublishByteFootprintAsync();
    }

    private static async Task AwaitBoth(Task a, Task b)
    {
        await a;
        await b;
    }
}
