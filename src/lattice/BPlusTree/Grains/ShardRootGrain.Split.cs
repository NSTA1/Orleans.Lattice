using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// adaptive split surface for the shard root.
/// <para>
/// During an adaptive split, the source shard <c>S</c> participates in three
/// hot-path behaviours driven by the persisted
/// <see cref="Orleans.Lattice.BPlusTree.State.ShardRootState.SplitInProgress"/>:
/// </para>
/// <list type="number">
/// <item><description>
/// In <see cref="Orleans.Lattice.BPlusTree.State.ShardSplitPhase.BeginShadowWrite"/>, <see cref="Orleans.Lattice.BPlusTree.State.ShardSplitPhase.Drain"/>,
/// or <see cref="ShardSplitPhase.Swap"/>, every successful write to a key in a
/// moved virtual slot is mirrored to the target shard <c>T</c> via
/// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.MergeManyAsync"/> with the original HLC, so that
/// CRDT LWW guarantees convergence regardless of interleaving with the
/// background drain.
/// </description></item>
/// <item><description>
/// In <see cref="Orleans.Lattice.BPlusTree.State.ShardSplitPhase.Reject"/>, every operation on a moved-slot
/// key throws <see cref="StaleShardRoutingException"/>, signalling the
/// caller's <c>LatticeGrain</c> to refresh its cached <see cref="ShardMap"/>
/// and retry against the new owner.
/// </description></item>
/// <item><description>
/// The target shard <c>T</c> never holds a <see cref="ShardSplitInProgress"/>
/// record, so neither hook fires there - this naturally prevents recursive
/// shadow-forwarding when <c>T</c> receives the mirrored
/// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.MergeManyAsync"/> call. A defensive assertion in
/// <c>TryForwardShadowWriteAsync</c> still guards against pathological
/// configurations where <c>T == S</c>.
/// </description></item>
/// </list>
/// </summary>
internal sealed partial class ShardRootGrain
{
    /// <summary>Lazily-parsed physical shard index from this grain's key (<c>{treeId}/{shardIndex}</c>).</summary>
    private int? _myShardIndex;

    /// <summary>Returns this shard root's physical shard index parsed from its grain key.</summary>
    private int MyShardIndex
    {
        get
        {
            if (_myShardIndex is { } cached) return cached;
            var key = context.GrainId.Key.ToString()!;
            _myShardIndex = ParseShardGrainKey(key).shardIndex;
            return _myShardIndex.Value;
        }
    }

    /// <inheritdoc />
    public async Task BeginSplitAsync(int targetShardIndex, int[] movedSlots, int virtualShardCount)
    {
        ArgumentNullException.ThrowIfNull(movedSlots);
        if (virtualShardCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(virtualShardCount), "Must be greater than 0.");
        if (targetShardIndex == MyShardIndex)
            throw new ArgumentException("Target shard index must differ from this shard.", nameof(targetShardIndex));
        if (movedSlots.Length == 0)
            throw new ArgumentException("At least one virtual slot must be moved.", nameof(movedSlots));

        await PrepareForOperationAsync();

        var existing = state.State.SplitInProgress;
        if (existing is not null && HasSameAim(existing, targetShardIndex, movedSlots, virtualShardCount))
        {
            // Idempotent re-entry: a coordinator re-asserting the window it
            // already owns after a crash between persisting intent and
            // reaching this shard.
            if (existing.Phase == ShardSplitPhase.BeginShadowWrite || existing.Phase == ShardSplitPhase.Drain)
                return;
        }
        else if (existing is not null)
        {
            // A shard carries exactly one migration record, and both an
            // adaptive split and an online consolidation open their
            // shadow-write window through this method. Silently re-aiming the
            // record at a second target loses acknowledged writes: the
            // in-flight migration's slots stop being shadow-forwarded to its
            // target, its reject-phase freeze then fences the *other*
            // migration's slots instead of its own, and CompleteSplitAsync
            // promotes the wrong slot set into MovedAwaySlots - so the shard
            // keeps accepting and serving writes on slots the routing map has
            // already handed to someone else, orphaning them.
            //
            // Refusing is the contract both coordinators already document and
            // both callers already handle. It is enforced here rather than in
            // the callers because this grain is non-reentrant, so the check
            // and the write happen in one turn and no caller-side
            // check-then-act window can slip between them.
            throw new InvalidOperationException(
                $"Shard {MyShardIndex} of tree '{TreeId}' already has a migration in progress to shard {existing.ShadowTargetShardIndex} (phase {existing.Phase}); it cannot be re-aimed at shard {targetShardIndex} until that migration completes or is aborted.");
        }

        // Defensive: validate every slot is in [0, virtualShardCount).
        for (int i = 0; i < movedSlots.Length; i++)
        {
            if (movedSlots[i] < 0 || movedSlots[i] >= virtualShardCount)
                throw new ArgumentOutOfRangeException(nameof(movedSlots),
                    $"Slot {movedSlots[i]} is outside [0, {virtualShardCount}).");
        }

        var sorted = (int[])movedSlots.Clone();
        Array.Sort(sorted);

        state.State.SplitInProgress = new ShardSplitInProgress
        {
            Phase = ShardSplitPhase.BeginShadowWrite,
            ShadowTargetShardIndex = targetShardIndex,
            MovedSlots = sorted,
            VirtualShardCount = virtualShardCount,
        };
        await WriteShardStateAsync();
    }

    /// <inheritdoc />
    public async Task EnterRejectPhaseAsync()
    {
        var sip = state.State.SplitInProgress;
        if (sip is null)
            return;
        if (sip.Phase == ShardSplitPhase.Reject || sip.Phase == ShardSplitPhase.Complete)
            return;

        state.State.SplitInProgress = sip with { Phase = ShardSplitPhase.Reject };
        await WriteShardStateAsync();
    }

    /// <inheritdoc />
    public async Task CompleteSplitAsync()
    {
        if (state.State.SplitInProgress is null)
            return;

        // Promote the just-completed split's moved slots to the permanent
        // MovedAwaySlots set so that future reads/writes from stale
        // LatticeGrain activations continue to throw StaleShardRoutingException
        // and refresh their cached ShardMap. Without this, after Complete
        // clears SplitInProgress, a stale [StatelessWorker] activation that
        // never observed the reject phase would route reads to this shard
        // indefinitely and return orphaned old values for moved slots.
        var sip = state.State.SplitInProgress;
        var moved = state.State.MovedAwaySlots;
        foreach (var slot in sip.MovedSlots)
        {
            moved[slot] = sip.ShadowTargetShardIndex;
        }
        state.State.MovedAwayVirtualShardCount = sip.VirtualShardCount;
        state.State.SplitInProgress = null;
        await WriteShardStateAsync();
    }

    /// <inheritdoc />
    public Task<bool> IsSplittingAsync() => Task.FromResult(state.State.SplitInProgress is not null);

    /// <inheritdoc />
    public Task<bool> HasPendingBulkOperationAsync()
        => Task.FromResult(state.State.PendingBulkGraft is not null);

    /// <summary>
    /// Hot-path write gate. Throws <see cref="StaleShardRoutingException"/>
    /// when (a) the shard is in <see cref="Orleans.Lattice.BPlusTree.State.ShardSplitPhase.Reject"/> and
    /// <paramref name="key"/> hashes to a moved virtual slot of the active
    /// split, or (b) <paramref name="key"/> hashes to a slot in
    /// <see cref="Orleans.Lattice.BPlusTree.State.ShardRootState.MovedAwaySlots"/> from a previously-completed
    /// split. No-op otherwise.
    /// <para>
    /// Writes at <see cref="ShardSplitPhase.Swap"/> are intentionally
    /// admitted: the source's local write is mirrored to the new owner via
    /// the shadow-forward pipeline, keeping both sides consistent through
    /// the swap → reject transition. Only the
    /// <see cref="Orleans.Lattice.BPlusTree.State.ShardSplitPhase.Reject"/> phase actively rejects writes
    /// on moved slots (because at that point the source has stopped
    /// accepting new mirrored work).
    /// </para>
    /// </summary>
    private void ThrowIfRejectedForKey(string key)
    {
        ThrowIfWriteFenced();

        var sip = state.State.SplitInProgress;
        if (sip is not null && sip.Phase == ShardSplitPhase.Reject)
        {
            var slot = ShardMap.GetVirtualSlot(key, sip.VirtualShardCount);
            if (sip.IsMovedSlot(slot))
                throw new StaleShardRoutingException(MyShardIndex, sip.ShadowTargetShardIndex, slot);
        }

        var moved = state.State.MovedAwaySlots;
        if (moved.Count > 0 && state.State.MovedAwayVirtualShardCount is { } vsc)
        {
            var slot = ShardMap.GetVirtualSlot(key, vsc);
            if (moved.TryGetValue(slot, out var target))
                throw new StaleShardRoutingException(MyShardIndex, target, slot);
        }
    }

    /// <summary>
    /// Hot-path write gate for batched operations. Throws on the first key
    /// in <paramref name="keys"/> that maps to a moved virtual slot during the
    /// reject phase or to a slot already in
    /// <see cref="Orleans.Lattice.BPlusTree.State.ShardRootState.MovedAwaySlots"/>. No-op when neither
    /// condition holds. See <see cref="ThrowIfRejectedForKey"/> for the
    /// rationale on excluding <see cref="ShardSplitPhase.Swap"/>.
    /// </summary>
    private void ThrowIfRejectedForAnyKey(IEnumerable<string> keys)
    {
        ThrowIfWriteFenced();

        var sip = state.State.SplitInProgress;
        var rejectActive = sip is not null && sip.Phase == ShardSplitPhase.Reject;
        var moved = state.State.MovedAwaySlots;
        var movedActive = moved.Count > 0 && state.State.MovedAwayVirtualShardCount is not null;
        if (!rejectActive && !movedActive) return;

        var movedVsc = state.State.MovedAwayVirtualShardCount ?? 0;
        foreach (var key in keys)
        {
            if (rejectActive)
            {
                var slot = ShardMap.GetVirtualSlot(key, sip!.VirtualShardCount);
                if (sip.IsMovedSlot(slot))
                    throw new StaleShardRoutingException(MyShardIndex, sip.ShadowTargetShardIndex, slot);
            }
            if (movedActive)
            {
                var slot = ShardMap.GetVirtualSlot(key, movedVsc);
                if (moved.TryGetValue(slot, out var target))
                    throw new StaleShardRoutingException(MyShardIndex, target, slot);
            }
        }
    }

    /// <summary>
    /// Allocation-free batch-write gate for the common
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.SetManyAsync"/> /
    /// <c>SetManyWherePredicateAsync</c> entry shape. Iterates the entry
    /// keys directly rather than through a <c>Select(e =&gt; e.Key)</c>
    /// projection, so the steady-state (no split, no moved slots) call
    /// allocates no <c>SelectListIterator</c>. Behaviourally identical to
    /// <see cref="ThrowIfRejectedForAnyKey(IEnumerable{string})"/>.
    /// </summary>
    private void ThrowIfRejectedForAnyKey(List<KeyValuePair<string, byte[]>> entries)
    {
        ThrowIfWriteFenced();

        var sip = state.State.SplitInProgress;
        var rejectActive = sip is not null && sip.Phase == ShardSplitPhase.Reject;
        var moved = state.State.MovedAwaySlots;
        var movedActive = moved.Count > 0 && state.State.MovedAwayVirtualShardCount is not null;
        if (!rejectActive && !movedActive) return;

        var movedVsc = state.State.MovedAwayVirtualShardCount ?? 0;
        for (int i = 0; i < entries.Count; i++)
        {
            var key = entries[i].Key;
            if (rejectActive)
            {
                var slot = ShardMap.GetVirtualSlot(key, sip!.VirtualShardCount);
                if (sip.IsMovedSlot(slot))
                    throw new StaleShardRoutingException(MyShardIndex, sip.ShadowTargetShardIndex, slot);
            }
            if (movedActive)
            {
                var slot = ShardMap.GetVirtualSlot(key, movedVsc);
                if (moved.TryGetValue(slot, out var target))
                    throw new StaleShardRoutingException(MyShardIndex, target, slot);
            }
        }
    }

    /// <summary>
    /// Read-path gate. Throws <see cref="StaleShardRoutingException"/> as
    /// soon as a split has advanced to <see cref="ShardSplitPhase.Swap"/>
    /// (the registry's <see cref="ShardMap"/> has been swapped to the new
    /// owner) or later, and <paramref name="key"/> hashes to a moved slot.
    /// Also fires for slots in <see cref="Orleans.Lattice.BPlusTree.State.ShardRootState.MovedAwaySlots"/>
    /// from a previously-completed split.
    /// <para>
    /// The gate is wider than the write-side
    /// <see cref="ThrowIfRejectedForKey"/> because reads carry a real
    /// orphan-visibility hazard the write path does not: a stale-routed
    /// read against the source returns the source's <c>Entries[K]</c>
    /// snapshot from the moment of last mirrored write, and the
    /// <c>LeafCacheGrain</c> happily warms with and serves that snapshot
    /// indefinitely. By rejecting at Swap, the read forces
    /// <c>LatticeGrain.GetAsyncCore</c> to invalidate its cached shard
    /// map and retry against the new owner T, which holds the
    /// authoritative copy.
    /// </para>
    /// <para>
    /// Matches the scan-side gate <see cref="IsSlotMovedAway"/> /
    /// <see cref="TryGetMovedAwaySlot"/>, which already filters
    /// moved-away slots from <see cref="ShardSplitPhase.Swap"/> onward.
    /// </para>
    /// </summary>
    private void ThrowIfMovedAwayForReadKey(string key)
    {
        var sip = state.State.SplitInProgress;
        if (sip is not null
            && (sip.Phase == ShardSplitPhase.Swap
                || sip.Phase == ShardSplitPhase.Reject))
        {
            var slot = ShardMap.GetVirtualSlot(key, sip.VirtualShardCount);
            if (sip.IsMovedSlot(slot))
            {
#if LATTICE_DIAG
                // [DIAG] trace read-gate throw via SplitInProgress.
                DiagSink.Write($"[DIAG shard-read-gate-throw-sip] shardIdx={MyShardIndex} key={key} slot={slot} target={sip.ShadowTargetShardIndex} phase={sip.Phase}");
#endif
                throw new StaleShardRoutingException(MyShardIndex, sip.ShadowTargetShardIndex, slot);
            }
        }

        var moved = state.State.MovedAwaySlots;
        if (moved.Count > 0 && state.State.MovedAwayVirtualShardCount is { } vsc)
        {
            var slot = ShardMap.GetVirtualSlot(key, vsc);
            if (moved.TryGetValue(slot, out var target))
            {
#if LATTICE_DIAG
                // [DIAG] trace read-gate throw via MovedAwaySlots (post-Complete).
                DiagSink.Write($"[DIAG shard-read-gate-throw-moved] shardIdx={MyShardIndex} key={key} slot={slot} target={target} vsc={vsc}");
#endif
                throw new StaleShardRoutingException(MyShardIndex, target, slot);
            }
        }
    }

    /// <summary>
    /// Forwards a successful local write to the shadow target if the split is
    /// in <see cref="Orleans.Lattice.BPlusTree.State.ShardSplitPhase.BeginShadowWrite"/>, <see cref="Orleans.Lattice.BPlusTree.State.ShardSplitPhase.Drain"/>,
    /// or <see cref="ShardSplitPhase.Swap"/> and <paramref name="key"/> hashes
    /// to a moved virtual slot. Uses <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.MergeManyAsync"/>
    /// so the original HLC is preserved and the write is idempotent under retry.
    /// No-op otherwise.
    /// <para>
    /// Recursion guard: <c>T</c> never has its own <see cref="ShardSplitInProgress"/>,
    /// so when <c>T</c> receives the forwarded merge call this hook does not fire.
    /// </para>
    /// </summary>
    private async Task TryForwardShadowWriteAsync(string key, LwwValue<byte[]> value)
    {
        var sip = state.State.SplitInProgress;
        if (sip is null) return;
        if (sip.Phase != ShardSplitPhase.BeginShadowWrite
            && sip.Phase != ShardSplitPhase.Drain
            && sip.Phase != ShardSplitPhase.Swap) return;

        // Defensive guard against pathological T == S configurations.
        if (sip.ShadowTargetShardIndex == MyShardIndex) return;

        var slot = ShardMap.GetVirtualSlot(key, sip.VirtualShardCount);
        if (!sip.IsMovedSlot(slot)) return;

        var target = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{sip.ShadowTargetShardIndex}");
        await ForwardWithDeadlineAsync(() =>
            target.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>(1) { [key] = value }, isCrossShardMigration: true));
    }

    /// <summary>
    /// Resolves the split-shadow-forward target shard for
    /// <paramref name="key"/>, or <c>null</c> when no active or
    /// post-complete split routes the key off this shard. Two windows are
    /// covered (shared verbatim by the write and delete shadow-forward
    /// paths):
    /// <list type="bullet">
    /// <item><description><b>(A) Active split</b> - <c>SplitInProgress</c>
    /// is non-null and the slot is moved. Phases admitted: BeginShadowWrite,
    /// Drain, Swap, AND Reject (closes the Swap → Reject race where an
    /// in-flight mutation that passed <c>ThrowIfRejectedForKey</c> at phase
    /// ≤ Swap reaches this helper after the coordinator advanced to
    /// Reject).</description></item>
    /// <item><description><b>(B) Post-complete</b> - <c>SplitInProgress</c>
    /// is null but <c>MovedAwaySlots</c> records the slot's post-split owner
    /// (closes the Reject → Complete race where the split state is cleared
    /// and <c>MovedAwaySlots</c> populated between the local mutation and
    /// this helper).</description></item>
    /// </list>
    /// </summary>
    private IShardRootGrain? TryResolveSplitShadowTarget(string key)
    {
        int? targetShardIndex = null;

        var sip = state.State.SplitInProgress;
        if (sip is not null
            && sip.ShadowTargetShardIndex != MyShardIndex
            && (sip.Phase == ShardSplitPhase.BeginShadowWrite
                || sip.Phase == ShardSplitPhase.Drain
                || sip.Phase == ShardSplitPhase.Swap
                || sip.Phase == ShardSplitPhase.Reject))
        {
            var slot = ShardMap.GetVirtualSlot(key, sip.VirtualShardCount);
            if (sip.IsMovedSlot(slot))
                targetShardIndex = sip.ShadowTargetShardIndex;
        }

        if (targetShardIndex is null
            && state.State.MovedAwaySlots.Count > 0
            && state.State.MovedAwayVirtualShardCount is { } movedVsc)
        {
            var movedSlot = ShardMap.GetVirtualSlot(key, movedVsc);
            if (state.State.MovedAwaySlots.TryGetValue(movedSlot, out var newOwner)
                && newOwner != MyShardIndex)
            {
                targetShardIndex = newOwner;
            }
        }

        if (targetShardIndex is null) return null;

        return grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{targetShardIndex.Value}");
    }

    /// <summary>
    /// After a successful local write, forward the post-write LWW value to the
    /// shadow target if a split is active and the key falls in a moved virtual
    /// slot. For non-prepared writes the post-write value is captured by reading
    /// back the leaf's persisted <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/> so TTL metadata
    /// (<c>ExpiresAtTicks</c>) is preserved verbatim. For prepared writes the
    /// caller-supplied <paramref name="value"/> is forwarded directly via
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.SetAsync(string, byte[])"/> - the leaf read-back
    /// is skipped because during a prepare the leaf's <c>Entries</c> still holds
    /// the pre-saga value (the prepare routed into the per-leaf pending-tx
    /// map, not the visible projection), and forwarding that stale value would
    /// bucket the wrong value into the destination's <c>_pendingTx[txid][key]</c>
    /// - which the saga's terminal mark would then flip into the destination's
    /// <c>Entries</c>, leaving a reader routed to the destination after the
    /// shard-map swap surfacing the pre-saga value indefinitely (a saga
    /// atomic-visibility violation across the migrating slot).
    /// <para>
    /// <b>Coverage</b>: this helper handles live values only. Tombstone
    /// forwarding for <c>DeleteAsync</c>/<c>DeleteRangeAsync</c> is performed
    /// by the comprehensive cleanup phase of the split coordinator after the
    /// shard map swap; until cleanup completes, deleted-during-shadow keys
    /// may transiently appear live on the target shard. The split's
    /// <c>IsCompleteAsync</c> contract returns <c>true</c> only after cleanup
    /// has run, so callers waiting on completion always see a consistent state.
    /// </para>
    /// <para>
    /// <b>Race</b>: a concurrent write between this read-back and the forward
    /// is benign - both writes' forwards will eventually arrive and CRDT LWW
    /// (highest HLC wins) will converge to the correct value on the target.
    /// </para>
    /// <para>
    /// <b>Phase coverage - Reject admission.</b> The active-split branch admits
    /// <see cref="Orleans.Lattice.BPlusTree.State.ShardSplitPhase.BeginShadowWrite"/>, <see cref="Orleans.Lattice.BPlusTree.State.ShardSplitPhase.Drain"/>,
    /// <see cref="ShardSplitPhase.Swap"/>, AND <see cref="Orleans.Lattice.BPlusTree.State.ShardSplitPhase.Reject"/>. Reject is included because
    /// <c>ThrowIfRejectedForKey</c> only gates writes whose <c>SetAsync</c>
    /// has not yet entered the leaf traversal - an in-flight write that
    /// already passed the gate (when the phase was Swap) can land its
    /// prepared value into the destination's <c>_pendingTx[txid][key]</c>
    /// - which the saga's terminal mark would then flip into the destination's
    /// <c>Entries</c>, leaving a reader routed to the destination after the
    /// shard-map swap surfacing the pre-saga value indefinitely (a saga
    /// atomic-visibility violation across the migrating slot).
    /// </para>
    /// <para>
    /// <b>Post-complete fallback.</b> A symmetric race exists at
    /// the Reject → Complete boundary: the coordinator clears
    /// <see cref="Orleans.Lattice.BPlusTree.State.ShardRootState.SplitInProgress"/> and populates
    /// <see cref="Orleans.Lattice.BPlusTree.State.ShardRootState.MovedAwaySlots"/> +
    /// <see cref="Orleans.Lattice.BPlusTree.State.ShardRootState.MovedAwayVirtualShardCount"/> in the
    /// same write. If a write's <c>SetAsync</c> entered before the clear
    /// but reaches this helper after, <c>sip</c> reads as <c>null</c>.
    /// The fallback consults <c>MovedAwaySlots</c> under the recorded
    /// virtual shard count and forwards to the post-split owner. This
    /// mirrors <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.GetSplitForwardTargetsAsync"/>
    /// which already enumerates the post-Complete destinations for the
    /// saga's terminal fan-out, closing the prepare-side gap symmetrically.
    /// </para>
    /// </summary>
    private async Task ForwardLocalWriteToShadowIfNeededAsync(string key, byte[] value, long expiresAtTicks = 0L)
    {
        var target = TryResolveSplitShadowTarget(key);
        if (target is null) return;

        // Saga prepare-phase shadow-forward branch. When the local write is
        // a saga prepare (LatticePreparedContext active and a non-empty
        // transaction id is set), MergeManyAsync would land the value
        // directly in the destination leaf's visible Entries - bypassing
        // the prepared / pending-tx semantics that BPlusLeafGrain.CommitSetAsync
        // applies on the foreground SetAsync path. The destination leaf
        // would then surface the prepared value to readers immediately
        // (post-saga visibility before commit) and never receive the
        // saga's terminal mark via the per-shard fan-out, leaving its
        // Entries stuck on the prepared value and breaking strict atomic
        // reader isolation across the migrating slot. Routing through
        // SetAsync instead lets the destination's BPlusLeafGrain see
        // the propagated LatticePreparedContext + LatticeTransactionContext
        // (via Orleans RequestContext) and bucket the value into its own
        // _pendingTx[txid][key], where it is correctly hidden from
        // readers until the saga's terminal mark - forwarded by
        // AppendTxTerminalAsync via the symmetric split-shadow forward -
        // flips it into Entries.
        //
        // Important: forward the caller-supplied value (the prepared
        // value), NOT a read-back of the leaf's Entries. During a prepare
        // the leaf's Entries still holds the pre-saga value (the prepare
        // wrote into _pendingTx, not Entries), so a leaf read-back here
        // would forward the stale pre-saga value and the destination's
        // _pendingTx[txid][key] would carry the wrong value - which the
        // saga's terminal would then commit into Entries, producing the
        // mid-saga atomic-visibility violation described in the method's
        // XML doc.
        if (LatticePreparedContext.Current && LatticeTransactionContext.Current != Guid.Empty)
        {
            var shadowTxId = LatticeTransactionContext.Current;
            if (expiresAtTicks > 0L)
                await ForwardWithDeadlineAsync(() => target.SetAsync(key, value, expiresAtTicks));
            else
                await ForwardWithDeadlineAsync(() => target.SetAsync(key, value));

            // Install the destination-side shadow marker for this in-flight
            // saga, mirroring TreeShardSplitGrain.RetroactiveSweepPreparedMutationsAsync.
            // The retroactive sweep installs this marker for sagas that were
            // in flight when the split began; a saga that prepares LATER -
            // during Drain / Swap, after the sweep has already walked past
            // this leaf - reaches the destination only through this live
            // shadow-forward, so the marker must be installed here too.
            //
            // Why it is required: the coordinator's drain migrates the
            // source's pre-saga value into the destination's Entries with
            // IsMigrated=true. Without a marker naming this saga as the owner
            // of the key, a reader that routes to the destination after the
            // shard-map swap - observing the saga as Committed (post
            // MarkCommittedAsync) but BEFORE the saga's backstop terminal
            // reaches the destination - surfaces that migrated pre-saga value
            // for this key while every sibling key already shows the post-saga
            // value. That is the non-atomic mixed-round batch the reshard
            // chaos fixture catches (round=N: split (pre=k, post=m)). The
            // marker makes the destination read gate raise
            // StaleShardRoutingException for the Committed-without-backstop
            // window (see BPlusLeafGrain.IsShadowedReadSafeAsync), forcing the
            // LatticeGrain deadline-bounded retry loop to re-fan once the
            // backstop lands.
            //
            // Ordering matches the sweep: target.SetAsync above registers the
            // destination as a saga participant (RecordAffectedLeafIfPreparedAsync)
            // BEFORE the marker lands, and the read gate keys its safety
            // decision off the per-leaf _recentlyTerminal set rather than
            // marker presence, so a terminal that races ahead of the marker
            // cannot strand an un-clearable guard - the marker degenerates to
            // a harmless no-op once the terminal has been applied.
            await ForwardWithDeadlineAsync(() => target.MarkSagaShadowAsync(shadowTxId, new[] { key }));
            return;
        }

        // Non-prepared path: read the raw LwwValue (not the filtered VersionedValue)
        // so the entry's ExpiresAtTicks is forwarded verbatim. Using the filtered path
        // would drop TTL metadata, leaving the target shard with a non-expiring
        // copy after the split commits. Decided by node TYPE so a corrupt
        // RootIsLeaf flag over an internal root (issue 899) descends instead of
        // blind-casting the internal root to IBPlusLeafGrain.
        var leafId = RootIsLeafTyped
            ? state.State.RootNodeId!.Value
            : await TraverseToLeafAsync(key);
        var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
        var raw = await leaf.GetRawEntryAsync(key);
        if (raw is null || raw.Value.IsTombstone) return; // deleted/missing - handled by cleanup phase.

        await ForwardWithDeadlineAsync(() =>
            target.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>(1) { [key] = raw.Value.ToLwwValue() }, isCrossShardMigration: true));
    }

    /// <summary>
    /// Delete analogue of <see cref="ForwardLocalWriteToShadowIfNeededAsync"/>'s
    /// prepared-context branch. A saga prepare-phase DELETE to a moved slot
    /// during an active split must forward the prepared tombstone to the
    /// destination shard AND install the destination-side shadow marker,
    /// exactly as the write path does for a prepared set.
    /// <para>
    /// Why it is required: the coordinator's drain migrates the source's
    /// pre-saga LIVE value into the destination's <c>Entries</c> with
    /// <c>IsMigrated=true</c>. Without a marker naming this saga as the owner
    /// of the key, a reader that routes to the destination after the
    /// shard-map swap - observing the saga as Committed (post
    /// <c>MarkCommittedAsync</c>) but BEFORE the saga's backstop terminal
    /// (the tombstone) reaches the destination - surfaces that migrated
    /// pre-saga live value for this key while every sibling key in the same
    /// atomic batch already shows the post-saga (deleted) state. That is the
    /// delete flavour of the non-atomic mixed-round batch the reshard chaos
    /// fixture catches (<c>ReshardTopologyTests</c> round=N: split
    /// (pre=k, post=m)), violating the <c>SetManyAtomicAsync</c>
    /// all-or-nothing visibility contract (issue 1117).
    /// </para>
    /// <para>
    /// Forwarding the tombstone as a <c>DeleteAsync</c> buckets it into the
    /// destination leaf's <c>_pendingTx[txid][key]</c> and registers the
    /// destination as a saga participant
    /// (<c>RecordAffectedLeafIfPreparedAsync</c>), so the saga's backstop
    /// terminal flips it into a tombstone in the destination's
    /// <c>Entries</c> - the delete-side mirror of the write path's
    /// <c>target.SetAsync</c> forward. Ordering matches the sweep and the
    /// write path: the value-forward registers participation BEFORE the
    /// marker lands, and the read gate keys its safety decision off the
    /// per-leaf <c>_recentlyTerminal</c> set (not marker presence), so a
    /// terminal that races ahead of the marker cannot strand an un-clearable
    /// guard - the marker degenerates to a harmless no-op once the terminal
    /// has been applied.
    /// </para>
    /// <para>
    /// Non-prepared deletes are intentionally NOT forwarded here: tombstone
    /// convergence for non-saga deletes is restored by the split
    /// coordinator's comprehensive cleanup phase after the swap (see the
    /// Coverage note on <see cref="ForwardLocalWriteToShadowIfNeededAsync"/>).
    /// Only a saga prepare can produce the cross-key atomic-visibility
    /// violation, so only the saga prepare path needs the marker.
    /// </para>
    /// </summary>
    private async Task ForwardLocalDeleteToShadowIfNeededAsync(string key)
    {
        // Only a saga prepare-phase delete can tear a cross-key atomic
        // batch; non-prepared deletes converge via the coordinator cleanup
        // phase and must not be forwarded here.
        if (!LatticePreparedContext.Current || LatticeTransactionContext.Current == Guid.Empty)
            return;

        var target = TryResolveSplitShadowTarget(key);
        if (target is null) return;

        var shadowTxId = LatticeTransactionContext.Current;

        // Forward the prepared tombstone (registers the destination as a
        // participant and buckets the tombstone into _pendingTx[txid][key]),
        // then install the destination-side shadow marker. Each hop is
        // ForwardWithDeadlineAsync-bounded so a forward parked against a
        // shard whose ownership is changing during the swap cannot pin the
        // foreground turn.
        await ForwardWithDeadlineAsync(() => target.DeleteAsync(key));
        await ForwardWithDeadlineAsync(() => target.MarkSagaShadowAsync(shadowTxId, new[] { key }));
    }

    private static bool SlotsEqual(int[] sortedExisting, int[] candidate)
    {
        if (sortedExisting.Length != candidate.Length) return false;
        var copy = (int[])candidate.Clone();
        Array.Sort(copy);
        for (int i = 0; i < copy.Length; i++)
            if (copy[i] != sortedExisting[i]) return false;
        return true;
    }

    /// <summary>
    /// Whether <paramref name="existing"/> describes the same migration as the
    /// incoming target, slot set and virtual shard count - that is, whether an
    /// incoming <see cref="BeginSplitAsync"/> is a re-assertion of the window
    /// already open rather than an attempt to re-aim it somewhere else.
    /// </summary>
    private static bool HasSameAim(
        ShardSplitInProgress existing, int targetShardIndex, int[] movedSlots, int virtualShardCount)
        => existing.ShadowTargetShardIndex == targetShardIndex
            && existing.VirtualShardCount == virtualShardCount
            && existing.MovedSlots.Length == movedSlots.Length
            && SlotsEqual(existing.MovedSlots, movedSlots);

    /// <inheritdoc />
    public async Task<int> MarkLeavesMovedAwayAsync(int[] sortedMovedSlots, int virtualShardCount)
    {
        ArgumentNullException.ThrowIfNull(sortedMovedSlots);
        if (virtualShardCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(virtualShardCount), "Must be greater than 0.");

        if (sortedMovedSlots.Length == 0 || state.State.RootNodeId is null)
            return 0;

        await PrepareForOperationAsync();

        // Walk the leaf chain from leftmost and mark every leaf with
        // the moved-slot set. Idempotent on identical inputs - leaves
        // that have already recorded the same set are no-ops. Decided by
        // node TYPE so a corrupt RootIsLeaf flag over an internal root
        // (issue 899) descends to the leftmost leaf rather than blind-casting.
        var leafId = RootIsLeafTyped
            ? state.State.RootNodeId!.Value
            : (await GetLeftmostLeafIdAsync())!.Value;

        var leavesMarked = 0;
        while (true)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            await leaf.MarkSlotsMovedAwayAsync(sortedMovedSlots, virtualShardCount);
            leavesMarked++;

            var next = await leaf.GetNextSiblingAsync();
            if (next is null) break;
            leafId = next.Value;
        }

        return leavesMarked;
    }

    /// <inheritdoc />
    public async Task MarkSagaShadowAsync(Guid transactionId, IReadOnlyList<string> keys)
    {
        ArgumentNullException.ThrowIfNull(keys);
        if (transactionId == Guid.Empty)
            throw new ArgumentException("Transaction id must be non-empty.", nameof(transactionId));

        if (keys.Count == 0 || state.State.RootNodeId is null)
            return;

        await PrepareForOperationAsync();

        // Group keys by owning leaf so each leaf receives a single
        // batched MarkSagaShadowAsync call rather than N per-key RPCs.
        // The leaf-side marker is keyed per-key, so the per-leaf
        // batch matches the contract on the other end.
        //
        // Root-is-leaf shards collapse to a single-leaf bucket; the
        // traversal path would otherwise try to read a routing table
        // from a leaf grain and throw InvalidCastException. Decided by node
        // TYPE so a corrupt RootIsLeaf flag over an internal root (issue 899)
        // takes the per-key routing path below instead of blind-casting.
        if (RootIsLeafTyped)
        {
            var rootLeafId = state.State.RootNodeId!.Value;
            var rootLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(rootLeafId);
            await rootLeaf.MarkSagaShadowAsync(transactionId, keys);
            return;
        }

        var byLeaf = new Dictionary<GrainId, List<string>>();
        foreach (var key in keys)
        {
            if (string.IsNullOrEmpty(key)) continue;
            var leafId = await TraverseToLeafAsync(key);
            if (!byLeaf.TryGetValue(leafId, out var list))
            {
                list = new List<string>();
                byLeaf[leafId] = list;
            }
            list.Add(key);
        }

        foreach (var (leafId, leafKeys) in byLeaf)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
            await leaf.MarkSagaShadowAsync(transactionId, leafKeys);
        }
    }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="key"/> hashes to a virtual slot
    /// that this shard no longer authoritively owns - either because it has
    /// been permanently split away (<see cref="Orleans.Lattice.BPlusTree.State.ShardRootState.MovedAwaySlots"/>),
    /// or because an active split has reached <see cref="ShardSplitPhase.Swap"/>
    /// or later (the registry's shard map already routes the slot to <c>T</c>
    /// and <c>T</c> holds the authoritative copy via the drain + shadow-write
    /// pipeline). Used by scan APIs to suppress orphan entries that would
    /// otherwise duplicate authoritative data on the new owner shard.
    /// </summary>
    internal bool IsSlotMovedAway(string key)
    {
        // Active in-progress split: once swap has happened, S's copy is no
        // longer authoritative for moved slots - scans must not yield it.
        var sip = state.State.SplitInProgress;
        if (sip is not null
            && (sip.Phase == ShardSplitPhase.Swap
                || sip.Phase == ShardSplitPhase.Reject
                || sip.Phase == ShardSplitPhase.Complete))
        {
            var sipSlot = ShardMap.GetVirtualSlot(key, sip.VirtualShardCount);
            if (sip.IsMovedSlot(sipSlot)) return true;
        }

        var moved = state.State.MovedAwaySlots;
        if (moved.Count == 0 || state.State.MovedAwayVirtualShardCount is not { } vsc)
            return false;
        var slot = ShardMap.GetVirtualSlot(key, vsc);
        return moved.ContainsKey(slot);
    }

    /// <summary>
    /// Slot-reporting variant of <see cref="IsSlotMovedAway"/>. When the key's
    /// virtual slot is in the active-split moved set or the permanent
    /// <see cref="Orleans.Lattice.BPlusTree.State.ShardRootState.MovedAwaySlots"/> map, returns <c>true</c>
    /// and outputs the slot index; otherwise returns <c>false</c> and
    /// <paramref name="slot"/> is <c>-1</c>. Used by strongly-consistent scan
    /// APIs to both filter the entry and report the affected slot to
    /// the orchestrator so it can re-fetch from the new owner.
    /// </summary>
    internal bool TryGetMovedAwaySlot(string key, out int slot)
    {
        var sip = state.State.SplitInProgress;
        if (sip is not null
            && (sip.Phase == ShardSplitPhase.Swap
                || sip.Phase == ShardSplitPhase.Reject
                || sip.Phase == ShardSplitPhase.Complete))
        {
            var s = ShardMap.GetVirtualSlot(key, sip.VirtualShardCount);
            if (sip.IsMovedSlot(s))
            {
                slot = s;
                return true;
            }
        }

        var moved = state.State.MovedAwaySlots;
        if (moved.Count > 0 && state.State.MovedAwayVirtualShardCount is { } vsc)
        {
            var s = ShardMap.GetVirtualSlot(key, vsc);
            if (moved.ContainsKey(s))
            {
                slot = s;
                return true;
            }
        }

        slot = -1;
        return false;
    }

    /// <summary>
    /// Read-path gate for batched operations. Throws on the first key in
    /// <paramref name="keys"/> that maps to a moved virtual slot from the
    /// swap phase onward or to a slot already in
    /// <see cref="Orleans.Lattice.BPlusTree.State.ShardRootState.MovedAwaySlots"/>. See
    /// <see cref="ThrowIfMovedAwayForReadKey"/> for the rationale on
    /// including <see cref="ShardSplitPhase.Swap"/>.
    /// </summary>
    private void ThrowIfMovedAwayForReadAnyKey(IEnumerable<string> keys)
    {
        var sip = state.State.SplitInProgress;
        var readGateActive = sip is not null
            && (sip.Phase == ShardSplitPhase.Swap || sip.Phase == ShardSplitPhase.Reject);
        var moved = state.State.MovedAwaySlots;
        var movedActive = moved.Count > 0 && state.State.MovedAwayVirtualShardCount is not null;
        if (!readGateActive && !movedActive) return;

        var movedVsc = state.State.MovedAwayVirtualShardCount ?? 0;
        foreach (var key in keys)
        {
            if (readGateActive)
            {
                var slot = ShardMap.GetVirtualSlot(key, sip!.VirtualShardCount);
                if (sip.IsMovedSlot(slot))
                {
#if LATTICE_DIAG
                    // [DIAG] trace batch read-gate throw via SIP.
                    DiagSink.Write($"[DIAG shard-read-gate-throw-sip-any] shardIdx={MyShardIndex} key={key} slot={slot} target={sip.ShadowTargetShardIndex} phase={sip.Phase}");
#endif
                    throw new StaleShardRoutingException(MyShardIndex, sip.ShadowTargetShardIndex, slot);
                }
            }
            if (movedActive)
            {
                var slot = ShardMap.GetVirtualSlot(key, movedVsc);
                if (moved.TryGetValue(slot, out var target))
                {
#if LATTICE_DIAG
                    // [DIAG] trace batch read-gate throw via MovedAwaySlots.
                    DiagSink.Write($"[DIAG shard-read-gate-throw-moved-any] shardIdx={MyShardIndex} key={key} slot={slot} target={target} vsc={movedVsc}");
#endif
                    throw new StaleShardRoutingException(MyShardIndex, target, slot);
                }
            }
        }
    }
}
