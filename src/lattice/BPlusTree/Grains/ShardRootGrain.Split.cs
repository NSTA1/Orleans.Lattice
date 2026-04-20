using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// adaptive split surface for the shard root.
/// <para>
/// During an adaptive split, the source shard <c>S</c> participates in three
/// hot-path behaviours driven by the persisted
/// <see cref="ShardRootState.SplitInProgress"/>:
/// </para>
/// <list type="number">
/// <item><description>
/// In <see cref="ShardSplitPhase.BeginShadowWrite"/>, <see cref="ShardSplitPhase.Drain"/>,
/// or <see cref="ShardSplitPhase.Swap"/>, every successful write to a key in a
/// moved virtual slot is mirrored to the target shard <c>T</c> via
/// <see cref="IShardRootGrain.MergeManyAsync"/> with the original HLC, so that
/// CRDT LWW guarantees convergence regardless of interleaving with the
/// background drain.
/// </description></item>
/// <item><description>
/// In <see cref="ShardSplitPhase.Reject"/>, every operation on a moved-slot
/// key throws <see cref="StaleShardRoutingException"/>, signalling the
/// caller's <c>LatticeGrain</c> to refresh its cached <see cref="ShardMap"/>
/// and retry against the new owner.
/// </description></item>
/// <item><description>
/// The target shard <c>T</c> never holds a <see cref="ShardSplitInProgress"/>
/// record, so neither hook fires there — this naturally prevents recursive
/// shadow-forwarding when <c>T</c> receives the mirrored
/// <see cref="IShardRootGrain.MergeManyAsync"/> call. A defensive assertion in
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
            var slash = key.LastIndexOf('/');
            var parsed = int.Parse(key.AsSpan(slash + 1), System.Globalization.CultureInfo.InvariantCulture);
            _myShardIndex = parsed;
            return parsed;
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
        if (existing is not null
            && existing.ShadowTargetShardIndex == targetShardIndex
            && existing.VirtualShardCount == virtualShardCount
            && existing.MovedSlots.Length == movedSlots.Length
            && (existing.Phase == ShardSplitPhase.BeginShadowWrite || existing.Phase == ShardSplitPhase.Drain))
        {
            // Idempotent re-entry — verify slot set matches.
            if (SlotsEqual(existing.MovedSlots, movedSlots))
                return;
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
        await state.WriteStateAsync();
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
        await state.WriteStateAsync();
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
        await state.WriteStateAsync();
    }

    /// <inheritdoc />
    public Task<bool> IsSplittingAsync() => Task.FromResult(state.State.SplitInProgress is not null);

    /// <inheritdoc />
    public Task<bool> HasPendingBulkOperationAsync()
        => Task.FromResult(state.State.PendingBulkGraft is not null);

    /// <summary>
    /// Hot-path read/write gate. Throws <see cref="StaleShardRoutingException"/>
    /// when (a) the shard is in <see cref="ShardSplitPhase.Reject"/> and
    /// <paramref name="key"/> hashes to a moved virtual slot of the active
    /// split, or (b) <paramref name="key"/> hashes to a slot in
    /// <see cref="ShardRootState.MovedAwaySlots"/> from a previously-completed
    /// split. No-op otherwise.
    /// </summary>
    private void ThrowIfRejectedForKey(string key)
    {
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
    /// Hot-path read/write gate for batched operations. Throws on the first key
    /// in <paramref name="keys"/> that maps to a moved virtual slot during the
    /// reject phase or to a slot already in
    /// <see cref="ShardRootState.MovedAwaySlots"/>. No-op when neither
    /// condition holds.
    /// </summary>
    private void ThrowIfRejectedForAnyKey(IEnumerable<string> keys)
    {
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
    /// Forwards a successful local write to the shadow target if the split is
    /// in <see cref="ShardSplitPhase.BeginShadowWrite"/>, <see cref="ShardSplitPhase.Drain"/>,
    /// or <see cref="ShardSplitPhase.Swap"/> and <paramref name="key"/> hashes
    /// to a moved virtual slot. Uses <see cref="IShardRootGrain.MergeManyAsync"/>
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
        await target.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>(1) { [key] = value });
    }

    /// <summary>
    /// Batched variant of <see cref="TryForwardShadowWriteAsync(string, LwwValue{byte[]})"/>:
    /// groups all moved-slot entries from <paramref name="entries"/> into a single
    /// <see cref="IShardRootGrain.MergeManyAsync"/> call to amortise the cross-grain hop.
    /// </summary>
    private async Task TryForwardShadowWritesAsync(IEnumerable<KeyValuePair<string, LwwValue<byte[]>>> entries)
    {
        var sip = state.State.SplitInProgress;
        if (sip is null) return;
        if (sip.Phase != ShardSplitPhase.BeginShadowWrite
            && sip.Phase != ShardSplitPhase.Drain
            && sip.Phase != ShardSplitPhase.Swap) return;
        if (sip.ShadowTargetShardIndex == MyShardIndex) return;

        Dictionary<string, LwwValue<byte[]>>? batch = null;
        foreach (var kvp in entries)
        {
            var slot = ShardMap.GetVirtualSlot(kvp.Key, sip.VirtualShardCount);
            if (!sip.IsMovedSlot(slot)) continue;
            (batch ??= new Dictionary<string, LwwValue<byte[]>>())[kvp.Key] = kvp.Value;
        }

        if (batch is null || batch.Count == 0) return;

        var target = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{sip.ShadowTargetShardIndex}");
        await target.MergeManyAsync(batch);
    }

    /// <summary>
    /// After a successful local write, forward the post-write LWW value to the
    /// shadow target if a split is active and the key falls in a moved virtual
    /// slot. The post-write value is captured by reading back the leaf's
    /// version via <see cref="TraverseForReadWithVersionAsync(string)"/>
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
    /// is benign — both writes' forwards will eventually arrive and CRDT LWW
    /// (highest HLC wins) will converge to the correct value on the target.
    /// </para>
    /// </summary>
    private async Task ForwardLocalWriteToShadowIfNeededAsync(string key)
    {
        var sip = state.State.SplitInProgress;
        if (sip is null) return;
        if (sip.Phase != ShardSplitPhase.BeginShadowWrite
            && sip.Phase != ShardSplitPhase.Drain
            && sip.Phase != ShardSplitPhase.Swap) return;
        if (sip.ShadowTargetShardIndex == MyShardIndex) return;

        var slot = ShardMap.GetVirtualSlot(key, sip.VirtualShardCount);
        if (!sip.IsMovedSlot(slot)) return;

        // read the raw LwwValue (not the filtered VersionedValue) so the
        // entry's ExpiresAtTicks is forwarded verbatim. Using the filtered path
        // would drop TTL metadata, leaving the target shard with a non-expiring
        // copy after the split commits.
        var leafId = await TraverseToLeafAsync(key);
        var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId);
        var raw = await leaf.GetRawEntryAsync(key);
        if (raw is null || raw.Value.IsTombstone) return; // deleted/missing — handled by cleanup phase.

        var target = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{sip.ShadowTargetShardIndex}");
        await target.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>(1) { [key] = raw.Value.ToLwwValue() });
    }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="key"/> hashes to a moved virtual
    /// slot under the active split. Used by tests and internal callers that
    /// need to know whether a key is subject to shadow-write or reject behaviour.
    /// </summary>
    internal bool IsKeyInMovedSlot(string key)
    {
        var sip = state.State.SplitInProgress;
        if (sip is null) return false;
        var slot = ShardMap.GetVirtualSlot(key, sip.VirtualShardCount);
        return sip.IsMovedSlot(slot);
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
    /// Returns <c>true</c> when <paramref name="key"/> hashes to a virtual slot
    /// that this shard no longer authoritatively owns — either because it has
    /// been permanently split away (<see cref="ShardRootState.MovedAwaySlots"/>),
    /// or because an active split has reached <see cref="ShardSplitPhase.Swap"/>
    /// or later (the registry's shard map already routes the slot to <c>T</c>
    /// and <c>T</c> holds the authoritative copy via the drain + shadow-write
    /// pipeline). Used by scan APIs to suppress orphan entries that would
    /// otherwise duplicate authoritative data on the new owner shard.
    /// </summary>
    internal bool IsSlotMovedAway(string key)
    {
        // Active in-progress split: once swap has happened, S's copy is no
        // longer authoritative for moved slots — scans must not yield it.
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
    /// <see cref="ShardRootState.MovedAwaySlots"/> map, returns <c>true</c>
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
}
