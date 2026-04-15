using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Leaf node grain implementation. Stores key → <see cref="LwwValue{T}"/> entries
/// in a sorted dictionary. Splits when the entry count exceeds <see cref="LatticeOptions.MaxLeafKeys"/>.
/// </summary>
internal sealed class BPlusLeafGrain(
    IGrainContext context,
    [PersistentState("leaf", "bplustree")] IPersistentState<LeafNodeState> state,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> optionsMonitor) : IBPlusLeafGrain
{
    private string ReplicaId => context.GrainId.ToString();
    private LatticeOptions Options => optionsMonitor.Get(state.State.TreeId ?? string.Empty);

    public Task<byte[]?> GetAsync(string key)
    {
        if (state.State.Entries.TryGetValue(key, out var lww) && !lww.IsTombstone)
        {
            return Task.FromResult<byte[]?>(lww.Value);
        }

        return Task.FromResult<byte[]?>(null);
    }

    public async Task<SplitResult?> SetAsync(string key, byte[] value)
    {
        // Recovery: if a previous split was interrupted, complete it first.
        if (state.State.SplitState == Primitives.SplitState.SplitInProgress)
        {
            var recovered = await CompleteSplitAsync();
            await state.WriteStateAsync();

            // Apply the caller's write to the correct leaf so it isn't silently dropped.
            if (string.Compare(key, state.State.SplitKey!, StringComparison.Ordinal) >= 0)
            {
                // The key belongs to the new sibling — forward it there.
                var sibling = grainFactory.GetGrain<IBPlusLeafGrain>(state.State.SplitSiblingId!.Value);
                await sibling.SetAsync(key, value);
            }
            else
            {
                // The key belongs to this leaf — write it here.
                state.State.Clock = HybridLogicalClock.Tick(state.State.Clock);
                state.State.Version.Tick(ReplicaId);
                var entry = LwwValue<byte[]>.Create(value, state.State.Clock);
                if (state.State.Entries.TryGetValue(key, out var prev))
                    state.State.Entries[key] = LwwValue<byte[]>.Merge(prev, entry);
                else
                    state.State.Entries[key] = entry;
                await state.WriteStateAsync();
            }

            return recovered;
        }

        state.State.Clock = HybridLogicalClock.Tick(state.State.Clock);
        state.State.Version.Tick(ReplicaId);
        var newEntry = LwwValue<byte[]>.Create(value, state.State.Clock);

        if (state.State.Entries.TryGetValue(key, out var existing))
        {
            state.State.Entries[key] = LwwValue<byte[]>.Merge(existing, newEntry);
        }
        else
        {
            state.State.Entries[key] = newEntry;
        }

        SplitResult? splitResult = null;
        if (state.State.Entries.Count > Options.MaxLeafKeys)
        {
            splitResult = await SplitAsync();
        }

        await state.WriteStateAsync();
        return splitResult;
    }

    public async Task<bool> DeleteAsync(string key)
    {
        if (!state.State.Entries.TryGetValue(key, out var existing) || existing.IsTombstone)
        {
            return false;
        }

        state.State.Clock = HybridLogicalClock.Tick(state.State.Clock);
        state.State.Version.Tick(ReplicaId);
        state.State.Entries[key] = LwwValue<byte[]>.Tombstone(state.State.Clock);
        await state.WriteStateAsync();
        return true;
    }

    public Task<GrainId?> GetNextSiblingAsync() =>
        Task.FromResult(state.State.NextSibling);

    public async Task SetNextSiblingAsync(GrainId? siblingId)
    {
        state.State.NextSibling = siblingId;
        await state.WriteStateAsync();
    }

    public async Task SetTreeIdAsync(string treeId)
    {
        if (state.State.TreeId is not null) return;
        state.State.TreeId = treeId;
        await state.WriteStateAsync();
    }

    public Task<StateDelta> GetDeltaSinceAsync(VersionVector sinceVersion)
    {
        // If the caller's version dominates ours, they already have everything.
        if (sinceVersion.DominatesOrEquals(state.State.Version))
        {
            return Task.FromResult(new StateDelta
            {
                Entries = new Dictionary<string, LwwValue<byte[]>>(),
                Version = state.State.Version.Clone(),
                SplitKey = state.State.SplitKey
            });
        }

        // Return all entries whose timestamp is newer than what the caller has seen.
        // We compare each entry's timestamp against the caller's clock for our replica.
        var callerClock = sinceVersion.GetClock(ReplicaId);
        var changed = new Dictionary<string, LwwValue<byte[]>>();

        foreach (var (key, lww) in state.State.Entries)
        {
            if (lww.Timestamp > callerClock)
            {
                changed[key] = lww;
            }
        }

        return Task.FromResult(new StateDelta
        {
            Entries = changed,
            Version = state.State.Version.Clone(),
            SplitKey = state.State.SplitKey
        });
    }

    public async Task MergeEntriesAsync(Dictionary<string, LwwValue<byte[]>> entries)
    {
        foreach (var (key, incoming) in entries)
        {
            if (state.State.Entries.TryGetValue(key, out var existing))
            {
                state.State.Entries[key] = LwwValue<byte[]>.Merge(existing, incoming);
            }
            else
            {
                state.State.Entries[key] = incoming;
            }
        }

        await state.WriteStateAsync();
    }

    private async Task<SplitResult> SplitAsync()
    {
        // Phase 1: Persist the split intent so we can resume after a crash.
        // Compute the split key and new sibling identity once, and write them
        // to durable state before making any cross-grain calls.
        var keys = state.State.Entries.Keys.ToList();
        int mid = keys.Count / 2;
        var splitKey = keys[mid];

        state.State.SplitState = state.State.SplitState.Merge(Primitives.SplitState.SplitInProgress);
        state.State.SplitKey = splitKey;
        state.State.SplitSiblingId = grainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid()).GetGrainId();
        state.State.NextSibling = state.State.SplitSiblingId;
        await state.WriteStateAsync();

        // Phase 2: Execute cross-grain operations using the persisted identity.
        return await CompleteSplitAsync();
    }

    /// <summary>
    /// Completes (or resumes) a split whose intent has already been persisted.
    /// Safe to call multiple times — <see cref="IBPlusLeafGrain.MergeEntriesAsync"/>
    /// is idempotent (LWW merge), and <see cref="IBPlusLeafGrain.SetNextSiblingAsync"/>
    /// is a simple overwrite of the same value.
    /// </summary>
    private async Task<SplitResult> CompleteSplitAsync()
    {
        var splitKey = state.State.SplitKey!;
        var siblingId = state.State.SplitSiblingId!.Value;
        var newLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(siblingId);

        await newLeaf.SetTreeIdAsync(state.State.TreeId!);

        // Collect entries that belong to the right sibling (keys >= splitKey).
        var rightEntries = new Dictionary<string, LwwValue<byte[]>>();
        foreach (var (key, lww) in state.State.Entries)
        {
            if (string.Compare(key, splitKey, StringComparison.Ordinal) >= 0)
            {
                rightEntries[key] = lww;
            }
        }

        // Bulk-merge preserves original timestamps and tombstones.
        if (rightEntries.Count > 0)
        {
            await newLeaf.MergeEntriesAsync(rightEntries);
        }

        // Remove the transferred entries from our local state.
        foreach (var key in rightEntries.Keys)
        {
            state.State.Entries.Remove(key);
        }

        state.State.SplitState = state.State.SplitState.Merge(Primitives.SplitState.SplitComplete);

        return new SplitResult
        {
            PromotedKey = splitKey,
            NewSiblingId = siblingId
        };
    }
}
