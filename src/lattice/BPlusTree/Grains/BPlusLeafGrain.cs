using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Leaf node grain implementation. Stores key → <see cref="LwwValue{T}"/> entries
/// in a sorted dictionary. Splits when the entry count exceeds <see cref="BPlusTreeOptions.MaxLeafKeys"/>.
/// </summary>
public sealed class BPlusLeafGrain(
    [PersistentState("leaf", "bplustree")] IPersistentState<LeafNodeState> state,
    IGrainFactory grainFactory) : Grain, IBPlusLeafGrain
{
    private string ReplicaId => this.GetGrainId().ToString();

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
        if (state.State.Entries.Count > BPlusTreeOptions.MaxLeafKeys)
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

    public Task<StateDelta> GetDeltaSinceAsync(VersionVector sinceVersion)
    {
        // If the caller's version dominates ours, they already have everything.
        if (sinceVersion.DominatesOrEquals(state.State.Version))
        {
            return Task.FromResult(new StateDelta
            {
                Entries = new Dictionary<string, LwwValue<byte[]>>(),
                Version = state.State.Version.Clone()
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
            Version = state.State.Version.Clone()
        });
    }

    private async Task<SplitResult> SplitAsync()
    {
        state.State.SplitState = state.State.SplitState.Merge(Primitives.SplitState.SplitInProgress);

        // Find the median key to split on.
        var keys = state.State.Entries.Keys.ToList();
        int mid = keys.Count / 2;
        var splitKey = keys[mid];

        // Create the new right sibling leaf.
        var newLeafId = grainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());

        // Move the upper half of entries to the new sibling.
        // Set the new leaf's sibling pointer to our current sibling.
        var oldNextSibling = state.State.NextSibling;
        await newLeafId.SetNextSiblingAsync(oldNextSibling);

        for (int i = mid; i < keys.Count; i++)
        {
            var k = keys[i];
            var v = state.State.Entries[k];
            if (!v.IsTombstone)
            {
                await newLeafId.SetAsync(k, v.Value!);
            }
            state.State.Entries.Remove(k);
        }

        // Point our sibling pointer to the new leaf.
        state.State.NextSibling = newLeafId.GetGrainId();
        state.State.SplitKey = splitKey;
        state.State.SplitSiblingId = newLeafId.GetGrainId();
        state.State.SplitState = state.State.SplitState.Merge(Primitives.SplitState.SplitComplete);

        return new SplitResult
        {
            PromotedKey = splitKey,
            NewSiblingId = newLeafId.GetGrainId()
        };
    }
}
