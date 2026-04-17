using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Leaf node grain implementation. Stores key → <see cref="LwwValue{T}"/> entries
/// in a sorted dictionary. Splits when the entry count exceeds <see cref="LatticeOptions.MaxLeafKeys"/>.
/// </summary>
internal sealed partial class BPlusLeafGrain(
    IGrainContext context,
    [PersistentState("leaf", LatticeOptions.StorageProviderName)] IPersistentState<LeafNodeState> state,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> optionsMonitor) : IBPlusLeafGrain
{
    private static readonly Dictionary<string, LwwValue<byte[]>> EmptyEntries = new();

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

    public Task<bool> ExistsAsync(string key)
    {
        return Task.FromResult(
            state.State.Entries.TryGetValue(key, out var lww) && !lww.IsTombstone);
    }

    public Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys)
    {
        var result = new Dictionary<string, byte[]>(keys.Count);
        foreach (var key in keys)
        {
            if (state.State.Entries.TryGetValue(key, out var lww) && !lww.IsTombstone)
            {
                result[key] = lww.Value!;
            }
        }
        return Task.FromResult(result);
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

    public async Task<SplitResult?> SetManyAsync(List<KeyValuePair<string, byte[]>> entries)
    {
        SplitResult? lastSplit = null;
        foreach (var entry in entries)
        {
            var split = await SetAsync(entry.Key, entry.Value);
            if (split is not null)
                lastSplit = split;
        }
        return lastSplit;
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

    public async Task<int> DeleteRangeAsync(string startInclusive, string endExclusive)
    {
        // Collect matching keys. Entries is a SortedDictionary so we can
        // break early once we pass endExclusive.
        List<string>? keysToDelete = null;
        foreach (var (key, lww) in state.State.Entries)
        {
            if (string.Compare(key, endExclusive, StringComparison.Ordinal) >= 0)
                break;

            if (string.Compare(key, startInclusive, StringComparison.Ordinal) >= 0 && !lww.IsTombstone)
                (keysToDelete ??= []).Add(key);
        }

        if (keysToDelete is null) return 0;

        state.State.Clock = HybridLogicalClock.Tick(state.State.Clock);
        state.State.Version.Tick(ReplicaId);
        var tombstone = LwwValue<byte[]>.Tombstone(state.State.Clock);

        foreach (var key in keysToDelete)
        {
            state.State.Entries[key] = tombstone;
        }

        await state.WriteStateAsync();
        return keysToDelete.Count;
    }

    public Task<GrainId?> GetNextSiblingAsync() =>
        Task.FromResult(state.State.NextSibling);

    public async Task SetNextSiblingAsync(GrainId? siblingId)
    {
        state.State.NextSibling = siblingId;
        await state.WriteStateAsync();
    }

    public Task<GrainId?> GetPrevSiblingAsync() =>
        Task.FromResult(state.State.PrevSibling);

    public async Task SetPrevSiblingAsync(GrainId? siblingId)
    {
        state.State.PrevSibling = siblingId;
        await state.WriteStateAsync();
    }

    public async Task SetTreeIdAsync(string treeId)
    {
        if (state.State.TreeId is not null) return;
        state.State.TreeId = treeId;
        await state.WriteStateAsync();
    }

    public Task<string?> GetTreeIdAsync() =>
        Task.FromResult(state.State.TreeId);

    public async Task<int> CompactTombstonesAsync(TimeSpan gracePeriod)
    {
        // Skip scan if nothing has changed since last compaction.
        if (state.State.LastCompactionVersion.DominatesOrEquals(state.State.Version))
            return 0;

        var cutoff = DateTimeOffset.UtcNow.Ticks - gracePeriod.Ticks;
        var toRemove = new List<string>();

        foreach (var (key, lww) in state.State.Entries)
        {
            if (lww.IsTombstone && lww.Timestamp.WallClockTicks <= cutoff)
            {
                toRemove.Add(key);
            }
        }

        if (toRemove.Count > 0)
        {
            foreach (var key in toRemove)
            {
                state.State.Entries.Remove(key);
            }
        }

        state.State.LastCompactionVersion = state.State.Version.Clone();
        await state.WriteStateAsync();
        return toRemove.Count;
    }

    public Task<StateDelta> GetDeltaSinceAsync(VersionVector sinceVersion)
    {
        // If the caller's version dominates ours, they already have everything.
        if (sinceVersion.DominatesOrEquals(state.State.Version))
        {
            return Task.FromResult(new StateDelta
            {
                Entries = EmptyEntries,
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

        // Tick the version so that LeafCacheGrain delta checks detect the new
        // entries. Without this, a freshly-split sibling has an empty version
        // vector and the cache short-circuits (empty dominates empty), never
        // populating its local cache.
        if (entries.Count > 0)
        {
            state.State.Version.Tick(ReplicaId);
        }

        await state.WriteStateAsync();
    }

    public Task<List<string>> GetKeysAsync(string? startInclusive = null, string? endExclusive = null)
    {
        var splitInProgress = state.State.SplitState == Primitives.SplitState.SplitInProgress;
        var splitKey = state.State.SplitKey;

        var keys = new List<string>();
        foreach (var (key, lww) in state.State.Entries)
        {
            if (endExclusive is not null && string.Compare(key, endExclusive, StringComparison.Ordinal) >= 0)
                break;

            if (splitInProgress && splitKey is not null &&
                string.Compare(key, splitKey, StringComparison.Ordinal) >= 0)
                break;

            if (lww.IsTombstone)
                continue;

            if (startInclusive is not null && string.Compare(key, startInclusive, StringComparison.Ordinal) < 0)
                continue;

            keys.Add(key);
        }

        return Task.FromResult(keys);
    }

    public Task<Dictionary<string, byte[]>> GetLiveEntriesAsync()
    {
        var result = new Dictionary<string, byte[]>();
        foreach (var (key, lww) in state.State.Entries)
        {
            if (!lww.IsTombstone)
            {
                result[key] = lww.Value!;
            }
        }
        return Task.FromResult(result);
    }

    public async Task ClearGrainStateAsync()
    {
        await state.ClearStateAsync();
        context.Deactivate(new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "Tree purged"));
    }
}
