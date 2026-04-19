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

    public Task<VersionedValue> GetWithVersionAsync(string key)
    {
        if (state.State.Entries.TryGetValue(key, out var lww) && !lww.IsTombstone)
        {
            return Task.FromResult(new VersionedValue { Value = lww.Value, Version = lww.Timestamp });
        }

        return Task.FromResult(new VersionedValue());
    }

    public Task<bool> ExistsAsync(string key)
    {
        return Task.FromResult(
            state.State.Entries.TryGetValue(key, out var lww) && !lww.IsTombstone);
    }

    public Task<GetOrSetResult> GetOrSetAsync(string key, byte[] value)
    {
        // Short-circuit: if the key already exists and is live, return its value without writing.
        if (state.State.Entries.TryGetValue(key, out var existing) && !existing.IsTombstone)
        {
            return Task.FromResult(new GetOrSetResult { ExistingValue = existing.Value });
        }

        // Key is absent or tombstoned — delegate to the write path and wrap the result.
        return GetOrSetWriteAsync(key, value);
    }

    private async Task<GetOrSetResult> GetOrSetWriteAsync(string key, byte[] value)
    {
        var splitResult = await SetAsync(key, value);
        return new GetOrSetResult { Split = splitResult };
    }

    public Task<CasResult> SetIfVersionAsync(string key, byte[] value, HybridLogicalClock expectedVersion)
    {
        // Check current entry version.
        if (state.State.Entries.TryGetValue(key, out var existing) && !existing.IsTombstone)
        {
            if (existing.Timestamp != expectedVersion)
            {
                return Task.FromResult(new CasResult
                {
                    Success = false,
                    CurrentVersion = existing.Timestamp
                });
            }
        }
        else
        {
            // Key is absent or tombstoned — expectedVersion must be Zero.
            if (expectedVersion != HybridLogicalClock.Zero)
            {
                return Task.FromResult(new CasResult
                {
                    Success = false,
                    CurrentVersion = HybridLogicalClock.Zero
                });
            }
        }

        // Version matches — delegate to the async write path.
        return SetIfVersionWriteAsync(key, value);
    }

    private async Task<CasResult> SetIfVersionWriteAsync(string key, byte[] value)
    {
        var splitResult = await SetAsync(key, value);
        // After SetAsync, the entry has a new timestamp.
        var newVersion = state.State.Entries[key].Timestamp;
        return new CasResult
        {
            Success = true,
            CurrentVersion = newVersion,
            Split = splitResult
        };
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

    public async Task<RangeDeleteResult> DeleteRangeAsync(string startInclusive, string endExclusive)
    {
        // Collect matching keys. Entries is a SortedDictionary so we can
        // break early once we pass endExclusive — but we must still report
        // whether we observed a key >= endExclusive (FX-011) so the shard
        // coordinator can terminate the chain walk deterministically.
        List<string>? keysToDelete = null;
        var pastRange = false;
        foreach (var (key, lww) in state.State.Entries)
        {
            if (string.Compare(key, endExclusive, StringComparison.Ordinal) >= 0)
            {
                pastRange = true;
                break;
            }

            if (string.Compare(key, startInclusive, StringComparison.Ordinal) >= 0 && !lww.IsTombstone)
                (keysToDelete ??= []).Add(key);
        }

        if (keysToDelete is null)
            return new RangeDeleteResult { Deleted = 0, PastRange = pastRange };

        state.State.Clock = HybridLogicalClock.Tick(state.State.Clock);
        state.State.Version.Tick(ReplicaId);
        var tombstone = LwwValue<byte[]>.Tombstone(state.State.Clock);

        foreach (var key in keysToDelete)
        {
            state.State.Entries[key] = tombstone;
        }

        await state.WriteStateAsync();
        return new RangeDeleteResult { Deleted = keysToDelete.Count, PastRange = pastRange };
    }

    public Task<int> CountAsync()
    {
        var count = 0;
        foreach (var lww in state.State.Entries.Values)
        {
            if (!lww.IsTombstone) count++;
        }
        return Task.FromResult(count);
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
        var anyInGraceRemaining = false;

        foreach (var (key, lww) in state.State.Entries)
        {
            if (!lww.IsTombstone) continue;

            if (lww.Timestamp.WallClockTicks <= cutoff)
            {
                toRemove.Add(key);
            }
            else
            {
                // Tombstone is still within the grace window — a future pass
                // must re-scan it once the grace has elapsed.
                anyInGraceRemaining = true;
            }
        }

        if (toRemove.Count > 0)
        {
            foreach (var key in toRemove)
            {
                state.State.Entries.Remove(key);
            }
        }

        // Only mark this version as "fully compacted" when no tombstones were
        // left in the grace window. Stamping while tombstones remain would
        // dead-end every subsequent pass until a new write ticks the version
        // vector (audit bug #2).
        if (!anyInGraceRemaining)
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

    public Task<StateDelta> GetDeltaSinceForSlotsAsync(VersionVector sinceVersion, int[] sortedMovedSlots, int virtualShardCount)
    {
        ArgumentNullException.ThrowIfNull(sinceVersion);
        ArgumentNullException.ThrowIfNull(sortedMovedSlots);

        if (sortedMovedSlots.Length == 0 || sinceVersion.DominatesOrEquals(state.State.Version))
        {
            return Task.FromResult(new StateDelta
            {
                Entries = EmptyEntries,
                Version = state.State.Version.Clone(),
                SplitKey = state.State.SplitKey
            });
        }

        var callerClock = sinceVersion.GetClock(ReplicaId);
        var changed = new Dictionary<string, LwwValue<byte[]>>();

        foreach (var (key, lww) in state.State.Entries)
        {
            if (lww.Timestamp <= callerClock) continue;
            var slot = ShardMap.GetVirtualSlot(key, virtualShardCount);
            if (Array.BinarySearch(sortedMovedSlots, slot) < 0) continue;
            changed[key] = lww;
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
        var maxIncoming = HybridLogicalClock.Zero;
        foreach (var (key, incoming) in entries)
        {
            if (incoming.Timestamp > maxIncoming)
                maxIncoming = incoming.Timestamp;

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
            // Advance the local HLC past the highest incoming timestamp so a
            // subsequent local write produces a stamp that dominates the just-
            // merged values (audit bug #3). Without this, a merged future-dated
            // entry silently wins LWW against every local write until wall clock
            // catches up.
            if (maxIncoming > state.State.Clock)
                state.State.Clock = maxIncoming;
            state.State.Version.Tick(ReplicaId);
        }

        await state.WriteStateAsync();
    }

    public Task<List<string>> GetKeysAsync(string? startInclusive = null, string? endExclusive = null, string? afterExclusive = null, string? beforeExclusive = null)
    {
        var splitInProgress = state.State.SplitState == Primitives.SplitState.SplitInProgress;
        var splitKey = state.State.SplitKey;

        var keys = new List<string>();
        foreach (var (key, lww) in state.State.Entries)
        {
            if (endExclusive is not null && string.Compare(key, endExclusive, StringComparison.Ordinal) >= 0)
                break;

            if (beforeExclusive is not null && string.Compare(key, beforeExclusive, StringComparison.Ordinal) >= 0)
                break;

            if (splitInProgress && splitKey is not null &&
                string.Compare(key, splitKey, StringComparison.Ordinal) >= 0)
                break;

            if (lww.IsTombstone)
                continue;

            if (startInclusive is not null && string.Compare(key, startInclusive, StringComparison.Ordinal) < 0)
                continue;

            if (afterExclusive is not null && string.Compare(key, afterExclusive, StringComparison.Ordinal) <= 0)
                continue;

            keys.Add(key);
        }

        return Task.FromResult(keys);
    }

    public Task<List<KeyValuePair<string, byte[]>>> GetEntriesAsync(string? startInclusive = null, string? endExclusive = null, string? afterExclusive = null, string? beforeExclusive = null)
    {
        var splitInProgress = state.State.SplitState == Primitives.SplitState.SplitInProgress;
        var splitKey = state.State.SplitKey;

        var entries = new List<KeyValuePair<string, byte[]>>();
        foreach (var (key, lww) in state.State.Entries)
        {
            if (endExclusive is not null && string.Compare(key, endExclusive, StringComparison.Ordinal) >= 0)
                break;

            if (beforeExclusive is not null && string.Compare(key, beforeExclusive, StringComparison.Ordinal) >= 0)
                break;

            if (splitInProgress && splitKey is not null &&
                string.Compare(key, splitKey, StringComparison.Ordinal) >= 0)
                break;

            if (lww.IsTombstone)
                continue;

            if (startInclusive is not null && string.Compare(key, startInclusive, StringComparison.Ordinal) < 0)
                continue;

            if (afterExclusive is not null && string.Compare(key, afterExclusive, StringComparison.Ordinal) <= 0)
                continue;

            entries.Add(new KeyValuePair<string, byte[]>(key, lww.Value!));
        }

        return Task.FromResult(entries);
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

    /// <summary>
    /// Returns all key-value entries in this leaf including tombstones,
    /// preserving the original <see cref="LwwValue{T}"/> timestamps.
    /// Internal method for unit testing — not exposed on the grain interface
    /// to avoid Orleans generic type serialization issues.
    /// </summary>
    internal Task<Dictionary<string, LwwValue<byte[]>>> GetAllRawEntriesAsync()
    {
        return Task.FromResult(
            new Dictionary<string, LwwValue<byte[]>>(state.State.Entries));
    }

    public async Task<SplitResult?> MergeManyAsync(Dictionary<string, LwwValue<byte[]>> entries)
    {
        // Recovery: if a previous split was interrupted, complete it first.
        if (state.State.SplitState == Primitives.SplitState.SplitInProgress)
        {
            var recovered = await CompleteSplitAsync();
            await state.WriteStateAsync();

            // Re-merge entries that belong to the new sibling.
            var siblingEntries = new Dictionary<string, LwwValue<byte[]>>();
            var localEntries = new Dictionary<string, LwwValue<byte[]>>();
            foreach (var (key, lww) in entries)
            {
                if (string.Compare(key, state.State.SplitKey!, StringComparison.Ordinal) >= 0)
                    siblingEntries[key] = lww;
                else
                    localEntries[key] = lww;
            }

            if (siblingEntries.Count > 0)
            {
                var sibling = grainFactory.GetGrain<IBPlusLeafGrain>(state.State.SplitSiblingId!.Value);
                await sibling.MergeManyAsync(siblingEntries);
            }

            // Merge remaining local entries.
            if (localEntries.Count > 0)
            {
                MergeIntoState(localEntries);
                await state.WriteStateAsync();
            }

            return recovered;
        }

        if (entries.Count == 0)
        {
            return null;
        }

        MergeIntoState(entries);

        SplitResult? splitResult = null;
        if (state.State.Entries.Count > Options.MaxLeafKeys)
        {
            splitResult = await SplitAsync();
        }

        await state.WriteStateAsync();
        return splitResult;
    }

    private void MergeIntoState(Dictionary<string, LwwValue<byte[]>> entries)
    {
        var maxIncoming = HybridLogicalClock.Zero;
        foreach (var (key, incoming) in entries)
        {
            if (incoming.Timestamp > maxIncoming)
                maxIncoming = incoming.Timestamp;

            if (state.State.Entries.TryGetValue(key, out var existing))
            {
                state.State.Entries[key] = LwwValue<byte[]>.Merge(existing, incoming);
            }
            else
            {
                state.State.Entries[key] = incoming;
            }
        }

        if (entries.Count > 0)
        {
            // Advance the local HLC past the highest incoming timestamp so
            // subsequent local writes dominate the merged values (audit bug #3).
            if (maxIncoming > state.State.Clock)
                state.State.Clock = maxIncoming;
            state.State.Version.Tick(ReplicaId);
        }
    }

    public async Task ClearGrainStateAsync()
    {
        await state.ClearStateAsync();
        context.Deactivate(new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "Tree purged"));
    }
}
