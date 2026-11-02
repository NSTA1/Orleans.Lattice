using System.Collections.Concurrent;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The exact-identity dedupe scope of a bootstrap drain (issue #5689). A drain
/// writes into one physical copy, and the import publishes that copy by
/// replacing the tree, not by merging into it. An identity a drain applied is
/// therefore only "already applied" for the copy it landed in. When a copy is
/// aborted and the import is re-driven into a new copy, the rows the aborted
/// copy held must apply again, and be judged again, in the new one.
/// </summary>
/// <remarks>
/// <para>
/// The shared per-tree cache (<see cref="_dedupeCaches"/>) outlived an aborted
/// copy. A re-driven drain's rows hit it as completed duplicates and were
/// acknowledged without being applied, so the published copy silently lacked
/// them, and under strict ingest they were never judged either.
/// </para>
/// <para>
/// A drain apply therefore dedupes first against a cache scoped to the copy it
/// writes into: the bootstrap shadow route's physical tree, or the tree itself
/// for a legacy in-place import, which no replacement discards. A copy's id is
/// <c>{tree}/resized/{operationId}</c>, and an aborted copy fails its bootstrap,
/// so the next import starts with a fresh operation id and a new copy id. A
/// transient retry within one attempt reuses the same copy and still dedupes, so
/// a refused row of that copy is dead-lettered once. Only the latest copy's
/// cache is kept per tree.
/// </para>
/// <para>
/// The live cache records stream deliveries without the physical copy they
/// landed in, so for a drain a hit there is only a candidate: the drain skips
/// the row only when its copy holds the key at the row's version or newer (the
/// live stream delivered it into this held copy). Otherwise it applies the row,
/// which is idempotent. A drain-applied identity is also recorded in the live
/// cache, so a stream re-delivery after the import is deduped as before. Live
/// applies are unchanged.
/// </para>
/// </remarks>
internal sealed partial class ReplicationApplier
{
    private readonly ConcurrentDictionary<string, DrainDedupeScope> _drainDedupeScopes =
        new(StringComparer.Ordinal);

    private sealed record DrainDedupeScope(string CopyId, RecentApplyCache Cache);

    /// <summary>
    /// The dedupe cache for a bootstrap drain apply to <paramref name="treeId"/>,
    /// scoped to the copy the drain writes into.
    /// </summary>
    private RecentApplyCache DrainDedupeCache(string treeId, int capacity)
    {
        var copyId = LatticeBootstrapShadowRouteContext.TryGetPhysicalTreeId(treeId, out var shadow)
            ? shadow
            : treeId;
        var scope = _drainDedupeScopes.AddOrUpdate(
            treeId,
            static (_, state) => new DrainDedupeScope(state.CopyId, new RecentApplyCache(state.Capacity)),
            static (_, existing, state) => string.Equals(existing.CopyId, state.CopyId, StringComparison.Ordinal)
                ? existing
                : new DrainDedupeScope(state.CopyId, new RecentApplyCache(state.Capacity)),
            (CopyId: copyId, Capacity: capacity));
        return scope.Cache;
    }

    /// <summary>
    /// Whether a drain row the live cache already recorded is held by the copy
    /// the drain writes into, so applying it again would only repeat it. The
    /// live cache cannot say which physical copy an identity landed in, so a
    /// hit there is a candidate the copy must confirm: the copy holds a live
    /// value for the key at the row's version or a newer one. A prepared row, a
    /// tombstone, or a row the copy does not confirm is applied, which is
    /// idempotent.
    /// </summary>
    private async Task<bool> IsDrainRowAlreadyInCopyAsync(WalRecord entry)
    {
        if (entry.IsPrepared
            || entry.Op != MutationKind.Set
            || entry.Value is null
            || !_dedupeCaches.TryGetValue(entry.TreeId, out var live)
            || !live.Contains(entry))
        {
            return false;
        }

        var stored = await grainFactory.GetGrain<IReplicationApplyGrain>(entry.TreeId)
            .ReadStoredForApplyAsync(entry.Key);
        return stored.Value is not null && stored.Version.CompareTo(entry.Timestamp) >= 0;
    }

    /// <summary>
    /// Records a drain-applied identity in the live cache too, so a stream
    /// re-delivery of it after the import is deduped as before. A drain only
    /// trusts a live-cache hit that its own copy confirms, so this cannot
    /// suppress a drain row the copy lacks.
    /// </summary>
    private void RecordDrainAppliedInLiveCache(WalRecord entry, int capacity)
    {
        var live = _dedupeCaches.GetOrAdd(
            entry.TreeId,
            static (_, size) => new RecentApplyCache(size),
            capacity);
        if (live.TryAdd(entry, out _))
        {
            live.Complete(entry);
        }
    }
}
