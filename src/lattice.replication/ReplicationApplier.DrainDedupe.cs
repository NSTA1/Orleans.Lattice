using System.Collections.Concurrent;

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
/// A drain apply therefore never consults or records into the live cache. It
/// uses a cache scoped to the copy it writes into: the bootstrap shadow route's
/// physical tree, or the tree itself for a legacy in-place import, which no
/// replacement discards. A copy's id is <c>{tree}/resized/{operationId}</c>, and
/// an aborted copy fails its bootstrap, so the next import starts with a fresh
/// operation id and a new copy id. A transient retry within one attempt reuses
/// the same copy and still dedupes, so a refused row of that copy is
/// dead-lettered once. Only the latest copy's cache is kept per tree, so the
/// scope adds one bounded cache per tree. Live-ship applies keep their
/// unscoped identity key.
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
}
