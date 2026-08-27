using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// The grain surface the catalog-enumeration workload pages against: a
/// synthetic tree registry plus a synthetic per-tree deletion grain, both of
/// which count and pay a modelled dispatch hop on every call.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a modelled hop.</b> Every read here yields (<see cref="Task.Yield"/>)
/// before answering, so the call completes asynchronously through the thread
/// pool rather than synchronously off a <c>Task.FromResult</c>. A
/// synchronously-completing fake would make <c>await</c> free, which would
/// erase precisely the cost this workload exists to measure: an Orleans grain
/// call is always a real dispatch, so a projection that awaits P of them in
/// sequence pays P scheduling latencies that a projection issuing one call plus
/// a concurrent fan-out does not. The scheduler hop is the cheapest faithful
/// stand-in for that dispatch and is therefore a <em>lower bound</em>: on a real
/// silo the per-hop cost is larger and the measured gap widens.
/// </para>
/// <para>
/// <b>The exact figure.</b> Latency under a modelled hop is indicative;
/// <see cref="RoundTrips"/> is exact. Round-trip count is deterministic, host
/// independent, and is the number the batching change actually targets, so the
/// suite reports it alongside the timings rather than inferring it from them.
/// </para>
/// </remarks>
internal sealed class CatalogGrainSurface
{
    private readonly Dictionary<string, TreeRegistryEntry> _entries;
    private readonly List<string> _orderedIds;
    private readonly ILatticeRegistry _registry;

    /// <summary>Grain calls made since the last <see cref="ResetCounters"/>.</summary>
    public int RoundTrips;

    /// <summary>Batched registry multi-gets made since the last <see cref="ResetCounters"/>.</summary>
    public int BatchedRegistryReads;

    /// <summary>Single-entry registry reads made since the last <see cref="ResetCounters"/>.</summary>
    public int SingleRegistryReads;

    /// <summary>
    /// Registry-internal single-key reads of the backing tree, made on the
    /// caller's behalf inside a batched read, since the last
    /// <see cref="ResetCounters"/>. Deliberately excluded from
    /// <see cref="RoundTrips"/>: these never cross the catalog caller's facade,
    /// and they are issued as one concurrent wave rather than in sequence.
    /// </summary>
    public int RegistryFanOutReads;

    /// <summary>Deletion probes made since the last <see cref="ResetCounters"/>.</summary>
    public int DeletionProbes;

    /// <summary>Registry enumerations made since the last <see cref="ResetCounters"/>.</summary>
    public int Enumerations;

    /// <summary>Seeds the surface with a registered entry per tree id.</summary>
    public CatalogGrainSurface(IReadOnlyList<string> treeIds)
    {
        _orderedIds = [.. treeIds];
        _orderedIds.Sort(StringComparer.Ordinal);
        _entries = new Dictionary<string, TreeRegistryEntry>(_orderedIds.Count, StringComparer.Ordinal);
        foreach (var id in _orderedIds)
        {
            _entries[id] = new TreeRegistryEntry { ShardCount = 4 };
        }

        _registry = new CountingRegistry(this);
    }

    /// <summary>The registry grain stand-in bound to this surface.</summary>
    public ILatticeRegistry Registry => _registry;

    /// <summary>Every seeded tree id, in ordinal order (the registry's own order).</summary>
    public IReadOnlyList<string> TreeIds => _orderedIds;

    /// <summary>Zeroes every counter ahead of a measured pass.</summary>
    public void ResetCounters()
    {
        RoundTrips = 0;
        BatchedRegistryReads = 0;
        SingleRegistryReads = 0;
        RegistryFanOutReads = 0;
        DeletionProbes = 0;
        Enumerations = 0;
    }

    /// <summary>Builds the deletion grain stand-in for <paramref name="treeId"/>.</summary>
    public ITreeDeletionGrain Deletion(string treeId) => new CountingDeletion(this, treeId);

    /// <summary>The single-entry registry read: one modelled hop per id.</summary>
    public async Task<TreeRegistryEntry?> ReadEntryAsync(string treeId)
    {
        Interlocked.Increment(ref RoundTrips);
        Interlocked.Increment(ref SingleRegistryReads);
        await Task.Yield();
        return _entries.GetValueOrDefault(treeId);
    }

    /// <summary>
    /// The batched registry read, modelled as the two layers the shipped
    /// implementation actually has: one caller-visible hop into the registry
    /// grain, then - inside that grain - one concurrent wave of single-key reads
    /// against the backing registry tree, exactly as
    /// <c>LatticeRegistryGrain.GetEntriesAsync</c> issues them. Modelling only
    /// the outer hop would flatter the change; the win is that the wave is
    /// concurrent and the caller crosses the facade once, not that the inner
    /// reads disappear.
    /// </summary>
    public async Task<Dictionary<string, TreeRegistryEntry>> ReadEntriesAsync(IReadOnlyList<string> treeIds)
    {
        if (treeIds.Count == 0)
        {
            return new Dictionary<string, TreeRegistryEntry>(0, StringComparer.Ordinal);
        }

        Interlocked.Increment(ref RoundTrips);
        Interlocked.Increment(ref BatchedRegistryReads);
        await Task.Yield();

        // Registry-internal fan-out: N concurrent single-key reads, one wave.
        var reads = new Task<TreeRegistryEntry?>[treeIds.Count];
        for (var i = 0; i < treeIds.Count; i++)
        {
            reads[i] = ReadRegistryTreeAsync(treeIds[i]);
        }

        await Task.WhenAll(reads);

        var found = new Dictionary<string, TreeRegistryEntry>(treeIds.Count, StringComparer.Ordinal);
        for (var i = 0; i < reads.Length; i++)
        {
            if (await reads[i] is { } entry)
            {
                found[treeIds[i]] = entry;
            }
        }

        return found;
    }

    /// <summary>
    /// One registry-internal single-key read of the backing tree. Counted apart
    /// from <see cref="RoundTrips"/> because it never crosses the catalog
    /// caller's facade: it is a hop the registry grain pays on its behalf.
    /// </summary>
    private async Task<TreeRegistryEntry?> ReadRegistryTreeAsync(string treeId)
    {
        Interlocked.Increment(ref RegistryFanOutReads);
        await Task.Yield();
        return _entries.GetValueOrDefault(treeId);
    }

    /// <summary>The per-tree deletion probe: one modelled hop per id.</summary>
    public async Task<bool> ProbeDeletedAsync()
    {
        Interlocked.Increment(ref RoundTrips);
        Interlocked.Increment(ref DeletionProbes);
        await Task.Yield();
        return false;
    }

    private async Task<IReadOnlyList<string>> EnumerateAsync(string? prefix)
    {
        Interlocked.Increment(ref RoundTrips);
        Interlocked.Increment(ref Enumerations);
        await Task.Yield();

        if (string.IsNullOrEmpty(prefix))
        {
            return _orderedIds;
        }

        // Mirrors the registry's contiguous prefix range scan (issue #1684): only
        // the ids inside the range are transferred, so a tenant-scoped catalog
        // never pays to ship the other tenants' ids.
        var scoped = new List<string>();
        foreach (var id in _orderedIds)
        {
            if (id.StartsWith(prefix, StringComparison.Ordinal))
            {
                scoped.Add(id);
            }
        }

        return scoped;
    }

    /// <summary>
    /// Registry stand-in. Only the catalog read surface is driven; every other
    /// member throws so an unexpected reach shows up loudly during a bench run.
    /// </summary>
    private sealed class CountingRegistry(CatalogGrainSurface surface) : ILatticeRegistry
    {
        public Task<IReadOnlyList<string>> GetAllTreeIdsAsync() => surface.EnumerateAsync(null);

        public Task<IReadOnlyList<string>> GetAllTreeIdsAsync(string? prefix) => surface.EnumerateAsync(prefix);

        public Task<TreeRegistryEntry?> GetEntryAsync(string treeId) => surface.ReadEntryAsync(treeId);

        public Task<Dictionary<string, TreeRegistryEntry>> GetEntriesAsync(IReadOnlyList<string> treeIds) =>
            surface.ReadEntriesAsync(treeIds);

        public Task<bool> ExistsAsync(string treeId) => throw NotDriven();

        public Task<string> ResolveAsync(string treeId) => throw NotDriven();

        public Task<ShardMap?> GetShardMapAsync(string treeId) => throw NotDriven();

        public Task RegisterAsync(string treeId, TreeRegistryEntry? entry = null) => throw NotDriven();

        public Task UpdateAsync(string treeId, TreeRegistryEntry entry) => throw NotDriven();

        public Task UnregisterAsync(string treeId) => throw NotDriven();

        public Task SetAliasAsync(string treeId, string physicalTreeId) => throw NotDriven();

        public Task RemoveAliasAsync(string treeId) => throw NotDriven();

        public Task SetShardMapAsync(string treeId, ShardMap map) => throw NotDriven();

        public Task SetPublishEventsAsync(string treeId, bool? enabled) => throw NotDriven();

        public Task SetHistoryRetentionAsync(string treeId, HistoryRetentionMode? mode, TimeSpan? window) =>
            throw NotDriven();

        public Task SetMaintainProjectionDigestAsync(string treeId, bool? enabled) => throw NotDriven();

        public Task SetMaxCacheValueBytesAsync(string treeId, long? maxCacheValueBytes) => throw NotDriven();

        public Task LatchProjectionDigestPermanentlyDisabledAsync(string treeId) => throw NotDriven();

        public Task<int> AllocateNextShardIndexAsync(string treeId, int currentMaxFromMap) => throw NotDriven();

        public Task<WalPlacementPin> GetWalPlacementAsync(string treeId) => throw NotDriven();

        public Task<WalPlacementPin> UpdateWalPlacementAsync(
            string treeId, long expectedVersion, int partition, string providerKey) => throw NotDriven();

        public Task<WalPlacementPin> UpdateWalPlacementAsync(
            string treeId, long expectedVersion, IReadOnlyCollection<(int Partition, string ProviderKey)> moves) =>
            throw NotDriven();

        private static NotSupportedException NotDriven() =>
            new("The catalog-enumeration workload drives only the registry read surface " +
                "(enumerate, GetEntryAsync, GetEntriesAsync). Implement the member here if a new " +
                "catalog benchmark needs it.");
    }

    /// <summary>Deletion-grain stand-in for one tree id.</summary>
    private sealed class CountingDeletion(CatalogGrainSurface surface, string treeId) : ITreeDeletionGrain
    {
        /// <summary>The tree this stand-in answers for; kept so a route mismatch is debuggable.</summary>
        public string TreeId => treeId;

        public Task<bool> IsDeletedAsync() => surface.ProbeDeletedAsync();

        public Task DeleteTreeAsync() => throw new NotSupportedException();

        public Task<TreeDeletionSnapshot> GetDeletionStatusAsync() => throw new NotSupportedException();

        public Task RecoverAsync() => throw new NotSupportedException();

        public Task PurgeNowAsync() => throw new NotSupportedException();
    }
}
