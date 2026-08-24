using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Allocation-free <see cref="ILatticeRegistry"/> for the microbench harness. It
/// replaces the previous <c>Substitute.For&lt;ILatticeRegistry&gt;()</c> so the
/// measured allocation profile reflects product code only.
/// <para>
/// <see cref="LatticeOptionsResolver.ResolveAsync"/> calls
/// <see cref="GetEntryAsync"/> on every user-tree resolve - i.e. once per write
/// on the saga hot path - so the mocked registry was a top allocator: each call
/// paid NSubstitute's dynamic-proxy interception plus, because the harness
/// configured the return as a <c>_ =&gt; Task.FromResult(new TreeRegistryEntry{...})</c>
/// lambda, a fresh <see cref="TreeRegistryEntry"/> and <see cref="Task"/>
/// allocation every single call. This fake returns a cached, pre-built
/// completed task per tree id, allocating nothing on the resolve path.
/// </para>
/// </summary>
/// <remarks>
/// Structural pins are seeded via <see cref="SetDefaultEntry"/> (the shape every
/// tree resolves to) and <see cref="SetEntry"/> (per-tree overrides the
/// multi-tree benchmarks bind, mirroring how the prior mock layered a
/// specific-argument <c>.Returns(...)</c> over an <c>Arg.Any&lt;string&gt;()</c>
/// default). The auxiliary members the harness previously left auto-mocked
/// (<see cref="ExistsAsync"/>, <see cref="ResolveAsync"/>,
/// <see cref="GetShardMapAsync"/>, and the fire-and-forget mutators) return the
/// same defaults the substitute did. The WAL-placement and adaptive-shard
/// members throw: the microbench constructs its resolver with a null WAL
/// provider catalog and pins <c>ShardCount = 1</c>, so those paths are never
/// exercised - a throw surfaces loudly if a new benchmark ever reaches them.
/// </remarks>
internal sealed class FakeLatticeRegistry : ILatticeRegistry
{
    private readonly Dictionary<string, Task<TreeRegistryEntry?>> _entries = new(StringComparer.Ordinal);
    private Task<TreeRegistryEntry?> _defaultEntry = Task.FromResult<TreeRegistryEntry?>(null);

    private static readonly Task<bool> TrueTask = Task.FromResult(true);
    private static readonly Task<ShardMap?> NullShardMap = Task.FromResult<ShardMap?>(null);
    private static readonly Task<IReadOnlyList<string>> EmptyTreeIds =
        Task.FromResult<IReadOnlyList<string>>(Array.Empty<string>());

    /// <summary>Sets the structural pin every unrouted tree id resolves to.</summary>
    public void SetDefaultEntry(TreeRegistryEntry entry) =>
        _defaultEntry = Task.FromResult<TreeRegistryEntry?>(entry);

    /// <summary>Binds a specific tree id to its own structural pin, overriding the default.</summary>
    public void SetEntry(string treeId, TreeRegistryEntry entry) =>
        _entries[treeId] = Task.FromResult<TreeRegistryEntry?>(entry);

    public Task<TreeRegistryEntry?> GetEntryAsync(string treeId) =>
        _entries.TryGetValue(treeId, out var entry) ? entry : _defaultEntry;

    public Task<bool> ExistsAsync(string treeId) => TrueTask;

    public Task<string> ResolveAsync(string treeId) => Task.FromResult(treeId);

    public Task<ShardMap?> GetShardMapAsync(string treeId) => NullShardMap;

    public Task<IReadOnlyList<string>> GetAllTreeIdsAsync() => EmptyTreeIds;

    public Task RegisterAsync(string treeId, TreeRegistryEntry? entry = null) => Task.CompletedTask;

    public Task UpdateAsync(string treeId, TreeRegistryEntry entry) => Task.CompletedTask;

    public Task UnregisterAsync(string treeId) => Task.CompletedTask;

    public Task SetAliasAsync(string treeId, string physicalTreeId) => Task.CompletedTask;

    public Task RemoveAliasAsync(string treeId) => Task.CompletedTask;

    public Task SetShardMapAsync(string treeId, ShardMap map) => Task.CompletedTask;

    public Task SetPublishEventsAsync(string treeId, bool? enabled) => Task.CompletedTask;

    public Task SetHistoryRetentionAsync(string treeId, HistoryRetentionMode? mode, TimeSpan? window) =>
        Task.CompletedTask;

    public Task SetMaintainProjectionDigestAsync(string treeId, bool? enabled) => Task.CompletedTask;

    public Task LatchProjectionDigestPermanentlyDisabledAsync(string treeId) => Task.CompletedTask;

    // ----- Not exercised: null WAL catalog + pinned ShardCount = 1 in the bench. -----

    public Task<int> AllocateNextShardIndexAsync(string treeId, int currentMaxFromMap) => throw NotUsed();

    public Task<WalPlacementPin> GetWalPlacementAsync(string treeId) => throw NotUsed();

    public Task<WalPlacementPin> UpdateWalPlacementAsync(
        string treeId, long expectedVersion, int partition, string providerKey) => throw NotUsed();

    public Task<WalPlacementPin> UpdateWalPlacementAsync(
        string treeId, long expectedVersion, IReadOnlyCollection<(int Partition, string ProviderKey)> moves) =>
        throw NotUsed();

    private static NotSupportedException NotUsed() =>
        new("FakeLatticeRegistry only implements the registry surface the microbench uses; " +
            "the WAL-placement and adaptive-shard members are never reached (null WAL catalog, ShardCount=1). " +
            "Implement it here if a new benchmark path needs it.");
}
