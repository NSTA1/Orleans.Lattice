using System.Collections.Concurrent;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

/// <summary>
/// Process-scope in-memory <see cref="IGrainStorage"/> used by the
/// public-API contract suite. Mirrors the
/// <see cref="InMemoryWalStorageProvider"/> pattern: state is held in
/// a static <see cref="ConcurrentDictionary{TKey, TValue}"/> so the
/// store survives <see cref="PublicApiContractClusterFixture.RestartClusterAsync"/>
/// even though the silo's own DI container is torn down. This is the
/// fixture-side counterpart to the WAL provider - together they let
/// the WAL-reactivation tests prove that the activation-time materialiser
/// rebuilds leaves from the WAL when grain-state would otherwise be
/// wiped by a process-internal cluster restart.
/// <para>
/// Per-silo memory grain storage (the Orleans-shipped
/// <c>AddMemoryGrainStorage</c>) is silo-local and dies on
/// <c>StopAllSilosAsync</c>; ShardRootGrain topology
/// (RootNodeId, RootIsLeaf, internal-node ids) is therefore lost
/// across restart, breaking the recovery contract the WAL is meant
/// to satisfy. This provider closes the gap for tests.
/// </para>
/// </summary>
internal sealed class ProcessScopeMemoryGrainStorage : IGrainStorage
{
    private static readonly ConcurrentDictionary<string, (string ETag, object State)> Store = new();

    /// <summary>
    /// Drops every persisted entry. Call from a fixture teardown when
    /// you want the next deployment to start from a clean slate;
    /// <see cref="PublicApiContractClusterFixture.RestartClusterAsync"/>
    /// deliberately does <i>not</i> call this - surviving the restart
    /// is the whole point.
    /// </summary>
    public static void Reset() => Store.Clear();

    /// <summary>
    /// Test-only corruption hook: flips the persisted <c>RootIsLeaf</c> flag to
    /// <see langword="true"/> on every stored <see cref="ShardRootState"/> whose
    /// <c>RootNodeId</c> equals <paramref name="internalRootNodeId"/>, leaving the
    /// root pointer addressing an internal node. This reproduces the exact
    /// baked-inconsistent topology observed live for issue 899's write-path crash
    /// - a shard root that persisted <c>RootIsLeaf = true</c> over an internal
    /// root - which a partial/raced promotion can leave on disk and which then
    /// crash-loops every mutation that blind-casts the root to a leaf grain.
    /// Mutates the stored POCO in place (the store holds it by reference), so a
    /// subsequent <see cref="PublicApiContractClusterFixture.RestartClusterAsync"/>
    /// rehydrates the corrupt flag cold from this provider. Returns the number of
    /// shard-root records corrupted.
    /// </summary>
    public static int ForceRootIsLeafOverInternalRoot(GrainId internalRootNodeId)
    {
        var corrupted = 0;
        foreach (var entry in Store.Values)
        {
            if (entry.State is ShardRootState shardRoot &&
                shardRoot.RootNodeId == internalRootNodeId &&
                !shardRoot.RootIsLeaf)
            {
                shardRoot.RootIsLeaf = true;
                corrupted++;
            }
        }
        return corrupted;
    }

    /// <inheritdoc />
    public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        ArgumentNullException.ThrowIfNull(stateName);
        ArgumentNullException.ThrowIfNull(grainState);

        var key = MakeKey(stateName, grainId);
        if (Store.TryGetValue(key, out var entry))
        {
            grainState.State = (T)entry.State;
            grainState.ETag = entry.ETag;
            grainState.RecordExists = true;
        }
        else
        {
            grainState.RecordExists = false;
        }
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        ArgumentNullException.ThrowIfNull(stateName);
        ArgumentNullException.ThrowIfNull(grainState);

        var key = MakeKey(stateName, grainId);
        var newEtag = Guid.NewGuid().ToString("N");
        Store[key] = (newEtag, grainState.State!);
        grainState.ETag = newEtag;
        grainState.RecordExists = true;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        ArgumentNullException.ThrowIfNull(stateName);
        ArgumentNullException.ThrowIfNull(grainState);

        var key = MakeKey(stateName, grainId);
        Store.TryRemove(key, out _);
        grainState.ETag = null!;
        grainState.RecordExists = false;
        return Task.CompletedTask;
    }

    private static string MakeKey(string stateName, GrainId grainId) =>
        $"{stateName}/{grainId}";
}
