using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Serialization;
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
/// <para>
/// It behaves like a durable provider in the two ways that matter to single-activation
/// evidence (issue #4196). A write or clear carrying an ETag other than the stored
/// record's throws <see cref="InconsistentStateException"/>, so a second activation of
/// one grain id fails its write instead of silently overwriting the first. And state
/// is deep-copied on write and on read, so no activation shares an object graph with
/// the store or with another activation.
/// </para>
/// </summary>
internal sealed class ProcessScopeMemoryGrainStorage : IGrainStorage
{
    private static readonly ConcurrentDictionary<string, (string ETag, object State)> Store = new();
    private static readonly object WriteGate = new();

    // A standalone copier, so state crosses into and out of the store as a copy - as
    // it would through a serialising provider - without depending on the silo.
    private static readonly Lazy<DeepCopier> Copier = new(static () =>
        new ServiceCollection().AddSerializer().BuildServiceProvider().GetRequiredService<DeepCopier>());

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
    /// Mutates the store's own copy of the state and gives the record a new ETag, as
    /// an out-of-band edit to a durable store would, so a subsequent
    /// <see cref="PublicApiContractClusterFixture.RestartClusterAsync"/>
    /// rehydrates the corrupt flag cold from this provider. Returns the number of
    /// shard-root records corrupted.
    /// </summary>
    public static int ForceRootIsLeafOverInternalRoot(GrainId internalRootNodeId)
    {
        var corrupted = 0;
        lock (WriteGate)
        {
            foreach (var (key, entry) in Store)
            {
                if (entry.State is ShardRootState shardRoot &&
                    shardRoot.RootNodeId == internalRootNodeId &&
                    !shardRoot.RootIsLeaf)
                {
                    shardRoot.RootIsLeaf = true;
                    Store[key] = (NewETag(), shardRoot);
                    corrupted++;
                }
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
            grainState.State = Copier.Value.Copy((T)entry.State);
            grainState.ETag = entry.ETag;
            grainState.RecordExists = true;
        }
        else
        {
            grainState.ETag = null!;
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
        var copy = Copier.Value.Copy(grainState.State!);
        var newEtag = NewETag();
        lock (WriteGate)
        {
            ThrowIfStale(key, grainState.ETag, "write");
            Store[key] = (newEtag, copy!);
        }
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
        lock (WriteGate)
        {
            ThrowIfStale(key, grainState.ETag, "clear");
            Store.TryRemove(key, out _);
        }
        grainState.ETag = null!;
        grainState.RecordExists = false;
        return Task.CompletedTask;
    }

    private static void ThrowIfStale(string key, string? presentedETag, string operation)
    {
        var storedETag = Store.TryGetValue(key, out var entry) ? entry.ETag : null;
        if (!string.Equals(storedETag, presentedETag, StringComparison.Ordinal))
        {
            throw new InconsistentStateException(
                $"ETag mismatch on {operation} of '{key}': the store holds '{storedETag ?? "<none>"}' but the "
                + $"caller presented '{presentedETag ?? "<none>"}'.",
                storedETag ?? string.Empty,
                presentedETag ?? string.Empty);
        }
    }

    private static string NewETag() => Guid.NewGuid().ToString("N");

    private static string MakeKey(string stateName, GrainId grainId) =>
        $"{stateName}/{grainId}";
}
