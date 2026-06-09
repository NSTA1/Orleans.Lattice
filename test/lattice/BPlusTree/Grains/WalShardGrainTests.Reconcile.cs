using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Activation-time reconciliation coverage for the
/// <see cref="IWalStorageProvider.ReconcileAsync"/> hook the WAL grain
/// invokes between resolving its provider and reading the tail offset,
/// plus the matching provider-resolution rule
/// (<see cref="WalShardGrain.ResolveStorageProvider"/>) that picks the
/// concrete backend for a given <c>(treeId, partition)</c> activation
/// key.
/// </summary>
public partial class WalShardGrainTests
{
    [Test]
    public async Task OnActivateAsync_invokes_ReconcileAsync_once_with_grain_coordinates()
    {
        var provider = new ReconcileRecordingWalStorageProvider();

        await CreateGrainAsync(provider);

        Assert.Multiple(() =>
        {
            Assert.That(provider.ReconcileCalls, Has.Count.EqualTo(1));
            Assert.That(provider.ReconcileCalls[0].TreeId, Is.EqualTo(TreeId));
            Assert.That(provider.ReconcileCalls[0].ShardIndex, Is.EqualTo(ShardIndex));
        });
    }

    [Test]
    public async Task OnActivateAsync_invokes_ReconcileAsync_before_GetHighestOffsetAsync()
    {
        // The grain's tail-recovery is read-after-reconcile so a
        // multi-phase backend gets a chance to roll missing manifest
        // rows forward before the grain commits to a _nextOffset
        // value. The order of the two recorded calls is the contract.
        var provider = new ReconcileRecordingWalStorageProvider();

        await CreateGrainAsync(provider);

        Assert.That(
            provider.OperationLog,
            Is.EqualTo(new[] { "Reconcile", "GetHighestOffset" }));
    }

    [Test]
    public async Task OnActivateAsync_observes_reconciled_tail_when_assigning_first_offset()
    {
        // Reconciliation that surfaces new committed entries (for
        // example by rolling a missed manifest commit forward) must be
        // visible to GetHighestOffsetAsync; the grain must therefore
        // resume numbering after the reconciled tail, not before.
        var inner = new InMemoryWalStorageProvider();
        var provider = new ReconcilingPrefixWalStorageProvider(inner, prefixCount: 3);

        var grain = await CreateGrainAsync(provider);
        var seq = await grain.AppendAsync(MakeEntry("post-reconcile"), CancellationToken.None);

        Assert.That(seq, Is.EqualTo(3L));
    }

    [Test]
    public void OnActivateAsync_propagates_reconcile_failures_to_activation()
    {
        var provider = new ReconcileThrowingWalStorageProvider("reconcile-boom");

        Assert.That(
            async () => await CreateGrainAsync(provider),
            Throws.InvalidOperationException.With.Message.EqualTo("reconcile-boom"));
    }

    /// <summary>
    /// <see cref="IWalStorageProvider"/> double that records every
    /// <c>ReconcileAsync</c> / <c>GetHighestOffsetAsync</c> invocation
    /// in order so tests can assert both the count and the relative
    /// activation-step ordering.
    /// </summary>
    private sealed class ReconcileRecordingWalStorageProvider : IWalStorageProvider
    {
        public List<(string TreeId, int ShardIndex)> ReconcileCalls { get; } = new();

        public List<string> OperationLog { get; } = new();

        public Task ReconcileAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
        {
            ReconcileCalls.Add((treeId, shardIndex));
            OperationLog.Add("Reconcile");
            return Task.CompletedTask;
        }

        public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => AsyncEnumerable.Empty<WalEntry>();

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
        {
            OperationLog.Add("GetHighestOffset");
            return Task.FromResult(-1L);
        }

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => Task.FromResult(-1L);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    /// <summary>
    /// <see cref="IWalStorageProvider"/> double that pre-loads a fixed
    /// number of committed entries into an inner provider during
    /// <c>ReconcileAsync</c>; models the "manifest roll-forward"
    /// behaviour a multi-phase backend exhibits when it discovers
    /// orphan batch partitions on activation.
    /// </summary>
    private sealed class ReconcilingPrefixWalStorageProvider(InMemoryWalStorageProvider inner, int prefixCount) : IWalStorageProvider
    {
        public async Task ReconcileAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
        {
            var entries = new WalEntry[prefixCount];
            for (var i = 0; i < prefixCount; i++)
            {
                entries[i] = new WalEntry
                {
                    Offset = i,
                    Mutation = WalRecordConverter.FromWalRecord(new WalRecord
                    {
                        TreeId = treeId,
                        Op = MutationKind.Set,
                        Key = $"recovered-{i}",
                        Value = new byte[] { 0x01 },
                        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                        OriginClusterId = "site-a",
                    }),
                };
            }
            await inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken).ConfigureAwait(false);
        }

        public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
            => inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken);

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => inner.ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken);

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => inner.GetLowestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => inner.TrimAsync(treeId, shardIndex, throughOffsetInclusive, cancellationToken);
    }

    /// <summary>
    /// <see cref="IWalStorageProvider"/> double that throws on
    /// <c>ReconcileAsync</c>; lets the activation contract test pin
    /// that reconciliation failures surface to the caller rather than
    /// being swallowed.
    /// </summary>
    private sealed class ReconcileThrowingWalStorageProvider(string message) : IWalStorageProvider
    {
        public Task ReconcileAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => throw new InvalidOperationException(message);

        public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken)
            => AsyncEnumerable.Empty<WalEntry>();

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => Task.FromResult(-1L);

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => Task.FromResult(-1L);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    // ---------------------------------------------------------------
    // Provider-resolution rule coverage.
    //
    // ResolveStorageProvider is the static helper OnActivateAsync calls
    // to decide which IWalStorageProvider a fresh activation binds to.
    // Its three-rung precedence (per-partition resolver, per-tree
    // resolver, DI singleton) is the seam that lets a host fan WAL
    // traffic for a single tree across multiple storage backends - the
    // shape that lifts past the single-storage-account throughput
    // ceiling.
    // ---------------------------------------------------------------

    [Test]
    public void ResolveStorageProvider_returns_partition_resolver_result_when_set()
    {
        var partitionProvider = new InMemoryWalStorageProvider();
        var treeProvider = new InMemoryWalStorageProvider();
        var singleton = new InMemoryWalStorageProvider();
        var services = BuildResolverServices(singleton);
        var options = new LatticeOptions
        {
            WalStorageProviderForPartition = (_, _) => partitionProvider,
            WalStorageProvider = _ => treeProvider,
        };

        var resolved = WalShardGrain.ResolveStorageProvider(options, services, "tree", 3);

        Assert.That(resolved, Is.SameAs(partitionProvider));
    }

    [Test]
    public void ResolveStorageProvider_passes_tree_id_and_partition_to_partition_resolver()
    {
        string? capturedTree = null;
        var capturedPartition = -1;
        var captured = new InMemoryWalStorageProvider();
        var services = BuildResolverServices(new InMemoryWalStorageProvider());
        var options = new LatticeOptions
        {
            WalStorageProviderForPartition = (treeId, partition) =>
            {
                capturedTree = treeId;
                capturedPartition = partition;
                return captured;
            },
        };

        _ = WalShardGrain.ResolveStorageProvider(options, services, "tree-x", 7);

        Assert.Multiple(() =>
        {
            Assert.That(capturedTree, Is.EqualTo("tree-x"));
            Assert.That(capturedPartition, Is.EqualTo(7));
        });
    }

    [Test]
    public void ResolveStorageProvider_falls_through_to_tree_resolver_when_partition_resolver_null()
    {
        var treeProvider = new InMemoryWalStorageProvider();
        var singleton = new InMemoryWalStorageProvider();
        var services = BuildResolverServices(singleton);
        var options = new LatticeOptions
        {
            WalStorageProvider = _ => treeProvider,
        };

        var resolved = WalShardGrain.ResolveStorageProvider(options, services, "tree", 0);

        Assert.That(resolved, Is.SameAs(treeProvider));
    }

    [Test]
    public void ResolveStorageProvider_falls_through_to_singleton_when_both_resolvers_null()
    {
        var singleton = new InMemoryWalStorageProvider();
        var services = BuildResolverServices(singleton);
        var options = new LatticeOptions();

        var resolved = WalShardGrain.ResolveStorageProvider(options, services, "tree", 0);

        Assert.That(resolved, Is.SameAs(singleton));
    }

    [Test]
    public void ResolveStorageProvider_routes_distinct_partitions_to_distinct_providers()
    {
        // The motivating use case: a tree whose WAL partitions are
        // distributed across multiple storage accounts. Each partition
        // gets its own provider activation; the resolver delegate is
        // the only seam that translates the (tree, partition) tuple
        // into the right backend.
        var partitionZero = new InMemoryWalStorageProvider();
        var partitionOne = new InMemoryWalStorageProvider();
        var partitionTwo = new InMemoryWalStorageProvider();
        var providers = new[] { partitionZero, partitionOne, partitionTwo };
        var services = BuildResolverServices(new InMemoryWalStorageProvider());
        var options = new LatticeOptions
        {
            WalStorageProviderForPartition = (_, partition) => providers[partition],
        };

        var resolvedZero = WalShardGrain.ResolveStorageProvider(options, services, "tree", 0);
        var resolvedOne = WalShardGrain.ResolveStorageProvider(options, services, "tree", 1);
        var resolvedTwo = WalShardGrain.ResolveStorageProvider(options, services, "tree", 2);

        Assert.Multiple(() =>
        {
            Assert.That(resolvedZero, Is.SameAs(partitionZero));
            Assert.That(resolvedOne, Is.SameAs(partitionOne));
            Assert.That(resolvedTwo, Is.SameAs(partitionTwo));
        });
    }

    [Test]
    public void ResolveStorageProvider_throws_on_null_options()
    {
        var services = BuildResolverServices(new InMemoryWalStorageProvider());

        Assert.That(
            () => WalShardGrain.ResolveStorageProvider(null!, services, "tree", 0),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ResolveStorageProvider_throws_on_null_services()
    {
        Assert.That(
            () => WalShardGrain.ResolveStorageProvider(new LatticeOptions(), null!, "tree", 0),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ResolveStorageProvider_throws_on_null_tree_id()
    {
        var services = BuildResolverServices(new InMemoryWalStorageProvider());

        Assert.That(
            () => WalShardGrain.ResolveStorageProvider(new LatticeOptions(), services, null!, 0),
            Throws.ArgumentNullException);
    }

    private static IServiceProvider BuildResolverServices(IWalStorageProvider singleton)
    {
        var collection = new ServiceCollection();
        collection.AddSingleton(singleton);
        return collection.BuildServiceProvider();
    }
}
