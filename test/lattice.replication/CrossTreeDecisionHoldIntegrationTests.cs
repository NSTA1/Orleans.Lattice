using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4684: the origin must keep a cross-tree sub-saga's decision until
/// every peer of every participant tree has acknowledged past that
/// participant's terminal. A receiver that later imports one participant
/// settles the tree's arrival at its cross-tree barrier from the decision row
/// the export carries; once the row is purged the export carries bare
/// committed rows and a sibling that delegated to the barrier waits for ever.
/// Runs the real registry, the real cross-tree decision hold, its tracker and
/// enrolment grains and the real shippers, with a transport that stands in for
/// a peer acknowledging (or not) per tree.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class CrossTreeDecisionHoldIntegrationTests
{
    private const string LocalClusterId = "xth-site-a";
    private const string PeerClusterId = "xth-site-b";

    private TestCluster _cluster = null!;

    private IServiceProvider SiloServices => ((InProcessSiloHandle)_cluster.Primary).SiloHost.Services;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        PerTreeGatedTransport.Refused.Clear();
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }

        PerTreeGatedTransport.Refused.Clear();
    }

    [Test]
    public async Task A_cross_tree_sub_saga_decision_is_held_until_the_peer_of_its_sibling_tree_acknowledged_past_it()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var treeA = "xth-a-" + suffix;
        var treeB = "xth-b-" + suffix;
        var client = _cluster.Client;
        foreach (var tree in new[] { treeA, treeB })
        {
            await client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(
                tree, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 64, MaxInternalChildren = 4 });
            await client.GetGrain<IReplicationShipperGrain>($"{tree}/{PeerClusterId}").EnsureActiveAsync(CancellationToken.None);
        }

        await client.SetManyAtomicAsync(
            [
                new LatticeTreeBatch(treeA, [new("k", [1])]),
                new LatticeTreeBatch(treeB, [new("k", [2])]),
            ],
            "xth-op-" + suffix);
        var txA = await CrossTreeSubSagaAsync(treeA);
        var registryA = TxRegistryRouting.GetRegistry(client, treeA, txA);
        await AwaitShippedAsync(treeA);
        await AwaitShippedAsync(treeB);

        // Tree B's peer stops acknowledging, and tree B is written again: the
        // peer has not acknowledged everything tree B's log holds. Tree A's
        // own log is trimmed entirely, so on tree A's side nothing but the
        // cross-tree hold can keep its decision.
        PerTreeGatedTransport.Refused[treeB] = true;
        await client.GetGrain<ILattice>(treeB).SetAsync("late", [9]);
        await TrimAllAsync(treeA);

        for (var i = 0; i < 6; i++)
        {
            await AgeAndPruneAsync(registryA);
        }

        Assert.That(await registryA.GetRecordedStatusAsync(txA), Is.EqualTo(TxStatus.Committed),
            "tree A's cross-tree decision must outlive its retention while tree B's peer has not acknowledged past tree B's part");

        // The peer catches up on tree B, and tree B's log is trimmed past it:
        // every peer of every participant has acknowledged, so the decision goes.
        PerTreeGatedTransport.Refused.TryRemove(treeB, out _);
        await client.GetGrain<IReplicationShipperGrain>($"{treeB}/{PeerClusterId}").OnDoorbellAsync(CancellationToken.None);
        await AwaitShippedAsync(treeB);
        await TrimAllAsync(treeB);
        var txB = await CrossTreeSubSagaAsync(treeB);
        var registryB = TxRegistryRouting.GetRegistry(client, treeB, txB);

        await TestPoll.UntilAsync(
            async () =>
            {
                await AgeAndPruneAsync(registryB);
                await AgeAndPruneAsync(registryA);
                return await registryA.GetRecordedStatusAsync(txA) == TxStatus.InFlight;
            },
            "tree A's cross-tree decision to be purged once every peer of every participant acknowledged past it",
            TimeSpan.FromSeconds(60));
    }

    [Test]
    public async Task While_a_silo_predates_the_hold_nothing_is_released_and_no_export_is_served()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var treeA = "xth-pre-a-" + suffix;
        var treeB = "xth-pre-b-" + suffix;
        var client = _cluster.Client;
        foreach (var tree in new[] { treeA, treeB })
        {
            await client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(
                tree, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 64, MaxInternalChildren = 4 });
            await client.GetGrain<IReplicationShipperGrain>($"{tree}/{PeerClusterId}").EnsureActiveAsync(CancellationToken.None);
        }

        await client.SetManyAtomicAsync(
            [
                new LatticeTreeBatch(treeA, [new("k", [1])]),
                new LatticeTreeBatch(treeB, [new("k", [2])]),
            ],
            "xth-pre-op-" + suffix);
        var txA = await CrossTreeSubSagaAsync(treeA);
        var txB = await CrossTreeSubSagaAsync(treeB);
        var registryA = TxRegistryRouting.GetRegistry(client, treeA, txA);
        var registryB = TxRegistryRouting.GetRegistry(client, treeB, txB);
        await AwaitShippedAsync(treeA);
        await AwaitShippedAsync(treeB);
        await TrimAllAsync(treeA);
        await TrimAllAsync(treeB);

        // Every peer of every participant has acknowledged, but a silo of this
        // cluster predates the hold: it would purge on its own rules, so the
        // export of a tree could omit the decision. The hold releases nothing
        // and the export is deferred.
        var gate = SiloServices.GetRequiredService<CrossTreeExportGate>();
        var service = LatticeRemoteSnapshotService.Create(SiloServices);
        gate.AllSilosHonourOverrideForTesting = () => false;
        LatticeSnapshotExportDeferredException? deferred;
        try
        {
            for (var i = 0; i < 6; i++)
            {
                await AgeAndPruneAsync(registryB);
                await AgeAndPruneAsync(registryA);
            }

            Assert.That(await registryA.GetRecordedStatusAsync(txA), Is.EqualTo(TxStatus.Committed),
                "nothing is released while a silo predates the hold");
            deferred = Assert.ThrowsAsync<LatticeSnapshotExportDeferredException>(
                () => service.GetMetadataAsync(treeA, LocalClusterId, HybridLogicalClock.Zero));
        }
        finally
        {
            gate.AllSilosHonourOverrideForTesting = null;
        }

        Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(deferred!), Is.True,
            "a deferred export is a transient fault the receiver retries");
        Assert.That((await service.GetMetadataAsync(treeA, LocalClusterId, HybridLogicalClock.Zero)).TreeName, Is.EqualTo(treeA),
            "the export is served once every silo honours the hold");
        await TestPoll.UntilAsync(
            async () =>
            {
                await AgeAndPruneAsync(registryB);
                await AgeAndPruneAsync(registryA);
                return await registryA.GetRecordedStatusAsync(txA) == TxStatus.InFlight;
            },
            "the decision to be purged once every silo honours the hold",
            TimeSpan.FromSeconds(60));
    }

    [Test]
    public void The_hold_compares_only_the_partitions_a_shipper_reads_and_holds_on_no_published_position()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ReplicationCrossTreeDecisionHold.IsPast(null, [1, 1]), Is.False, "a detached shipper, or one bound elsewhere, holds");
            Assert.That(ReplicationCrossTreeDecisionHold.IsPast([], [1, 1]), Is.False, "a shipper that has not published holds");
            Assert.That(ReplicationCrossTreeDecisionHold.IsPast([1, 0], [1, 1]), Is.False);
            Assert.That(ReplicationCrossTreeDecisionHold.IsPast([1, 1], [1, 1]), Is.True);
            Assert.That(ReplicationCrossTreeDecisionHold.IsPast([2], [1, 5]), Is.True, "a partition the shipper does not read holds nothing");
        });
    }

    /// <summary>The txid of <paramref name="tree"/>'s sub-saga of the cross-tree write, from the registry membership.</summary>
    private async Task<Guid> CrossTreeSubSagaAsync(string tree)
    {
        var shards = TxRegistryRouting.ResolveShardCountFromServices(SiloServices);
        foreach (var key in TxRegistryRouting.EnumerateKeys(tree, shards))
        {
            var registry = _cluster.Client.GetGrain<ITxRegistryGrain>(key);
            var decided = await registry.SnapshotAsync();
            var memberships = await registry.GetCrossTreeMembershipsAsync([.. decided.Keys]);
            if (memberships.Count > 0)
            {
                return memberships.Keys.Single();
            }
        }

        Assert.Fail($"tree '{tree}' recorded no cross-tree sub-saga");
        return Guid.Empty;
    }

    private async Task<long[]> TailsAsync(string tree)
    {
        var partitions = await SiloServices.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(tree);
        var tails = new long[partitions];
        for (var p = 0; p < partitions; p++)
        {
            tails[p] = await _cluster.Client.GetGrain<IWalShardGrain>($"{tree}/{p}").GetNextSequenceAsync(CancellationToken.None);
        }

        return tails;
    }

    private async Task AwaitShippedAsync(string tree)
    {
        var tails = await TailsAsync(tree);
        var consumer = _cluster.Client.GetGrain<IReplicationShipperGrain>($"{tree}/{PeerClusterId}").AsReference<IWalOffsetConsumer>();
        await TestPoll.UntilAsync(
            async () => ReplicationCrossTreeDecisionHold.IsPast(await consumer.GetDurableReadPositionsAsync(tree), [.. tails]),
            $"the peer to acknowledge everything tree '{tree}' holds",
            TimeSpan.FromSeconds(30));
    }

    private async Task TrimAllAsync(string tree)
    {
        var tails = await TailsAsync(tree);
        var provider = WalProvider();
        for (var p = 0; p < tails.Length; p++)
        {
            if (tails[p] > 0)
            {
                await provider.TrimAsync(tree, p, tails[p] - 1, CancellationToken.None);
            }
        }
    }

    /// <summary>
    /// Waits out the retention and the guards' refresh intervals, then retires
    /// another saga on the registry: its forget refreshes the guards and prunes
    /// every tombstone that may go.
    /// </summary>
    private static async Task AgeAndPruneAsync(ITxRegistryGrain registry)
    {
        await Task.Delay(TimeSpan.FromMilliseconds(500));
        var other = Guid.NewGuid();
        await registry.MarkCommittedAsync(other);
        await registry.ForgetAsync(other);
    }

    private IWalStorageProvider WalProvider()
    {
        Assert.That(
            SiloServices.GetRequiredService<IWalStorageProviderCatalog>().TryGet(IWalStorageProviderCatalog.DefaultProviderKey, out var provider),
            Is.True);
        return provider!;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.ConfigureLattice(o =>
            {
                o.TxDecisionRetention = TimeSpan.FromMilliseconds(300);
                // The test trims the logs itself.
                o.WalGcInterval = TimeSpan.Zero;
            });
            siloBuilder.AddLatticeReplication(o =>
            {
                o.ClusterId = LocalClusterId;
                o.ReplicationPeers = [PeerClusterId];
                o.ShipCursorWriteInterval = 1;
            });
            siloBuilder.Services.AddSingleton<IReplicationTransport, PerTreeGatedTransport>();
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, LwwResolver>();
        }
    }

    private sealed class LwwResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }

    /// <summary>A peer that acknowledges every batch except those of a refused tree.</summary>
    private sealed class PerTreeGatedTransport : IReplicationTransport
    {
        public static readonly ConcurrentDictionary<string, bool> Refused = new(StringComparer.Ordinal);

        public Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken) =>
            Task.FromResult(new ReplicationAck
            {
                Accepted = !Refused.ContainsKey(batch.TreeName),
                HighestAppliedHlc = HybridLogicalClock.Zero,
            });
    }
}
