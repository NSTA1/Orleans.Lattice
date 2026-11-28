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
/// Issue #5699: a tree that stops resolving a write class (disabled at runtime,
/// or its merge mode ambiguous) has its records dropped by every peer, so a
/// shipper that kept shipping would advance its cursor past them and lose them.
/// The shipper re-resolves the write class on every tick, detaches from the log
/// while none resolves, and re-attaches with a re-seed once one resolves again.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ShipperWriteClassSuspendTests
{
    private const string LocalClusterId = "site-a";
    private const string PeerClusterId = "site-b";

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        RecordingTransport.Reset();
        SwitchableResolver.Reset();
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }

        RecordingTransport.Reset();
        SwitchableResolver.Reset();
    }

    [Test]
    public async Task Shipper_suspends_while_the_tree_resolves_no_write_class_and_reseeds_its_peer_when_one_resolves()
    {
        var (tree, lattice, shipper, registry) = await ArrangeAsync("write-class-suspend-");
        await shipper.EnsureActiveAsync(CancellationToken.None);
        await lattice.SetAsync("before", [1]);
        await TestPoll.UntilAsync(
            () => RecordingTransport.Shipped(tree, "before"),
            "the shipper to deliver a write while the tree resolves a write class",
            TimeSpan.FromSeconds(30));

        SwitchableResolver.Disable(tree);
        await lattice.SetAsync("while-disabled", [2]);
        await TestPoll.UntilAsync(
            async () => !(await registry.GetConsumersAsync()).Contains(shipper.GetGrainId()),
            "the shipper to detach from the log while the tree resolves no write class",
            TimeSpan.FromSeconds(30));
        await Task.Delay(TimeSpan.FromSeconds(1));
        Assert.That(RecordingTransport.Shipped(tree, "while-disabled"), Is.False,
            "a shipper must not ship a record every peer drops, or its cursor passes the record and loses it");

        // The driver's ensure must not re-attach while no write class resolves.
        await shipper.EnsureActiveAsync(CancellationToken.None);
        Assert.That((await registry.GetConsumersAsync()).Contains(shipper.GetGrainId()), Is.False,
            "ensuring a suspended shipper must not re-attach it while the tree resolves no write class");

        RecordingTransport.ClearReseed(tree);
        SwitchableResolver.Enable(tree);
        await lattice.SetAsync("after", [3]);
        await TestPoll.UntilAsync(
            () => RecordingTransport.ReseedRequested(tree) && RecordingTransport.Shipped(tree, "after"),
            "the re-attached shipper to ask its peer for a re-seed and resume shipping",
            TimeSpan.FromSeconds(30));
        await TestPoll.UntilAsync(
            async () => (await registry.GetConsumersAsync()).Contains(shipper.GetGrainId()),
            "the re-attached shipper to register with the log again",
            TimeSpan.FromSeconds(30));
    }

    [Test]
    public async Task Gate_does_not_reattach_a_removed_peers_shipper_until_the_driver_ensures_it()
    {
        var (tree, lattice, shipper, registry) = await ArrangeAsync("write-class-removed-");
        await shipper.EnsureActiveAsync(CancellationToken.None);
        await lattice.SetAsync("before", [1]);
        await TestPoll.UntilAsync(
            async () => (await registry.GetConsumersAsync()).Contains(shipper.GetGrainId()),
            "the shipper to register as an offset consumer of the tree's log",
            TimeSpan.FromSeconds(30));

        await shipper.DetachFromLogAsync(CancellationToken.None);
        await lattice.SetAsync("after-detach", [2]);
        await Task.Delay(TimeSpan.FromSeconds(1));
        Assert.That((await registry.GetConsumersAsync()).Contains(shipper.GetGrainId()), Is.False,
            "the write-class gate must not re-attach a shipper detached because its peer was removed");

        // Ensured while no write class resolves, the shipper is handed to the
        // gate instead, which re-attaches it once a write class resolves.
        SwitchableResolver.Disable(tree);
        await shipper.EnsureActiveAsync(CancellationToken.None);
        Assert.That((await registry.GetConsumersAsync()).Contains(shipper.GetGrainId()), Is.False,
            "ensuring a detached shipper must not re-attach it while the tree resolves no write class");

        RecordingTransport.ClearReseed(tree);
        SwitchableResolver.Enable(tree);
        await lattice.SetAsync("after-enable", [3]);
        await TestPoll.UntilAsync(
            async () => (await registry.GetConsumersAsync()).Contains(shipper.GetGrainId()),
            "the gate to re-attach the shipper once the tree resolves a write class",
            TimeSpan.FromSeconds(30));
        await TestPoll.UntilAsync(
            () => RecordingTransport.ReseedRequested(tree),
            "the re-attached shipper to ask its peer for a re-seed",
            TimeSpan.FromSeconds(30));
    }

    private async Task<(string Tree, ILattice Lattice, IReplicationShipperGrain Shipper, IWalOffsetConsumerRegistryGrain Registry)>
        ArrangeAsync(string prefix)
    {
        var tree = prefix + Guid.NewGuid().ToString("N")[..8];
        var client = _cluster.Client;
        await client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(
            tree,
            new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 64, MaxInternalChildren = 4 });
        return (
            tree,
            client.GetGrain<ILattice>(tree),
            client.GetGrain<IReplicationShipperGrain>($"{tree}/{PeerClusterId}"),
            client.GetGrain<IWalOffsetConsumerRegistryGrain>(tree));
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(o =>
            {
                o.ClusterId = LocalClusterId;
                o.ReplicationPeers = [PeerClusterId];
                o.ShipCursorWriteInterval = 1;
            });
            siloBuilder.Services.AddSingleton<IReplicationTransport, RecordingTransport>();
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, SwitchableResolver>();
        }
    }

    /// <summary>
    /// Resolves last-writer-wins for every tree except those disabled, which
    /// resolve no write class, as a runtime-disabled or ambiguous tree does.
    /// </summary>
    private sealed class SwitchableResolver : ILatticeMergeModeResolver
    {
        private static readonly ConcurrentDictionary<string, bool> Disabled = new(StringComparer.Ordinal);

        public static void Reset() => Disabled.Clear();

        public static void Disable(string tree) => Disabled[tree] = true;

        public static void Enable(string tree) => Disabled.TryRemove(tree, out _);

        public LatticeMergeMode? Resolve(string treeId)
            => Disabled.ContainsKey(treeId) ? null : LatticeMergeMode.LwwRegister;
    }

    /// <summary>Records every shipped key by tree and every re-seed request, and acknowledges every batch.</summary>
    private sealed class RecordingTransport(IWalRecordEncoder encoder) : IReplicationTransport
    {
        private static readonly ConcurrentDictionary<(string Tree, string Key), bool> Delivered = new();
        private static readonly ConcurrentDictionary<string, bool> ReseedRequests = new(StringComparer.Ordinal);

        public static void Reset()
        {
            Delivered.Clear();
            ReseedRequests.Clear();
        }

        public static bool Shipped(string tree, string key) => Delivered.ContainsKey((tree, key));

        public static bool ReseedRequested(string tree) => ReseedRequests.ContainsKey(tree);

        public static void ClearReseed(string tree) => ReseedRequests.TryRemove(tree, out _);

        public Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)
        {
            if (batch.ReseedAfterEpoch is not null)
            {
                ReseedRequests[batch.TreeName] = true;
            }

            if (batch.EncodedEnvelope is { } envelope)
            {
                foreach (var segment in envelope.EncodedEntries.Span)
                {
                    var record = encoder.Decode(segment.AsSpan(), batch.TreeName, envelope.Header.Mode);
                    Delivered[(batch.TreeName, record.Key)] = true;
                }
            }

            return Task.FromResult(new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });
        }
    }
}
