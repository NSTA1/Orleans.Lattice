using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Backup;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #5731. In contract-c1 east re-seeded its <c>sys-replication-config</c>
/// from west (a WAL trim made west's link fall behind), and A7 enabled
/// <c>factory-orders</c> at runtime on east while that import was staging on a
/// shadow copy. The enable was acknowledged, but after the cutover east's
/// receiver rejected every <c>factory-orders</c> shipment as "not enrolled" for
/// four minutes. An acknowledged enable must survive a concurrent config-tree
/// re-seed: the config tree holds it after the cutover, and every enrolment seam
/// on the receiver (membership and merge-mode resolution) reports it.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ReplicationConfigReseedEnrolmentIntegrationTests
{
    private const string ClusterId = "east";
    private const string Peer = "west";
    private const string PeerConfigTree = "peer-replication-config";
    private const string PeerEnabledTree = "peer-enabled-tree";

    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(60);

    // One epoch poll plus slack for a loaded CI host.
    private static readonly TimeSpan Budget =
        ReplicationConfigEpochSynchronizer.DefaultPollInterval + TimeSpan.FromSeconds(10);

    private TestCluster _cluster = null!;
    private IServiceProvider _silo = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        _silo = _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        Source.ReleaseAll();
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [TearDown]
    public void ReleaseGates() => Source.ReleaseAll();

    private ILatticeBootstrapCoordinator Coordinator => new LatticeBootstrapCoordinator(_cluster.Client);

    private ILatticeReplicationConfigAuthority Authority => _silo.GetRequiredService<ILatticeReplicationConfigAuthority>();

    private ILatticeReplicationConfigStore Store => _silo.GetRequiredService<ILatticeReplicationConfigStore>();

    private async Task EnableAsync(string tree)
    {
        using (LatticeSystemOrigin.Enter())
        {
            await Authority.EnableReplicationAsync(tree, LatticeMergeMode.LwwRegister);
        }
    }

    /// <summary>West's converged copy of the config map: one tree it enrolled itself.</summary>
    private async Task SeedPeerConfigAsync()
    {
        var entry = new LatticeReplicationConfigEntry();
        entry.SetMode(Peer, LatticeMergeMode.LwwRegister);
        entry.Enable(Peer, 1);
        using (LatticeSystemOrigin.Enter())
        {
            await _cluster.Client.GetGrain<ILattice>(PeerConfigTree)
                .OrMap<string, LatticeReplicationConfigEntry>(LatticeSystemTreeNames.ReplicationConfigMapKey)
                .SetAsync(PeerEnabledTree, Peer, entry);
        }
    }

    private async Task WaitLiveAsync(string tree)
    {
        var deadline = DateTime.UtcNow + Patience;
        while (true)
        {
            var status = await Coordinator.GetStatusAsync(tree);
            if (status.Phase == LatticeBootstrapState.LiveIncremental)
            {
                return;
            }

            Assert.That(status.Phase, Is.Not.EqualTo(LatticeBootstrapState.Failed),
                $"PRECONDITION: the re-seed of '{tree}' did not fail");
            Assert.That(DateTime.UtcNow, Is.LessThan(deadline), $"the re-seed of '{tree}' did not complete; last phase {status.Phase}");
            await Task.Delay(100);
        }
    }

    private async Task AssertEnrolledAsync(string tree)
    {
        var entry = await Store.ReadEntryAsync(tree);
        var membership = _silo.GetRequiredService<IReplicatedTreeMembership>();
        var context = _silo.GetRequiredService<ILatticeReplicationContext>();
        var clock = Stopwatch.StartNew();
        while (!(membership.IsReplicated(tree) && context.ResolveMergeMode(tree) == LatticeMergeMode.LwwRegister)
            && clock.Elapsed < Budget)
        {
            await Task.Delay(50);
        }

        Assert.Multiple(() =>
        {
            Assert.That(entry?.IsEnabled, Is.True,
                $"the config tree holds the acknowledged enable of '{tree}' after the re-seed cutover");
            Assert.That(membership.IsReplicated(tree), Is.True,
                $"the receiver's membership reports '{tree}' enrolled within {Budget} of the cutover");
            Assert.That(context.ResolveMergeMode(tree), Is.EqualTo(LatticeMergeMode.LwwRegister),
                $"the receiver's admission gate resolves '{tree}''s merge mode, so it admits the tree's shipments");
        });
    }

    [Test]
    public async Task An_enable_acknowledged_during_or_after_a_config_tree_re_seed_takes_effect_on_the_receiver()
    {
        const string tree = "factory-orders";
        var config = LatticeSystemTreeNames.ReplicationConfig;

        // East already enrolled a tree of its own, so the config tree exists and is
        // compiled before the re-seed starts.
        await EnableAsync("east-baseline-tree");
        await SeedPeerConfigAsync();
        Source.FromPeer(config, PeerConfigTree, gate: true);

        await Coordinator.BootstrapAsync(config, Peer);
        await Source.FirstRowAsync(config, Patience);
        Assert.That(await _cluster.Client.GetGrain<ITreeResizeGrain>(config).GetBootstrapCopyTreeIdAsync(), Is.Not.Null,
            "PRECONDITION: the config tree's re-seed is draining into a held shadow copy");

        await EnableAsync(tree);

        Source.Release(config);
        await WaitLiveAsync(config);
        Assert.That(await _cluster.Client.GetLatticeRegistry().ResolveAsync(config), Does.Contain("/resized/"),
            "PRECONDITION: the config tree was replaced by its re-seed shadow copy");

        await AssertEnrolledAsync(tree);
        await AssertEnrolledAsync("east-baseline-tree");

        // The peer's enrolment reached the tree only through the re-seed, which
        // publishes no config mutation: the cutover itself must recompile it.
        await AssertEnrolledAsync(PeerEnabledTree);

        // As A7's east enable did: the re-seed imported the peer's enrolment, so the
        // local enable is acknowledged as already enabled and authors nothing. The
        // acknowledgement must still mean the receiver admits the tree.
        LatticeReplicationEnableResult acknowledged;
        using (LatticeSystemOrigin.Enter())
        {
            acknowledged = await Authority.EnableReplicationAsync(PeerEnabledTree, LatticeMergeMode.LwwRegister);
        }

        Assert.That(acknowledged.AlreadyEnabled, Is.True,
            "PRECONDITION: the re-seed imported the peer's enrolment, so the local enable authors nothing");
        await AssertEnrolledAsync(PeerEnabledTree);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<IBootstrapSnapshotSource, Source>();
            siloBuilder.AddOrMapShape<string, LatticeReplicationConfigEntry>(PeerConfigTree);
            siloBuilder.AddLatticeReplication(
                opts =>
                {
                    opts.ClusterId = ClusterId;
                    opts.AutoBootstrapOnFallOffLog = false;
                    opts.BootstrapTransientRetry = new BoundedExponentialRetryPolicyOptions
                    {
                        MaxAttempts = 1,
                        InitialDelay = TimeSpan.Zero,
                        MaxDelay = TimeSpan.Zero,
                    };
                },
                enableRuntimeConfig: true);
        }
    }

    /// <summary>
    /// The peer's export of each armed tree: the real export of another tree of this
    /// cluster standing in for the peer region's copy, paused after its first row
    /// until released. Static because the silo constructs it.
    /// </summary>
    private sealed class Source(ISnapshotProvider exporter) : IBootstrapSnapshotSource
    {
        private static readonly ConcurrentDictionary<string, Arm> Arms = new(StringComparer.Ordinal);

        private sealed record Arm(string PeerTree, bool Gate)
        {
            public TaskCompletionSource FirstRow { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

            public TaskCompletionSource Released { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public static void FromPeer(string tree, string peerTree, bool gate) => Arms[tree] = new Arm(peerTree, gate);

        public static void Release(string tree)
        {
            if (Arms.TryGetValue(tree, out var arm))
            {
                arm.Released.TrySetResult();
            }
        }

        public static void ReleaseAll()
        {
            foreach (var arm in Arms.Values)
            {
                arm.Released.TrySetResult();
            }
        }

        public static async Task FirstRowAsync(string tree, TimeSpan timeout)
        {
            var arm = Arms[tree];
            Assert.That(await Task.WhenAny(arm.FirstRow.Task, Task.Delay(timeout)), Is.SameAs(arm.FirstRow.Task),
                $"PRECONDITION: the import into '{tree}' applied its first row and paused");
        }

        public async Task<SnapshotStream> ExportAsync(string treeName, HybridLogicalClock asOfHlc, CancellationToken cancellationToken = default)
        {
            var arm = Arms[treeName];
            var peer = await exporter.ExportAsync(arm.PeerTree, asOfHlc, cancellationToken);
            return new SnapshotStream(treeName, peer.AsOfHlc, peer.CausalStableFrontier, GateAsync(arm, peer.Entries));
        }

        private static async IAsyncEnumerable<SnapshotEntry> GateAsync(Arm arm, IAsyncEnumerable<SnapshotEntry> entries)
        {
            var first = true;
            await foreach (var entry in entries)
            {
                yield return entry;

                if (first)
                {
                    first = false;
                    arm.FirstRow.TrySetResult();
                    if (arm.Gate)
                    {
                        await arm.Released.Task;
                    }
                }
            }

            arm.FirstRow.TrySetResult();
        }
    }
}
