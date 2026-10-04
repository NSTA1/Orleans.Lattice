using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// An in-place re-bootstrap must ship the source's deletes (issue #4504). A
/// receiver that fell off the log re-bootstraps over its existing copy, which the
/// drain does not clear, and the delete's WAL record is behind the source's trim
/// point, so the incremental stream never delivers it. Before the fix the export
/// enumerated live keys only, so the receiver kept every key the source deleted
/// while it was behind. Two real clusters: site A is the bootstrap source, site B
/// the receiver, driven through the real bootstrap coordinator over the real
/// remote snapshot service. Keys a third cluster wrote reach both sites through
/// each site's replication applier, so the receiver's copy does not carry the
/// source as its origin.
/// </summary>
[TestFixture]
[Category("Integration")]
public class InPlaceReBootstrapDeleteIntegrationTests
{
    private const string SiteAClusterId = "iprd-site-a";
    private const string SiteBClusterId = "iprd-site-b";
    private const string SiteCClusterId = "iprd-site-c";

    private static readonly ConcurrentDictionary<string, IRemoteSnapshotTransport> SiteATransports = new();

    private TestCluster _siteA = null!;
    private TestCluster _siteB = null!;
    private LatticeSnapshotProvider _siteAProvider = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var aBuilder = new TestClusterBuilder(initialSilosCount: 1);
        aBuilder.AddSiloBuilderConfigurator<SiteASiloConfigurator>();
        _siteA = aBuilder.Build();
        await _siteA.DeployAsync();

        _siteAProvider = new LatticeSnapshotProvider(
            _siteA.Client,
            new InMemoryWalCursorRegistry(),
            LatticeSnapshotProviderUnitTests.TestOptions());
        SiteATransports[SiteAClusterId] = new LatticeRemoteSnapshotService(
            _siteAProvider,
            new StubReplicationContext(SiteAClusterId, LatticeMergeMode.LwwRegister),
            NullLogger<LatticeRemoteSnapshotService>.Instance);

        var bBuilder = new TestClusterBuilder(initialSilosCount: 1);
        bBuilder.AddSiloBuilderConfigurator<SiteBSiloConfigurator>();
        _siteB = bBuilder.Build();
        await _siteB.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_siteB is not null)
        {
            await _siteB.StopAllSilosAsync();
            await _siteB.DisposeAsync();
        }

        if (_siteA is not null)
        {
            await _siteA.StopAllSilosAsync();
            await _siteA.DisposeAsync();
        }

        SiteATransports.TryRemove(SiteAClusterId, out _);
    }

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static IReplicationApplier Applier(TestCluster cluster) =>
        cluster.Silos.OfType<InProcessSiloHandle>().First()
            .SiloHost.Services.GetRequiredService<IReplicationApplier>();

    /// <summary>Delivers a third-cluster write to <paramref name="cluster"/> the way replication would.</summary>
    private static Task ShipFromSiteCAsync(TestCluster cluster, string tree, string key, byte value, HybridLogicalClock hlc) =>
        Applier(cluster).ApplyAsync(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = key,
            Value = new[] { value },
            Timestamp = hlc,
            OriginClusterId = SiteCClusterId,
        });

    private async Task ReBootstrapSiteBAsync(string tree)
    {
        var coordinator = _siteB.Client.GetGrain<ILatticeBootstrapCoordinatorGrain>(tree);
        await coordinator.BootstrapAsync(SiteAClusterId, CancellationToken.None);

        var deadline = Environment.TickCount64 + (long)TimeSpan.FromSeconds(60).TotalMilliseconds;
        LatticeBootstrapState state;
        do
        {
            await Task.Delay(250);
            state = await coordinator.GetStateAsync(CancellationToken.None);
        }
        while (state != LatticeBootstrapState.LiveIncremental
            && state != LatticeBootstrapState.Failed
            && Environment.TickCount64 < deadline);

        Assert.That(state, Is.EqualTo(LatticeBootstrapState.LiveIncremental), "the re-bootstrap must complete");
    }

    [Test]
    public async Task Re_bootstrap_over_a_populated_receiver_deletes_a_key_the_source_deleted_while_the_receiver_was_behind()
    {
        const string tree = "iprd-plain-delete";
        const string keyA = "kept";
        const string keyB = "deleted-on-source";
        var written = Hlc(1_000);

        // Site C wrote both keys and both sites received them.
        foreach (var cluster in new[] { _siteA, _siteB })
        {
            await ShipFromSiteCAsync(cluster, tree, keyA, 1, written);
            await ShipFromSiteCAsync(cluster, tree, keyB, 2, written);
        }

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        Assert.That(await siteA.GetAsync(keyB), Is.EqualTo(new byte[] { 2 }), "precondition: the source holds keyB");
        Assert.That(await siteB.GetAsync(keyB), Is.EqualTo(new byte[] { 2 }), "precondition: the receiver holds keyB");

        // The source deletes keyB while the receiver is behind; nothing ships it.
        await siteA.DeleteAsync(keyB);
        Assert.That(await siteA.GetAsync(keyB), Is.Null, "precondition: the source deleted keyB");

        var exported = new List<SnapshotEntry>();
        var stream = await _siteAProvider.ExportAsync(tree, HybridLogicalClock.Zero);
        await foreach (var entry in stream.Entries)
        {
            exported.Add(entry);
        }

        await ReBootstrapSiteBAsync(tree);

        Assert.Multiple(async () =>
        {
            Assert.That(exported.Where(e => e.Key == keyB),
                Has.Some.Matches<SnapshotEntry>(e => e.IsTombstone && !e.IsPrepared && e.Timestamp > written),
                "the export must carry keyB as a committed tombstone row stamped above the write it deletes");
            Assert.That(await siteB.GetAsync(keyB), Is.Null,
                "keyB was deleted on the source, so the re-bootstrapped receiver must not keep its old value");
            Assert.That(await siteB.GetAsync(keyA), Is.EqualTo(new byte[] { 1 }), "keyA is untouched");
        });
    }

    [Test]
    public async Task Re_bootstrap_keeps_a_receiver_local_key_the_source_never_held()
    {
        const string tree = "iprd-receiver-local";
        const string shared = "shared";
        const string local = "receiver-only";

        await ShipFromSiteCAsync(_siteA, tree, shared, 1, Hlc(1_000));
        await ShipFromSiteCAsync(_siteB, tree, shared, 1, Hlc(1_000));
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteB.SetAsync(local, new byte[] { 7 });

        await ReBootstrapSiteBAsync(tree);

        Assert.Multiple(async () =>
        {
            Assert.That(await siteB.GetAsync(local), Is.EqualTo(new byte[] { 7 }),
                "absence on the source is not a delete: a key the source never held must survive");
            Assert.That(await siteB.GetAsync(shared), Is.EqualTo(new byte[] { 1 }));
        });
    }

    [Test]
    public async Task Re_bootstrap_keeps_a_receiver_value_written_after_the_source_delete()
    {
        const string tree = "iprd-newer-than-delete";
        const string key = "rewritten";

        await ShipFromSiteCAsync(_siteA, tree, key, 1, Hlc(1_000));
        await ShipFromSiteCAsync(_siteB, tree, key, 1, Hlc(1_000));
        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        await siteA.DeleteAsync(key);

        // The receiver overwrites the key after the source's delete: last-writer-wins
        // must keep the newer value, so the shipped tombstone does not apply.
        await Task.Delay(TimeSpan.FromMilliseconds(50));
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteB.SetAsync(key, new byte[] { 3 });

        await ReBootstrapSiteBAsync(tree);

        Assert.That(await siteB.GetAsync(key), Is.EqualTo(new byte[] { 3 }),
            "a tombstone older than the receiver's own write must lose under last-writer-wins");
    }

    private sealed class SiteASiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = SiteAClusterId);
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
        }
    }

    private sealed class SiteBSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = SiteBClusterId);

            if (SiteATransports.TryGetValue(SiteAClusterId, out var transport))
            {
                siloBuilder.Services.AddSingleton(transport);
            }

            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
        }
    }

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }
}
