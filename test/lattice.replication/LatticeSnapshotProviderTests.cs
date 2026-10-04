using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Integration tests for <see cref="LatticeSnapshotProvider"/>. Brings
/// up a single-site Orleans cluster, populates a tree via the public
/// <see cref="ILattice"/> surface, and asserts the export captures
/// every live entry with a stable HLC and a non-null causal-stable
/// frontier.
/// </summary>
[TestFixture]
[Category("Integration")]
public class LatticeSnapshotProviderTests
{
    private const string ClusterId = "snap-site";

    private TestCluster _cluster = null!;
    private ISnapshotProvider _provider = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        _provider = new LatticeSnapshotProvider(
            _cluster.Client,
            new InMemoryWalCursorRegistry(),
            LatticeSnapshotProviderUnitTests.TestOptions());
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    private static async Task<List<SnapshotEntry>> DrainAsync(SnapshotStream stream)
    {
        var collected = new List<SnapshotEntry>();
        await foreach (var entry in stream.Entries)
        {
            collected.Add(entry);
        }
        return collected;
    }

    [Test]
    public async Task ExportAsync_throws_when_tree_name_is_null()
    {
        Assert.That(
            async () => await _provider.ExportAsync(null!, HybridLogicalClock.Zero),
            Throws.InstanceOf<ArgumentException>());
        await Task.CompletedTask;
    }

    [Test]
    public async Task ExportAsync_throws_when_tree_name_is_whitespace()
    {
        Assert.That(
            async () => await _provider.ExportAsync("   ", HybridLogicalClock.Zero),
            Throws.InstanceOf<ArgumentException>());
        await Task.CompletedTask;
    }

    [Test]
    public async Task ExportAsync_returns_empty_stream_for_unpopulated_tree()
    {
        const string tree = "snap-empty";

        var stream = await _provider.ExportAsync(tree, HybridLogicalClock.Zero);
        var entries = await DrainAsync(stream);

        Assert.Multiple(() =>
        {
            Assert.That(stream.TreeName, Is.EqualTo(tree));
            Assert.That(stream.AsOfHlc, Is.EqualTo(HybridLogicalClock.Zero));
            Assert.That(stream.CausalStableFrontier, Is.Not.Null);
            Assert.That(entries, Is.Empty);
        });
    }

    [Test]
    public async Task ExportAsync_with_zero_hlc_emits_every_live_entry()
    {
        const string tree = "snap-all";
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync("a", new byte[] { 1 });
        await lattice.SetAsync("b", new byte[] { 2 });
        await lattice.SetAsync("c", new byte[] { 3 });

        var stream = await _provider.ExportAsync(tree, HybridLogicalClock.Zero);
        var entries = await DrainAsync(stream);

        var keys = entries.Select(e => e.Key).OrderBy(k => k, StringComparer.Ordinal).ToArray();
        Assert.That(keys, Is.EqualTo(new[] { "a", "b", "c" }));
        Assert.That(entries.All(e => e.Timestamp > HybridLogicalClock.Zero), Is.True);
    }

    [Test]
    public async Task ExportAsync_ships_a_tombstoned_entry_as_a_committed_tombstone_row()
    {
        const string tree = "snap-tombstone";
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync("live", new byte[] { 1 });
        await lattice.SetAsync("dead", new byte[] { 2 });
        await lattice.DeleteAsync("dead");

        var stream = await _provider.ExportAsync(tree, HybridLogicalClock.Zero);
        var entries = await DrainAsync(stream);

        // A receiver bootstrapping in place over an older copy must learn of the
        // delete, so it ships as a committed tombstone rather than as absence (#4504).
        var dead = entries.Where(e => e.Key == "dead").ToList();
        Assert.That(dead, Has.Count.EqualTo(1));
        Assert.That(dead[0].IsTombstone, Is.True);
        Assert.That(dead[0].IsPrepared, Is.False);
        Assert.That(dead[0].Timestamp, Is.GreaterThan(HybridLogicalClock.Zero));
        Assert.That(entries.Where(e => e.Key == "live").Select(e => e.IsTombstone), Is.EqualTo(new[] { false }));
    }

    [Test]
    public async Task ExportAsync_filters_entries_above_as_of_hlc()
    {
        const string tree = "snap-asof";
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync("x", new byte[] { 1 });

        // Take the as-of HLC after the first write but before the second.
        var firstPass = await DrainAsync(await _provider.ExportAsync(tree, HybridLogicalClock.Zero));
        Assert.That(firstPass, Has.Count.EqualTo(1));
        var asOf = firstPass[0].Timestamp;

        await lattice.SetAsync("y", new byte[] { 2 });

        var bounded = await DrainAsync(await _provider.ExportAsync(tree, asOf));
        Assert.That(bounded.Select(e => e.Key), Does.Contain("x"));
        Assert.That(bounded.Select(e => e.Key), Does.Not.Contain("y"));
    }

    [Test]
    public async Task ExportAsync_carries_the_expiry_of_a_committed_ttl_entry()
    {
        // Regression: the committed-projection pass used to emit every row
        // with ExpiresAtTicks = 0, so a bootstrap or anti-entropy fallback
        // seeded from this export installed a TTL key as durable on the peer.
        const string tree = "snap-ttl";
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync("ttl", new byte[] { 1 }, TimeSpan.FromHours(1));
        await lattice.SetAsync("durable", new byte[] { 2 });
        var stored = await lattice.GetWithVersionAsync("ttl");

        var entries = await DrainAsync(await _provider.ExportAsync(tree, HybridLogicalClock.Zero));

        var ttl = entries.Single(e => e.Key == "ttl");
        var durable = entries.Single(e => e.Key == "durable");
        Assert.Multiple(() =>
        {
            Assert.That(stored.ExpiresAtTicks, Is.GreaterThan(0),
                "precondition: the source row carries an absolute expiry");
            Assert.That(ttl.IsPrepared, Is.False);
            Assert.That(ttl.ExpiresAtTicks, Is.EqualTo(stored.ExpiresAtTicks),
                "the exported committed row must carry the source row's expiry");
            Assert.That(durable.ExpiresAtTicks, Is.Zero,
                "a durable row must stay durable");
        });
    }

    [Test]
    public async Task ExportAsync_returns_non_null_causal_stable_frontier()
    {
        const string tree = "snap-frontier";
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync("k", new byte[] { 1 });

        var stream = await _provider.ExportAsync(tree, HybridLogicalClock.Zero);

        Assert.That(stream.CausalStableFrontier, Is.Not.Null);
    }

    [Test]
    public async Task ExportAsync_propagates_cancellation()
    {
        const string tree = "snap-cancel";
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await _provider.ExportAsync(tree, HybridLogicalClock.Zero, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
        await Task.CompletedTask;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = ClusterId);
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
        }
    }

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }
}
