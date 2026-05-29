using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Integration tests that exercise <see cref="IChangeFeed"/> through the
/// real WAL. Brings up a single-site Orleans cluster with the default
/// <see cref="IReplogSink"/> (no override) so every <c>ILattice</c>
/// mutation appears on the change feed.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ChangeFeedIntegrationTests
{
    private const string ClusterId = "feed-site";

    private TestCluster _cluster = null!;
    private IChangeFeed _feed = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        // ChangeFeed only needs an IGrainFactory and an options monitor.
        // Construct one against the cluster client so the test doesn't have
        // to plumb the silo's service provider.
        var options = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions
        {
            ClusterId = ClusterId,
            ReplogPartitions = 1,
        });
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);
        _feed = new ChangeFeed(_cluster.Client, options, resolver);
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

    private static async Task<List<WalRecord>> CollectAsync(IAsyncEnumerable<WalRecord> source)
    {
        var result = new List<WalRecord>();
        await foreach (var entry in source)
        {
            result.Add(entry);
        }
        return result;
    }

    [Test]
    public async Task Subscribe_streams_committed_set_mutations()
    {
        const string tree = "feed-set";
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);

        await lattice.SetAsync("a", new byte[] { 1 });
        await lattice.SetAsync("b", new byte[] { 2 });

        var entries = await CollectAsync(_feed.Subscribe(tree, HybridLogicalClock.Zero));

        var keys = entries.Where(e => e.Op == MutationKind.Set).Select(e => e.Key).ToArray();
        Assert.That(keys, Is.SupersetOf(new[] { "a", "b" }));
    }

    [Test]
    public async Task Subscribe_streams_tombstone_for_delete()
    {
        const string tree = "feed-del";
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync("k", new byte[] { 9 });
        await lattice.DeleteAsync("k");

        var entries = await CollectAsync(_feed.Subscribe(tree, HybridLogicalClock.Zero));

        Assert.That(
            entries.Any(e => e.Op == MutationKind.Delete && e.Key == "k" && e.IsTombstone),
            Is.True);
    }

    [Test]
    public async Task Subscribe_advances_when_re_subscribing_with_updated_cursor()
    {
        // D1c: cursor shape is now per-partition WAL offset. Capture
        // the cursor between passes via GetCurrentCursorAsync and the
        // second pass yields only writes that landed after the cursor
        // capture.
        const string tree = "feed-cursor";
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);

        await lattice.SetAsync("first", new byte[] { 1 });
        var firstPass = await CollectAsync(_feed.Subscribe(tree, ChangeFeedCursor.Initial));
        Assert.That(firstPass, Is.Not.Empty);
        var cursor = await _feed.GetCurrentCursorAsync(tree);

        await lattice.SetAsync("second", new byte[] { 2 });

        var secondPass = await CollectAsync(_feed.Subscribe(tree, cursor));
        Assert.Multiple(() =>
        {
            Assert.That(secondPass.Select(e => e.Key), Does.Contain("second"));
            Assert.That(secondPass.Select(e => e.Key), Does.Not.Contain("first"));
        });
    }

    [Test]
    public async Task Subscribe_with_include_local_origin_false_filters_local_writes()
    {
        const string tree = "feed-origin";
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync("local", new byte[] { 1 });

        var entries = await CollectAsync(
            _feed.Subscribe(tree, HybridLogicalClock.Zero, includeLocalOrigin: false));

        Assert.That(entries.Where(e => e.OriginClusterId == ClusterId), Is.Empty);
    }

    [Test]
    public async Task Subscribe_yields_entries_in_hlc_ascending_order()
    {
        const string tree = "feed-order";
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        for (var i = 0; i < 8; i++)
        {
            await lattice.SetAsync($"k{i}", new byte[] { (byte)i });
        }

        var entries = await CollectAsync(_feed.Subscribe(tree, HybridLogicalClock.Zero));
        var setEntries = entries.Where(e => e.Op == MutationKind.Set).ToArray();

        for (var i = 1; i < setEntries.Length; i++)
        {
            Assert.That(
                setEntries[i].Timestamp.CompareTo(setEntries[i - 1].Timestamp),
                Is.GreaterThanOrEqualTo(0),
                $"entry {i} ({setEntries[i].Key}) is out of order relative to entry {i - 1} ({setEntries[i - 1].Key})");
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            // The ChangeFeed stub in OneTimeSetUp pins ReplogPartitions=1.
            // Align the silo's WalPartitions so every mutation lands on the
            // single partition the feed reads from.
            siloBuilder.ConfigureLattice(o => o.WalPartitions = 1);
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = ClusterId);

            // Tests use ad-hoc tree ids; opt every tree in to LwwRegister so
            // the commit-time observer doesn't short-circuit.
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
        }
    }

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }
}
