using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Integration tests for the per-shard WAL grain wired in by the core
/// <c>AddLattice</c> registration. Brings up a single-site Orleans cluster
/// and verifies every committed mutation flows through the real WAL grain.
/// </summary>
[TestFixture]
[Category("Integration")]
public class WalShardWalIntegrationTests
{
    private const string ClusterId = "wal-site";

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
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
    }

    [Test]
    public async Task Direct_grain_call_appends_and_reads_back()
    {
        var wal = _cluster.Client.GetGrain<IWalShardGrain>("direct/0");

        var entry = new WalRecord
        {
            TreeId = "direct",
            Op = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1, 2 },
            Timestamp = Orleans.Lattice.HybridLogicalClock.Tick(Orleans.Lattice.HybridLogicalClock.Zero),
            OriginClusterId = ClusterId,
        };

        var seq = await wal.AppendAsync(entry, CancellationToken.None);
        var page = await wal.ReadAsync(0, 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(seq, Is.EqualTo(0L));
            Assert.That(page.Entries, Has.Count.EqualTo(1));
            Assert.That(page.Entries[0].Entry.Key, Is.EqualTo("k"));
        });
    }

    [Test]
    public async Task SetAsync_appends_entry_to_wal_grain()
    {
        const string tree = "wal-set";
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        var wal = _cluster.Client.GetGrain<IWalShardGrain>($"{tree}/0");

        var before = await wal.GetLiveEntryCountAsync(CancellationToken.None);
        await lattice.SetAsync("k", new byte[] { 1, 2, 3 });
        var after = await wal.GetLiveEntryCountAsync(CancellationToken.None);

        Assert.That(after, Is.GreaterThan(before));
    }

    [Test]
    public async Task ReadAsync_returns_appended_entries_in_sequence_order()
    {
        const string tree = "wal-read";
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        var wal = _cluster.Client.GetGrain<IWalShardGrain>($"{tree}/0");

        var startSeq = await wal.GetNextSequenceAsync(CancellationToken.None);

        await lattice.SetAsync("a", new byte[] { 1 });
        await lattice.SetAsync("b", new byte[] { 2 });
        await lattice.SetAsync("c", new byte[] { 3 });

        var page = await wal.ReadAsync(startSeq, 100, CancellationToken.None);

        var keys = page.Entries
            .Where(e => e.Entry.TreeId == tree)
            .Select(e => e.Entry.Key)
            .ToArray();
        Assert.That(keys, Is.SupersetOf(new[] { "a", "b", "c" }));
    }

    [Test]
    public async Task DeleteAsync_appends_tombstone_entry_to_wal()
    {
        const string tree = "wal-del";
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        var wal = _cluster.Client.GetGrain<IWalShardGrain>($"{tree}/0");

        await lattice.SetAsync("gone", new byte[] { 9 });
        var snapshot = await wal.GetNextSequenceAsync(CancellationToken.None);
        await lattice.DeleteAsync("gone");

        var page = await wal.ReadAsync(snapshot, 100, CancellationToken.None);
        var deletes = page.Entries.Where(e => e.Entry.Op == MutationKind.Delete && e.Entry.Key == "gone").ToArray();
        Assert.That(deletes, Is.Not.Empty);
        Assert.That(deletes[0].Entry.IsTombstone, Is.True);
    }

    [Test]
    public async Task Sequence_numbers_are_dense_for_a_single_partition()
    {
        const string tree = "wal-dense";
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        var wal = _cluster.Client.GetGrain<IWalShardGrain>($"{tree}/0");

        var start = await wal.GetNextSequenceAsync(CancellationToken.None);
        for (var i = 0; i < 5; i++)
        {
            await lattice.SetAsync($"k{i}", new byte[] { (byte)i });
        }
        var end = await wal.GetNextSequenceAsync(CancellationToken.None);

        var page = await wal.ReadAsync(start, (int)(end - start), CancellationToken.None);
        var seqs = page.Entries.Select(e => e.Sequence).ToArray();
        Assert.That(seqs, Is.EqualTo(Enumerable.Range((int)start, (int)(end - start)).Select(i => (long)i).ToArray()));
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();

            // Pin WalPartitions=1 so tests that query a specific
            // {tree}/0 IWalShardGrain see deterministic single-
            // partition routing (the silo-wide default is 8, which
            // would scatter per-key writes across 8 partitions and
            // make per-grain entry-count assertions flaky).
            siloBuilder.ConfigureLattice(o => o.WalPartitions = 1);

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
