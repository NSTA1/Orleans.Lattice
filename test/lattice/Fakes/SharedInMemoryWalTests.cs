using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice.Tests.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Harness-fidelity tests for multi-silo in-process clusters (issue #4196): every
/// silo of one cluster must read the WAL every other silo writes, separate clusters
/// must not share a WAL, and the fixture-level assertion must reject a cluster whose
/// silos each keep their own.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SharedInMemoryWalTests
{
    private ClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task A_wal_write_through_one_silo_of_the_core_cluster_fixture_is_read_through_the_other()
    {
        var silos = _fixture.Cluster.Silos.Cast<InProcessSiloHandle>().ToArray();
        Assert.That(silos, Has.Length.EqualTo(2), "precondition: the core fixture deploys two silos");
        var writer = Wal(silos[0]);
        var reader = Wal(silos[1]);
        var treeId = $"harness-wal-{Guid.NewGuid():N}";

        await writer.AppendBatchAsync(treeId, 0, [Entry(treeId, 0, "a"), Entry(treeId, 1, "b")], CancellationToken.None);

        var read = await ReadAllAsync(reader, treeId);
        Assert.Multiple(async () =>
        {
            Assert.That(read.Select(e => e.Mutation.Key), Is.EqualTo(new[] { "a", "b" }));
            Assert.That(await reader.GetHighestOffsetAsync(treeId, 0, CancellationToken.None), Is.EqualTo(1L));
        });
    }

    [Test]
    public void The_fixture_level_wal_assertion_passes_on_the_core_cluster_fixture() =>
        Assert.DoesNotThrowAsync(() => SharedInMemoryWal.AssertAllSilosShareOneWalAsync(_fixture.Cluster));

    [Test]
    public async Task The_fixture_level_wal_assertion_rejects_a_cluster_whose_silos_each_keep_their_own_wal()
    {
        var builder = new TestClusterBuilder(2);
        builder.AddSiloBuilderConfigurator<DefaultWalSiloConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        try
        {
            var ex = Assert.ThrowsAsync<InvalidOperationException>(
                () => SharedInMemoryWal.AssertAllSilosShareOneWalAsync(cluster));
            Assert.That(ex!.Message, Does.Contain("does not observe the WAL"));
        }
        finally
        {
            await cluster.StopAllSilosAsync();
            await cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task Separate_clusters_keep_separate_wal_stores_and_a_store_is_dropped_with_its_last_silo()
    {
        var first = await DeploySharedAsync();
        var second = await DeploySharedAsync();
        try
        {
            var treeId = $"harness-isolation-{Guid.NewGuid():N}";
            await Wal(first.Primary).AppendBatchAsync(treeId, 0, [Entry(treeId, 0, "only-in-first")], CancellationToken.None);

            Assert.Multiple(async () =>
            {
                Assert.That(Wal(second.Primary), Is.Not.SameAs(Wal(first.Primary)));
                Assert.That(await ReadAllAsync(Wal(second.Primary), treeId), Is.Empty, "A second cluster must not see the first cluster's WAL.");
                Assert.That(SharedInMemoryWal.IsLive(Options(first.Primary)), Is.True);
            });
        }
        finally
        {
            var firstOptions = Options(first.Primary);
            await first.StopAllSilosAsync();
            await first.DisposeAsync();
            await second.StopAllSilosAsync();
            await second.DisposeAsync();
            Assert.That(SharedInMemoryWal.IsLive(firstOptions), Is.False, "The store is released with the cluster's last silo.");
        }
    }

    private static async Task<TestCluster> DeploySharedAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<DefaultWalSiloConfigurator>();
        builder.UseSharedInMemoryWal();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;
    }

    private static IWalStorageProvider Wal(SiloHandle silo) =>
        ((InProcessSiloHandle)silo).SiloHost.Services.GetRequiredService<IWalStorageProvider>();

    private static ClusterOptions Options(SiloHandle silo) =>
        ((InProcessSiloHandle)silo).SiloHost.Services.GetRequiredService<IOptions<ClusterOptions>>().Value;

    private static async Task<List<WalEntry>> ReadAllAsync(IWalStorageProvider provider, string treeId)
    {
        var entries = new List<WalEntry>();
        await foreach (var entry in provider.ReadAsync(treeId, 0, -1L, 100, CancellationToken.None))
        {
            entries.Add(entry);
        }

        return entries;
    }

    private static WalEntry Entry(string treeId, long offset, string key) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation { TreeId = treeId, Kind = MutationKind.Set, Key = key, Value = [1] },
    };

    private sealed class DefaultWalSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
