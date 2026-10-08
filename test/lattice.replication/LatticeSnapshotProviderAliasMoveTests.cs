using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// A snapshot export reads its rows from the physical copy it stamps. A
/// <c>LatticeGrain</c> caches its physical routing per activation and drops it
/// only on a routing error, so after an alias move that leaves the old copy
/// intact the logical tree's grain keeps reading the old copy. An export that
/// read its rows through that grain, while taking its log heads, generation,
/// lineage and close check from the registry-resolved copy, shipped the old
/// copy's rows under the new copy's lineage - which a re-seed after a source
/// lineage change (issue #4673) installs on the peer as the new lineage.
/// </summary>
[TestFixture]
[Category("Integration")]
public class LatticeSnapshotProviderAliasMoveTests
{
    private const string ClusterId = "snap-alias-move";

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
    public async Task Export_after_an_alias_move_reads_the_rows_of_the_copy_it_stamps()
    {
        const string tree = "snap-alias-move-tree";
        const string moved = tree + "-moved";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(tree, new TreeRegistryEntry { MaxLeafKeys = 16, ShardCount = 1 });

        var logical = _cluster.Client.GetGrain<ILattice>(tree);
        await logical.SetAsync("shared", V("old"));
        await logical.SetAsync("only-in-old-copy", V("old"));

        await registry.RegisterAsync(moved, new TreeRegistryEntry { MaxLeafKeys = 16, ShardCount = 1 });
        var movedCopy = _cluster.Client.GetGrain<ILattice>(moved);
        await movedCopy.SetAsync("shared", V("new"));
        await movedCopy.SetAsync("only-in-new-copy", V("new"));
        await registry.SetAliasAsync(tree, moved);

        Assert.That(S(await logical.GetAsync("shared")), Is.EqualTo("new"),
            "the warmed logical router follows the alias move without retiring the old copy");

        var provider = new LatticeSnapshotProvider(
            _cluster.Client,
            new InMemoryWalCursorRegistry(),
            LatticeSnapshotProviderUnitTests.TestOptions());
        var stream = await provider.ExportAsync(tree, HybridLogicalClock.Zero);
        var rows = new Dictionary<string, string>(StringComparer.Ordinal);
        await foreach (var entry in stream.Entries)
        {
            if (!entry.IsPrepared && !entry.IsTombstone)
            {
                rows[entry.Key] = S(entry.Value);
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(rows.GetValueOrDefault("shared"), Is.EqualTo("new"),
                "the export ships the value of the copy the alias resolves to");
            Assert.That(rows, Does.ContainKey("only-in-new-copy"));
            Assert.That(rows, Does.Not.ContainKey("only-in-old-copy"),
                "a row only the superseded copy holds is not part of the export");
        });
    }

    private static byte[] V(string s) => Encoding.UTF8.GetBytes(s);

    private static string S(byte[]? bytes) => bytes is null ? "<absent>" : Encoding.UTF8.GetString(bytes);

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
