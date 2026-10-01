using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Issue #4146: once a reshard reports <c>InProgress = false</c>, every tree-admin
/// read that names the tree's shards must agree on the physical shard count and
/// the map version - the live routing inspection, the registry's persisted map,
/// the reshard status and the tree statistics. Driven on a real single-silo
/// cluster, with the facade over the cluster client, through a grow and a
/// shrink, on a plain tree and on an aliased one.
/// </summary>
/// <remarks>
/// Each test reads every surface once before the reshard, as an operator's
/// open page does. That warms the per-activation routing the tree's stateless
/// worker keeps, which is exactly what used to go on answering with the
/// pre-reshard map. The diagnostics caches are switched off so this fixture
/// measures that unbounded staleness rather than their brief, by-design one.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class LatticeTreeAdminReshardConsistencyTests
{
    private const int ShardCount = 4;
    private const int MaxLeafKeys = 4;

    private TestCluster _cluster = null!;
    private LatticeTreeAdmin _admin = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder { Options = { InitialSilosCount = 1 } };
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        _admin = new LatticeTreeAdmin(
            Substitute.For<ILatticeSchemaControl>(),
            _cluster.Client,
            new TreeAdminAccessAuthorizer(new AllowGate()),
            Options.Create(new LatticeApiTreeAdminOptions()),
            new NullTenantContextResolver());
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [Test]
    public async Task Every_shard_surface_agrees_after_a_grow()
    {
        var tree = await CreatePopulatedTreeAsync($"grow-{Guid.NewGuid():N}");
        await ReadEverySurfaceAsync(tree);

        await _admin.ReshardTreeAsync(tree, 8);
        await DriveGrowAsync(tree);

        await AssertEverySurfaceAgreesAsync(tree, expectedShards: 8);
    }

    [Test]
    public async Task Every_shard_surface_agrees_after_a_shrink()
    {
        var tree = await CreatePopulatedTreeAsync($"shrink-{Guid.NewGuid():N}");
        await ReadEverySurfaceAsync(tree);

        await _admin.ReshardTreeAsync(tree, 2);
        await DriveShrinkAsync(tree, tree);

        await AssertEverySurfaceAgreesAsync(tree, expectedShards: 2);
    }

    [Test]
    public async Task Every_shard_surface_agrees_after_a_grow_then_a_shrink_on_an_aliased_tree()
    {
        var physical = await CreatePopulatedTreeAsync($"physical-{Guid.NewGuid():N}");
        var logical = $"aliased-{Guid.NewGuid():N}";
        await Registry.RegisterAsync(logical, new TreeRegistryEntry { MaxLeafKeys = MaxLeafKeys, ShardCount = ShardCount });
        await _admin.SetTreeAliasAsync(logical, physical);
        Assert.That((await _admin.ResolveTreeAliasAsync(logical)).PhysicalTreeId, Is.EqualTo(physical), "precondition: the tree is aliased");
        await ReadEverySurfaceAsync(logical);

        await _admin.ReshardTreeAsync(logical, 8);
        await DriveGrowAsync(logical);
        await AssertEverySurfaceAgreesAsync(logical, expectedShards: 8);

        await _admin.ReshardTreeAsync(logical, 2);
        await DriveShrinkAsync(logical, physical);
        await AssertEverySurfaceAgreesAsync(logical, expectedShards: 2);
    }

    private ILatticeRegistry Registry => _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    private async Task<string> CreatePopulatedTreeAsync(string treeId)
    {
        await Registry.RegisterAsync(treeId, new TreeRegistryEntry { MaxLeafKeys = MaxLeafKeys, ShardCount = ShardCount });
        var lattice = _cluster.Client.GetGrain<ILattice>(treeId);
        for (var i = 0; i < 120; i++)
        {
            await lattice.SetAsync($"k-{i:D4}", [(byte)i]);
        }

        return treeId;
    }

    private async Task ReadEverySurfaceAsync(string treeId)
    {
        var inspection = await _admin.InspectShardMapAsync(treeId);
        var stats = await _admin.GetTreeStatsAsync(treeId);
        Assert.Multiple(() =>
        {
            Assert.That(inspection.PhysicalShardCount, Is.EqualTo(ShardCount), "precondition: the inspection starts at the pinned count");
            Assert.That(stats.ShardCount, Is.EqualTo(ShardCount), "precondition: the statistics start at the pinned count");
        });
    }

    private async Task AssertEverySurfaceAgreesAsync(string treeId, int expectedShards)
    {
        var status = await _admin.GetReshardStatusAsync(treeId);
        Assert.That(status.InProgress, Is.False, "precondition: the reshard has finished");

        var inspection = await _admin.InspectShardMapAsync(treeId);
        var persisted = await _admin.GetShardMapAsync(treeId);
        var stats = await _admin.GetTreeStatsAsync(treeId);
        var diagnostics = await _admin.GetDiagnosticsAsync(treeId);
        var config = await _admin.GetTreeConfigAsync(treeId);

        var figures =
            $"inspection {inspection.PhysicalShardCount} shards v{inspection.MapVersion}, " +
            $"persisted {persisted.PhysicalShardCount} shards v{persisted.MapVersion}, " +
            $"status {status.CurrentPhysicalShardCount} shards v{status.MapVersion}, " +
            $"stats {stats.ShardCount} shards, diagnostics {diagnostics.Shards.Length} shards, config {config.ShardCount} shards";
        Assert.Multiple(() =>
        {
            Assert.That(config.ShardCount, Is.EqualTo(expectedShards), figures);
            Assert.That(persisted.HasCustomMap, Is.True, figures);
            Assert.That(persisted.PhysicalShardCount, Is.EqualTo(expectedShards), figures);
            Assert.That(status.CurrentPhysicalShardCount, Is.EqualTo(expectedShards), figures);
            Assert.That(inspection.PhysicalShardCount, Is.EqualTo(expectedShards), figures);
            Assert.That(inspection.MapVersion, Is.EqualTo(persisted.MapVersion), figures);
            Assert.That(inspection.MapVersion, Is.EqualTo(status.MapVersion), figures);
            Assert.That(inspection.PhysicalShardIndices, Is.EqualTo(persisted.PhysicalShardIndices), figures);
            Assert.That(inspection.SlotCounts, Has.Length.EqualTo(expectedShards), figures);
            Assert.That(inspection.SlotCounts.Sum(), Is.EqualTo(inspection.VirtualShardCount), figures);
            Assert.That(inspection.SlotCounts, Has.All.GreaterThan(1), "a shard owns many of the 4,096 virtual slots, never one");
            Assert.That(stats.ShardCount, Is.EqualTo(expectedShards), figures);
            Assert.That(diagnostics.Shards.Select(shard => shard.ShardIndex), Is.EqualTo(persisted.PhysicalShardIndices), figures);
        });
    }

    /// <summary>Drives the reshard coordinator and the splits it starts until it is idle.</summary>
    private async Task DriveGrowAsync(string treeId)
    {
        var reshard = _cluster.Client.GetGrain<ITreeReshardGrain>(treeId);
        for (var pass = 0; pass < 100; pass++)
        {
            if (await reshard.IsIdleAsync())
            {
                return;
            }

            await reshard.RunReshardPassAsync();
            var map = await Registry.GetShardMapAsync(treeId)
                ?? ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, ShardCount);
            foreach (var index in map.GetPhysicalShardIndices())
            {
                var split = _cluster.Client.GetGrain<ITreeShardSplitGrain>($"{treeId}/{index}");
                if (!await split.IsIdleAsync())
                {
                    await split.RunSplitPassAsync();
                }
            }

            await Task.Delay(50);
        }

        Assert.Fail("The grow did not converge.");
    }

    /// <summary>Drives the reshard coordinator and the folds it starts until it is idle.</summary>
    private async Task DriveShrinkAsync(string treeId, string physicalTreeId)
    {
        var reshard = _cluster.Client.GetGrain<ITreeReshardGrain>(treeId);
        for (var pass = 0; pass < 200; pass++)
        {
            if (await reshard.IsIdleAsync())
            {
                return;
            }

            await reshard.RunReshardPassAsync();
            for (var index = 0; index < 16; index++)
            {
                foreach (var key in new[] { $"{treeId}/{index}", $"{physicalTreeId}/{index}" }.Distinct())
                {
                    var fold = _cluster.Client.GetGrain<ITreeShardConsolidationGrain>(key);
                    if (!await fold.IsIdleAsync())
                    {
                        await fold.RunConsolidationPassAsync();
                    }
                }
            }

            await Task.Delay(50);
        }

        Assert.Fail("The shrink did not converge.");
    }

    private sealed class AllowGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(in LatticeAccessRequest request, CancellationToken cancellationToken = default)
            => new(LatticeAccessDecision.Allow());
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(options =>
            {
                options.DiagnosticsCacheTtl = TimeSpan.Zero;
                options.StorageUsageCacheTtl = TimeSpan.Zero;
            });
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
