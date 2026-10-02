using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// Issue #4263: routing reads the shard map under the logical tree id, so an
/// explicit <see cref="LatticeTreeAdmin.SetTreeAliasAsync"/> that swapped only the
/// alias left the logical tree addressing the target's shards by its own map. Every
/// key the target placed by a different map then read back as absent through the
/// logical tree. The verb now carries the target's map onto the logical entry, as
/// the resize, restore and schema cutovers do (#3880, #4250), and writes the map it
/// replaces to the previous physical tree's own entry so a later alias back onto it
/// finds its layout.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeTreeAdminAliasShardMapIntegrationTests
{
    private const int InitialShards = 2;
    private const int GrownShards = 4;
    private const int KeyCount = 64;

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

    private ILatticeRegistry Registry => _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    private static string Key(int i) => $"k-{i:D4}";

    [Test]
    public async Task SetTreeAliasAsync_onto_a_resharded_target_keeps_every_target_key_readable()
    {
        var physical = await RegisterAndGrowAsync($"target-{Guid.NewGuid():N}");
        await WriteAsync(physical);
        var logical = $"logical-{Guid.NewGuid():N}";
        await Registry.RegisterAsync(logical, new TreeRegistryEntry { ShardCount = InitialShards });

        await _admin.SetTreeAliasAsync(logical, physical);

        Assert.That(await MissingKeysAsync(logical), Is.Empty,
            "the logical tree must route the target's shards by the map the target placed its keys by");
    }

    [Test]
    public async Task SetTreeAliasAsync_onto_a_target_pinned_at_a_different_shard_count_keeps_every_target_key_readable()
    {
        var physical = $"target-{Guid.NewGuid():N}";
        await Registry.RegisterAsync(physical, new TreeRegistryEntry { ShardCount = GrownShards });
        await WriteAsync(physical);
        var logical = $"logical-{Guid.NewGuid():N}";
        await Registry.RegisterAsync(logical, new TreeRegistryEntry { ShardCount = InitialShards });

        await _admin.SetTreeAliasAsync(logical, physical);

        Assert.That(await MissingKeysAsync(logical), Is.Empty,
            "a target with no persisted map is routed by the default map for its own shard-count pin");
    }

    [Test]
    public async Task SetTreeAliasAsync_back_onto_a_previous_target_resharded_through_the_alias_keeps_every_key_readable()
    {
        var first = $"first-{Guid.NewGuid():N}";
        var second = $"second-{Guid.NewGuid():N}";
        var logical = $"logical-{Guid.NewGuid():N}";
        await Registry.RegisterAsync(first, new TreeRegistryEntry { ShardCount = InitialShards });
        await Registry.RegisterAsync(second, new TreeRegistryEntry { ShardCount = InitialShards });
        await Registry.RegisterAsync(logical, new TreeRegistryEntry { ShardCount = InitialShards });

        await _admin.SetTreeAliasAsync(logical, first);
        await RegisterAndGrowAsync(logical);
        Assert.That(await Registry.GetShardMapAsync(first), Is.Null,
            "precondition: the reshard through the alias wrote no map under the physical id");
        await WriteAsync(logical);

        await _admin.SetTreeAliasAsync(logical, second);
        await _admin.SetTreeAliasAsync(logical, first);

        Assert.That(await MissingKeysAsync(logical), Is.Empty,
            "the map the logical tree addressed the first target by must travel with it and back");
    }

    [Test]
    public async Task SetTreeAliasAsync_refused_by_the_registry_leaves_the_logical_map_unchanged()
    {
        var physical = await RegisterAndGrowAsync($"target-{Guid.NewGuid():N}");
        var inner = $"inner-{Guid.NewGuid():N}";
        await Registry.RegisterAsync(inner, new TreeRegistryEntry { ShardCount = InitialShards });
        await _admin.SetTreeAliasAsync(physical, inner);
        var logical = $"logical-{Guid.NewGuid():N}";
        await Registry.RegisterAsync(logical, new TreeRegistryEntry { ShardCount = InitialShards });
        var before = await Registry.GetEntryAsync(logical);

        Assert.That(async () => await _admin.SetTreeAliasAsync(logical, physical),
            Throws.InvalidOperationException, "precondition: a multi-level alias is refused");

        var after = await Registry.GetEntryAsync(logical);
        Assert.Multiple(() =>
        {
            Assert.That(after?.ShardMap, Is.EqualTo(before?.ShardMap), "a refused alias must not move the logical map");
            Assert.That(after?.PhysicalTreeId, Is.Null);
        });
    }

    private async Task<string> RegisterAndGrowAsync(string treeId)
    {
        await Registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = InitialShards });
        await _cluster.Client.GetGrain<ILattice>(treeId).ReshardAsync(GrownShards);
        Assert.That(
            (await Registry.GetShardMapAsync(treeId))?.GetPhysicalShardIndices(),
            Has.Count.EqualTo(GrownShards),
            "precondition: the empty tree was re-pinned");
        return treeId;
    }

    private async Task WriteAsync(string treeId)
    {
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        for (var i = 0; i < KeyCount; i++)
        {
            await tree.SetAsync(Key(i), [(byte)i]);
        }
    }

    private async Task<List<string>> MissingKeysAsync(string treeId)
    {
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        _ = await tree.GetRoutingAsync(forceRefresh: true);
        var missing = new List<string>();
        for (var i = 0; i < KeyCount; i++)
        {
            if (await tree.GetAsync(Key(i)) is null)
            {
                missing.Add(Key(i));
            }
        }

        return missing;
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
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
