using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Issue #4250: routing reads the shard map under the logical tree id, while the
/// remediation destination is written by routing under its own map. A cutover that
/// swapped only the alias left the logical tree routing by the source's map, so every
/// value the destination placed on a shard that map does not route it to read back as
/// absent. The cutover carries the destination's map onto the logical entry, as a
/// resize does (#3880).
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SchemaRemediationCutoverShardMapIntegrationTests
{
    private const int InitialShards = 2;
    private const int GrownShards = 4;
    private const int KeyCount = 64;

    private SchemaRemediationClusterFixture _fixture = null!;

    private IGrainFactory Grains => _fixture.Cluster.GrainFactory;

    private ILatticeRegistry Registry => Grains.GetLatticeRegistry();

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SchemaRemediationClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static string Key(int i) => $"k-{i:D4}";

    [Test]
    public async Task Cutover_of_a_resharded_tree_keeps_every_value_readable()
    {
        var treeId = $"remediate-map-{Guid.NewGuid():N}";
        await RegisterAndGrowAsync(treeId);
        await WriteAsync(treeId);

        await RemediateAsync(treeId);

        Assert.That(await MissingKeysAsync(treeId), Is.Empty,
            "after the cutover the logical tree must route by the map the destination was written under");
    }

    [Test]
    public async Task Cutover_of_a_resharded_aliased_tree_keeps_every_value_readable()
    {
        var (treeId, _) = await RegisterAliasedAndGrowAsync();
        await WriteAsync(treeId);

        await RemediateAsync(treeId);

        Assert.That(await MissingKeysAsync(treeId), Is.Empty,
            "after the cutover the logical tree must route by the map the destination was written under");
    }

    [Test]
    public async Task Cutover_of_a_resharded_aliased_tree_arms_every_source_shard()
    {
        var (treeId, physical) = await RegisterAliasedAndGrowAsync();

        await RemediateAsync(treeId);

        Assert.That(await UnarmedShardsAsync(physical, treeId), Is.Empty,
            "the reshard of an aliased tree routes its physical tree by the logical map, so every shard of that map must be armed");
    }

    /// <summary>
    /// Registers a logical tree aliased onto an empty physical tree pinned at
    /// <see cref="InitialShards"/>, then reshards it through the logical id, which
    /// writes the grown map under the logical entry only.
    /// </summary>
    private async Task<(string Logical, string Physical)> RegisterAliasedAndGrowAsync()
    {
        var treeId = $"remediate-aliased-{Guid.NewGuid():N}";
        var physical = $"{treeId}-physical";
        await Registry.RegisterAsync(physical, new TreeRegistryEntry { ShardCount = InitialShards });
        await Registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = InitialShards });
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Registry.SetAliasAsync(treeId, physical);
        }

        await RegisterAndGrowAsync(treeId);
        Assert.That(await Registry.GetShardMapAsync(physical), Is.Null,
            "precondition: the reshard wrote no map under the physical id");
        return (treeId, physical);
    }

    private async Task RegisterAndGrowAsync(string treeId)
    {
        await Registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = InitialShards });
        await Grains.GetGrain<ILattice>(treeId).ReshardAsync(GrownShards);
        Assert.That(
            (await Registry.GetShardMapAsync(treeId))?.GetPhysicalShardIndices(),
            Has.Count.EqualTo(GrownShards),
            "precondition: the empty tree was re-pinned");
    }

    private async Task WriteAsync(string treeId)
    {
        var tree = Grains.GetGrain<ILattice>(treeId);
        for (var i = 0; i < KeyCount; i++)
        {
            await tree.SetAsync(Key(i), Encoding.UTF8.GetBytes($"{{\"i\":{i}}}"));
        }
    }

    private async Task RemediateAsync(string treeId)
    {
        var report = await Grains.GetGrain<ILatticeSchemaRemediationGrain>(treeId).StartAsync(
            LatticeValueTransform.Passthrough(),
            new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() }));
        Assert.That(report.Succeeded, Is.True, "precondition: the remediation cut over");
    }

    private async Task<List<string>> MissingKeysAsync(string treeId)
    {
        var tree = Grains.GetGrain<ILattice>(treeId);
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

    private async Task<List<int>> UnarmedShardsAsync(string physicalTreeId, string logicalTreeId)
    {
        var unarmed = new List<int>();
        RequestContext.Set(LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey, logicalTreeId);
        try
        {
            using (LatticeAccessGateContext.EnterSystemOrigin())
            {
                for (var index = 0; index < GrownShards; index++)
                {
                    try
                    {
                        await Grains.GetGrain<IShardRootGrain>($"{physicalTreeId}/{index}").GetAsync("probe");
                        unarmed.Add(index);
                    }
                    catch (StaleTreeRoutingException)
                    {
                    }
                }
            }
        }
        finally
        {
            RequestContext.Remove(LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey);
        }

        return unarmed;
    }
}
