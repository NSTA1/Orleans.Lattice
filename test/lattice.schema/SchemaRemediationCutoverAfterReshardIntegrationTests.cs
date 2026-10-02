using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Issue #4206: the tree's <c>[StatelessWorker]</c> grain caches its routing per
/// activation, and a reshard never invalidates it. The remediation cutover arms the
/// source tree's shards from that routing to redirect logical traffic onto the
/// remediated destination, so a map cached before a reshard leaves the shards the
/// reshard added unarmed, and a stale router reading one of them serves the
/// pre-remediation copy.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SchemaRemediationCutoverAfterReshardIntegrationTests
{
    private const int InitialShards = 2;
    private const int GrownShards = 4;

    private SchemaRemediationClusterFixture _fixture = null!;

    private IGrainFactory Grains => _fixture.Cluster.GrainFactory;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SchemaRemediationClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task Cutover_after_a_reshard_arms_every_source_shard()
    {
        var treeId = $"remediate-resharded-{Guid.NewGuid():N}";
        var registry = Grains.GetLatticeRegistry();
        await registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = InitialShards });
        var tree = Grains.GetGrain<ILattice>(treeId);
        var warmed = await tree.GetRoutingAsync();
        Assert.That(warmed.Map.GetPhysicalShardIndices(), Has.Count.EqualTo(InitialShards), "precondition: the warmed map");

        await tree.ReshardAsync(GrownShards);
        Assert.That(
            (await registry.GetShardMapAsync(treeId))?.GetPhysicalShardIndices(),
            Has.Count.EqualTo(GrownShards),
            "precondition: the empty tree was re-pinned");

        var report = await Grains.GetGrain<ILatticeSchemaRemediationGrain>(treeId).StartAsync(
            LatticeValueTransform.Passthrough(),
            new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() }));
        Assert.That(report.Succeeded, Is.True, "precondition: the remediation cut over");

        var unarmed = new List<int>();
        RequestContext.Set(LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey, treeId);
        try
        {
            using (LatticeAccessGateContext.EnterSystemOrigin())
            {
                for (var index = 0; index < GrownShards; index++)
                {
                    try
                    {
                        await Grains.GetGrain<IShardRootGrain>($"{treeId}/{index}").GetAsync("probe");
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

        Assert.That(unarmed, Is.Empty, "the cutover must arm every shard of the source's live map");
    }
}
