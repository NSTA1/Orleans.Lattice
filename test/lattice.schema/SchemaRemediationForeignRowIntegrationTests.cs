using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Issue #4361: a remediation copies its tree by scanning it, and a shard can hold rows
/// for slots it does not own - an atomic write's cross-migration backstop writes the
/// whole batch into every split shard it may reach. Copying a scanned row put a value
/// into the remediated tree that no reader of the original ever saw. The build takes
/// each scanned key's value from a point read instead, routed to the key's owner, and
/// skips a key a point read finds absent. The foreign rows are planted here directly
/// on a shard that does not own their keys' slots.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SchemaRemediationForeignRowIntegrationTests
{
    private const int ShardCount = 4;
    private const string Current = "{\"v\":\"current\"}";
    private const string Stale = "{\"v\":\"stale\"}";

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
    public async Task The_remediated_copy_holds_what_a_point_read_of_the_original_served()
    {
        var treeId = $"remediate-foreign-{Guid.NewGuid():N}";
        await Grains.GetLatticeRegistry().RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = ShardCount });
        var tree = Grains.GetGrain<ILattice>(treeId);
        var keys = Enumerable.Range(0, 12).Select(i => $"k{i:D2}").ToList();
        foreach (var key in keys)
            await tree.SetAsync(key, Encoding.UTF8.GetBytes(Current));

        var routing = await tree.GetRoutingAsync(forceRefresh: true);
        foreach (var key in keys)
            await PlantForeignRowAsync(routing, key, Stale);
        await PlantForeignRowAsync(routing, "orphan", Stale);
        Assert.That(await tree.GetAsync("orphan"), Is.Null, "precondition: no point read serves the orphan");

        var report = await Grains.GetGrain<ILatticeSchemaRemediationGrain>(treeId).StartAsync(
            LatticeValueTransform.Passthrough(),
            new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Json() }));
        Assert.That(report.Succeeded, Is.True, "precondition: the remediation cut over");

        _ = await tree.GetRoutingAsync(forceRefresh: true);
        var after = await tree.GetManyAsync(keys.Append("orphan").ToList());
        Assert.Multiple(() =>
        {
            Assert.That(keys.Select(k => after.TryGetValue(k, out var v) ? Encoding.UTF8.GetString(v) : null),
                Is.All.EqualTo(Current), "a stale copy was written into the remediated tree");
            Assert.That(after.ContainsKey("orphan"), Is.False, "a key no reader of the original saw was copied");
        });
    }

    /// <summary>Writes <paramref name="value"/> for <paramref name="key"/> on a shard that does not own its slot.</summary>
    private async Task PlantForeignRowAsync(RoutingInfo routing, string key, string value)
    {
        var foreign = (routing.Map.Resolve(key) + 1) % ShardCount;
        using (LatticeSystemOrigin.Enter())
        {
            await Grains.GetGrain<IShardRootGrain>($"{routing.PhysicalTreeId}/{foreign}")
                .SetAsync(key, Encoding.UTF8.GetBytes(value));
        }
    }
}
