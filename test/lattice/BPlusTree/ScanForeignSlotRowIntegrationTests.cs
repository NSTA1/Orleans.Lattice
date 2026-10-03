using System.Text;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4361: when several shards hold a row for a key, an entries scan takes the
/// key's value from the shard the map routes the key to, as a point read does. A shard
/// can hold rows for slots it never owned - an atomic write's cross-migration backstop
/// writes the whole batch into every transitively-discovered split shard - and a scan
/// used to keep whichever copy it dequeued first, returning a stale value (and,
/// through a schema remediation that copies a tree by scanning it, writing the stale
/// value into the remediated copy). The foreign row is planted here directly on a
/// shard that does not own its key's slot.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ScanForeignSlotRowIntegrationTests
{
    private const int ShardCount = 4;

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder { Options = { InitialSilosCount = 1 } };
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private IGrainFactory Grains => _cluster.Client;

    [Test]
    public async Task Entry_scans_return_the_owners_value_not_a_foreign_copy()
    {
        var (tree, routing) = await TreeAsync();
        var keys = Enumerable.Range(0, 12).Select(i => $"k{i:D2}").ToList();
        foreach (var key in keys)
            await tree.SetAsync(key, Encoding.UTF8.GetBytes("current"));
        foreach (var key in keys)
            await PlantForeignRowAsync(routing, key, "stale");

        var forward = new Dictionary<string, string>();
        await foreach (var entry in tree.ScanEntriesAsync())
            forward[entry.Key] = Encoding.UTF8.GetString(entry.Value);
        var reverse = new Dictionary<string, string>();
        await foreach (var entry in tree.ScanEntriesAsync(reverse: true))
            reverse[entry.Key] = Encoding.UTF8.GetString(entry.Value);

        Assert.Multiple(() =>
        {
            Assert.That(forward.Keys, Is.EquivalentTo(keys));
            Assert.That(forward.Values, Is.All.EqualTo("current"), "a forward scan took a foreign copy");
            Assert.That(reverse.Values, Is.All.EqualTo("current"), "a reverse scan took a foreign copy");
        });
    }

    private async Task<(ILattice Tree, RoutingInfo Routing)> TreeAsync()
    {
        var treeId = $"scan-foreign-{Guid.NewGuid():N}";
        await Grains.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = ShardCount });
        var tree = Grains.GetGrain<ILattice>(treeId);
        return (tree, await tree.GetRoutingAsync(forceRefresh: true));
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

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
