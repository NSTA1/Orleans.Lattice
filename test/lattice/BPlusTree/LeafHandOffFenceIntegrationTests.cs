using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end detectors for the leaf-level hand-off fences (epic #5605, member 2) on
/// a live cluster with the append-fence gate open: every split opens its marker on
/// the sibling at the sibling's birth and closes it by the donor's epoch bump with the
/// heads the bump's re-claims returned; every reclaim opens its marker on the survivor
/// before the survivor widens and closes it once the absorbed leaf's entries are
/// retired. No marker is left open, and every write survives the fences.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LeafHandOffFenceIntegrationTests
{
    private SmallLeafClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SmallLeafClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private IServiceProvider SiloServices => ((InProcessSiloHandle)_cluster.Primary).SiloHost.Services;

    private IGrainStorage Storage => SiloServices.GetRequiredKeyedService<IGrainStorage>(LatticeOptions.StorageProviderName);

    private async Task<(ILattice Tree, IShardRootGrain Shard)> NewTreeAsync(string name)
    {
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(name, new TreeRegistryEntry
        {
            ShardCount = 1,
            MaxLeafKeys = SmallLeafClusterFixture.SmallMaxLeafKeys,
        });
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(name);
        for (var i = 0; i < 120; i++)
        {
            await tree.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        return (tree, _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{name}/0"));
    }

    private async Task<List<GrainId>> WalkChainAsync(IShardRootGrain shard)
    {
        var chain = new List<GrainId>();
        var leafId = await shard.GetLeftmostLeafIdAsync();
        while (leafId is { } id && chain.Count < 5_000)
        {
            chain.Add(id);
            leafId = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(id).GetNextSiblingAsync();
        }

        return chain;
    }

    private Task<LeafHandOffState> MarkerAsync(GrainId leaf) =>
        HandOffMarkerRows.ReadLeafAsync(Storage, leaf.GetGuidKey()).ContinueWith(t => t.Result.State, TaskScheduler.Default);

    [Test]
    public async Task Every_split_opens_its_marker_at_the_siblings_birth_and_closes_it_by_the_donors_bump()
    {
        const string Name = "handoff-split";
        var (tree, shard) = await NewTreeAsync(Name);
        var chain = await WalkChainAsync(shard);
        Assert.That(chain, Has.Count.GreaterThan(4), "precondition: the seed split the tree");
        var partitions = await SiloServices.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(Name);

        foreach (var sibling in chain.Skip(1))
        {
            var marker = await MarkerAsync(sibling);
            var split = marker.Closes.Where(c => c.Token.StartsWith("split:", StringComparison.Ordinal)).ToList();
            Assert.Multiple(() =>
            {
                Assert.That(marker.Initialised, Is.True, $"{sibling}: a leaf born under the gate initialises its marker at birth");
                Assert.That(marker.Open, Is.Empty, $"{sibling}: its split's marker was closed");
                Assert.That(split, Is.Not.Empty, $"{sibling}: it was born by a split, whose close is recorded");
                Assert.That(split.All(c => c.Heads.Length == partitions), Is.True, "one head per WAL partition");
                Assert.That(split.All(c => c.Heads.Any(h => h >= 0)), Is.True,
                    "the donor's bump re-claimed at least the partition it had written");
            });
        }

        var donorRow = await WalAppendFenceRows.ReadLeafEpochAsync(Storage, chain[0].GetGuidKey());
        Assert.That(donorRow.State.Epoch, Is.GreaterThan(1), "the leftmost leaf split, so its own epoch was bumped by the close");
        for (var i = 0; i < 120; i++)
        {
            Assert.That(await tree.GetAsync($"k{i:D3}"), Is.Not.Null, $"k{i:D3} survived the fences");
        }
    }

    [Test]
    public async Task Every_reclaim_opens_its_marker_on_the_survivor_and_closes_it_after_retiring_the_absorbed_leaf()
    {
        const string Name = "handoff-reclaim";
        var (tree, shard) = await NewTreeAsync(Name);
        await tree.DeleteRangeAsync("k030", "k090");

        var reclaimed = await shard.ReclaimEmptyLeavesAsync(int.MaxValue);
        var chain = await WalkChainAsync(shard);
        var reclaimCloses = 0;
        foreach (var leaf in chain)
        {
            var marker = await MarkerAsync(leaf);
            Assert.That(marker.Open, Is.Empty, $"{leaf}: no hand-off left open");
            reclaimCloses += marker.Closes.Count(c => c.Token.StartsWith("reclaim:", StringComparison.Ordinal));
        }

        Assert.Multiple(() =>
        {
            Assert.That(reclaimed, Is.GreaterThan(0), "precondition: the pass folded leaves");
            Assert.That(reclaimCloses, Is.InRange(1, reclaimed), "a folded leaf's reclaim marker closes on its surviving predecessor (a survivor later folded itself takes its markers with it)");
        });
        Assert.That(await tree.CountAsync(), Is.EqualTo(60));
    }
}
