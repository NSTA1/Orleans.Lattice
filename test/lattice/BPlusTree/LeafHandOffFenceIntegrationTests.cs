using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end detectors for the leaf-level hand-off fences (epic #5605, member 2) on
/// a live cluster with the append-fence gate open, in both claim modes (#5833): every
/// split opens its marker on the sibling at the sibling's birth and closes it - in
/// eager mode by the donor's epoch bump with the heads the bump's re-claims returned,
/// in pre-arm mode by the shard root's deferred fence at link time; every reclaim
/// opens its marker on the survivor before the survivor widens and closes it. No
/// marker is left open, and every write survives the fences.
/// </summary>
[TestFixture(false)]
[TestFixture(true)]
[Category("Integration")]
public sealed class LeafHandOffFenceIntegrationTests(bool eagerClaims)
{
    private SmallLeafClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SmallLeafClusterFixture { EagerAppendFenceClaims = eagerClaims };
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
    public async Task Every_split_opens_its_marker_at_the_siblings_birth_and_closes_it_by_the_modes_fence()
    {
        // Eager mode: the donor's epoch bump closes the sibling's marker with the heads its
        // re-claims returned. Pre-arm mode (#5833, revision 2, C2, C3, C6): the shard root
        // raises its routing generation in its own turn as it links the split, and closes
        // the sibling's marker with every head deferred; the donor, never armed, claims
        // nothing.
        const string Name = "handoff-split";
        var (tree, shard) = await NewTreeAsync(Name);
        var chain = await WalkChainAsync(shard);
        Assert.That(chain, Has.Count.GreaterThan(4), "precondition: the seed split the tree");
        var partitions = await SiloServices.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(Name);
        var physical = (await tree.GetRoutingAsync()).PhysicalTreeId;
        var shardMarker = (await HandOffMarkerRows.ReadShardAsync(Storage, physical, 0)).State;

        foreach (var sibling in chain.Skip(1))
        {
            var marker = await MarkerAsync(sibling);
            var split = marker.Closes.Where(c => c.Token.StartsWith("split:", StringComparison.Ordinal)).ToList();
            var siblingArmed = (await WalAppendFenceRows.ReadLeafEpochAsync(Storage, sibling.GetGuidKey())).RecordExists;
            Assert.Multiple(() =>
            {
                Assert.That(marker.Initialised, Is.True, $"{sibling}: a leaf born under the gate initialises its marker at birth");
                Assert.That(marker.Open, Is.Empty, $"{sibling}: its split's marker was closed");
                Assert.That(split, Is.Not.Empty, $"{sibling}: it was born by a split, whose close is recorded");
                Assert.That(split.All(c => c.Heads.Length == partitions), Is.True, "one head per WAL partition");
                if (eagerClaims)
                {
                    Assert.That(split.All(c => c.DeferredGeneration == 0), Is.True, $"{sibling}: an eager close was deferred");
                    Assert.That(split.All(c => c.Heads.Any(h => h >= 0)), Is.True,
                        "the donor's bump re-claimed at least the partition it had written");
                }
                else
                {
                    Assert.That(split.All(c => c.DeferredShard == 0 && c.DeferredGeneration > 0), Is.True,
                        $"{sibling}: the split was not closed by the shard root's deferred fence");
                    Assert.That(split.All(c => c.Heads.All(h => h == HandOffMarkerCore.DeferredHead || (siblingArmed && h >= 0))), Is.True,
                        $"{sibling}: a deferred close was resolved without an arm");
                    Assert.That(split.All(c => c.DeferredGeneration <= shardMarker.RoutingGeneration), Is.True,
                        "a deferred close names a generation the shard root never raised");
                }
            });
        }

        var donorRow = await WalAppendFenceRows.ReadLeafEpochAsync(Storage, chain[0].GetGuidKey());
        if (eagerClaims)
        {
            Assert.That(donorRow.State.Epoch, Is.GreaterThan(1), "the leftmost leaf split, so its own epoch was bumped by the close");
        }
        else
        {
            Assert.That(donorRow.RecordExists, Is.False, "an unarmed donor claimed at its split's close");
        }

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
        var closesOfTheMode = 0;
        foreach (var leaf in chain)
        {
            var marker = await MarkerAsync(leaf);
            Assert.That(marker.Open, Is.Empty, $"{leaf}: no hand-off left open");
            var reclaims = marker.Closes.Where(c => c.Token.StartsWith("reclaim:", StringComparison.Ordinal)).ToList();
            reclaimCloses += reclaims.Count;

            // Eager mode retires and records heads at close time; pre-arm mode (#5833,
            // C3, C6) raises the shard root's generation and defers every head.
            closesOfTheMode += reclaims.Count(c => eagerClaims
                ? c.DeferredGeneration == 0
                : c.DeferredShard == 0 && c.DeferredGeneration > 0);
        }

        Assert.Multiple(() =>
        {
            Assert.That(reclaimed, Is.GreaterThan(0), "precondition: the pass folded leaves");
            Assert.That(reclaimCloses, Is.InRange(1, reclaimed), "a folded leaf's reclaim marker closes on its surviving predecessor (a survivor later folded itself takes its markers with it)");
            Assert.That(closesOfTheMode, Is.EqualTo(reclaimCloses), eagerClaims ? "an eager reclaim close was deferred" : "a reclaim close was not deferred in pre-arm mode");
        });
        Assert.That(await tree.CountAsync(), Is.EqualTo(60));
    }

    [Test]
    public async Task A_reclaim_close_retires_the_absorbed_leaf_at_every_partition_before_reading_the_heads()
    {
        // The reclaim close's own fence, which backstops the absorbed leaf's clear: a
        // duplicate of the absorbed leaf still holding its epoch must be refused at
        // every partition once the close has read its heads.
        const string Name = "handoff-reclaim-fence";
        var (tree, _) = await NewTreeAsync(Name);
        var partitions = await SiloServices.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(Name);
        var absorbed = Guid.NewGuid();
        for (var partition = 0; partition < partitions; partition++)
        {
            var claim = await _cluster.GrainFactory.GetGrain<IWalShardGrain>($"{Name}/{partition}")
                .ClaimAppendEpochAsync(absorbed, 3, CancellationToken.None);
            Assert.That(claim.Accepted, Is.True, "precondition: the absorbed leaf holds epoch 3 everywhere");
        }

        var heads = await ShardRootGrain.RetireAbsorbedAndCaptureHeadsAsync(
            SiloServices.GetRequiredService<IGrainFactory>(), Name, partitions, absorbed);

        Assert.That(heads, Has.Length.EqualTo(partitions));
        for (var partition = 0; partition < partitions; partition++)
        {
            var duplicate = await _cluster.GrainFactory.GetGrain<IWalShardGrain>($"{Name}/{partition}").AppendStampedAsync(
                new WalRecord
                {
                    TreeId = Name,
                    Op = MutationKind.Set,
                    Key = "late",
                    Value = [1],
                    Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                },
                WalAppendStamp.ForLeaf(absorbed, 3),
                CancellationToken.None);
            Assert.That(duplicate.Outcome, Is.Not.EqualTo(WalAppendFenceOutcome.Admitted),
                $"partition {partition}: the absorbed leaf's late append is rejected, never allocated above the heads");
        }

        Assert.That(await tree.CountAsync(), Is.EqualTo(120));
    }
}
