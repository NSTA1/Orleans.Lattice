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
        // re-claims returned. Pre-arm mode (#5837, rules S and C6): the donor closes its
        // sibling's marker itself with heads and its deferred bound, the shard root only
        // raises its routing generation as it links the split, and the donor, never armed,
        // claims nothing.
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
                    Assert.That(split.All(c => c.DonorLeaf != Guid.Empty && c.DonorToken != Guid.Empty && c.DeferredGeneration == 0), Is.True,
                        $"{sibling}: the split was not closed by its donor with its bound");
                    Assert.That(split.All(c => c.AdmittedShard == 0 && c.AdmittedGeneration > 0), Is.True,
                        $"{sibling}: the donor bound names no admitted generation");
                    Assert.That(split.All(c => c.Heads.All(h => h >= 0)), Is.True,
                        $"{sibling}: the donor's close left a head unset");
                    Assert.That(split.All(c => c.AdmittedGeneration <= shardMarker.RoutingGeneration), Is.True,
                        "a donor bound names a generation the shard root never raised");
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

    [Test]
    public async Task A_range_delete_walk_during_a_pending_deferred_split_close_holds_the_receiver_until_both_close()
    {
        // #5839 / H's gap register: a tree range delete's walk is stamped under the range
        // delete's own generation, not the shard root's routing generation a deferred split
        // close raises, so the deferred close does not bound the walk's records. The range
        // delete's own window on the shard marker does: its close records the claim heads
        // taken after the walk, and a receiver's live-advance view carries them alongside
        // the split close still pending on its own marker.
        const string Name = "handoff-rd-deferred";
        var (tree, shard) = await NewTreeAsync(Name);
        var partitions = await SiloServices.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(Name);
        var physical = (await tree.GetRoutingAsync()).PhysicalTreeId;
        var before = new long[partitions];
        for (var p = 0; p < partitions; p++)
        {
            before[p] = await _cluster.GrainFactory.GetGrain<IWalShardGrain>($"{physical}/{p}").GetNextSequenceAsync(CancellationToken.None);
        }

        await tree.DeleteRangeAsync("k030", "k090");

        var after = new long[partitions];
        for (var p = 0; p < partitions; p++)
        {
            after[p] = await _cluster.GrainFactory.GetGrain<IWalShardGrain>($"{physical}/{p}").GetNextSequenceAsync(CancellationToken.None);
        }

        var shardMarker = (await HandOffMarkerRows.ReadShardAsync(Storage, physical, 0)).State;
        var rdCloses = shardMarker.Closes.Where(c => c.Token.StartsWith("rd:", StringComparison.Ordinal)).ToList();
        Assert.That(rdCloses, Has.Count.EqualTo(1), "the range delete's close is recorded on the shard marker");
        var rdHeads = rdCloses[0].Heads;

        // The receivers: leaves the walk reached, each still holding its split close.
        var receivers = 0;
        var heldByBoth = 0;
        foreach (var leaf in await WalkChainAsync(shard))
        {
            var range = (await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leaf).GetKeyRangeAndNextSiblingAsync()).KeyRange;
            if (string.CompareOrdinal(range.LowKeyInclusive ?? string.Empty, "k090") >= 0
                || (range.HighKeyExclusive is { } high && string.CompareOrdinal(high, "k030") <= 0))
            {
                continue;
            }

            receivers++;
            var view = await shard.GetLiveAdvanceViewAsync(leaf, range.LowKeyInclusive, range.HighKeyExclusive);
            var marker = await MarkerAsync(leaf);
            // A split close still bearing on the receiver: a deferred head, or under rule S a
            // donor-bound close whose bound is pending or which the receiver still carries
            // (a5e2e2ef's S1 on #5868; G4 on #5866).
            var splitPending = marker.Closes.Any(c => c.Token.StartsWith("split:", StringComparison.Ordinal)
                && (c.Heads.Any(h => h == HandOffMarkerCore.DeferredHead)
                    || (c.DonorLeaf != Guid.Empty
                        && (HandOffMarkerCore.IsPendingDonorBound(c) || !HandOffMarkerCore.IsCarryResolvedEverywhere(c, partitions)))));
            Assert.Multiple(() =>
            {
                Assert.That(view.HeldOpen, Is.False, $"{leaf}: the range delete's window closed");
                for (var p = 0; p < partitions; p++)
                {
                    Assert.That(HandOffMarkerCore.HeadAt(view.CloseHeads, p), Is.GreaterThanOrEqualTo(HandOffMarkerCore.HeadAt(rdHeads, p)).And.GreaterThanOrEqualTo(0L),
                        $"{leaf}, partition {p}: the view does not bound the walk's records by the range delete's own close head");
                }
            });
            if (splitPending)
            {
                heldByBoth++;
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(shardMarker.Open.Keys.Where(t => t.StartsWith("rd:", StringComparison.Ordinal)), Is.Empty, "the range delete's window was closed");
            Assert.That(rdHeads, Has.Length.EqualTo(partitions), "one head per WAL partition");
            Assert.That(rdHeads.All(h => h >= 0 && h != HandOffMarkerCore.DeferredHead), Is.True,
                "the range delete's close records real claim heads, never deferred");
            Assert.That(rdCloses[0].DeferredGeneration, Is.Zero, "the range delete's own window is not a deferred routing close");
            Assert.That(after.Sum() - before.Sum(), Is.GreaterThan(0L), "precondition: the walk wrote records");
            for (var p = 0; p < partitions; p++)
            {
                Assert.That(rdHeads[p], Is.InRange(after[p] - 1, after[p]),
                    $"partition {p}: the close head is taken after the walk's last record");
            }

            Assert.That(receivers, Is.GreaterThan(0), "precondition: the walk reached live leaves");
            if (!eagerClaims)
            {
                Assert.That(heldByBoth, Is.GreaterThan(0),
                    "precondition: a receiver still holds a pending or carried split close (pre-arm: no sibling armed)");
            }
        });
        Assert.That(await tree.CountAsync(), Is.EqualTo(60));
    }
}
