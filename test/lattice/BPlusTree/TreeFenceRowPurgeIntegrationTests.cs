using System.Diagnostics.Metrics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end detectors for the purge's per-tree fence-row retirement (epic #5605,
/// #5668) on a live cluster with the append-fence gate open: a purge leaves none of a
/// tree's shard markers, routing-generation entries or range-delete fence state, only
/// the tree id's routing-generation floor; a tree re-created under the same id starts
/// every generation above that floor, so a straddling initiator of the purged
/// incarnation is refused at every partition; and a soft delete followed by a recover
/// keeps every row.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class TreeFenceRowPurgeIntegrationTests
{
    private const int Shards = 2;
    private const string FenceStateName = "tree-range-delete-fence";

    private ClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
        await _cluster.WaitForAppendFenceGateAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private IServiceProvider SiloServices => ((InProcessSiloHandle)_cluster.Primary).SiloHost.Services;

    private IGrainStorage Storage => SiloServices.GetRequiredKeyedService<IGrainStorage>(LatticeOptions.StorageProviderName);

    private async Task<ILattice> NewTreeAsync(string name)
    {
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(name, new TreeRegistryEntry
        {
            MaxLeafKeys = 1024,
            MaxInternalChildren = 16,
            ShardCount = Shards,
        });
        var tree = _cluster.Client.GetGrain<ILattice>(name);
        await WriteAlphabetAsync(tree);
        return tree;
    }

    private static async Task WriteAlphabetAsync(ILattice tree)
    {
        for (var c = 'a'; c <= 'z'; c++)
        {
            await tree.SetAsync(c.ToString(), Encoding.UTF8.GetBytes("v"));
        }
    }

    private Task<int> PartitionsAsync(string name) =>
        SiloServices.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(name).AsTask();

    private static MeterListener ListenRefusedGenerationClaims(string tree, Action onRefused) =>
        MeterListening.StartForInstrument(
            LatticeMetrics.WalAppendFenceClaims,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                var treeMatches = false;
                var generation = false;
                var refused = false;
                foreach (var tag in tags)
                {
                    treeMatches |= tag.Key == LatticeMetrics.TagTree && Equals(tag.Value, tree);
                    generation |= tag.Key == LatticeMetrics.TagKind && Equals(tag.Value, LatticeMetrics.AppendFenceKindGeneration);
                    refused |= tag.Key == LatticeMetrics.AppendFenceClaimRefused.Key && Equals(tag.Value, LatticeMetrics.AppendFenceClaimRefused.Value);
                }

                if (treeMatches && generation && refused)
                {
                    onRefused();
                }
            }));

    private async Task<GrainState<TreeRangeDeleteFenceState>> FenceStateAsync(string name)
    {
        var row = new GrainState<TreeRangeDeleteFenceState>(new TreeRangeDeleteFenceState());
        await Storage.ReadStateAsync(FenceStateName, _cluster.Client.GetGrain<ITreeRangeDeleteFenceGrain>(name).GetGrainId(), row);
        return row;
    }

    /// <summary>Every routing-generation row of the tree, by partition and initiator, that exists.</summary>
    private async Task<Dictionary<(int Partition, WalRoutingInitiator Initiator), long>> GenerationRowsAsync(string name)
    {
        var rows = new Dictionary<(int, WalRoutingInitiator), long>();
        var partitions = await PartitionsAsync(name);
        for (var partition = 0; partition < partitions; partition++)
        {
            foreach (var initiator in TreeRoutingFenceRetirement.Initiators(Shards))
            {
                var row = await WalAppendFenceRows.ReadGenerationAsync(Storage, name, partition, initiator);
                if (row.RecordExists)
                {
                    rows[(partition, initiator)] = row.State.Generation;
                }
            }
        }

        return rows;
    }

    private async Task<long[]> MarkerGenerationsAsync(string name)
    {
        var generations = new long[Shards];
        for (var shard = 0; shard < Shards; shard++)
        {
            var row = await HandOffMarkerRows.ReadShardAsync(Storage, name, shard);
            generations[shard] = row.RecordExists ? row.State.RoutingGeneration : -1;
        }

        return generations;
    }

    [Test]
    public async Task A_purge_retires_every_fence_row_and_the_re_created_tree_starts_above_the_floor()
    {
        const string Name = "purge-fence-rows";
        var tree = await NewTreeAsync(Name);
        await tree.DeleteRangeAsync("c", "x");
        var markersBefore = await MarkerGenerationsAsync(Name);
        var generationRowsBefore = await GenerationRowsAsync(Name);
        var fenceBefore = await FenceStateAsync(Name);
        var highestBefore = Math.Max(
            Math.Max(markersBefore.Max(), generationRowsBefore.Values.DefaultIfEmpty(0).Max()),
            Math.Max(fenceBefore.State.Generation, fenceBefore.State.ClaimedGeneration));
        Assert.Multiple(() =>
        {
            Assert.That(markersBefore, Is.All.GreaterThan(0), "precondition: every shard root raised its generation");
            Assert.That(generationRowsBefore.Keys.Select(k => k.Initiator), Does.Contain(WalRoutingInitiator.TreeRangeDelete),
                "precondition: the range delete claimed its generation at the partitions");
            Assert.That(fenceBefore.RecordExists, Is.True, "precondition: the range-delete fence holds state");
        });

        await tree.DeleteTreeAsync();
        await tree.PurgeTreeAsync();

        var markersAfter = await MarkerGenerationsAsync(Name);
        var generationRowsAfter = await GenerationRowsAsync(Name);
        var fenceAfter = await FenceStateAsync(Name);
        var floors = (await WalAppendFenceRows.ReadRoutingFloorAsync(Storage, Name)).State;
        var floor = WalAppendFenceRows.FloorOf(floors, WalRoutingInitiator.ShardRoot(0));
        Assert.Multiple(() =>
        {
            Assert.That(markersAfter, Is.All.EqualTo(-1), "no shard marker survives the purge");
            Assert.That(generationRowsAfter, Is.Empty, "no routing-generation entry survives the purge");
            Assert.That(fenceAfter.RecordExists, Is.False, "the range-delete fence's state is cleared");
            Assert.That(floor, Is.EqualTo(highestBefore), "the floor covers the highest generation the purge retired");
            foreach (var initiator in TreeRoutingFenceRetirement.Initiators(Shards))
            {
                Assert.That(WalAppendFenceRows.FloorOf(floors, initiator), Is.EqualTo(highestBefore),
                    $"the purge raised every initiator's floor, {initiator} among them, in its one write");
            }
        });

        // The id is reused: a write registers a new tree. Its first initialisation is a
        // fence: before any range delete, every shard root and the range-delete fence
        // have claimed above the floor at every partition, and the heads are recorded.
        var refusedAtInit = 0;
        using (ListenRefusedGenerationClaims(Name, () => Interlocked.Increment(ref refusedAtInit)))
        {
            await WriteAlphabetAsync(tree);
        }

        Assert.That(refusedAtInit, Is.Zero,
            "every first generation was seeded above the floor, so no claim of the re-created tree was refused at it");
        var partitions = await PartitionsAsync(Name);
        var fencedAtInit = await GenerationRowsAsync(Name);
        for (var shard = 0; shard < Shards; shard++)
        {
            var marker = (await HandOffMarkerRows.ReadShardAsync(Storage, Name, shard)).State;
            var close = marker.Closes.SingleOrDefault(c => c.Token == ShardRootGrain.RecreationFenceToken(floor));
            Assert.Multiple(() =>
            {
                Assert.That(marker.Initialised, Is.True, $"shard {shard}'s re-created marker reads initialised");
                Assert.That(close, Is.Not.Null,
                    $"shard {shard}: the purged marker read uninitialised, so its first initialisation ran the fence");
                Assert.That(close?.Heads ?? [], Has.Length.EqualTo(partitions), "with one head per partition");
                for (var partition = 0; partition < partitions; partition++)
                {
                    Assert.That(fencedAtInit.GetValueOrDefault((partition, WalRoutingInitiator.ShardRoot(shard))), Is.EqualTo(floor + 1),
                        $"partition {partition}: shard {shard}'s seeded generation is claimed before its marker reads initialised");
                    Assert.That(fencedAtInit.GetValueOrDefault((partition, WalRoutingInitiator.TreeRangeDelete)), Is.GreaterThan(floor),
                        $"partition {partition}: and so is the range-delete fence's");
                }
            });
        }

        await tree.DeleteRangeAsync("a", "c");
        var markersRecreated = await MarkerGenerationsAsync(Name);
        var generationRowsRecreated = await GenerationRowsAsync(Name);
        Assert.Multiple(() =>
        {
            Assert.That(markersRecreated, Is.All.EqualTo(floor + 1), "each re-created shard root starts its generation above the floor");
            Assert.That(generationRowsRecreated.Values, Is.All.GreaterThan(floor), "and every generation claimed since is above it");
        });

        // A straddling initiator of the purged incarnation presents its old generation.
        for (var partition = 0; partition < partitions; partition++)
        {
            var wal = _cluster.Client.GetGrain<IWalShardGrain>($"{Name}/{partition}");
            for (var shard = 0; shard < Shards; shard++)
            {
                var stale = await wal.ClaimRoutingGenerationAsync(WalRoutingInitiator.ShardRoot(shard), markersBefore[shard], CancellationToken.None);
                Assert.That(stale.Accepted, Is.False, $"partition {partition}: shard {shard}'s purged generation is refused");
            }

            var staleFence = await wal.ClaimRoutingGenerationAsync(
                WalRoutingInitiator.TreeRangeDelete, fenceBefore.State.Generation, CancellationToken.None);
            Assert.That(staleFence.Accepted, Is.False, $"partition {partition}: the purged fence's generation is refused");
        }

        Assert.That(await tree.CountAsync(), Is.EqualTo(24), "the re-created tree serves its own writes");
    }

    [Test]
    public async Task A_shard_root_of_a_tree_id_with_a_floor_seeds_and_claims_its_first_generation_above_it()
    {
        // The seeding alone (#5668), on a fresh activation: no purge ran here, so no
        // cached marker of an earlier incarnation is involved.
        const string Name = "floored-fence-rows";
        const long Floor = 4;
        await WalAppendFenceRows.RaiseRoutingFloorsAsync(
            Storage, Name, TreeRoutingFenceRetirement.Initiators(Shards).ToDictionary(i => i, _ => Floor));
        var refused = 0;
        using (ListenRefusedGenerationClaims(Name, () => Interlocked.Increment(ref refused)))
        {
            await NewTreeAsync(Name);
        }

        var markers = await MarkerGenerationsAsync(Name);
        var rows = await GenerationRowsAsync(Name);
        var partitions = await PartitionsAsync(Name);
        Assert.Multiple(() =>
        {
            Assert.That(refused, Is.Zero, "no first generation was seeded at or below the floor, so none was refused at it");
            Assert.That(markers, Is.All.EqualTo(Floor + 1), "each shard root seeds its first generation at the floor plus one");
            for (var partition = 0; partition < partitions; partition++)
            {
                for (var shard = 0; shard < Shards; shard++)
                {
                    Assert.That(rows.GetValueOrDefault((partition, WalRoutingInitiator.ShardRoot(shard))), Is.EqualTo(Floor + 1),
                        $"partition {partition}: shard {shard} claimed its seeded generation before its marker read initialised");
                }
            }
        });
    }

    [Test]
    public async Task A_tree_re_created_after_a_purge_that_retired_nothing_never_adopts_its_markers()
    {
        // A purge that finished on a 10.2.0 silo retires nothing (#5703 review, S2). It
        // is emulated by restoring the markers and dropping the floor after the purge.
        const string Name = "purge-on-old-silo";
        var tree = await NewTreeAsync(Name);
        var markersBefore = await MarkerGenerationsAsync(Name);
        await tree.DeleteTreeAsync();
        await tree.PurgeTreeAsync();
        for (var shard = 0; shard < Shards; shard++)
        {
            var generation = markersBefore[shard];
            await HandOffMarkerRows.UpdateShardAsync(Storage, Name, shard, m =>
            {
                m.RoutingGeneration = generation;
                m.Initialised = true;
                return true;
            });
        }

        var floorRow = await WalAppendFenceRows.ReadRoutingFloorAsync(Storage, Name);
        await Storage.ClearStateAsync(
            WalAppendFenceRows.RoutingFloorGrainType,
            WalAppendFenceRows.RowId(WalAppendFenceRows.RoutingFloorGrainType, WalAppendFenceRows.RoutingFloorKey(Name)),
            floorRow);

        await WriteAlphabetAsync(tree);

        var floors = (await WalAppendFenceRows.ReadRoutingFloorAsync(Storage, Name)).State;
        var partitions = await PartitionsAsync(Name);
        var rows = await GenerationRowsAsync(Name);
        var lineage = (await _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).GetEntryAsync(Name))!.Lineage;
        for (var shard = 0; shard < Shards; shard++)
        {
            var marker = (await HandOffMarkerRows.ReadShardAsync(Storage, Name, shard)).State;
            var initiator = WalRoutingInitiator.ShardRoot(shard);
            var floor = WalAppendFenceRows.FloorOf(floors, initiator);
            Assert.Multiple(() =>
            {
                Assert.That(floor, Is.EqualTo(markersBefore[shard]), $"shard {shard}'s re-seed raised its own floor over the marker it found");
                Assert.That(marker.RoutingGeneration, Is.EqualTo(floor + 1), "the re-created shard root did not adopt the old generation");
                Assert.That(marker.Closes.Any(c => c.Token == ShardRootGrain.RecreationFenceToken(floor)), Is.True,
                    "its first initialisation ran the fence");
                Assert.That(marker.ReseedLineage, Is.Not.Null.And.EqualTo(lineage),
                    "the re-seed stamped the re-created registration's lineage (#5716)");
                for (var partition = 0; partition < partitions; partition++)
                {
                    Assert.That(rows.GetValueOrDefault((partition, initiator)), Is.EqualTo(floor + 1),
                        $"partition {partition}: shard {shard}'s new generation is claimed there");
                }
            });
        }
    }

    [Test]
    public async Task A_live_re_created_trees_shard_root_reactivated_after_its_lineage_moved_runs_no_catch_up()
    {
        // #5716: the catch-up runs only on the re-seed that lifts a purge tombstone. A live
        // re-created tree whose lineage later moves (an alias set and removed again, as an
        // undo-resize does) keeps its markers across a shard root reactivation.
        const string Name = "recreated-lineage-moves";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var tree = await NewTreeAsync(Name);
        await tree.DeleteTreeAsync();
        await tree.PurgeTreeAsync();
        await WriteAlphabetAsync(tree);
        var stamped = (await registry.GetEntryAsync(Name))!.Lineage;
        var markersBefore = new ShardHandOffState[Shards];
        for (var shard = 0; shard < Shards; shard++)
        {
            markersBefore[shard] = (await HandOffMarkerRows.ReadShardAsync(Storage, Name, shard)).State;
        }

        var floorsBefore = (await WalAppendFenceRows.ReadRoutingFloorAsync(Storage, Name)).State.Floors.ToDictionary(kv => kv.Key, kv => kv.Value);

        const string Other = Name + "-other";
        await registry.RegisterAsync(Other, new TreeRegistryEntry { MaxLeafKeys = 1024, MaxInternalChildren = 16, ShardCount = Shards });
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await registry.SetAliasAsync(Name, Other);
            await registry.RemoveAliasAsync(Name);
        }

        var moved = (await registry.GetEntryAsync(Name))!.Lineage;
        Assert.That(moved, Is.Not.EqualTo(stamped), "precondition: the alias moves restamped the tree's lineage");

        for (var shard = 0; shard < Shards; shard++)
        {
            await _cluster.Client.GetGrain<IShardRootGrain>($"{Name}/{shard}").ForceDeactivateAsync();
        }

        await Task.Delay(500);

        // Admitted on the reactivated shard roots, never refused as a deleted tree.
        Assert.That(
            async () =>
            {
                await tree.SetAsync("after-move", Encoding.UTF8.GetBytes("v")).WaitAsync(TimeSpan.FromSeconds(20));
                await tree.DeleteRangeAsync("a", "c").WaitAsync(TimeSpan.FromSeconds(20));
            },
            Throws.Nothing,
            "the live tree's writes are admitted after the reactivation");

        var floorsAfter = (await WalAppendFenceRows.ReadRoutingFloorAsync(Storage, Name)).State.Floors;
        for (var shard = 0; shard < Shards; shard++)
        {
            var marker = (await HandOffMarkerRows.ReadShardAsync(Storage, Name, shard)).State;
            var before = markersBefore[shard];
            Assert.Multiple(() =>
            {
                Assert.That(marker.ReseedLineage, Is.EqualTo(stamped), $"shard {shard}: no catch-up restamped the marker");
                Assert.That(marker.RoutingGeneration, Is.GreaterThanOrEqualTo(before.RoutingGeneration), $"shard {shard}: its generation was not reset");
                Assert.That(marker.Initialised, Is.True, $"shard {shard}: its marker still reads initialised");
            });
        }

        Assert.That(floorsAfter, Is.EquivalentTo(floorsBefore), "no floor was raised");
    }

    [Test]
    public async Task A_tree_id_never_purged_initialises_without_the_recreation_fence()
    {
        const string Name = "never-purged-fence-rows";
        await NewTreeAsync(Name);

        var generationRows = await GenerationRowsAsync(Name);
        Assert.That(generationRows.Keys.Select(k => k.Initiator), Does.Not.Contain(WalRoutingInitiator.TreeRangeDelete),
            "floor 0: the first initialisation claims nothing extra, so the range-delete fence never claimed");
        for (var shard = 0; shard < Shards; shard++)
        {
            var marker = (await HandOffMarkerRows.ReadShardAsync(Storage, Name, shard)).State;
            Assert.Multiple(() =>
            {
                Assert.That(marker.Initialised, Is.True);
                Assert.That(marker.RoutingGeneration, Is.EqualTo(1));
                Assert.That(marker.Closes, Is.Empty, $"shard {shard} recorded no recreation close");
            });
        }
    }

    [Test]
    public async Task A_soft_delete_followed_by_a_recover_keeps_every_fence_row()
    {
        const string Name = "recover-fence-rows";
        var tree = await NewTreeAsync(Name);
        await tree.DeleteRangeAsync("c", "x");
        var markersBefore = await MarkerGenerationsAsync(Name);
        var generationRowsBefore = await GenerationRowsAsync(Name);
        var fenceBefore = await FenceStateAsync(Name);

        await tree.DeleteTreeAsync();
        await tree.RecoverTreeAsync();

        var markersAfter = await MarkerGenerationsAsync(Name);
        var generationRowsAfter = await GenerationRowsAsync(Name);
        var fenceAfter = await FenceStateAsync(Name);
        var floor = await WalAppendFenceRows.ReadRoutingFloorAsync(Storage, Name);
        Assert.Multiple(() =>
        {
            Assert.That(markersAfter, Is.EqualTo(markersBefore), "the recovered tree keeps its shard markers");
            Assert.That(generationRowsAfter, Is.EquivalentTo(generationRowsBefore), "and its routing-generation entries");
            Assert.That(fenceAfter.RecordExists, Is.True, "and its range-delete fence's state");
            Assert.That(fenceAfter.State.Generation, Is.EqualTo(fenceBefore.State.Generation));
            Assert.That(floor.RecordExists, Is.False, "nothing was retired, so no floor is written");
        });
    }
}
