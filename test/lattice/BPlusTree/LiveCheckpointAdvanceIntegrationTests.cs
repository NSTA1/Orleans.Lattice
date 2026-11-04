using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end detectors for the live checkpoint advance (issues #5570, #5572, #5593;
/// epic #5605, member 4) on a live cluster whose append-fence gate is open, driven
/// only by the leaf's real coverage-lag timer:
/// <list type="bullet">
///   <item><description><b>#5570.</b> A leaf written only through the live write path
///   banks a loadable snapshot over its own writes without the timer ever dispatching
///   a starvation replay drive: the latency that held every tree's WAL floor on a fresh
///   estate.</description></item>
///   <item><description><b>#5593.</b> A stale duplicate activation of the leaf appended an
///   acknowledged write before the live activation's claim. The live activation must
///   read it before it advances past it: the duplicate's row reaches the snapshot.</description></item>
///   <item><description><b>Unstamped reported.</b> After the leaf has live-advanced, a
///   foreign unstamped append of a key it owns is reported to it; it falls back to replay
///   and the row reaches the snapshot.</description></item>
/// </list>
/// Without the fences each of the last two would be passed over unread, and a WAL trim
/// would then lose the row for good.
/// </summary>
[TestFixture(false)]
[TestFixture(true)]
[Category("Integration")]
public sealed partial class LiveCheckpointAdvanceIntegrationTests(bool eagerClaims)
{
    private const string LatencyTree = "live-advance-latency";
    private const string DuplicateTree = "live-advance-duplicate";
    private const string UnstampedTree = "live-advance-unstamped";
    private const string OrphanTree = "live-advance-orphan";
    private const string BulkTree = "live-advance-bulk";
    private static readonly string[] Trees = [LatencyTree, DuplicateTree, UnstampedTree, OrphanTree, BulkTree, BulkRawTree, BulkGraftTree];
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(45);
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task SetUpAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        if (eagerClaims)
        {
            // #5833: every test also runs in eager-claim mode, the path while the
            // pre-arm capability is not advertised by every silo.
            builder.AddSiloBuilderConfigurator<EagerAppendFenceSiloConfigurator>();
        }

        _cluster = builder.Build();
        await _cluster.DeployAsync();
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        foreach (var tree in Trees)
        {
            await registry.RegisterAsync(tree, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 128, WalPartitions = 1 });
        }
    }

    [OneTimeTearDown]
    public async Task TearDownAsync()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private IServiceProvider SiloServices => ((InProcessSiloHandle)_cluster.Primary).SiloHost.Services;

    private IGrainStorage Storage => SiloServices.GetRequiredKeyedService<IGrainStorage>(LatticeOptions.StorageProviderName);

    [Test]
    public async Task A_live_written_leaf_banks_its_snapshot_without_a_replay_drive()
    {
        var tree = _cluster.Client.GetGrain<ILattice>(LatencyTree);
        await tree.GetAsync("warm");
        var (_, snapshot) = await ResolveLeafAsync(LatencyTree);
        Assert.That(SiloServices.GetRequiredService<IWalAppendFenceGate>().IsOpen, Is.True, "precondition: the gate is open");

        var reasons = new List<string>();
        LeafSnapshotBlob? banked;
        long head;
        using (ListenForDriverDeclines(LatencyTree, reasons))
        {
            for (var i = 0; i < 5; i++)
            {
                await tree.SetAsync($"k{i}", [(byte)i]);
            }

            head = await HeadAsync(LatencyTree);
            banked = await WaitForCoverageAsync(snapshot, head - 1, "k4");
        }

        Assert.Multiple(() =>
        {
            Assert.That(banked, Is.Not.Null, "the timer banks a loadable snapshot over the leaf's own writes");
            Assert.That(reasons, Does.Not.Contain(LatticeMetrics.DriverDeclineRecheckNoDurableCheckpoint.Value),
                "#5570: the tick never took the starved arm, whose drive needs a replay permit; the live advance banked the writes");
        });
    }

    [Test]
    public async Task A_stale_duplicates_append_before_the_claim_is_read_before_the_leaf_advances_past_it()
    {
        var tree = _cluster.Client.GetGrain<ILattice>(DuplicateTree);
        await tree.GetAsync("warm");
        var (leaf, snapshot) = await ResolveLeafAsync(DuplicateTree);
        var leafId = leaf.GetGrainId().GetGuidKey();

        // A stale duplicate activation D of the leaf (#5593): it holds epoch 1 - its row,
        // its claim at the partition - and appends an acknowledged write there before the
        // live activation's first append claims.
        var row = await WalAppendFenceRows.ReadLeafEpochAsync(Storage, leafId);
        Assert.That(row.State.Epoch, Is.Zero, "precondition: the live activation has not claimed yet");
        row.State.Epoch = 1;
        await WalAppendFenceRows.WriteAsync(Storage, WalAppendFenceRows.LeafEpochGrainType, WalAppendFenceRows.LeafEpochKey(leafId), row);
        var wal = _cluster.Client.GetGrain<IWalShardGrain>($"{DuplicateTree}/0");
        Assert.That((await wal.ClaimAppendEpochAsync(leafId, 1, CancellationToken.None)).Accepted, Is.True);
        var duplicate = await wal.AppendStampedAsync(Record(DuplicateTree, "dup", 7), WalAppendStamp.ForLeaf(leafId, 1), CancellationToken.None);
        Assert.That(duplicate.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted), "precondition: the duplicate's write is acknowledged");

        // The live activation writes: it claims epoch 2, above the duplicate's append.
        await tree.SetAsync("own", [1]);
        var head = await HeadAsync(DuplicateTree);
        var banked = await WaitForCoverageAsync(snapshot, head - 1, "dup");

        var read = await tree.GetAsync("dup");

        Assert.Multiple(() =>
        {
            Assert.That(banked, Is.Not.Null,
                "the duplicate's acknowledged row reaches the leaf's snapshot: the leaf read to its claim head before advancing past it (#5593)");
            Assert.That(read, Is.EqualTo(new byte[] { 7 }));
        });
    }

    [Test]
    public async Task A_foreign_unstamped_append_reported_after_arming_is_read_by_replay()
    {
        var tree = _cluster.Client.GetGrain<ILattice>(UnstampedTree);
        await tree.GetAsync("warm");
        var (_, snapshot) = await ResolveLeafAsync(UnstampedTree);
        await tree.SetAsync("own1", [1]);
        Assert.That(await WaitForCoverageAsync(snapshot, await HeadAsync(UnstampedTree) - 1, "own1"), Is.Not.Null,
            "precondition: the leaf banks its first write");

        // An unstamped append of a key the leaf owns - an older-build silo, or a path that
        // goes unstamped by design - lands between the leaf's own appends. The leaf's next
        // stamped append reports it.
        var wal = _cluster.Client.GetGrain<IWalShardGrain>($"{UnstampedTree}/0");
        await wal.AppendAsync(Record(UnstampedTree, "foreign", 9), CancellationToken.None);
        await tree.SetAsync("own2", [2]);
        var head = await HeadAsync(UnstampedTree);
        var banked = await WaitForCoverageAsync(snapshot, head - 1, "foreign");

        Assert.That(banked, Is.Not.Null,
            "the reported unstamped row reaches the snapshot: the leaf fell back to replay instead of advancing past it unread");
    }

    [Test]
    public async Task An_orphans_append_is_unstamped_so_the_reachable_owner_reads_it_before_it_advances()
    {
        // #5709: an orphan O shares the live leaf L's bounds but descent never reaches
        // it. A write still reaches O - a sibling-chain span forward, or a stale route -
        // and O appends it. O must not stamp: its append is reported to L, whose replay
        // applies it by key range before L can advance past it.
        var tree = _cluster.Client.GetGrain<ILattice>(OrphanTree);
        await tree.GetAsync("warm");
        var (_, snapshot) = await ResolveLeafAsync(OrphanTree);
        await tree.SetAsync("own1", [1]);
        Assert.That(await WaitForCoverageAsync(snapshot, await HeadAsync(OrphanTree) - 1, "own1"), Is.Not.Null,
            "precondition: the reachable leaf banks its first write live");

        // Minted as a crashed split leaves one (the explorer journey's orphan injector):
        // a sibling born with the live leaf's bounds that the parent never learned of.
        var orphan = _cluster.Client.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());
        using (LatticeNewLeafIntentContext.BeginScope(orphan.GetGrainId()))
        {
            await orphan.InitializeSiblingAsync(new SiblingInitialization { TreeId = OrphanTree, ShardIndex = 0 });
        }

        await orphan.SetStampedAsync("orphan", [5], 0, WalRoutingStamp.None);
        await tree.SetAsync("own2", [2]);
        var head = await HeadAsync(OrphanTree);
        var banked = await WaitForCoverageAsync(snapshot, head - 1, "orphan");

        Assert.That(banked, Is.Not.Null,
            "the orphan's acknowledged row reaches the reachable owner's snapshot: it was unstamped and reported, so the owner replayed it");
    }

    [Test]
    public async Task A_bulk_appended_leaf_arms_once_its_switch_has_closed()
    {
        // A leaf a bulk append creates is held while the shard's bulk: token is open over
        // its range. Once the switch closes, its activation replay - from #5702's birth
        // hint, or from -1 where none was applied - has read to its claim head, so it
        // banks its live writes without the starvation drive.
        var tree = _cluster.Client.GetGrain<ILattice>(BulkTree);
        for (var i = 0; i < 5; i++)
        {
            await tree.SetAsync($"a{i}", [(byte)i]);
        }

        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{BulkTree}/0");
        var appended = Enumerable.Range(0, 300).Select(i => KeyValuePair.Create($"z{i:D4}", new byte[] { 1 })).ToList();
        await shard.BulkAppendAsync("live-advance-bulk-1", appended);
        await WaitForBulkSwitchClosedAsync();

        var rightmost = await RightmostLeafAsync(shard);
        Assert.That(rightmost, Is.Not.EqualTo(await shard.GetLeftmostLeafIdAsync()), "precondition: the append created leaves");
        var snapshot = _cluster.Client.GetGrain<ILeafSnapshotStorageGrain>(rightmost.GetGuidKey());

        var reasons = new List<string>();
        LeafSnapshotBlob? banked;
        using (ListenForDriverDeclines(BulkTree, reasons))
        {
            await tree.SetAsync("z9999", [9]);
            banked = await WaitForCoverageAsync(snapshot, await HeadAsync(BulkTree) - 1, "z9999");
        }

        Assert.Multiple(() =>
        {
            Assert.That(banked, Is.Not.Null, "the bulk-born leaf banks its live write");
            Assert.That(reasons, Does.Not.Contain(LatticeMetrics.DriverDeclineRecheckNoDurableCheckpoint.Value),
                "the bulk-born leaf never took the starved arm");
        });
    }

    private async Task WaitForBulkSwitchClosedAsync(string tree = BulkTree)
    {
        var deadline = DateTime.UtcNow + ShardRootGrain.BulkSwitchIdleClose + TimeSpan.FromSeconds(20);
        while (DateTime.UtcNow < deadline)
        {
            var marker = (await HandOffMarkerRows.ReadShardAsync(Storage, tree, 0)).State;
            if (marker.Open.Count == 0 && marker.BulkBirthHints.Count == 0)
            {
                return;
            }

            await Task.Delay(250);
        }

        Assert.Fail("precondition: the bulk switch closed");
    }

    private async Task<GrainId> RightmostLeafAsync(IShardRootGrain shard)
    {
        var id = (await shard.GetLeftmostLeafIdAsync())!.Value;
        for (var i = 0; i < 1000; i++)
        {
            var link = await _cluster.Client.GetGrain<IBPlusLeafGrain>(id.GetGuidKey()).GetKeyRangeAndNextSiblingAsync();
            if (link.NextSibling is not { } next)
            {
                return id;
            }

            id = next;
        }

        throw new InvalidOperationException("the sibling chain did not end");
    }

    private static WalRecord Record(string tree, string key, byte value) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Value = [value],
        Timestamp = HybridLogicalClock.Tick(new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks }),
        ShardIndex = 0,
    };

    private Task<long> HeadAsync(string tree) =>
        _cluster.Client.GetGrain<ILeafReplayCoordinatorGrain>($"{tree}/0").GetHeadOffsetAsync(CancellationToken.None);

    /// <summary>
    /// Polls until the leaf's durable snapshot covers <paramref name="offset"/> and holds
    /// <paramref name="key"/>, or the budget runs out. Returns the snapshot, or null.
    /// </summary>
    private static async Task<LeafSnapshotBlob?> WaitForCoverageAsync(ILeafSnapshotStorageGrain snapshot, long offset, string key)
    {
        var clock = Stopwatch.StartNew();
        while (clock.Elapsed < Budget)
        {
            var loaded = await snapshot.LoadAsync(CancellationToken.None);
            if (loaded?.SnapshotOffsetsByPartition is { Length: > 0 } coverage
                && coverage[0] >= offset
                && HoldsLiveRow(loaded, key))
            {
                return loaded;
            }

            await Task.Delay(100);
        }

        return null;
    }

    private static bool HoldsLiveRow(LeafSnapshotBlob snapshot, string key)
    {
        foreach (var row in snapshot.EnumerateRows())
        {
            if (row.Key == key && !row.Value.IsTombstone)
            {
                return true;
            }
        }

        return false;
    }

    private async Task<(IBPlusLeafGrain Leaf, ILeafSnapshotStorageGrain Snapshot)> ResolveLeafAsync(string tree)
    {
        var id = await _cluster.Client.GetGrain<IShardRootGrain>($"{tree}/0").GetLeftmostLeafIdAsync();
        Assert.That(id, Is.Not.Null);
        var key = id!.Value.GetGuidKey();
        return (_cluster.Client.GetGrain<IBPlusLeafGrain>(key), _cluster.Client.GetGrain<ILeafSnapshotStorageGrain>(key));
    }

    private static IDisposable ListenForDriverDeclines(string treeId, List<string> reasons) =>
        MeterListening.StartForInstrument(
            LatticeMetrics.LeafSnapshotDriverDeclines,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                var matchesTree = false;
                string? reason = null;
                foreach (var t in tags)
                {
                    if (t.Key == LatticeMetrics.TagTree && (t.Value as string) == treeId)
                    {
                        matchesTree = true;
                    }
                    else if (t.Key == "reason" && t.Value is string value)
                    {
                        reason = value;
                    }
                }

                if (matchesTree && reason is not null)
                {
                    lock (reasons)
                    {
                        reasons.Add(reason);
                    }
                }
            }));

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.AddWalCursorRegistry();
            siloBuilder.AddLatticeWalGc();
            siloBuilder.ConfigureLattice(options => options.WalGcInterval = TimeSpan.Zero);
            siloBuilder.UseInMemoryReminderService();
            foreach (var tree in Trees)
            {
                siloBuilder.ConfigureLattice(tree, options =>
                {
                    options.WalPartitions = 1;
                    options.LeafSnapshotMaxCoverageLagSeconds = 1;
                    options.LeafSnapshotReClassifyEveryNCheckpoints = 0;
                    options.MaterialiserCheckpointInterval = TimeSpan.FromHours(1);
                    options.MaterialiserCheckpointEntries = 1_000_000;
                    options.WalMaterialiserMaxConcurrentReplays = 4;
                });
            }
        }
    }
}
