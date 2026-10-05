using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Wal;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Real-grain detector for issue #4634: a leaf whose snapshot licensed the WAL
/// GC to trim the prefix it covered, and which then finds that snapshot
/// <b>absent</b> on a cold start, must not rebuild silently from the surviving
/// WAL suffix.
/// <para>
/// The cold-start fall-off guard compares the WAL tail against the persisted
/// projection checkpoint, which the trim legitimately stays within while the
/// snapshot exists. When the snapshot vanishes, the guard still passes and the
/// leaf comes up holding only the suffix: every key whose only durable copy
/// was the snapshot reads as absent, with no error anywhere.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class VanishedSnapshotColdStartIntegrationTests
{
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(1);
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

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private async Task<(ILattice Tree, IShardRootGrain Shard, string TreeId)> CreateSingleLeafTreeAsync(string prefix)
    {
        var treeId = $"{prefix}-{Guid.NewGuid():N}";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 1024 });
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        var physical = (await tree.GetRoutingAsync()).PhysicalTreeId;
        return (tree, _cluster.Client.GetGrain<IShardRootGrain>($"{physical}/0"), physical);
    }

    /// <summary>
    /// Waits for the tree's leaf cursors to settle, then runs one WAL GC pass and
    /// returns how many entries it trimmed.
    /// </summary>
    private static async Task<long> TrimWalAsync(string treeId)
    {
        var services = SiloServiceProviderCaptureForWalTests.Captured
            ?? throw new InvalidOperationException("Silo IServiceProvider was not captured by the fixture.");
        var registry = services.GetRequiredService<IWalCursorRegistry>();
        var deadline = Environment.TickCount64 + (long)TimeSpan.FromSeconds(30).TotalMilliseconds;
        HybridLogicalClock? last = null;
        var stable = 0;
        while (Environment.TickCount64 < deadline)
        {
            var min = await registry.GetMinCursorAsync(treeId);
            if (min is { } floor && floor.CompareTo(HybridLogicalClock.Zero) > 0)
            {
                if (last is { } prev && floor.CompareTo(prev) == 0)
                {
                    if (++stable >= 2) break;
                }
                else
                {
                    stable = 0;
                }

                last = floor;
            }

            await Task.Delay(100);
        }

        var report = await services.GetRequiredService<ILatticeWalGc>().RunOnceAsync(treeId);
        TestContext.Out.WriteLine($"gc: trimmed={report.EntriesTrimmed} min={report.MinCursor} blocked={report.BlockedFloor} state={report.CursorFloorState} blocking={report.BlockingConsumerId} shards={report.ShardsScanned}");
        return report.EntriesTrimmed;
    }

/// <summary>
    /// Moves the leaf's projection checkpoint to the WAL head, as the materialiser
    /// would, and captures a snapshot covering it, then writes once more so the
    /// next checkpoint flush publishes the durable pin that licenses the trim.
    /// </summary>
    private async Task CoverWithSnapshotAsync(ILattice tree, IBPlusLeafGrain leaf, string treeId)
    {
        var head = await _cluster.Client.GetGrain<ILeafReplayCoordinatorGrain>($"{treeId}/0").GetHeadOffsetAsync(CancellationToken.None);
        await leaf.SetCheckpointOffsetHintsAsync([head - 1]);
        await leaf.CaptureSnapshotAsync();
        await tree.SetAsync("k-after-capture", Bytes("x"));
        TestContext.Out.WriteLine($"head={head} checkpoint={await leaf.GetProjectionCheckpointOffsetAsync()}");
    }
    [Test]
    public async Task A_cold_start_over_a_vanished_snapshot_does_not_silently_lose_the_prefix_it_covered()
    {
        var (tree, shard, treeId) = await CreateSingleLeafTreeAsync("vanished-snapshot");
        for (var i = 0; i < 20; i++)
        {
            await tree.SetAsync($"k-{i:D2}", Bytes($"v-{i}"));
        }

        var leafId = await shard.GetLeftmostLeafIdAsync();
        Assert.That(leafId, Is.Not.Null);
        var leafKey = leafId!.Value.GetGuidKey();
        var leaf = _cluster.Client.GetGrain<IBPlusLeafGrain>(leafKey);
        await CoverWithSnapshotAsync(tree, leaf, treeId);

        var trimmed = await TrimWalAsync(treeId);
        Assert.That(trimmed, Is.GreaterThan(0),
            "precondition: the WAL GC trimmed the prefix the snapshot covers; without a trim the test is vacuous");

        // The leaf goes cold, and while it is inactive its snapshot disappears.
        await leaf.ForceDeactivateAsync();
        await Task.Delay(200);
        await _cluster.Client.GetGrain<ILeafSnapshotStorageGrain>(leafKey).ClearAsync(CancellationToken.None);

        byte[]? read = null;
        Exception? failure = null;
        try
        {
            read = await tree.GetAsync("k-00");
        }
        catch (Exception ex)
        {
            failure = ex;
        }

        TestContext.Out.WriteLine($"trimmed={trimmed}; read={(read is null ? "null" : Encoding.UTF8.GetString(read))}; "
            + $"failure={failure?.GetType().Name}: {failure?.Message}");
        Assert.That(read is not null || failure is not null, Is.True,
            "k-00 was acknowledged, and its only durable copy was the vanished snapshot: the leaf must fail closed "
            + "rather than come up from the WAL suffix and report the key absent");
        if (read is not null)
        {
            Assert.That(read, Is.EqualTo(Bytes("v-0")));
        }
    }

    /// <summary>
    /// The trim does not have to happen before the cold start. A kept snapshot's
    /// durable pin cannot be lowered once the snapshot vanishes, so the WAL GC
    /// stays entitled to trim the prefix it covered for as long as the cold
    /// rebuild runs. Here the tail still reads <c>0</c> when the leaf activates,
    /// and a GC pass runs just before the rebuild's first slice read (found by the
    /// WAL spec's <c>SnapshotLoss</c> variant, depth 18). A cold start that trusts
    /// the untrimmed tail comes up without the prefix; the leaf must fail closed
    /// whatever the tail reads.
    /// </summary>
    [Test]
    public async Task A_cold_start_over_a_vanished_snapshot_fails_closed_even_when_the_tail_is_untrimmed_at_activation()
    {
        var (tree, shard, treeId) = await CreateSingleLeafTreeAsync("vanished-untrimmed");
        for (var i = 0; i < 20; i++)
        {
            await tree.SetAsync($"k-{i:D2}", Bytes($"v-{i}"));
        }

        var leafId = await shard.GetLeftmostLeafIdAsync();
        var leafKey = leafId!.Value.GetGuidKey();
        var leaf = _cluster.Client.GetGrain<IBPlusLeafGrain>(leafKey);
        await CoverWithSnapshotAsync(tree, leaf, treeId);

        var tail = await _cluster.Client.GetGrain<ILeafReplayCoordinatorGrain>($"{treeId}/0").GetTailOffsetAsync(CancellationToken.None);
        Assert.That(tail, Is.Zero, "precondition: nothing is trimmed before the cold start");

        await leaf.ForceDeactivateAsync();
        await Task.Delay(200);
        await _cluster.Client.GetGrain<ILeafSnapshotStorageGrain>(leafKey).ClearAsync(CancellationToken.None);

        MidRebuildTrim.Arm(treeId);
        byte[]? read = null;
        Exception? failure = null;
        try
        {
            read = await tree.GetAsync("k-00");
        }
        catch (Exception ex)
        {
            failure = ex;
        }
        finally
        {
            MidRebuildTrim.Disarm();
        }

        TestContext.Out.WriteLine($"mid-rebuild trim={MidRebuildTrim.Trimmed}; read={(read is null ? "null" : Encoding.UTF8.GetString(read))}; "
            + $"failure={failure?.GetType().Name}: {failure?.Message}");
        Assert.That(read is not null || failure is not null, Is.True,
            $"k-00 was acknowledged; the GC trimmed {MidRebuildTrim.Trimmed} entries under the vanished snapshot's pin while "
            + "the cold rebuild ran, so the leaf must fail closed rather than come up without them");
        if (read is not null)
        {
            Assert.That(read, Is.EqualTo(Bytes("v-0")));
        }
    }

    /// <summary>
    /// Runs one WAL GC pass for the armed tree immediately before the first
    /// replay slice read a leaf sends, i.e. while its cold rebuild is under way.
    /// </summary>
    private sealed class MidRebuildTrim(IServiceProvider services) : IOutgoingGrainCallFilter
    {
        private static string? _armedTree;

        public static long Trimmed { get; private set; }

        public static void Arm(string treeId)
        {
            Trimmed = 0;
            Volatile.Write(ref _armedTree, treeId);
        }

        public static void Disarm() => Volatile.Write(ref _armedTree, null);

        public async Task Invoke(IOutgoingGrainCallContext context)
        {
            if (context.InterfaceMethod?.Name == nameof(ILeafReplayCoordinatorGrain.ReadSliceAsync)
                && Volatile.Read(ref _armedTree) is { } armed
                && context.TargetId.Key.ToString()!.StartsWith(armed + "/", StringComparison.Ordinal)
                && Interlocked.CompareExchange(ref _armedTree, null, armed) == armed)
            {
                var report = await services.GetRequiredService<ILatticeWalGc>().RunOnceAsync(armed)
                    .WaitAsync(TimeSpan.FromSeconds(20));
                Trimmed = report.EntriesTrimmed;
            }

            await context.Invoke();
        }
    }
    /// <summary>
    /// The control: a leaf whose snapshot is still there rehydrates from it and
    /// serves every key, trim or no trim.
    /// </summary>
    [Test]
    public async Task A_cold_start_over_a_present_snapshot_serves_the_prefix_it_covered()
    {
        var (tree, shard, treeId) = await CreateSingleLeafTreeAsync("present-snapshot");
        for (var i = 0; i < 20; i++)
        {
            await tree.SetAsync($"k-{i:D2}", Bytes($"v-{i}"));
        }

        var leafId = await shard.GetLeftmostLeafIdAsync();
        var leaf = _cluster.Client.GetGrain<IBPlusLeafGrain>(leafId!.Value.GetGuidKey());
        await CoverWithSnapshotAsync(tree, leaf, treeId);
        Assert.That(await TrimWalAsync(treeId), Is.GreaterThan(0));

        await leaf.ForceDeactivateAsync();
        await Task.Delay(200);

        Assert.That(await tree.GetAsync("k-00"), Is.EqualTo(Bytes("v-0")));
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.AddWalCursorRegistry();
            siloBuilder.AddLatticeWalGc();
            siloBuilder.ConfigureLattice(o =>
            {
                o.DigestCoalescingWindowMs = 0;
                o.MaterialiserCheckpointInterval = TimeSpan.Zero;
                // One WAL partition, so the single leaf's pin covers the whole log
                // and no unwritten partition holds a blocking pin.
                o.WalPartitions = 1;
            });
            siloBuilder.Services.AddSingleton<IOutgoingGrainCallFilter, MidRebuildTrim>();
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<SiloServiceProviderCaptureForWalTests>();
            siloBuilder.Services.AddHostedService(sp => sp.GetRequiredService<SiloServiceProviderCaptureForWalTests>());
        }
    }
}
