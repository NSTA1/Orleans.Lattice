using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.Wal;

/// <summary>
/// End-to-end regression harness for issue #919: durable WAL storage
/// silently paired with an in-memory cursor registry, losing committed
/// writes to a tree across a full-cluster restart.
/// <para>
/// The fix mirrors every leaf-as-materialiser checkpoint frontier into
/// the cluster-wide durable <see cref="IWalMaterialiserPinGrain"/> store
/// so the WAL GC's trim floor survives a restart that wipes the
/// process-local <see cref="InMemoryWalCursorRegistry"/>. These tests
/// reproduce the loss shape: a leaf with a checkpointed-and-snapshotted
/// head plus a still-live uncheckpointed WAL tail, then a simulated
/// restart that wipes the in-memory leaf pin and re-reports a forward
/// "shipper" cursor (the replication shipper persists its cursor and
/// re-reports it eagerly). Without the durable floor the GC trims the
/// tail past the leaf's durable frontier; with it the tail survives.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class WalCursorRegistryDurabilityTests
{
    private const int BatchSize = 6;

    private TestCluster _cluster = null!;

    private static IServiceProvider RequireSiloServices()
    {
        var services = SiloServiceProviderCaptureForPinDurabilityTests.Captured;
        Assert.That(services, Is.Not.Null,
            "Silo IServiceProvider was not captured by the pin-durability WAL fixture.");
        return services!;
    }

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        SiloServiceProviderCaptureForPinDurabilityTests.Reset();
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
        SiloServiceProviderCaptureForPinDurabilityTests.Reset();
    }

    [Test]
    public async Task Note_durable_frontier_persists_a_pin_through_the_grain_factory()
    {
        var sp = RequireSiloServices();
        var reporter = sp.GetService<ILeafCursorReporter>();
        Assert.That(reporter, Is.InstanceOf<LeafCursorReporter>(),
            "Core AddWalCursorRegistry must register the durable-pin-aware LeafCursorReporter.");

        var treeId = "wcr-note-" + Guid.NewGuid().ToString("N")[..8];
        var consumerId = ILeafCursorReporter.MaterialiserConsumerIdPrefix + treeId + "_leaf0";
        var frontier = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks, Counter = 1 };
        reporter!.NoteDurableMaterialiserFrontier(treeId, consumerId, frontier);

        var pins = await WaitForPinAsync(treeId, consumerId);
        Assert.That(pins, Does.ContainKey(consumerId),
            "NoteDurableMaterialiserFrontier must persist a durable pin (grain factory must be injected).");
        Assert.That(pins[consumerId], Is.EqualTo(frontier));
    }

    [Test]
    public async Task Leaf_checkpoint_persists_a_real_durable_materialiser_pin()
    {
        var treeId = "wcr-pin-" + Guid.NewGuid().ToString("N")[..8];
        var setup = await SeedCheckpointedHeadWithLiveTailAsync(treeId, captureSnapshot: false);

        Assert.That(setup.DurableFrontier, Is.GreaterThan(HybridLogicalClock.Zero),
            "A checkpointed leaf must publish a non-Zero durable frontier.");

        var pins = await GetPinsAsync(treeId);
        Assert.That(pins, Is.Not.Empty,
            "A real leaf checkpoint must seed at least one durable materialiser pin.");
        foreach (var (consumerId, frontier) in pins)
        {
            Assert.That(consumerId, Does.StartWith(ILeafCursorReporter.MaterialiserConsumerIdPrefix),
                "Only leaf-materialiser consumers may write to the durable pin store.");
            Assert.That(frontier, Is.GreaterThan(HybridLogicalClock.Zero));
        }
    }

    [Test]
    public async Task Durable_pin_floors_gc_after_registry_wipe_so_committed_tail_is_retained()
    {
        var treeId = "wcr-floor-" + Guid.NewGuid().ToString("N")[..8];
        var setup = await SeedCheckpointedHeadWithLiveTailAsync(treeId, captureSnapshot: false);

        var provider = RequireSiloServices().GetRequiredService<IWalStorageProvider>();
        var (lowestBefore, highestBefore) = await WalBoundsAsync(provider, treeId);
        Assert.That(highestBefore, Is.GreaterThanOrEqualTo(2L * BatchSize - 1),
            "Both write batches must have committed to the WAL before the simulated restart.");

        // Simulate a full silo/cluster restart: the in-memory registry is
        // wiped (the leaf pin disappears) while the durable WAL and the
        // durable pin store survive. We model the wipe by unregistering the
        // leaf-materialiser consumers, then re-report a forward "shipper"
        // cursor far ahead of the leaf's durable frontier.
        var registry = RequireSiloServices().GetRequiredService<IWalCursorRegistry>();
        await WipeInMemoryLeafPinsAsync(registry, treeId);
        var forwardShipper = ForwardShipperClock();
        await registry.ReportCursorAsync(treeId, "_shipper_sim", forwardShipper);

        Assert.That(await registry.GetMinCursorAsync(treeId), Is.EqualTo(forwardShipper),
            "After the wipe the bare registry minimum is the forward shipper cursor.");

        var gc = RequireSiloServices().GetRequiredService<ILatticeWalGc>();
        var report = await gc.RunOnceAsync(treeId);

        var (lowestAfter, highestAfter) = await WalBoundsAsync(provider, treeId);
        Assert.Multiple(() =>
        {
            Assert.That(report.MinCursor, Is.EqualTo(setup.DurableFrontier),
                "The WAL GC must floor its trim point at the durable leaf pin, not the forward shipper, after a registry wipe.");
            Assert.That(report.MinCursor, Is.LessThan(forwardShipper),
                "The durable floor must hold the trim point strictly below the forward shipper cursor.");
            // The committed-but-not-yet-checkpointed tail must survive: the
            // forward shipper alone would have trimmed it.
            Assert.That(highestAfter, Is.EqualTo(highestBefore),
                "The WAL head (the live tail) must not be trimmed.");
            Assert.That(lowestAfter, Is.GreaterThan(lowestBefore),
                "The checkpointed prefix must be trimmed.");
            Assert.That(lowestAfter, Is.GreaterThanOrEqualTo((long)BatchSize),
                "Only the checkpointed prefix may be trimmed; the live tail must remain.");
        });
    }

    [Test]
    public async Task Reactivated_leaf_replays_live_tail_after_gc_following_registry_wipe()
    {
        var treeId = "wcr-read-" + Guid.NewGuid().ToString("N")[..8];
        var setup = await SeedCheckpointedHeadWithLiveTailAsync(treeId, captureSnapshot: true);

        var registry = RequireSiloServices().GetRequiredService<IWalCursorRegistry>();
        await WipeInMemoryLeafPinsAsync(registry, treeId);
        await registry.ReportCursorAsync(treeId, "_shipper_sim", ForwardShipperClock());

        var gc = RequireSiloServices().GetRequiredService<ILatticeWalGc>();
        var report = await gc.RunOnceAsync(treeId);
        Assert.That(report.MinCursor, Is.EqualTo(setup.DurableFrontier),
            "The GC must floor at the durable leaf pin so the live tail survives the trim.");

        // Force the leaf to cold-restart. The committed-but-uncheckpointed
        // tail (batch B, the exact write shape #919 lost) lives only in the
        // WAL above the leaf's checkpoint. The durable floor kept those WAL
        // entries, so the activation-time replay over (checkpoint, head]
        // must reconstruct them with neither a LeafProjectionStaleException
        // (which the bug's too-aggressive trim would raise: the first
        // needed offset would be gone) nor a silent drop of the tail keys.
        await setup.Leaf.ForceDeactivateAsync();

        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        foreach (var key in setup.TailKeys)
        {
            var value = await tree.GetAsync(key);
            Assert.That(value, Is.Not.Null,
                $"Live-tail write {key} (above the leaf checkpoint) must survive the GC after the registry wipe.");
            Assert.That(System.Text.Encoding.UTF8.GetString(value!), Is.EqualTo(key));
        }
    }

    /// <summary>
    /// Seeds <paramref name="treeId"/> into the loss-prone shape: a first
    /// batch of writes that is replayed (via a forced reactivation) into a
    /// durable checkpoint and durable pin (optionally snapshotted), followed
    /// by a second batch that stays live in the WAL with no checkpoint
    /// advance. Returns the leaf reference, the durable frontier the leaf
    /// pinned, and the full key set.
    /// </summary>
    private async Task<TreeSetup> SeedCheckpointedHeadWithLiveTailAsync(string treeId, bool captureSnapshot)
    {
        var latticeRegistry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await latticeRegistry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = 1000,
            ShardCount = 1,
        });
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);

        var batchA = new List<string>();
        for (var i = 0; i < BatchSize; i++)
        {
            var key = $"a{i:D4}";
            batchA.Add(key);
            await tree.SetAsync(key, Bytes(key));
        }

        // Force the leaf to replay batch A so it advances its durable
        // checkpoint and publishes a real (non-Zero) durable pin. The
        // foreground write path does not advance the materialiser
        // checkpoint on its own - that happens on activation replay.
        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{treeId}/0");
        var leafId = await shard.GetLeftmostLeafIdAsync();
        Assert.That(leafId, Is.Not.Null, "Single-leaf shard must expose its leaf id.");
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId!.Value.GetGuidKey());
        await leaf.ForceDeactivateAsync();
        // Reactivate and replay: this read drives the activation-time
        // checkpoint advance + cursor report + durable-pin write.
        await tree.GetAsync(batchA[0]);

        var pins = await WaitForAnyRealPinAsync(treeId);
        var durableFrontier = MinFrontier(pins);

        if (captureSnapshot)
        {
            // Snapshot the checkpointed prefix so the leaf can rehydrate it
            // after the trimmed prefix is GC'd, while the live tail below is
            // recovered from the retained WAL.
            await leaf.CaptureSnapshotAsync();
        }

        // Second batch: stays live in the WAL with no checkpoint advance, so
        // the durable pin remains pinned at the batch-A frontier.
        var batchB = new List<string>();
        for (var i = 0; i < BatchSize; i++)
        {
            var key = $"b{i:D4}";
            batchB.Add(key);
            await tree.SetAsync(key, Bytes(key));
        }

        var allKeys = new List<string>(batchA);
        allKeys.AddRange(batchB);
        return new TreeSetup(leaf, durableFrontier, allKeys, batchB);
    }

    private static async Task WipeInMemoryLeafPinsAsync(IWalCursorRegistry registry, string treeId)
    {
        var snapshot = await registry.SnapshotAsync(treeId);
        foreach (var consumer in snapshot)
        {
            if (consumer.ConsumerId.StartsWith(ILeafCursorReporter.MaterialiserConsumerIdPrefix, StringComparison.Ordinal))
            {
                await registry.UnregisterAsync(treeId, consumer.ConsumerId);
            }
        }
    }

    private static HybridLogicalClock ForwardShipperClock() => new()
    {
        WallClockTicks = DateTime.UtcNow.AddHours(1).Ticks,
        Counter = 0,
    };

    private static async Task<(long Lowest, long Highest)> WalBoundsAsync(IWalStorageProvider provider, string treeId)
    {
        var highest = await provider.GetHighestOffsetAsync(treeId, 0, CancellationToken.None);
        var lowest = -1L;
        await foreach (var entry in provider.ReadAsync(treeId, 0, -1, int.MaxValue, CancellationToken.None))
        {
            lowest = entry.Offset;
            break;
        }

        return (lowest, highest);
    }

    private async Task<IReadOnlyDictionary<string, HybridLogicalClock>> GetPinsAsync(string treeId)
    {
        var factory = RequireSiloServices().GetRequiredService<IGrainFactory>();
        return await factory.GetGrain<IWalMaterialiserPinGrain>(treeId).GetPinsAsync();
    }

    private async Task<IReadOnlyDictionary<string, HybridLogicalClock>> WaitForAnyRealPinAsync(string treeId)
    {
        var factory = RequireSiloServices().GetRequiredService<IGrainFactory>();
        var pinGrain = factory.GetGrain<IWalMaterialiserPinGrain>(treeId);
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(20);
        while (DateTime.UtcNow < deadline)
        {
            var pins = await pinGrain.GetPinsAsync();
            if (pins.Count > 0 && pins.Values.All(v => v > HybridLogicalClock.Zero))
            {
                return pins;
            }
            await Task.Delay(50);
        }

        return await pinGrain.GetPinsAsync();
    }

    private async Task<IReadOnlyDictionary<string, HybridLogicalClock>> WaitForPinAsync(string treeId, string consumerId)
    {
        var factory = RequireSiloServices().GetRequiredService<IGrainFactory>();
        var pinGrain = factory.GetGrain<IWalMaterialiserPinGrain>(treeId);
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        IReadOnlyDictionary<string, HybridLogicalClock> pins = await pinGrain.GetPinsAsync();
        while (DateTime.UtcNow < deadline && !pins.ContainsKey(consumerId))
        {
            await Task.Delay(50);
            pins = await pinGrain.GetPinsAsync();
        }

        return pins;
    }

    private static HybridLogicalClock MinFrontier(IReadOnlyDictionary<string, HybridLogicalClock> pins)
    {
        HybridLogicalClock? min = null;
        foreach (var frontier in pins.Values)
        {
            if (min is null || frontier < min)
            {
                min = frontier;
            }
        }

        return min ?? HybridLogicalClock.Zero;
    }

    private static byte[] Bytes(string value) => System.Text.Encoding.UTF8.GetBytes(value);

    private sealed record TreeSetup(
        IBPlusLeafGrain Leaf,
        HybridLogicalClock DurableFrontier,
        IReadOnlyList<string> AllKeys,
        IReadOnlyList<string> TailKeys);

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.AddWalCursorRegistry();
            siloBuilder.AddLatticeWalGc();
            siloBuilder.ConfigureLattice(o =>
            {
                // Single WAL partition so the test reasons about a single
                // sequential offset space, and every-write checkpoint mode so
                // the activation replay flushes a real (non-Zero) checkpoint
                // and durable pin deterministically.
                o.WalPartitions = 1;
                o.MaterialiserCheckpointInterval = TimeSpan.Zero;
            });
            siloBuilder.UseInMemoryReminderService();

            siloBuilder.Services.AddSingleton<SiloServiceProviderCaptureForPinDurabilityTests>();
            siloBuilder.Services.AddHostedService(
                sp => sp.GetRequiredService<SiloServiceProviderCaptureForPinDurabilityTests>());
        }
    }
}

/// <summary>
/// Hosted service that captures the silo-side
/// <see cref="IServiceProvider"/> so the pin-durability fixture can
/// resolve silo-scoped singletons (registry, GC, grain factory) without
/// rebuilding a parallel container.
/// </summary>
internal sealed class SiloServiceProviderCaptureForPinDurabilityTests(IServiceProvider services) : IHostedService
{
    public static IServiceProvider? Captured { get; private set; }

    public static void Reset() => Captured = null;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        Captured = services;
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
