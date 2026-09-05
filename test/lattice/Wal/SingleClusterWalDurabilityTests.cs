using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.Wal;

/// <summary>
/// Single-silo, single-cluster harness asserting that a host without
/// <c>AddLatticeReplication</c> still resolves every WAL maintenance
/// seam from <c>Orleans.Lattice</c>, drives the leaf-as-materialiser
/// cursor reporter, and trims WAL partitions via the in-core
/// <see cref="ILatticeWalGc"/> when the leaf cursor advances. The WAL
/// backing store under test is the in-memory
/// <see cref="InMemoryWalStorageProvider"/> registered through the
/// new core <c>AddWalStorage</c> extension.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SingleClusterWalDurabilityTests
{
    private TestCluster _cluster = null!;

    private static IServiceProvider RequireSiloServices()
    {
        var services = SiloServiceProviderCaptureForWalTests.Captured;
        Assert.That(services, Is.Not.Null,
            "Silo IServiceProvider was not captured by the single-cluster WAL fixture.");
        return services!;
    }

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        SiloServiceProviderCaptureForWalTests.Reset();
        // Pin to a single silo so the hosted-service IServiceProvider
        // capture observes the same `InMemoryWalStorageProvider`
        // singleton that hosts the `WalShardGrain` activations. The
        // default TestClusterBuilder spins up two silos, each with its
        // own singleton, which would route some WAL appends to a
        // provider the test cannot resolve.
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
        SiloServiceProviderCaptureForWalTests.Reset();
    }

    [Test]
    public void Single_cluster_silo_resolves_wal_storage_and_cursor_registry_and_gc_from_DI()
    {
        var sp = RequireSiloServices();
        Assert.Multiple(() =>
        {
            Assert.That(sp.GetService<IWalStorageProvider>(), Is.InstanceOf<InMemoryWalStorageProvider>(),
                "Default core WAL storage provider must resolve without AddLatticeReplication.");
            Assert.That(sp.GetService<IWalCursorRegistry>(), Is.InstanceOf<InMemoryWalCursorRegistry>(),
                "Default core cursor registry must resolve without AddLatticeReplication.");
            Assert.That(sp.GetService<ILatticeWalGc>(), Is.InstanceOf<LatticeWalGc>(),
                "Default core WAL GC must resolve without AddLatticeReplication.");
            Assert.That(sp.GetService<ILeafCursorReporter>(), Is.Not.Null,
                "Leaf cursor reporter must be registered alongside the cursor registry.");
        });
    }

    [Test]
    public async Task Leaf_writes_commit_through_commit_log_into_the_wal_storage_provider()
    {
        var treeId = "sc-wal-commit-" + Guid.NewGuid().ToString("N")[..8];
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);

        const int count = 20;
        for (var i = 0; i < count; i++)
        {
            await tree.SetAsync($"k{i:D4}", Bytes($"v{i}"));
        }

        var provider = RequireSiloServices().GetRequiredService<IWalStorageProvider>();
        // Resolve through LatticeOptionsResolver so the test loop
        // iterates the partitions the writer-side actually routed
        // to (the tree-registry pin), not whatever the silo's live
        // IOptionsMonitor<LatticeOptions> value happens to be.
        var partitions = (await RequireSiloServices()
            .GetRequiredService<LatticeOptionsResolver>()
            .ResolveAsync(treeId)).WalPartitions;
        Assert.That(partitions, Is.GreaterThanOrEqualTo(1));

        var totalAppended = 0L;
        for (var shard = 0; shard < partitions; shard++)
        {
            var highest = await provider.GetHighestOffsetAsync(treeId, shard, CancellationToken.None);
            if (highest >= 0)
            {
                totalAppended += highest + 1;
            }
        }

        Assert.That(totalAppended, Is.GreaterThanOrEqualTo(count),
            "Every foreground write must commit at least one WAL entry across the tree's partitions.");
    }

    [Test]
    public async Task LatticeWalGc_RunOnceAsync_emits_wal_entries_trimmed_metric_when_log_is_advanced()
    {
        var treeId = "sc-wal-gc-" + Guid.NewGuid().ToString("N")[..8];
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);

        // Drive enough writes that at least one leaf advances a
        // checkpoint and reports a non-Zero cursor.
        for (var i = 0; i < 10; i++)
        {
            await tree.SetAsync($"k{i:D4}", Bytes($"v{i}"));
        }

        var registry = RequireSiloServices().GetRequiredService<IWalCursorRegistry>();
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            var min = await registry.GetMinCursorAsync(treeId);
            if (min is { } floor && floor.CompareTo(HybridLogicalClock.Zero) > 0)
            {
                break;
            }
            await Task.Delay(50);
        }

        var trimmed = 0L;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (inst, lst) =>
        {
            if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter) && inst.Name == "orleans.lattice.wal.entries_trimmed")
            {
                lst.EnableMeasurementEvents(inst);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, value, _, _) =>
        {
            Interlocked.Add(ref trimmed, value);
        });
        listener.Start();

        var gc = RequireSiloServices().GetRequiredService<ILatticeWalGc>();
        var report = await gc.RunOnceAsync(treeId);
        listener.RecordObservableInstruments();

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeName, Is.EqualTo(treeId));
            Assert.That(report.ShardsScanned, Is.GreaterThanOrEqualTo(1));
            // The metric must agree with the report. When the run
            // trimmed zero entries the counter records zero, so the
            // assertion is the equality rather than a strict-positive.
            // That equality is also the only non-vacuous claim available
            // about EntriesTrimmed, which is a count and so can never be
            // negative in the first place.
            Assert.That(trimmed, Is.EqualTo(report.EntriesTrimmed),
                "orleans.lattice.wal.entries_trimmed must record the count reported by RunOnceAsync.");
        });
    }

    [Test]
    public async Task LatticeWalGcScheduler_drives_RunOnceAsync_and_trims_the_wal_for_a_non_replicated_tree()
    {
        // Issue #920: a durable-WAL host without replication must get its
        // WAL bounded once the core scheduler is enabled. This proves the
        // end-to-end path: write -> advance the leaf cursor -> the
        // background scheduler runs a GC pass over the registered tree ->
        // the WAL is trimmed, with no replication package and no manual
        // RunOnceAsync call.
        var treeId = "sc-wal-sched-" + Guid.NewGuid().ToString("N")[..8];
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);

        for (var i = 0; i < 10; i++)
        {
            await tree.SetAsync($"k{i:D4}", Bytes($"v{i}"));
        }

        var sp = RequireSiloServices();
        var registry = sp.GetRequiredService<IWalCursorRegistry>();
        var cursorDeadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
        while (DateTime.UtcNow < cursorDeadline)
        {
            var min = await registry.GetMinCursorAsync(treeId);
            if (min is { } floor && floor.CompareTo(HybridLogicalClock.Zero) > 0)
            {
                break;
            }
            await Task.Delay(50);
        }

        // Accumulate trimmed-entry measurements attributed to this tree so
        // a sibling fixture tree's GC does not bleed into the assertion.
        var trimmedForTree = 0L;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (inst, lst) =>
        {
            if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter)
                && inst.Name == "orleans.lattice.wal.entries_trimmed")
            {
                lst.EnableMeasurementEvents(inst);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == LatticeMetrics.TagTree
                    && tag.Value is string t
                    && string.Equals(t, treeId, StringComparison.Ordinal))
                {
                    Interlocked.Add(ref trimmedForTree, value);
                }
            }
        });
        listener.Start();

        // Drive the scheduler over a WAL GC configured with a short
        // wall-clock retention TTL so the already-written entries are
        // unconditionally trim-eligible - this makes the end-to-end
        // assertion deterministic rather than racing the leaf checkpoint
        // advance. The GC still resolves the silo's in-memory WAL provider
        // and placement through the captured silo IServiceProvider.
        var ttlGc = new LatticeWalGc(
            sp,
            registry,
            new FixedLatticeOptionsMonitor(new LatticeOptions { WalRetention = TimeSpan.FromMilliseconds(1) }));

        // Before the core scheduler existed this non-replicated tree had no
        // GC driver at all; here we drive it on a fast cadence (the first
        // pass is staggered by up to one interval, so 50ms keeps the test
        // prompt) and assert it trims.
        var scheduler = new LatticeWalGcScheduler(
            sp.GetRequiredService<IGrainFactory>(),
            ttlGc,
            new FixedLatticeOptionsMonitor(new LatticeOptions { WalGcInterval = TimeSpan.FromMilliseconds(50) }),
            NullLogger<LatticeWalGcScheduler>.Instance);

        await scheduler.StartAsync(CancellationToken.None);
        try
        {
            var trimDeadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
            while (DateTime.UtcNow < trimDeadline && Interlocked.Read(ref trimmedForTree) <= 0)
            {
                await Task.Delay(50);
            }
        }
        finally
        {
            await scheduler.StopAsync(CancellationToken.None);
        }

        Assert.That(Interlocked.Read(ref trimmedForTree), Is.GreaterThan(0),
            "the enabled core WAL GC scheduler must trim the non-replicated tree's WAL once its leaf cursor has advanced.");
    }

    [Test]
    public async Task LatticeWalGc_RunOnceAsync_returns_empty_report_for_unknown_tree()
    {
        var treeId = "sc-wal-empty-" + Guid.NewGuid().ToString("N")[..8];
        var gc = RequireSiloServices().GetRequiredService<ILatticeWalGc>();
        var report = await gc.RunOnceAsync(treeId);
        Assert.Multiple(() =>
        {
            Assert.That(report.TreeName, Is.EqualTo(treeId));
            Assert.That(report.EntriesTrimmed, Is.EqualTo(0));
            Assert.That(report.MinCursor, Is.Null);
        });
    }

    [Test]
    public async Task Write_burst_stays_responsive_and_drains_through_a_bounded_sharded_durable_pin_floor()
    {
        // Issue #1030 end-to-end regression guard. A burst of writes into a
        // freshly-created tree births and splits multiple leaves and advances
        // many per-partition checkpoints. Before the fix this funnelled
        // O(leaves x partitions) serialized durable pin writes through a single
        // per-tree grain, saturating the silo and wedging the drain path; after
        // the fix the durable pins fan across WalMaterialiserPinShards shard
        // activations and coalesce behind a debounced flush, so the cluster
        // stays responsive and the WAL still drains.
        var treeId = "sc-wal-burst-" + Guid.NewGuid().ToString("N")[..8];
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);

        const int count = 300;
        for (var i = 0; i < count; i++)
        {
            await tree.SetAsync($"k{i:D5}", Bytes($"v{i}"));
        }

        // Responsiveness: every sampled key reads back immediately after the
        // burst. A wedged drain path would stall these foreground reads.
        for (var i = 0; i < count; i += 25)
        {
            var value = await tree.GetAsync($"k{i:D5}");
            Assert.That(value, Is.Not.Null,
                $"key k{i:D5} must read back after the burst (cluster stays responsive).");
        }

        var sp = RequireSiloServices();
        var registry = sp.GetRequiredService<IWalCursorRegistry>();

        // Wait for at least one leaf materialiser to advance its checkpoint and
        // report a non-Zero durable floor (the drain making progress).
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            var min = await registry.GetMinCursorAsync(treeId);
            if (min is { } floor && floor.CompareTo(HybridLogicalClock.Zero) > 0)
            {
                break;
            }
            await Task.Delay(50);
        }

        // The durable pin floor is maintained across the shard activations the
        // WAL GC fans its read in over: the union of every shard's pins (plus
        // the legacy single-key shape) is non-empty, proving the sharded
        // durable-pin store the GC relies on is being written and is readable.
        var options = sp.GetRequiredService<IOptionsMonitor<LatticeOptions>>();
        var shardCount = WalMaterialiserPinRouting.ResolveShardCount(options);
        Assert.That(shardCount, Is.GreaterThanOrEqualTo(1));

        var unionPins = 0;
        foreach (var key in WalMaterialiserPinRouting.EnumerateReadKeys(treeId, shardCount))
        {
            var pinGrain = _cluster.Client.GetGrain<IWalMaterialiserPinGrain>(key);
            var pins = await pinGrain.GetPinsAsync();
            unionPins += pins.Count;
        }
        Assert.That(unionPins, Is.GreaterThanOrEqualTo(1),
            "the sharded durable leaf-materialiser pin floor must be maintained after a write burst.");

        // The drain is not wedged: a GC pass over the sharded durable-pin
        // store completes promptly and reports a coherent result rather than
        // stalling (the pre-fix failure mode was an unbounded serialized pin
        // write storm through one grain).
        var gc = sp.GetRequiredService<ILatticeWalGc>();
        var report = await gc.RunOnceAsync(treeId);
        Assert.Multiple(() =>
        {
            Assert.That(report.TreeName, Is.EqualTo(treeId));
            // A completed pass over the shards IS the "not wedged" evidence, so
            // the message belongs on this assertion. The previous bound on
            // EntriesTrimmed was a tautology (a count is never negative) and so
            // could not have detected the wedge its message described.
            Assert.That(report.ShardsScanned, Is.GreaterThanOrEqualTo(1),
                "after the burst the WAL GC must complete a pass against the sharded pin floor rather than wedging.");
        });
    }

    private static byte[] Bytes(string value) => System.Text.Encoding.UTF8.GetBytes(value);

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            // Single-cluster wiring: AddLattice itself registers the
            // in-memory WAL provider, plus the commit-log adapters.
            // AddWalCursorRegistry + AddLatticeWalGc layer the WAL
            // maintenance seams on top. No AddLatticeReplication call.
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.AddWalCursorRegistry();
            siloBuilder.AddLatticeWalGc();
            siloBuilder.UseInMemoryReminderService();

            siloBuilder.Services.AddSingleton<SiloServiceProviderCaptureForWalTests>();
            siloBuilder.Services.AddHostedService(
                sp => sp.GetRequiredService<SiloServiceProviderCaptureForWalTests>());
        }
    }
}

/// <summary>
/// Hosted service that copies the silo-side
/// <see cref="IServiceProvider"/> it was constructed with into a static
/// field so tests can resolve silo-scoped singletons without
/// rebuilding a parallel container.
/// </summary>
internal sealed class SiloServiceProviderCaptureForWalTests(IServiceProvider services) : IHostedService
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

/// <summary>
/// Minimal <see cref="IOptionsMonitor{TOptions}"/> that returns a fixed
/// <see cref="LatticeOptions"/> instance for every name, so a test can
/// drive <see cref="LatticeWalGcScheduler"/> with an explicit
/// <see cref="LatticeOptions.WalGcInterval"/> without reconfiguring the
/// silo's shared options.
/// </summary>
internal sealed class FixedLatticeOptionsMonitor(LatticeOptions options) : IOptionsMonitor<LatticeOptions>
{
    public LatticeOptions CurrentValue => options;

    public LatticeOptions Get(string? name) => options;

    public IDisposable? OnChange(Action<LatticeOptions, string?> listener) => null;
}
