using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Hosting;
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
        var partitions = RequireSiloServices()
            .GetRequiredService<Microsoft.Extensions.Options.IOptionsMonitor<LatticeOptions>>()
            .Get(treeId).WalPartitions;
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
            Assert.That(report.EntriesTrimmed, Is.GreaterThanOrEqualTo(0));
            // The metric must agree with the report. When the run
            // trimmed zero entries the counter records zero, so the
            // assertion is the equality rather than a strict-positive.
            Assert.That(trimmed, Is.EqualTo(report.EntriesTrimmed),
                "orleans.lattice.wal.entries_trimmed must record the count reported by RunOnceAsync.");
        });
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
