using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class LatticeReplicationGcTests
{
    private const string Tree = "tree";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(LatticeReplicationOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static IServiceProvider Services(IWalStorageProvider provider)
    {
        var sc = new ServiceCollection();
        sc.AddSingleton(provider);
        return sc.BuildServiceProvider();
    }

    private static async Task SeedAsync(IWalStorageProvider provider, int shard, params ReplogEntry[] entries)
    {
        var wal = entries.Select((e, i) => new WalEntry { Offset = i, Entry = e }).ToArray();
        await provider.AppendBatchAsync(Tree, shard, wal, CancellationToken.None);
    }

    private static ReplogEntry SetEntry(string key, HybridLogicalClock ts) => new()
    {
        TreeId = Tree,
        Op = ReplogOp.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = ts,
        OriginClusterId = "site-a",
    };

    [Test]
    public async Task RunOnceAsync_returns_zero_trim_when_no_consumers_and_no_ttl()
    {
        var provider = new InMemoryWalStorageProvider();
        await SeedAsync(provider, 0, SetEntry("k1", Hlc(10)), SetEntry("k2", Hlc(20)));

        var registry = new InMemoryReplicationCursorRegistry();
        var sut = new LatticeReplicationGc(
            Services(provider),
            registry,
            Monitor(new LatticeReplicationOptions { ClusterId = "c", ReplogPartitions = 1 }));

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(0));
        Assert.That(report.MinCursor, Is.Null);
        Assert.That(report.TtlCeilingHlc, Is.Null);
        Assert.That(await provider.GetHighestOffsetAsync(Tree, 0, CancellationToken.None), Is.EqualTo(1L));
    }

    [Test]
    public async Task RunOnceAsync_trims_entries_at_or_below_min_cursor()
    {
        var provider = new InMemoryWalStorageProvider();
        await SeedAsync(provider, 0,
            SetEntry("a", Hlc(10)),
            SetEntry("b", Hlc(20)),
            SetEntry("c", Hlc(30)));

        var registry = new InMemoryReplicationCursorRegistry();
        await registry.ReportCursorAsync(Tree, "peer-A", Hlc(20));

        var sut = new LatticeReplicationGc(
            Services(provider),
            registry,
            Monitor(new LatticeReplicationOptions { ClusterId = "c", ReplogPartitions = 1 }));

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(2));
        Assert.That(report.MinCursor, Is.EqualTo(Hlc(20)));

        // Surviving entry is offset 2 with key "c".
        var survivors = new List<WalEntry>();
        await foreach (var entry in provider.ReadAsync(Tree, 0, fromOffsetExclusive: -1, maxEntries: 100, CancellationToken.None))
        {
            survivors.Add(entry);
        }
        Assert.That(survivors, Has.Count.EqualTo(1));
        Assert.That(survivors[0].Offset, Is.EqualTo(2L));
    }

    [Test]
    public async Task RunOnceAsync_pins_log_to_slowest_consumer()
    {
        var provider = new InMemoryWalStorageProvider();
        await SeedAsync(provider, 0,
            SetEntry("a", Hlc(10)),
            SetEntry("b", Hlc(20)),
            SetEntry("c", Hlc(30)));

        var registry = new InMemoryReplicationCursorRegistry();
        await registry.ReportCursorAsync(Tree, "peer-fast", Hlc(30));
        await registry.ReportCursorAsync(Tree, "peer-slow", Hlc(10));

        var sut = new LatticeReplicationGc(
            Services(provider),
            registry,
            Monitor(new LatticeReplicationOptions { ClusterId = "c", ReplogPartitions = 1 }));

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(1));
        Assert.That(report.MinCursor, Is.EqualTo(Hlc(10)));
    }

    [Test]
    public async Task RunOnceAsync_trims_across_every_partition()
    {
        var provider = new InMemoryWalStorageProvider();
        await SeedAsync(provider, 0, SetEntry("a", Hlc(10)), SetEntry("b", Hlc(20)));
        await SeedAsync(provider, 1, SetEntry("c", Hlc(15)), SetEntry("d", Hlc(25)));

        var registry = new InMemoryReplicationCursorRegistry();
        await registry.ReportCursorAsync(Tree, "peer-A", Hlc(20));

        var sut = new LatticeReplicationGc(
            Services(provider),
            registry,
            Monitor(new LatticeReplicationOptions { ClusterId = "c", ReplogPartitions = 2 }));

        var report = await sut.RunOnceAsync(Tree);

        // Shard 0: both entries (10,20) eligible -> 2 trimmed.
        // Shard 1: only entry 15 eligible -> 1 trimmed.
        Assert.That(report.EntriesTrimmed, Is.EqualTo(3));
        Assert.That(report.ShardsScanned, Is.EqualTo(2));
    }

    [Test]
    public async Task RunOnceAsync_applies_ttl_ceiling_when_consumers_lag()
    {
        var provider = new InMemoryWalStorageProvider();
        var nowTicks = DateTime.UtcNow.Ticks;
        await SeedAsync(provider, 0,
            SetEntry("old", Hlc(nowTicks - TimeSpan.FromHours(2).Ticks)),
            SetEntry("new", Hlc(nowTicks - TimeSpan.FromMinutes(1).Ticks)));

        var registry = new InMemoryReplicationCursorRegistry();
        // Consumer pinned at HLC=1 (essentially the start of time) so by
        // cursor alone nothing would be trimmed; the TTL ceiling
        // overrides for the entry older than one hour.
        await registry.ReportCursorAsync(Tree, "peer-A", Hlc(1));

        var sut = new LatticeReplicationGc(
            Services(provider),
            registry,
            Monitor(new LatticeReplicationOptions
            {
                ClusterId = "c",
                ReplogPartitions = 1,
                WalRetention = TimeSpan.FromHours(1),
            }));

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(1));
        Assert.That(report.TtlCeilingHlc, Is.Not.Null);
    }

    [Test]
    public async Task RunOnceAsync_stops_at_first_non_eligible_entry()
    {
        // Offsets are dense and append-only; HLCs may be non-monotonic
        // due to clock skew, so the conservative scan must stop at the
        // first entry that fails the predicate even if a later entry
        // would pass it.
        var provider = new InMemoryWalStorageProvider();
        await SeedAsync(provider, 0,
            SetEntry("a", Hlc(10)),
            SetEntry("b", Hlc(50)),  // not eligible (above cursor)
            SetEntry("c", Hlc(20))); // would be eligible by HLC, but dense ordering forbids skipping

        var registry = new InMemoryReplicationCursorRegistry();
        await registry.ReportCursorAsync(Tree, "peer-A", Hlc(30));

        var sut = new LatticeReplicationGc(
            Services(provider),
            registry,
            Monitor(new LatticeReplicationOptions { ClusterId = "c", ReplogPartitions = 1 }));

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(1));
    }

    [Test]
    public async Task RunOnceAsync_empty_wal_is_no_op()
    {
        var provider = new InMemoryWalStorageProvider();
        var registry = new InMemoryReplicationCursorRegistry();
        await registry.ReportCursorAsync(Tree, "peer-A", Hlc(100));

        var sut = new LatticeReplicationGc(
            Services(provider),
            registry,
            Monitor(new LatticeReplicationOptions { ClusterId = "c", ReplogPartitions = 1 }));

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(0));
    }

    [Test]
    public async Task RunOnceAsync_walks_every_page_when_full_prefix_eligible()
    {
        // Seed more than one ScanPageSize (256) of entries so the GC
        // must page through to find the cutover.
        var provider = new InMemoryWalStorageProvider();
        var batch = Enumerable.Range(0, 600)
            .Select(i => SetEntry("k" + i, Hlc(i + 1)))
            .ToArray();
        await SeedAsync(provider, 0, batch);

        var registry = new InMemoryReplicationCursorRegistry();
        await registry.ReportCursorAsync(Tree, "peer-A", Hlc(500));

        var sut = new LatticeReplicationGc(
            Services(provider),
            registry,
            Monitor(new LatticeReplicationOptions { ClusterId = "c", ReplogPartitions = 1 }));

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(500));
        Assert.That(await provider.GetHighestOffsetAsync(Tree, 0, CancellationToken.None), Is.EqualTo(599L));
    }

    [Test]
    public async Task RunOnceAsync_reports_provider_min_cursor_in_report()
    {
        var provider = new InMemoryWalStorageProvider();
        await SeedAsync(provider, 0, SetEntry("a", Hlc(10)));

        var registry = new InMemoryReplicationCursorRegistry();
        await registry.ReportCursorAsync(Tree, "peer-A", Hlc(50));

        var sut = new LatticeReplicationGc(
            Services(provider),
            registry,
            Monitor(new LatticeReplicationOptions { ClusterId = "c", ReplogPartitions = 1 }));

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.TreeName, Is.EqualTo(Tree));
        Assert.That(report.MinCursor, Is.EqualTo(Hlc(50)));
        Assert.That(report.ShardsScanned, Is.EqualTo(1));
    }

    [Test]
    public async Task RunOnceAsync_uses_per_tree_storage_provider_override()
    {
        var dummy = new InMemoryWalStorageProvider();
        var perTree = new InMemoryWalStorageProvider();
        await SeedAsync(perTree, 0, SetEntry("a", Hlc(10)));
        var registry = new InMemoryReplicationCursorRegistry();
        await registry.ReportCursorAsync(Tree, "peer-A", Hlc(50));

        var options = new LatticeReplicationOptions
        {
            ClusterId = "c",
            ReplogPartitions = 1,
            WalStorageProvider = _ => perTree,
        };

        var sut = new LatticeReplicationGc(Services(dummy), registry, Monitor(options));
        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(1));
        // The DI-default 'dummy' was never touched.
        Assert.That(await dummy.GetHighestOffsetAsync(Tree, 0, CancellationToken.None), Is.EqualTo(-1L));
    }

    [Test]
    public void RunOnceAsync_throws_on_null_tree_name()
    {
        var provider = new InMemoryWalStorageProvider();
        var registry = new InMemoryReplicationCursorRegistry();
        var sut = new LatticeReplicationGc(
            Services(provider),
            registry,
            Monitor(new LatticeReplicationOptions { ClusterId = "c", ReplogPartitions = 1 }));

        Assert.That(async () => await sut.RunOnceAsync(null!), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void RunOnceAsync_observes_cancellation()
    {
        var provider = new InMemoryWalStorageProvider();
        var registry = new InMemoryReplicationCursorRegistry();
        var sut = new LatticeReplicationGc(
            Services(provider),
            registry,
            Monitor(new LatticeReplicationOptions { ClusterId = "c", ReplogPartitions = 1 }));

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await sut.RunOnceAsync(Tree, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    // ---- Causal-stable frontier predicate ----------------------------

    private static VersionVector Vc(params (string origin, long ticks)[] entries)
    {
        var vc = new VersionVector();
        foreach (var (origin, ticks) in entries)
        {
            vc.Entries[origin] = Hlc(ticks);
        }
        return vc;
    }

    private static ReplogEntry SetEntryWithVc(string key, HybridLogicalClock ts, VersionVector? vc) => new()
    {
        TreeId = Tree,
        Op = ReplogOp.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = ts,
        OriginClusterId = "site-a",
        VectorClock = vc,
    };

    [Test]
    public async Task RunOnceAsync_blocks_trim_when_entry_vc_exceeds_causal_stable_frontier()
    {
        var provider = new InMemoryWalStorageProvider();
        // Entry has site-b at 50 but the slowest VC consumer has only
        // observed site-b up to 20, so the entry must remain even though
        // its HLC is below the cursor.
        await SeedAsync(provider, 0,
            SetEntryWithVc("a", Hlc(10), Vc(("site-a", 10), ("site-b", 50))));

        var registry = new InMemoryReplicationCursorRegistry();
        await registry.ReportCursorAsync(Tree, "peer-A", Hlc(100), Vc(("site-a", 100), ("site-b", 100)));
        await registry.ReportCursorAsync(Tree, "peer-B", Hlc(100), Vc(("site-a", 100), ("site-b", 20)));

        var sut = new LatticeReplicationGc(
            Services(provider),
            registry,
            Monitor(new LatticeReplicationOptions { ClusterId = "c", ReplogPartitions = 1 }));

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(0));
        Assert.That(report.CausalStable, Is.Not.Null);
        Assert.That(report.CausalStable!.Entries["site-b"], Is.EqualTo(Hlc(20)));
    }

    [Test]
    public async Task RunOnceAsync_trims_entry_when_vc_dominated_by_causal_stable_and_hlc_below_cursor()
    {
        var provider = new InMemoryWalStorageProvider();
        await SeedAsync(provider, 0,
            SetEntryWithVc("a", Hlc(10), Vc(("site-a", 10), ("site-b", 5))));

        var registry = new InMemoryReplicationCursorRegistry();
        await registry.ReportCursorAsync(Tree, "peer-A", Hlc(100), Vc(("site-a", 100), ("site-b", 100)));
        await registry.ReportCursorAsync(Tree, "peer-B", Hlc(100), Vc(("site-a", 50), ("site-b", 50)));

        var sut = new LatticeReplicationGc(
            Services(provider),
            registry,
            Monitor(new LatticeReplicationOptions { ClusterId = "c", ReplogPartitions = 1 }));

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(1));
    }

    [Test]
    public async Task RunOnceAsync_treats_entry_with_null_vc_as_dominated_by_any_frontier()
    {
        var provider = new InMemoryWalStorageProvider();
        // Legacy entry: VectorClock is null (pre-causal+ peer).
        // It should still trim under HLC alone even when a frontier
        // exists, because a null entry VC has no demands on the
        // frontier.
        await SeedAsync(provider, 0, SetEntryWithVc("legacy", Hlc(10), vc: null));

        var registry = new InMemoryReplicationCursorRegistry();
        await registry.ReportCursorAsync(Tree, "peer-A", Hlc(100), Vc(("site-a", 1)));

        var sut = new LatticeReplicationGc(
            Services(provider),
            registry,
            Monitor(new LatticeReplicationOptions { ClusterId = "c", ReplogPartitions = 1 }));

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(1));
    }

    [Test]
    public async Task RunOnceAsync_degrades_to_hlc_only_predicate_when_no_consumer_reports_vc()
    {
        var provider = new InMemoryWalStorageProvider();
        await SeedAsync(provider, 0,
            SetEntryWithVc("a", Hlc(10), Vc(("site-a", 999))));

        var registry = new InMemoryReplicationCursorRegistry();
        await registry.ReportCursorAsync(Tree, "peer-A", Hlc(100));

        var sut = new LatticeReplicationGc(
            Services(provider),
            registry,
            Monitor(new LatticeReplicationOptions { ClusterId = "c", ReplogPartitions = 1 }));

        var report = await sut.RunOnceAsync(Tree);

        // Causal-stable is null because no consumer reported a VC, so
        // the predicate degrades to the existing R-061 HLC behaviour.
        Assert.That(report.CausalStable, Is.Null);
        Assert.That(report.EntriesTrimmed, Is.EqualTo(1));
    }

    [Test]
    public async Task RunOnceAsync_blocks_trim_when_entry_vc_carries_origin_missing_from_frontier()
    {
        var provider = new InMemoryWalStorageProvider();
        // Entry names site-z but no consumer has reported site-z, so
        // the frontier excludes it. Entry's VC has site-z=10; frontier
        // treats site-z as unknown (clock 0) and the entry is NOT
        // dominated -> not trimmed.
        await SeedAsync(provider, 0,
            SetEntryWithVc("a", Hlc(5), Vc(("site-a", 5), ("site-z", 10))));

        var registry = new InMemoryReplicationCursorRegistry();
        await registry.ReportCursorAsync(Tree, "peer-A", Hlc(100), Vc(("site-a", 100)));

        var sut = new LatticeReplicationGc(
            Services(provider),
            registry,
            Monitor(new LatticeReplicationOptions { ClusterId = "c", ReplogPartitions = 1 }));

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(0));
        Assert.That(report.CausalStable!.Entries.ContainsKey("site-z"), Is.False);
    }

    [Test]
    public async Task RunOnceAsync_reports_causal_stable_in_diagnostic_even_when_no_trim_happens()
    {
        var provider = new InMemoryWalStorageProvider();
        await SeedAsync(provider, 0,
            SetEntryWithVc("a", Hlc(50), Vc(("site-a", 50))));

        var registry = new InMemoryReplicationCursorRegistry();
        // Consumer's HLC cursor (10) is below the entry's HLC (50),
        // so nothing is trim-eligible by cursor. The frontier is
        // still surfaced in the diagnostic.
        await registry.ReportCursorAsync(Tree, "peer-A", Hlc(10), Vc(("site-a", 5)));

        var sut = new LatticeReplicationGc(
            Services(provider),
            registry,
            Monitor(new LatticeReplicationOptions { ClusterId = "c", ReplogPartitions = 1 }));

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(0));
        Assert.That(report.CausalStable, Is.Not.Null);
        Assert.That(report.CausalStable!.Entries["site-a"], Is.EqualTo(Hlc(5)));
    }

    [Test]
    public async Task RunOnceAsync_stops_at_first_vc_blocked_entry_even_when_later_entries_would_dominate()
    {
        var provider = new InMemoryWalStorageProvider();
        await SeedAsync(provider, 0,
            SetEntryWithVc("a", Hlc(10), Vc(("site-a", 10))),
            SetEntryWithVc("b", Hlc(20), Vc(("site-a", 999))), // blocked by frontier
            SetEntryWithVc("c", Hlc(30), Vc(("site-a", 5))));   // would pass but unreachable

        var registry = new InMemoryReplicationCursorRegistry();
        await registry.ReportCursorAsync(Tree, "peer-A", Hlc(100), Vc(("site-a", 100)));

        var sut = new LatticeReplicationGc(
            Services(provider),
            registry,
            Monitor(new LatticeReplicationOptions { ClusterId = "c", ReplogPartitions = 1 }));

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(1));
    }
}
