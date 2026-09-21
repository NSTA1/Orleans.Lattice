using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Regression tests for the WAL GC trim-stop reason instrument (issue #3149).
/// <para>
/// The defect these cover is an <em>observability</em> one with a real cost. A
/// tree whose durable leaf-checkpoint offset floor sits below its entire
/// retained range stops every trim scan on the first entry it examines, reclaims
/// nothing, and grows without bound - yet because the offset floor is a
/// different gate from the consumer-cursor floor, it still reports
/// <see cref="WalGcCursorFloorState.Available"/> and classifies every pass as
/// <c>over_ceiling</c>. That is the one outcome arm on which the blocking-pin
/// diagnostic is never written, so the tree published a breach with no stated
/// cause and nothing in the metric surface could distinguish it from a tree that
/// was trimming as hard as it could.
/// </para>
/// <para>
/// The load-bearing property here is <b>discrimination</b>, not merely emission:
/// the two ways a scan can stop having reclaimed nothing must land on different
/// arms, because they indict different subsystems. A test that only asserted
/// "some arm advanced" would pass against a build that collapsed them.
/// </para>
/// </summary>
[TestFixture]
public sealed partial class LatticeWalGcTrimStopReasonTests
{
    private const string Tree = "tree";
    private const string LeafConsumer = "_lattice_materialiser_tree_leaf-1";

    private sealed record Stop(string Reason, long Value, string? Tree);

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static WalEntry Entry(long offset, HybridLogicalClock ts) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = $"k{offset}",
            Value = new byte[] { 1 },
            Timestamp = ts,
            OriginClusterId = "site-a",
        },
    };

    private static IOptionsMonitor<LatticeOptions> Monitor(int partitions = 1)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        // Issue #3300: the durability hold engages by default for any tree that
        // has never published a durable offset floor, which is true of every
        // tree reached through this helper. These tests assert trim STOP
        // reasons, and a hold stops the scan before the reason under test is
        // reached. Opt out explicitly (0 disables the hold); the hold's own
        // arm is covered by the DurabilityHold partial, which builds its
        // options separately.
        var options = new LatticeOptions { WalPartitions = partitions, WalDurabilityHoldCeilingBytes = 0 };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static IServiceProvider Services(
        IWalStorageProvider provider,
        IReadOnlyDictionary<string, HybridLogicalClock> durablePins,
        IReadOnlyDictionary<string, long>? durableOffsets)
    {
        var sc = new ServiceCollection();
        sc.AddSingleton(provider);

        var pinGrain = Substitute.For<IWalMaterialiserPinGrain>();
        pinGrain.GetPinsAsync().Returns(Task.FromResult(durablePins));
        if (durableOffsets is not null)
        {
            pinGrain.GetPinOffsetsAsync().Returns(Task.FromResult(durableOffsets));
        }

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pinGrain);
        sc.AddSingleton(factory);

        return sc.BuildServiceProvider();
    }

    /// <summary>
    /// Builds a collector over a seeded WAL. <paramref name="checkpointOffset"/>
    /// is the durable leaf checkpoint the offset floor is taken from; pass
    /// <see langword="null"/> to report no offsets at all.
    /// </summary>
    private static async Task<LatticeWalGc> CollectorAsync(
        InMemoryWalStorageProvider provider,
        long? checkpointOffset,
        int partitions = 1)
    {
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(20));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(20),
        };
        var durableOffsets = checkpointOffset is { } offset
            ? new Dictionary<string, long>(StringComparer.Ordinal) { [LeafConsumer] = offset }
            : null;

        return new LatticeWalGc(
            Services(provider, durablePins, durableOffsets), registry, Monitor(partitions));
    }

    private static async Task<(LatticeWalGcReport Report, List<Stop> Stops)> RunAsync(LatticeWalGc sut)
    {
        var stops = new List<Stop>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.WalGcTrimStops,
            l => l.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
            {
                string? reason = null;
                string? tree = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagReason)
                    {
                        reason = tag.Value as string;
                    }
                    else if (tag.Key == LatticeMetrics.TagTree)
                    {
                        tree = tag.Value as string;
                    }
                }

                stops.Add(new Stop(reason ?? "<untagged>", measurement, tree));
            }));

        var report = await sut.RunOnceAsync(Tree);
        return (report, stops);
    }

    private static List<string> Advanced(IEnumerable<Stop> stops) =>
        stops.Where(static s => s.Value > 0).Select(static s => s.Reason).ToList();

    [Test]
    public async Task RunOnceAsync_scan_stopped_by_the_offset_floor_reports_offset_floor_having_reclaimed_nothing()
    {
        // The exact production signature from issue #3149. The retained range
        // begins at offset 5 (an earlier trim already removed the prefix) while
        // the durable leaf checkpoint is stranded at offset 2, so the very first
        // entry the scan examines is above the floor and it stops immediately.
        // Every entry is HLC-eligible, so nothing but the offset floor is
        // holding this WAL.
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree,
            0,
            new[] { Entry(5, Hlc(10)), Entry(6, Hlc(11)), Entry(7, Hlc(12)) },
            CancellationToken.None);

        var sut = await CollectorAsync(provider, checkpointOffset: 2);

        var (report, stops) = await RunAsync(sut);

        Assert.That(report.EntriesTrimmed, Is.Zero,
            "The stranded offset floor stops the scan on its first entry, so the pass reclaims nothing.");
        Assert.That(Advanced(stops), Is.EqualTo(new[] { "offset_floor" }),
            "A pass held by the durable checkpoint floor must say so, rather than presenting as a healthy tree "
            + "that merely had nothing to do.");
    }

    [Test]
    public async Task RunOnceAsync_scan_stopped_by_hlc_eligibility_reports_cursor_floor_having_reclaimed_nothing()
    {
        // The other way to reclaim nothing, and the one that must NOT be
        // confused with the above. The offset floor is generous (10, above every
        // seeded offset) so it never fires; the entries carry HLCs above the
        // cursor floor of 20, so the eligibility predicate stops the scan on its
        // first entry instead.
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree,
            0,
            new[] { Entry(0, Hlc(100)), Entry(1, Hlc(101)), Entry(2, Hlc(102)) },
            CancellationToken.None);

        var sut = await CollectorAsync(provider, checkpointOffset: 10);

        var (report, stops) = await RunAsync(sut);

        Assert.That(report.EntriesTrimmed, Is.Zero);
        Assert.That(Advanced(stops), Is.EqualTo(new[] { "cursor_floor" }),
            "A scan held by the HLC clause is a consumer-cursor problem and must not be reported on the "
            + "offset-floor arm, nor on the arms that indict a causal frontier or a buffer pin.");
    }

    [Test]
    public async Task RunOnceAsync_reports_a_different_arm_for_each_way_a_pass_can_reclaim_nothing()
    {
        // The property the instrument exists for, asserted directly rather than
        // left to be inferred from two tests passing. Both passes reclaim zero
        // and both would have been indistinguishable before this change.
        var floorBlocked = new InMemoryWalStorageProvider();
        await floorBlocked.AppendBatchAsync(
            Tree, 0, new[] { Entry(5, Hlc(10)) }, CancellationToken.None);

        var hlcBlocked = new InMemoryWalStorageProvider();
        await hlcBlocked.AppendBatchAsync(
            Tree, 0, new[] { Entry(0, Hlc(100)) }, CancellationToken.None);

        var (floorReport, floorStops) = await RunAsync(await CollectorAsync(floorBlocked, checkpointOffset: 2));
        var (hlcReport, hlcStops) = await RunAsync(await CollectorAsync(hlcBlocked, checkpointOffset: 10));

        Assert.Multiple(() =>
        {
            Assert.That(floorReport.EntriesTrimmed, Is.Zero);
            Assert.That(hlcReport.EntriesTrimmed, Is.Zero);
        });

        Assert.That(Advanced(floorStops), Is.Not.EqualTo(Advanced(hlcStops)),
            "Two passes that both reclaimed nothing for opposite reasons must not land on the same arm - that "
            + "collapse is the defect this instrument was added to remove.");
    }

    [Test]
    public async Task RunOnceAsync_scan_consuming_the_whole_log_reports_exhausted()
    {
        // The healthy reading. Every entry is eligible and below the floor, so
        // the scan runs out of entries rather than stopping on one.
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree,
            0,
            new[] { Entry(0, Hlc(10)), Entry(1, Hlc(11)), Entry(2, Hlc(12)) },
            CancellationToken.None);

        var sut = await CollectorAsync(provider, checkpointOffset: 10);

        var (report, stops) = await RunAsync(sut);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(3),
            "Every seeded entry is eligible and below the offset floor.");
        Assert.That(Advanced(stops), Is.EqualTo(new[] { "exhausted" }));
    }

    [Test]
    public async Task RunOnceAsync_empty_shard_reports_empty_rather_than_exhausted()
    {
        // Distinct from exhausted on purpose: an empty shard has no backlog to
        // account for, whereas an exhausted one has just reclaimed its whole
        // log. Collapsing them would put every idle shard in a fleet onto the
        // same arm as the shards doing the most work.
        var provider = new InMemoryWalStorageProvider();

        var sut = await CollectorAsync(provider, checkpointOffset: 10);

        var (report, stops) = await RunAsync(sut);

        Assert.That(report.EntriesTrimmed, Is.Zero);
        Assert.That(Advanced(stops), Is.EqualTo(new[] { "empty" }));
    }

    [Test]
    public async Task RunOnceAsync_primes_every_arm_so_an_absent_series_means_this_silo_is_not_reporting()
    {
        // Priming is what lets a reader treat a flat zero on offset_floor as a
        // measured absence. Unprimed, "this tree is healthy" and "WAL GC is not
        // running here" would be the same silence - which is the ambiguity the
        // whole instrument exists to remove, reproduced one level down.
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree, 0, new[] { Entry(0, Hlc(10)) }, CancellationToken.None);

        var sut = await CollectorAsync(provider, checkpointOffset: 10);

        var (_, stops) = await RunAsync(sut);

        Assert.That(
            stops.Select(static s => s.Reason).Distinct().OrderBy(static r => r, StringComparer.Ordinal),
            Is.EqualTo(new[] { "block_pin", "causal_frontier", "cursor_floor", "durability_hold", "durability_unverified", "durable_offset_refusal", "empty", "exhausted", "offset_floor" }),
            "Every arm must carry a series after a single pass, whether or not it advanced.");

        Assert.That(stops.Select(static s => s.Tree), Is.All.EqualTo(Tree),
            "Every measurement is attributed to the tree, so one stranded tree is never averaged away.");
    }

    [Test]
    public async Task RunOnceAsync_primes_every_arm_even_when_the_pass_returns_before_the_trim_loop()
    {
        // The early-return path: no consumer has reported a cursor and no TTL is
        // configured, so the pass returns without scanning anything. This is
        // precisely the pass a reader investigating a stuck tree is most likely
        // to meet, so it must still publish the four arms.
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree, 0, new[] { Entry(0, Hlc(10)) }, CancellationToken.None);

        var sc = new ServiceCollection();
        sc.AddSingleton<IWalStorageProvider>(provider);
        var sut = new LatticeWalGc(
            sc.BuildServiceProvider(), new InMemoryWalCursorRegistry(), Monitor());

        var (report, stops) = await RunAsync(sut);

        Assert.That(report.EntriesTrimmed, Is.Zero, "No cursor and no TTL: the pass is a no-op by design.");
        Assert.That(Advanced(stops), Is.Empty, "A pass that never scanned must not advance any arm.");
        Assert.That(
            stops.Select(static s => s.Reason).Distinct().OrderBy(static r => r, StringComparer.Ordinal),
            Is.EqualTo(new[] { "block_pin", "causal_frontier", "cursor_floor", "durability_hold", "durability_unverified", "durable_offset_refusal", "empty", "exhausted", "offset_floor" }),
            "The arms are primed above the early return, not merely inside the trim loop.");
    }

    [Test]
    public async Task RunOnceAsync_records_one_stop_per_shard_so_the_arms_sum_to_the_shard_count()
    {
        // Per-shard accounting. A tree whose shards are in different states must
        // report each of them, rather than reducing the tree to a single verdict
        // and hiding the one shard that is stranded.
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree, 0, new[] { Entry(0, Hlc(10)) }, CancellationToken.None);
        await provider.AppendBatchAsync(
            Tree, 1, new[] { Entry(5, Hlc(10)) }, CancellationToken.None);

        var sut = await CollectorAsync(provider, checkpointOffset: 2, partitions: 2);

        var (_, stops) = await RunAsync(sut);

        var advanced = Advanced(stops);
        Assert.That(advanced, Has.Count.EqualTo(2),
            "Two shards were scanned, so exactly two measurements must advance.");
        Assert.That(advanced, Does.Contain("exhausted"),
            "Shard 0 sits below the floor at offset 2 and is fully reclaimable.");
        Assert.That(advanced, Does.Contain("offset_floor"),
            "Shard 1 begins at offset 5, above the stranded floor, and is held - which must stay visible "
            + "rather than being absorbed by its healthy sibling.");
    }
}
