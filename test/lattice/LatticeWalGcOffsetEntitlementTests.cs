using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Regression tests for the WAL GC trim <em>entitlement</em> rule (issue #3172).
/// <para>
/// Before this fix entitlement was HLC-only: an entry could be trimmed when the
/// minimum consumer cursor or the retention TTL ceiling accepted it, and the
/// durable materialiser <em>offset</em> floor was applied as a pure stop. The
/// floor could therefore only ever subtract trim rights and never grant them,
/// so a consumer that advanced its durable applied offset while its HLC
/// checkpoint stayed flat earned no trim right at all and its WAL was retained
/// forever. That is not a degenerate shape: <c>WalMaterialiserPinGrain.Merge</c>
/// documents the two axes as advancing independently, and names the concrete
/// case (a tombstone-compaction reap advances a leaf's applied offset while its
/// HLC checkpoint does not move).
/// </para>
/// <para>
/// The rule is now a disjunction over two independent axes - the HLC axis OR the
/// offset axis - with the causal-frontier and block-pin clauses still applying
/// conjunctively in both cases. The offset axis is the stronger of the two where
/// it applies: the floor is a minimum of last-durably-applied offsets, so an
/// entry at or below it has been applied and persisted, which is a firmer
/// statement than "some HLC frontier moved past it".
/// </para>
/// <para>
/// The load-bearing subtlety these tests pin is that the offset floor speaks for
/// the leaf materialisers that reported an offset AND NOTHING ELSE. A view
/// maintainer, a WAL log subscriber, the backup capture service and the
/// replication shipper all report HLC cursors and never offsets, so an offset
/// axis that ignored them would trim straight past a live consumer and strand it
/// off its own log. The admission is therefore gated on a second minimum taken
/// over exactly the consumers the floor does not cover.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeWalGcOffsetEntitlementTests
{
    private const string Tree = "tree";
    private const string LeafConsumer = "_lattice_materialiser_tree_leaf-1";

    private sealed record Stop(string Reason, long Value);

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

    /// <summary>
    /// Four entries whose HLCs all sit ABOVE the flat cursor the tests use, so
    /// the HLC axis refuses every one of them and only the offset axis can
    /// admit anything. This is the live payload-tree shape: offsets advance,
    /// the frontier does not.
    /// </summary>
    private static async Task<InMemoryWalStorageProvider> SeededProviderAsync(int partition = 0)
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree,
            partition,
            new[] { Entry(0, Hlc(10)), Entry(1, Hlc(20)), Entry(2, Hlc(30)), Entry(3, Hlc(40)) },
            CancellationToken.None);
        return provider;
    }

    private static IOptionsMonitor<LatticeOptions> Monitor(int partitions = 1)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        // Issue #3300: the durability hold engages by default for any tree that
        // has never published a durable offset floor, which is the very state
        // these tests construct in order to assert that the offset axis admits
        // nothing. The hold would stop the trim before that assertion is
        // reached, so opt out explicitly (0 disables the hold).
        var options = new LatticeOptions { WalPartitions = partitions, WalDurabilityHoldCeilingBytes = 0 };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static IServiceProvider Services(
        IWalStorageProvider provider,
        IReadOnlyDictionary<string, HybridLogicalClock> durablePins,
        IReadOnlyDictionary<string, long>? durableOffsets,
        bool throwOnOffsetRead = false)
    {
        var sc = new ServiceCollection();
        sc.AddSingleton(provider);

        var pinGrain = Substitute.For<IWalMaterialiserPinGrain>();
        pinGrain.GetPinsAsync().Returns(Task.FromResult(durablePins));
        if (throwOnOffsetRead)
        {
            pinGrain.GetPinOffsetsAsync().Returns<Task<IReadOnlyDictionary<string, long>>>(
                _ => throw new InvalidOperationException("pin store unreachable"));
        }
        else if (durableOffsets is not null)
        {
            pinGrain.GetPinOffsetsAsync().Returns(Task.FromResult(durableOffsets));
        }

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pinGrain);
        sc.AddSingleton(factory);

        return sc.BuildServiceProvider();
    }

    private static async Task<(LatticeWalGcReport Report, List<string> Stops)> RunAsync(LatticeWalGc sut)
    {
        var stops = new List<Stop>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.WalGcTrimStops,
            l => l.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
            {
                string? reason = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagReason)
                    {
                        reason = tag.Value as string;
                    }
                }

                stops.Add(new Stop(reason ?? "<untagged>", measurement));
            }));

        var report = await sut.RunOnceAsync(Tree);
        return (report, stops.Where(static s => s.Value > 0).Select(static s => s.Reason).ToList());
    }

    /// <summary>
    /// Builds the live-defect shape: a single leaf materialiser whose HLC cursor
    /// and durable pin are both stuck at tick 1 (the dead frontier axis) while
    /// its durable applied offset has advanced to <paramref name="checkpointOffset"/>.
    /// </summary>
    private static LatticeWalGc FlatFrontierGc(
        IWalStorageProvider provider,
        long? checkpointOffset,
        InMemoryWalCursorRegistry registry,
        bool throwOnOffsetRead = false,
        int partitions = 1)
    {
        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(1),
        };
        var durableOffsets = checkpointOffset is { } offset
            ? new Dictionary<string, long>(StringComparer.Ordinal) { [LeafConsumer] = offset }
            : null;

        return new LatticeWalGc(
            Services(provider, durablePins, durableOffsets, throwOnOffsetRead),
            registry,
            Monitor(partitions));
    }

    private static async Task<InMemoryWalCursorRegistry> FlatLeafRegistryAsync()
    {
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(1));
        return registry;
    }

    [Test]
    public async Task RunOnceAsync_durable_offset_floor_admits_entries_the_flat_hlc_cursor_refuses()
    {
        // The founding defect of issue #3172, in its purest form. The only
        // consumer is a leaf materialiser that has durably applied every entry
        // (offset 3) while its HLC checkpoint has never moved past tick 1. Every
        // entry's HLC is above that cursor, so the HLC axis refuses all four and
        // the pre-fix build trims NOTHING, forever - the 1.15 GB stranded-WAL
        // signature measured on the live payload tree.
        //
        // Post-fix the offset axis admits them: the floor IS the leaf's own
        // last-durably-applied offset, and there is no other consumer for it to
        // fail to speak for, so every entry at or below it has provably been
        // applied and persisted by everyone who could ever replay it.
        var provider = await SeededProviderAsync();
        var registry = await FlatLeafRegistryAsync();
        var sut = FlatFrontierGc(provider, checkpointOffset: 3, registry);

        var (report, stops) = await RunAsync(sut);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(4),
            "An entry at or below the durable applied-offset floor has been applied and persisted by every "
            + "consumer that could replay it, so a dead HLC frontier must not retain it forever.");
        Assert.That(stops, Is.EqualTo(new[] { "exhausted" }),
            "Having trimmed the whole retained range the scan must report the healthy arm, not a floor arm.");
    }

    [Test]
    public async Task RunOnceAsync_offset_admission_still_stops_at_the_floor_and_attributes_it()
    {
        // The floor grants entitlement up to itself and not one entry further.
        // The leaf has durably applied through offset 1 only, so offsets 0 and 1
        // are admitted by the offset axis and offset 2 is above the floor. The
        // pre-existing offset-floor STOP is untouched and still fires there, so
        // the pass is attributed to the constraint that actually bound it rather
        // than to the cursor, which refused every entry and is no longer the
        // binding reason the scan halted where it did.
        var provider = await SeededProviderAsync();
        var registry = await FlatLeafRegistryAsync();
        var sut = FlatFrontierGc(provider, checkpointOffset: 1, registry);

        var (report, stops) = await RunAsync(sut);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(2),
            "Only the entries at or below the durable applied-offset floor may be admitted by the offset axis.");
        Assert.That(stops, Is.EqualTo(new[] { "offset_floor" }),
            "With the offset axis granting entitlement, the floor is an honest binding constraint and must be "
            + "the reported stop reason.");
    }

    [Test]
    public async Task RunOnceAsync_offset_admission_does_not_trim_past_a_consumer_the_floor_does_not_cover()
    {
        // THE safety property of the whole change. The durable offset floor is a
        // minimum over leaf materialisers that reported an offset, and over
        // nothing else: view maintainers, WAL log subscribers, the backup
        // capture service and the replication shipper all report cursors and
        // never offsets. A naive disjunction would read the leaf's offset 3 as
        // "everyone has applied through 3" and trim the whole log out from under
        // the shipper, which is the fall-off-the-log data loss the Coyote trim
        // model and the shipping chaos suite both assert against.
        //
        // The shipper here sits at HLC 5, below every entry, so it must hold the
        // entire retained range even though the leaf has durably applied all of
        // it.
        var provider = await SeededProviderAsync();
        var registry = await FlatLeafRegistryAsync();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(5));
        var sut = FlatFrontierGc(provider, checkpointOffset: 3, registry);

        var (report, stops) = await RunAsync(sut);

        Assert.That(report.EntriesTrimmed, Is.Zero,
            "A consumer the offset floor does not speak for must still hold the WAL; the offset axis may only "
            + "relieve the consumers it is actually evidence about.");
        Assert.That(stops, Is.EqualTo(new[] { "cursor_floor" }),
            "The refusal is a cursor-side one and must be attributed to the cursor, which is what names the "
            + "uncovered consumer population as the holder.");
    }

    [Test]
    public async Task RunOnceAsync_offset_admission_applies_when_the_uncovered_consumer_is_ahead()
    {
        // The complement of the previous test, and what stops the safety gate
        // from being a blanket refusal. The same uncovered shipper is present,
        // but at HLC 50 it is ahead of every entry, so it holds nothing. The
        // leaf's flat HLC cursor still drags the tree-wide minimum down to tick
        // 1 and still refuses all four entries on the HLC axis; the offset axis
        // must now carry the pass.
        var provider = await SeededProviderAsync();
        var registry = await FlatLeafRegistryAsync();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(50));
        var sut = FlatFrontierGc(provider, checkpointOffset: 3, registry);

        var (report, stops) = await RunAsync(sut);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(4),
            "An uncovered consumer that is ahead of the retained range holds nothing, so the offset axis must "
            + "still admit the entries the leaf has durably applied.");
        Assert.That(stops, Is.EqualTo(new[] { "exhausted" }));
    }

    [Test]
    public async Task RunOnceAsync_absent_offset_floor_admits_nothing()
    {
        // Fail closed. A pin store that reports no offsets at all (an older pin
        // grain mid-rolling-upgrade, or a host that never wired the offset
        // contract) yields no floor, and a null floor must grant NO offset
        // entitlement whatsoever - leaving the predicate byte-identical to its
        // pre-#3172 self rather than merely close to it.
        var provider = await SeededProviderAsync();
        var registry = await FlatLeafRegistryAsync();
        var sut = FlatFrontierGc(provider, checkpointOffset: null, registry);

        var (report, stops) = await RunAsync(sut);

        Assert.That(report.EntriesTrimmed, Is.Zero,
            "With no durable offset floor the offset axis must admit nothing, preserving pre-fix behaviour exactly.");
        Assert.That(stops, Is.EqualTo(new[] { "cursor_floor" }));
    }

    [Test]
    public async Task RunOnceAsync_unreachable_pin_store_admits_nothing()
    {
        // The same fail-closed contract on the fault path rather than the empty
        // path. An unreachable pin store is already instrumented
        // (WalGcOffsetFloorUnavailable); what matters here is that the swallowed
        // failure removes the offset entitlement rather than leaving a stale or
        // assumed floor behind to trim against. Since issue #3576 it removes
        // more than that: the whole pass fails closed and never enters the trim
        // scan, so no stop reason is recorded at all.
        var provider = await SeededProviderAsync();
        var registry = await FlatLeafRegistryAsync();
        var sut = FlatFrontierGc(provider, checkpointOffset: 3, registry, throwOnOffsetRead: true);

        var (report, stops) = await RunAsync(sut);

        Assert.That(report.EntriesTrimmed, Is.Zero,
            "A pin-store failure must remove the offset entitlement, not be trimmed through on a remembered floor.");
        Assert.That(stops, Is.Empty,
            "An unreadable offset census skips the trim scan entirely rather than scanning on the HLC floor.");
    }

    [Test]
    public async Task RunOnceAsync_blocked_partition_grants_no_offset_admission()
    {
        // A partition blocked by an unusable (HLC Zero) durable pin has a leaf
        // that never reached a durable checkpoint. That leaf reports offset -1,
        // which is excluded from the floor, so the floor demonstrably does not
        // speak for it. The cursor branch is already disabled there (issues
        // #2849 / #2702) and the offset branch must be too, or the new axis
        // would walk straight around a protection the old one honours.
        //
        // Two partitions so the tree still has a cursor predicate somewhere and
        // the pass genuinely reaches the trim scan instead of returning early.
        var provider = await SeededProviderAsync();
        var registry = await FlatLeafRegistryAsync();

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(1),
            // Registry-absent, HLC Zero: blocks partition 0 specifically.
            ["_lattice_materialiser_tree_leaf-2_0"] = HybridLogicalClock.Zero,
        };
        var durableOffsets = new Dictionary<string, long>(StringComparer.Ordinal)
        {
            [LeafConsumer] = 3,
        };
        var sut = new LatticeWalGc(
            Services(provider, durablePins, durableOffsets), registry, Monitor(partitions: 2));

        var (report, stops) = await RunAsync(sut);

        Assert.That(report.EntriesTrimmed, Is.Zero,
            "A partition blocked by an unusable durable pin must not be trimmed by the offset axis either.");
        Assert.That(stops, Does.Not.Contain("exhausted"),
            "Nothing in the blocked partition may be reclaimed, so no scan there may report having run to the end.");
    }

    [Test]
    public void ClassifyEntry_with_no_offset_admission_is_the_pre_fix_hlc_only_predicate()
    {
        // The fail-closed contract stated directly against the decision core,
        // with no GC pass in the way. A null admission is what every caller
        // supplies when no durable offset floor could be established, and it
        // must leave the predicate exactly as it was before issue #3172: an
        // entry above the cursor is refused on the cursor, whatever its offset.
        var verdict = WalGcTrimCore.ClassifyEntry(
            entryTimestamp: Hlc(10),
            entryVectorClock: null,
            entryOffset: 0,
            minCursor: Hlc(1),
            ttlCeiling: null,
            causalStable: null,
            blockedFloor: null,
            offsetAdmission: null);

        Assert.That(verdict, Is.EqualTo(WalGcTrimEligibility.CursorFloor),
            "With no offset admission the entitlement clause must be its HLC-only self.");
    }

    [Test]
    public void ClassifyEntry_offset_admission_does_not_bypass_the_causal_frontier_clause()
    {
        // The disjunction is scoped to the entitlement clause ALONE. The causal
        // frontier guards replication origins, which the durable materialiser
        // offset floor knows nothing whatsoever about - a leaf applying an entry
        // locally says nothing about whether a peer region's stability has
        // advanced past it. An offset axis that short-circuited this clause
        // would trim a replicated entry out from under an origin that has not
        // stabilised, so it must remain conjunctive in both arms.
        var admission = new WalGcOffsetAdmission(Floor: 10, UncoveredCursor: null);
        var entryVector = new VersionVector { Entries = { ["site-b"] = Hlc(99) } };
        var stable = new VersionVector { Entries = { ["site-b"] = Hlc(1) } };

        var verdict = WalGcTrimCore.ClassifyEntry(
            entryTimestamp: Hlc(10),
            entryVectorClock: entryVector,
            entryOffset: 0,
            minCursor: Hlc(1),
            ttlCeiling: null,
            causalStable: stable,
            blockedFloor: null,
            offsetAdmission: admission);

        Assert.That(verdict, Is.EqualTo(WalGcTrimEligibility.CausalFrontier),
            "The offset axis supplements the entitlement clause only; it must never satisfy the causal frontier.");
    }

    [Test]
    public void ClassifyEntry_offset_admission_does_not_bypass_the_block_pin_clause()
    {
        // The same scoping argument for the other conjunctive clause. A block
        // pin is a buffering receiver deliberately holding WAL so it can recover
        // from buffer state; it is a hold rather than a lag, and the offset
        // floor is not evidence about it either.
        var admission = new WalGcOffsetAdmission(Floor: 10, UncoveredCursor: null);

        var verdict = WalGcTrimCore.ClassifyEntry(
            entryTimestamp: Hlc(10),
            entryVectorClock: null,
            entryOffset: 0,
            minCursor: Hlc(1),
            ttlCeiling: null,
            causalStable: null,
            blockedFloor: Hlc(5),
            offsetAdmission: admission);

        Assert.That(verdict, Is.EqualTo(WalGcTrimEligibility.BlockPin),
            "A buffering receiver's pin must hold the entry regardless of what the durable offset floor admits.");
    }

    [Test]
    public void Admits_refuses_an_entry_above_the_floor()
    {
        // The admission is the exact complement of the offset-floor STOP the
        // scan already applies, so it can never reach an entry the stop would
        // not have walked past. That keeps the cross-partition conservatism of
        // the single global minimum intact: offsets are only comparable within a
        // partition, and the admission re-uses the stop's own comparison rather
        // than deriving a second, looser one.
        var admission = new WalGcOffsetAdmission(Floor: 2, UncoveredCursor: null);

        Assert.Multiple(() =>
        {
            Assert.That(admission.Admits(Hlc(10), entryOffset: 2), Is.True, "At the floor is applied and durable.");
            Assert.That(admission.Admits(Hlc(10), entryOffset: 3), Is.False, "Above the floor is not.");
        });
    }
}
