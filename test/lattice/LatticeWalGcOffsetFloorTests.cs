using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Regression tests for the durable leaf-materialiser <em>offset</em> floor in
/// <see cref="LatticeWalGc"/>. These reproduce the repocontext semantic-search
/// livelock: tombstone-compaction reap envelopes reuse the reaped entry's OLD
/// (low) HLC but are appended at HIGH WAL offsets, breaking the
/// HLC-monotonic-in-offset invariant the HLC trim floor relies on. A reap's low
/// HLC is <see cref="LatticeWalGc"/>-eligible under any positive cursor, so the
/// GC would trim it PAST a lagging leaf's projection checkpoint offset, tripping
/// the offset-space fall-off detector and wedging ingest. The offset floor makes
/// the GC never trim an entry at or above the lowest durable leaf checkpoint
/// offset, so the low-HLC/high-offset reaps survive until the leaf has read
/// them.
/// <para>
/// Those checkpoints are SCANNED-through, not applied-through (issue #2270): a
/// leaf advances its checkpoint over entries it skips as another leaf's, so a
/// surviving entry is not pinned "until the leaf applies it". Taking the MINIMUM
/// is what makes the floor sound - skipping only inflates the checkpoint of a
/// leaf that does NOT own the entry, and the one leaf that does own it cannot
/// skip it, so it holds the minimum down until it genuinely applies.
/// </para>
/// </summary>
[TestFixture]
public sealed partial class LatticeWalGcOffsetFloorTests
{
    private const string Tree = "tree";
    private const string LeafConsumer = "_lattice_materialiser_tree_leaf-1";

    /// <summary>
    /// A second leaf holding a durable HLC pin. Issue #2314 turns on whether this
    /// leaf appears in the OFFSETS plane at all, so the tests below vary only its
    /// offset-plane presence while holding its pin and cursor constant.
    /// </summary>
    private const string SilentConsumer = "_lattice_materialiser_tree_leaf-2";

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

    // Offsets 0 and 1 carry rising HLCs (a healthy applied prefix ending at the
    // leaf's checkpoint offset 1 / HLC 20). Offsets 2 and 3 are tombstone-reap
    // envelopes: appended AFTER offset 1 but carrying the reaped entries' OLD
    // low HLCs (5, 6) - lower than the checkpoint HLC yet at higher offsets.
    private static async Task<InMemoryWalStorageProvider> SeededProviderAsync()
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree,
            0,
            new[] { Entry(0, Hlc(10)), Entry(1, Hlc(20)), Entry(2, Hlc(5)), Entry(3, Hlc(6)) },
            CancellationToken.None);
        return provider;
    }

    private static IOptionsMonitor<LatticeOptions> Monitor()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        // Issue #3300: the durability hold engages by default for any tree that
        // has never published a durable offset floor. Several tests here model
        // exactly that state deliberately (a pre-fix build, or an old pin grain
        // during a rolling upgrade) in order to assert what the offset floor
        // does with it, so the hold would mask the axis under test. Opt out
        // explicitly (0 disables the hold).
        var options = new LatticeOptions { WalPartitions = 1, WalDurabilityHoldCeilingBytes = 0 };
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

    private static IServiceProvider ServicesWithThrowingOffsetRead(
        IWalStorageProvider provider,
        IReadOnlyDictionary<string, HybridLogicalClock> durablePins)
    {
        var sc = new ServiceCollection();
        sc.AddSingleton(provider);

        var pinGrain = Substitute.For<IWalMaterialiserPinGrain>();
        pinGrain.GetPinsAsync().Returns(Task.FromResult(durablePins));
        pinGrain.GetPinOffsetsAsync().Returns<Task<IReadOnlyDictionary<string, long>>>(
            _ => throw new InvalidOperationException("pin store unreachable"));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pinGrain);
        sc.AddSingleton(factory);

        return sc.BuildServiceProvider();
    }

    private static async Task<List<long>> SurvivingOffsetsAsync(IWalStorageProvider provider)
    {
        var survivors = new List<long>();
        await foreach (var entry in provider.ReadAsync(Tree, 0, fromOffsetExclusive: -1, maxEntries: 100, CancellationToken.None))
        {
            survivors.Add(entry.Offset);
        }
        return survivors;
    }

    [Test]
    public async Task RunOnceAsync_offset_floor_retains_low_hlc_reaps_above_leaf_checkpoint()
    {
        // The forward consumer (shipper) is at the WAL head; the leaf's durable
        // pin is at its checkpoint (HLC 20, offset 1). The reaps at offsets 2/3
        // carry HLCs 5/6 - BELOW the HLC floor (20) - so without the offset
        // floor the GC would trim them along with the applied prefix, dropping
        // committed WAL the lagging leaf has not yet applied and tripping the
        // offset-space fall-off detector.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(20));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(20),
        };
        var durableOffsets = new Dictionary<string, long>(StringComparer.Ordinal)
        {
            [LeafConsumer] = 1,
        };
        var sut = new LatticeWalGc(Services(provider, durablePins, durableOffsets), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(2),
            "The offset floor stops the trim just above the durable leaf checkpoint offset (the applied prefix 0..1 is trimmed).");

        var survivors = await SurvivingOffsetsAsync(provider);
        Assert.That(survivors, Is.EqualTo(new[] { 2L, 3L }),
            "The low-HLC/high-offset reap envelopes above the leaf checkpoint must survive.");
    }

    [Test]
    public async Task RunOnceAsync_without_offset_floor_trims_low_hlc_reaps_the_pre_fix_bug()
    {
        // Control: the SAME WAL and HLC pins, but the pin store reports NO
        // offsets (as an old pin grain would during a rolling upgrade, or the
        // pre-fix build). The HLC floor alone trims every entry whose HLC is at
        // or below the floor (20) - including the reaps at offsets 2/3 - which
        // is exactly the fall-off-inducing over-trim the offset floor prevents.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(20));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(20),
        };
        var sut = new LatticeWalGc(Services(provider, durablePins, durableOffsets: null), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(4),
            "Without the offset floor the HLC-eligible reaps are trimmed - the reproduced bug.");

        var survivors = await SurvivingOffsetsAsync(provider);
        Assert.That(survivors, Is.Empty);
    }

    [Test]
    public async Task RunOnceAsync_pin_store_unreachable_increments_offset_floor_unavailable_counter()
    {
        // Positive control for the issue #2314 instrument: force the durable
        // pin-offset read to throw (a persistently unreachable pin store) and
        // observe the counter move. Absent a forced fault this counter reads a
        // structural zero, so a test that never faults the store would assert
        // nothing about it.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(20));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(20),
        };
        var sut = new LatticeWalGc(
            ServicesWithThrowingOffsetRead(provider, durablePins), registry, Monitor());

        long observed = 0;
        string? observedTree = null;
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.WalGcOffsetFloorUnavailable,
            l => l.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
            {
                observed += measurement;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree)
                    {
                        observedTree = tag.Value as string;
                    }
                }
            }));

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(observed, Is.EqualTo(1),
            "The swallowed pin-store failure must tick the offset-floor-unavailable counter exactly once per pass.");
        Assert.That(observedTree, Is.EqualTo(Tree), "The measurement must be tagged with the tree.");

        // Behaviour is preserved: the pass still completes on the HLC floor
        // alone (no offset floor), trimming exactly as the null-offset control.
        Assert.That(report.EntriesTrimmed, Is.EqualTo(4));
    }

    [Test]
    public async Task RunOnceAsync_healthy_offset_read_leaves_counter_flat()
    {
        // Denominator companion: a pass that reads offsets successfully must NOT
        // tick the counter. This is what makes a non-zero reading mean
        // "unreachable" rather than merely "a pass ran".
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(20));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(20),
        };
        var durableOffsets = new Dictionary<string, long>(StringComparer.Ordinal)
        {
            [LeafConsumer] = 1,
        };
        var sut = new LatticeWalGc(
            Services(provider, durablePins, durableOffsets), registry, Monitor());

        long observed = 0;
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.WalGcOffsetFloorUnavailable,
            l => l.SetMeasurementEventCallback<long>((_, measurement, _, _) => observed += measurement));

        await sut.RunOnceAsync(Tree);

        Assert.That(observed, Is.EqualTo(0),
            "A successful offset read must not tick the unavailable counter.");
    }

    // ---------------------------------------------------------------------
    // The "-1" sentinel (issue #2699). The GetPinOffsetsAsync contract used to
    // document -1 as the STRONGEST pin ("pins the entire WAL"). It is the exact
    // opposite: -1 is excluded from the floor and constrains nothing. Nothing
    // executable pinned that, which is how the doc drifted to the inverse of the
    // code and stayed there. These three tests make the corrected contract
    // executable so it cannot drift back silently.
    // ---------------------------------------------------------------------

    [Test]
    public async Task RunOnceAsync_a_reported_minus_one_does_not_lower_the_offset_floor()
    {
        // Two participating leaves: one with a real checkpoint at offset 1, one
        // reporting -1 (no WAL-replay dependency). Under the documented-but-wrong
        // reading, the -1 would pin the whole WAL and nothing could ever be
        // trimmed. Under the real contract the -1 is skipped and the floor is the
        // surviving real checkpoint, so the applied prefix 0..1 still trims.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(20));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(20),
        };
        var durableOffsets = new Dictionary<string, long>(StringComparer.Ordinal)
        {
            [LeafConsumer] = 1,
            ["_lattice_materialiser_tree_leaf-empty"] = -1,
        };
        var sut = new LatticeWalGc(Services(provider, durablePins, durableOffsets), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(2),
            "A -1 alongside a real checkpoint must leave the floor at the real checkpoint (1), "
            + "not collapse it to -1.");

        var survivors = await SurvivingOffsetsAsync(provider);
        Assert.That(survivors, Is.EqualTo(new[] { 2L, 3L }),
            "The floor is still the real leaf checkpoint, so the reaps above it survive.");
    }

    [Test]
    public async Task RunOnceAsync_every_offset_minus_one_leaves_the_floor_unset_and_does_not_wedge_the_trim()
    {
        // The case the wrong doc made unanswerable, and the one that actually
        // occurs: a fleet of leaves that ALL report -1 (genuinely empty
        // partitions report -1 indefinitely and legitimately). If -1 pinned the
        // WAL, this tree could never trim again. It must fall through to the HLC
        // floor alone - the same outcome as a pin store that reports nothing.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(20));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(20),
        };
        var durableOffsets = new Dictionary<string, long>(StringComparer.Ordinal)
        {
            [LeafConsumer] = -1,
            ["_lattice_materialiser_tree_leaf-2"] = -1,
        };
        var sut = new LatticeWalGc(Services(provider, durablePins, durableOffsets), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(4),
            "An all--1 pin set imposes NO offset floor, so the HLC floor alone governs the trim "
            + "- identical to reporting no offsets at all. Were -1 treated as a pin the trim "
            + "would wedge at zero entries forever.");
    }

    [Test]
    public async Task RunOnceAsync_a_consumer_absent_from_the_pin_set_does_not_constrain_the_floor()
    {
        // Half of the corrected contract, and STILL true: the floor is computed
        // as a minimum over the leaves that REPORTED, so a leaf present in the
        // HLC pin set but absent from the offset set does not drag the floor to
        // 0. Absence is deliberately not read as offset 0 - that would be a
        // floor over a population the pin grain never measured, and would pin
        // the WAL forever for a leaf that has departed.
        //
        // ISSUE #2314 CHANGED WHAT THE PASS DOES WITH THAT FACT. Excluding the
        // silent leaf from the floor is sound arithmetic and unsound licence:
        // the resulting floor provably does not speak for it, so the pass no
        // longer trims on it. This test's assertion therefore moved from
        // "trims 2" to "trims 0"; the floor arithmetic it was written to pin is
        // unchanged and is asserted directly below, since a blocked pass would
        // otherwise hide a regression that DID drag the floor to 0.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(20));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(20),
            ["_lattice_materialiser_tree_leaf-silent"] = Hlc(20),
        };
        var durableOffsets = new Dictionary<string, long>(StringComparer.Ordinal)
        {
            [LeafConsumer] = 1,
        };
        var sut = new LatticeWalGc(Services(provider, durablePins, durableOffsets), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(0),
            "The floor cannot speak for the silent leaf, so the pass blocks rather than trimming on it.");

        var survivors = await SurvivingOffsetsAsync(provider);
        Assert.That(survivors, Is.EqualTo(new[] { 0L, 1L, 2L, 3L }),
            "Blocked, not trimmed-to-floor-0: had absence been read as offset 0 the entry at "
            + "offset 0 would have been trimmed, leaving three survivors.");
    }

    // ---------------------------------------------------------------------
    // ISSUE #2314: the floor is a minimum over the leaves that REPORTED, not
    // over the leaves that OWE entries, and an absent leaf used to be
    // indistinguishable from one reporting -1. These tests pin both halves:
    // an absent leaf now blocks the pass, and an abstaining leaf still does not.
    // ---------------------------------------------------------------------

    [Test]
    public async Task RunOnceAsync_pinned_consumer_absent_from_the_offsets_plane_blocks_the_trim()
    {
        // THE DEFECT, IN THE SHAPE THAT LOSES DATA. Two leaves hold durable
        // pins. leaf-1 has durably applied everything (offset 3). leaf-2 is in
        // the pin set and has never reported an offset - it has genuinely
        // applied only offset 1 and still owes the reaps at 2 and 3.
        //
        // Pre-fix the floor is min over REPORTERS = 3, which refuses nothing.
        // Because the offset floor can only ever REFUSE, a floor that is too
        // high hands the decision back to the HLC cursor axis, and the reaps at
        // offsets 2/3 carry HLCs 5/6 - below the HLC floor of 20 - so the axis
        // admits them and all four entries are trimmed. leaf-2 loses two
        // committed entries it had not applied.
        //
        // Post-fix the population gap is detected and the pass blocks, so the
        // WAL is retained until leaf-2 reports.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(20));
        await registry.ReportCursorAsync(Tree, SilentConsumer, Hlc(20));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(20),
            [SilentConsumer] = Hlc(20),
        };
        var durableOffsets = new Dictionary<string, long>(StringComparer.Ordinal)
        {
            [LeafConsumer] = 3,
        };
        var sut = new LatticeWalGc(Services(provider, durablePins, durableOffsets), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(0),
            "A floor computed without leaf-2 must not license a trim past what leaf-2 owes.");

        var survivors = await SurvivingOffsetsAsync(provider);
        Assert.That(survivors, Is.EqualTo(new[] { 0L, 1L, 2L, 3L }),
            "The low-HLC reaps leaf-2 has not applied must survive - pre-fix they were trimmed.");
    }

    [Test]
    public async Task RunOnceAsync_pinned_consumer_reporting_minus_one_does_not_block_the_trim()
    {
        // THE DISCRIMINATING CONTROL, and the half of issue #2314 that makes the
        // gate safe to ship. Identical to the test above in every respect except
        // that leaf-2 REPORTS -1 instead of being absent. A reported -1 is a real
        // answer from a participating leaf - "I hold no WAL-replay dependency" -
        // and is covered either by a paired zero-HLC block pin or by there being
        // no committed prefix to lose. It must therefore leave the tree
        // collecting exactly as before.
        //
        // Were absence and a reported -1 still rendered identically, this test
        // and the one above could not both pass: that is precisely the
        // indistinguishability issue #2314 reports.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(20));
        await registry.ReportCursorAsync(Tree, SilentConsumer, Hlc(20));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(20),
            [SilentConsumer] = Hlc(20),
        };
        var durableOffsets = new Dictionary<string, long>(StringComparer.Ordinal)
        {
            [LeafConsumer] = 3,
            [SilentConsumer] = -1,
        };
        var sut = new LatticeWalGc(Services(provider, durablePins, durableOffsets), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(4),
            "An abstaining leaf is a reporting leaf: it must not be mistaken for a population gap.");
    }

    [Test]
    public async Task RunOnceAsync_every_pinned_consumer_reporting_leaves_the_trim_at_the_real_floor()
    {
        // The healthy control. leaf-2 reports a real, lagging checkpoint, so the
        // floor is the minimum over both (1) and the pass trims the applied
        // prefix and stops - the pre-#2314 behaviour of a fully-reported
        // population, unchanged. This is what makes the block above mean "a
        // consumer is missing" rather than "a second consumer exists".
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(20));
        await registry.ReportCursorAsync(Tree, SilentConsumer, Hlc(20));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(20),
            [SilentConsumer] = Hlc(20),
        };
        var durableOffsets = new Dictionary<string, long>(StringComparer.Ordinal)
        {
            [LeafConsumer] = 3,
            [SilentConsumer] = 1,
        };
        var sut = new LatticeWalGc(Services(provider, durablePins, durableOffsets), registry, Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(2));

        var survivors = await SurvivingOffsetsAsync(provider);
        Assert.That(survivors, Is.EqualTo(new[] { 2L, 3L }),
            "The lagging leaf's real checkpoint holds the reaps down, without blocking the pass.");
    }

    [Test]
    public async Task RunOnceAsync_offset_population_gap_increments_the_population_gap_counter()
    {
        // The instrument's positive control, and its ARITY: the counter is
        // charged the number of unreported CONSUMERS, not a pass count, so two
        // silent leaves add two.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(20));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(20),
            [SilentConsumer] = Hlc(20),
            ["_lattice_materialiser_tree_leaf-3"] = Hlc(20),
        };
        var durableOffsets = new Dictionary<string, long>(StringComparer.Ordinal)
        {
            [LeafConsumer] = 3,
        };
        var sut = new LatticeWalGc(Services(provider, durablePins, durableOffsets), registry, Monitor());

        long observed = 0;
        string? observedTree = null;
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.WalGcOffsetFloorPopulationGap,
            l => l.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
            {
                observed += measurement;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree)
                    {
                        observedTree = tag.Value as string;
                    }
                }
            }));

        await sut.RunOnceAsync(Tree);

        Assert.That(observed, Is.EqualTo(2),
            "The counter records how many pinned consumers never reported, not how many passes saw a gap.");
        Assert.That(observedTree, Is.EqualTo(Tree), "The measurement must be tagged with the tree.");
    }

    [Test]
    public async Task RunOnceAsync_fully_reported_population_leaves_the_population_gap_counter_flat()
    {
        // Denominator companion. Without this, a counter that ticked on every
        // pass would pass the positive control above and mean nothing.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(20));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(20),
            [SilentConsumer] = Hlc(20),
        };
        var durableOffsets = new Dictionary<string, long>(StringComparer.Ordinal)
        {
            [LeafConsumer] = 3,
            [SilentConsumer] = -1,
        };
        var sut = new LatticeWalGc(Services(provider, durablePins, durableOffsets), registry, Monitor());

        long observed = 0;
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.WalGcOffsetFloorPopulationGap,
            l => l.SetMeasurementEventCallback<long>((_, measurement, _, _) => observed += measurement));

        await sut.RunOnceAsync(Tree);

        Assert.That(observed, Is.EqualTo(0),
            "A population in which every pinned consumer reported - including by abstaining - has no gap.");
    }
}
