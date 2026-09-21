using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Regression tests for the durability arms of the WAL GC trim-stop instrument
/// and for the bounded durability hold (issue #3300).
/// <para>
/// The defect these cover destroyed data rather than merely hiding it. A tree
/// whose durable materialiser offset floor was <b>absent</b> - no leaf had
/// durably applied anything - had its offset gate skipped entirely, because that
/// gate is written <c>if (offsetFloor is { } floor &amp;&amp; ...)</c> and a null
/// floor fails the pattern. The scan then walked the whole shard, released every
/// entry, and reported the same <c>exhausted</c> arm a perfectly healthy tree
/// reports. In the field that ran for eleven hours across 120+ writes without a
/// single series changing value.
/// </para>
/// <para>
/// Two independent properties are asserted here, and they fail differently.
/// <b>Naming</b>: a pass that released entries without a durable floor must land
/// on its own arm, so "I checked and everything was releasable" and "I could not
/// check, so I released everything" stop being byte-identical readings.
/// <b>Bounding</b>: with a hold configured, those entries must be retained
/// instead - but only up to a ceiling, past which the collector trims and says
/// loudly that it did. An unbounded hold would be the easy fix and the wrong
/// one; it recreates issue #3094 on every tree that legitimately has no
/// materialiser wired.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeWalGcDurabilityHoldTests
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

    private static IOptionsMonitor<LatticeOptions> Monitor(long? holdCeiling, bool useDefaultCeiling)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var options = new LatticeOptions
        {
            WalPartitions = 1,
        };

        if (!useDefaultCeiling)
        {
            options.WalDurabilityHoldCeilingBytes = holdCeiling;
        }

        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    /// <summary>
    /// A provider that can append, read and trim but cannot weigh itself: it
    /// leaves both byte-accounting members at their <c>-1</c> interface
    /// defaults. That is the shape in which the durability hold has no ceiling
    /// to measure against, and therefore - since the ceiling is the only thing
    /// bounding the hold - the shape in which the hold must decline to engage.
    /// </summary>
    private sealed class UnweighableWalStorageProvider(InMemoryWalStorageProvider inner) : IWalStorageProvider
    {
        public Task AppendBatchAsync(string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken) =>
            inner.AppendBatchAsync(treeId, shardIndex, entries, cancellationToken);

        public IAsyncEnumerable<WalEntry> ReadAsync(string treeId, int shardIndex, long fromOffsetExclusive, int maxEntries, CancellationToken cancellationToken) =>
            inner.ReadAsync(treeId, shardIndex, fromOffsetExclusive, maxEntries, cancellationToken);

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken) =>
            inner.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken) =>
            inner.GetLowestOffsetAsync(treeId, shardIndex, cancellationToken);

        public Task TrimAsync(string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken) =>
            inner.TrimAsync(treeId, shardIndex, throughOffsetInclusive, cancellationToken);
    }

    /// <summary>
    /// Builds a collector over a seeded WAL. Pass <see langword="null"/> for
    /// <paramref name="checkpointOffset"/> to report no durable offsets at all,
    /// which is the absent-floor state this whole fixture is about.
    /// </summary>
    /// <param name="includeForeignConsumer">
    /// Registers a <c>shipper</c> cursor alongside the leaf materialiser.
    /// <para>
    /// Defaults to <see langword="false"/>, and the default is load-bearing
    /// rather than tidiness. The hold's predicate is not "no offset floor" - it
    /// is "every cursor admitting this trim is a leaf materialiser the offset
    /// floor does not speak for". A shipper reports a cursor from outside this
    /// process, so its presence is durable evidence and correctly suppresses the
    /// hold. A tree carrying one is therefore NOT the issue #3300 shape, and a
    /// fixture that registered one while asserting the hold would be asserting
    /// against a tree the hold is designed to leave alone.
    /// </para>
    /// <para>
    /// Setting it <see langword="true"/> builds exactly that control: the
    /// separability case whose whole point is that the hold must decline.
    /// </para>
    /// </param>
    private static async Task<LatticeWalGc> CollectorAsync(
        IWalStorageProvider provider,
        long? checkpointOffset,
        long? holdCeiling,
        bool useDefaultCeiling = false,
        bool includeForeignConsumer = false)
    {
        var registry = new InMemoryWalCursorRegistry();
        if (includeForeignConsumer)
        {
            await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
        }

        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(20));

        var durablePins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
        {
            [LeafConsumer] = Hlc(20),
        };

        var sc = new ServiceCollection();
        sc.AddSingleton<IWalStorageProvider>(provider);

        var pinGrain = Substitute.For<IWalMaterialiserPinGrain>();
        pinGrain.GetPinsAsync().Returns(Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(durablePins));
        if (checkpointOffset is { } offset)
        {
            pinGrain.GetPinOffsetsAsync().Returns(Task.FromResult<IReadOnlyDictionary<string, long>>(
                new Dictionary<string, long>(StringComparer.Ordinal) { [LeafConsumer] = offset }));
        }

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pinGrain);
        sc.AddSingleton(factory);

        return new LatticeWalGc(sc.BuildServiceProvider(), registry, Monitor(holdCeiling, useDefaultCeiling));
    }

    private static async Task<(LatticeWalGcReport Report, List<Stop> Stops, List<string> ForcedReasons, List<string> EngagedReasons)> RunAsync(LatticeWalGc sut)
    {
        var stops = new List<Stop>();
        var forcedReasons = new List<string>();
        var engagedReasons = new List<string>();

        using var stopListener = MeterListening.StartForInstrument(
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

        using var forcedListener = MeterListening.StartForInstrument(
            LatticeMetrics.WalGcDurabilityHoldForced,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                string? reason = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagReason)
                    {
                        reason = tag.Value as string;
                    }
                }

                lock (forcedReasons)
                {
                    forcedReasons.Add(reason ?? "<untagged>");
                }
            }));

        using var engagedListener = MeterListening.StartForInstrument(
            LatticeMetrics.WalGcDurabilityHoldEngaged,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                string? reason = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagReason)
                    {
                        reason = tag.Value as string;
                    }
                }

                lock (engagedReasons)
                {
                    engagedReasons.Add(reason ?? "<untagged>");
                }
            }));

        var report = await sut.RunOnceAsync(Tree);
        return (report, stops, forcedReasons, engagedReasons);
    }

    private static List<string> Advanced(IEnumerable<Stop> stops) =>
        stops.Where(static s => s.Value > 0).Select(static s => s.Reason).ToList();

    private static async Task<InMemoryWalStorageProvider> SeededAsync()
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree,
            0,
            new[] { Entry(0, Hlc(1)), Entry(1, Hlc(2)), Entry(2, Hlc(3)) },
            CancellationToken.None);
        return provider;
    }

    [Test]
    public async Task RunOnceAsync_trimming_a_non_empty_shard_with_no_durable_floor_does_not_report_exhausted()
    {
        // The naming half, and the exact #3300 signature. Every entry is
        // HLC-eligible and no durable offset exists, so the scan runs to the end
        // of the shard and releases all three. That is the behaviour this test
        // deliberately does NOT change - the hold is unconfigured - but it must
        // no longer be indistinguishable from a healthy tree.
        var provider = await SeededAsync();
        var sut = await CollectorAsync(provider, checkpointOffset: null, holdCeiling: null);

        var (report, stops, forcedReasons, _) = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.EqualTo(3),
                "With no hold configured the pass must trim exactly as it did before, so the naming change "
                + "cannot be blamed for a behaviour change.");
            Assert.That(Advanced(stops), Is.EqualTo(new[] { "durability_unverified" }),
                "A scan that released a non-empty shard having never established a durable floor must say so. "
                + "Reporting 'exhausted' here is what let issue #3300 run for eleven hours behind healthy-looking "
                + "series.");
            Assert.That(forcedReasons, Is.Empty,
                "Forced progress must not be reported when no hold was configured: the collector did not try to "
                + "retain anything and then give up, it never tried.");
        });
    }

    [Test]
    public async Task RunOnceAsync_with_a_durable_floor_still_reports_exhausted()
    {
        // The control, and the discrimination property. Same seeded WAL, same
        // eligibility, same trimmed count - the ONLY difference is that a
        // durable floor exists to judge the entries against. If this landed on
        // the same arm as the test above, the new arm would be measuring
        // something other than durability and the pair would be worthless.
        var provider = await SeededAsync();
        var sut = await CollectorAsync(provider, checkpointOffset: 99, holdCeiling: null);

        var (report, stops, _, _) = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.EqualTo(3));
            Assert.That(Advanced(stops), Is.EqualTo(new[] { "exhausted" }),
                "A tree with a durable floor that consumed everything below it is the healthy case and must keep "
                + "the healthy arm.");
        });
    }

    [Test]
    public async Task RunOnceAsync_with_a_hold_configured_and_budget_remaining_retains_the_shard_untouched()
    {
        // The bounding half. Same absent floor as the first test, but now the
        // collector has been told to hold, and the seeded WAL is far below the
        // ceiling. Nothing may be released.
        var provider = await SeededAsync();
        var sut = await CollectorAsync(provider, checkpointOffset: null, holdCeiling: 1024L * 1024L);

        var (report, stops, forcedReasons, _) = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.Zero,
                "This is the whole point: entries nothing is known to have applied must survive the pass.");
            Assert.That(Advanced(stops), Is.EqualTo(new[] { "durability_hold" }),
                "Retaining for want of a durable floor is a different fact from releasing for want of one, and "
                + "the two must be separable without inference because they differ in whether data survived.");
            Assert.That(forcedReasons, Is.Empty,
                "The ceiling was not reached, so no progress was forced.");
        });
    }

    [Test]
    public async Task RunOnceAsync_with_a_hold_configured_and_the_ceiling_exhausted_trims_and_reports_forced_progress()
    {
        // The bound itself. A hold that could never end would grow the WAL
        // without limit on any tree with no materialiser wired, which is issue
        // #3094 - so past the ceiling the collector trims. The requirement is
        // that it never does so silently.
        var provider = await SeededAsync();
        var sut = await CollectorAsync(provider, checkpointOffset: null, holdCeiling: 1);

        var (report, stops, forcedReasons, _) = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.EqualTo(3),
                "Past its ceiling the hold must yield, because an unbounded WAL is the worse of the two outages.");
            Assert.That(forcedReasons, Is.EqualTo(new[] { "ceiling_exhausted" }),
                "Trimming records nothing is known to have applied must increment the forced-progress counter, "
                + "on the arm that says the hold ran and exhausted its budget. A silent yield here would restore "
                + "the original defect with extra configuration.");
            Assert.That(Advanced(stops), Is.EqualTo(new[] { "durability_unverified" }),
                "Having yielded, the pass is once again releasing entries with no durable floor, so it belongs on "
                + "that arm rather than on the hold arm it just left.");
        });
    }

    [Test]
    public async Task RunOnceAsync_with_a_hold_configured_does_not_hold_a_tree_that_has_a_durable_floor()
    {
        // The blast radius. Configuring the hold must not change retention for
        // any healthy tree - only for one that cannot demonstrate durability at
        // all. Ceiling of 1 byte, so if the hold were keyed on bytes rather than
        // on the absent floor this tree would be forced and counted.
        var provider = await SeededAsync();
        var sut = await CollectorAsync(provider, checkpointOffset: 99, holdCeiling: 1);

        var (report, stops, forcedReasons, _) = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.EqualTo(3));
            Assert.That(Advanced(stops), Is.EqualTo(new[] { "exhausted" }));
            Assert.That(forcedReasons, Is.Empty,
                "Forced progress indicts a tree that discarded unverified records. A tree with a durable floor "
                + "discarded nothing of the kind, and counting it would make the signal unalertable.");
        });
    }

    [Test]
    public async Task RunOnceAsync_with_a_hold_configured_reports_an_empty_shard_as_empty_rather_than_held()
    {
        // An empty shard has no data to protect, so putting it on a retention
        // arm would make every idle shard in a hold-configured fleet look like a
        // stranded one - and the hold arm is meant to be alertable.
        var provider = new InMemoryWalStorageProvider();
        var sut = await CollectorAsync(provider, checkpointOffset: null, holdCeiling: 1024L * 1024L);

        var (report, stops, forcedReasons, _) = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.Zero);
            Assert.That(Advanced(stops), Is.EqualTo(new[] { "empty" }));
            Assert.That(forcedReasons, Is.Empty);
        });
    }

    [Test]
    public async Task WalGcTrimStops_primes_both_durability_arms_at_zero()
    {
        // Zero-priming, asserted because an arm that is absent until it first
        // fires cannot be distinguished from an arm that is never evaluated -
        // which is the same family of ambiguity the arms themselves were added
        // to remove. A flat zero must be a measurement, not silence.
        var provider = await SeededAsync();
        var sut = await CollectorAsync(provider, checkpointOffset: 99, holdCeiling: null);

        var (_, stops, _, _) = await RunAsync(sut);

        var reasons = stops.Select(static s => s.Reason).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(reasons, Does.Contain("durability_unverified"));
            Assert.That(reasons, Does.Contain("durability_hold"));
        });
    }

    [Test]
    public void WalDurabilityHoldCeilingBytes_defaults_to_a_positive_ceiling()
    {
        // Default-on, asserted as a property of the options object rather than
        // only through a collector run, because this is the line that decides
        // whether the fix for issue #3300 reaches a deployment that has never
        // heard of issue #3300. Shipped default-off it protected nobody; the
        // instrument was legible and the data still went.
        //
        // The positivity assertion is the load-bearing half. The collector reads
        // `ceiling is { } hc && hc > 0`, so a default of null OR of zero would
        // leave the hold disabled while this property still looked configured.
        var options = new LatticeOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.WalDurabilityHoldCeilingBytes,
                Is.EqualTo(LatticeOptions.DefaultWalDurabilityHoldCeilingBytes));
            Assert.That(options.WalDurabilityHoldCeilingBytes, Is.Not.Null.And.GreaterThan(0),
                "A null or non-positive default leaves the hold switched off no matter what the property "
                + "appears to say, which is the default-off failure wearing a number.");
        });
    }

    [Test]
    public async Task RunOnceAsync_with_default_options_holds_a_tree_that_has_no_durable_floor()
    {
        // The end-to-end statement of the default. Identical to the very first
        // test in this fixture - same seeded WAL, same absent floor - except
        // that nothing configures the hold. Before the default flipped, that
        // pass released all three entries and reported durability_unverified.
        // It must now retain them.
        //
        // Deliberately asserted through a collector rather than by reading the
        // option back: the option being non-null proves only that a value
        // exists, not that the trim path consults it.
        var provider = await SeededAsync();
        var sut = await CollectorAsync(provider, checkpointOffset: null, holdCeiling: null, useDefaultCeiling: true);

        var (report, stops, forcedReasons, _) = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.Zero,
                "With no configuration at all, entries nothing is known to have applied must now survive. This "
                + "single assertion is the difference between issue #3300 being named and being fixed.");
            Assert.That(Advanced(stops), Is.EqualTo(new[] { "durability_hold" }));
            Assert.That(forcedReasons, Is.Empty,
                "The default ceiling is 256 MiB and this WAL holds three entries, so nothing was forced.");
        });
    }

    [Test]
    public async Task RunOnceAsync_with_default_options_does_not_hold_a_tree_that_has_a_durable_floor()
    {
        // The blast radius of the DEFAULT, which is the whole basis on which
        // defaulting it on is safe. A healthy tree - one with a materialiser
        // wired, hence a floor - must be trimmed exactly as before, without
        // anyone opting out. If this failed, the default would be changing
        // retention for every correctly-configured deployment in the fleet,
        // which is a far larger change than the one being made.
        var provider = await SeededAsync();
        var sut = await CollectorAsync(provider, checkpointOffset: 99, holdCeiling: null, useDefaultCeiling: true);

        var (report, stops, forcedReasons, _) = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.EqualTo(3),
                "A tree with a durable floor must be unaffected by the default, or turning the hold on by "
                + "default would be a fleet-wide retention change rather than a targeted fix.");
            Assert.That(Advanced(stops), Is.EqualTo(new[] { "exhausted" }));
            Assert.That(forcedReasons, Is.Empty);
        });
    }

    [Test]
    public async Task RunOnceAsync_declines_the_hold_when_the_provider_cannot_report_bytes_and_names_that_reason()
    {
        // The unbounded shape, and the one place this change deliberately trims
        // data it could have retained.
        //
        // The ceiling is the only thing bounding the hold, so against a provider
        // that reports no bytes the hold would never end. While the hold was
        // opt-in this path held anyway, on the reasoning that trimming against a
        // measurement you do not have is the same error as trimming against a
        // durability check you did not run - and for an operator who had chosen
        // the hold, that was right. Default-on changes who bears it: the
        // unbounded shape would arrive unannounced on every deployment using
        // such a provider, which is issue #3094 shipped by us rather than
        // configured by them. Bounded retention is the entire safety property,
        // so where the bound cannot exist the mechanism does not run.
        //
        // The requirement is that it is never quiet about it, and never
        // indistinguishable from the hold working and running out.
        var inner = await SeededAsync();
        var provider = new UnweighableWalStorageProvider(inner);
        var sut = await CollectorAsync(provider, checkpointOffset: null, holdCeiling: null, useDefaultCeiling: true);

        var (report, stops, forcedReasons, _) = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.EqualTo(3),
                "A hold with nothing to bound it must not engage, so this pass trims as it did before the hold "
                + "existed.");
            Assert.That(forcedReasons, Is.EqualTo(new[] { "unmeasurable_footprint" }),
                "Declining the hold for want of a byte measurement must be reported, and must be reported "
                + "SEPARATELY from exhausting the ceiling. The two call for different repairs - a materialiser "
                + "versus a provider that can weigh itself - and collapsing them would rebuild the exact "
                + "one-arm-means-two-things conflation issue #3309 was raised to undo.");
            Assert.That(Advanced(stops), Is.EqualTo(new[] { "durability_unverified" }),
                "The pass released entries with no durable floor, so it belongs on that arm regardless of why "
                + "the hold did not save it.");
        });
    }

    [Test]
    public async Task RunOnceAsync_keeps_holding_on_later_passes_when_the_trim_is_deferred_past_the_write()
    {
        // Guards against keying the hold to the write's own pass.
        //
        // A write is not trimmed by the pass that races it; it is trimmed by
        // the NEXT garbage-collection pass, which on a live tree can be minutes
        // away. So the hold has to be a function of the tree's state - floor
        // absent, ceiling not yet consumed - and not of anything about recent
        // activity. If it were accidentally keyed to the write's own pass, a
        // quiet interval would let the deferred trim through and the entries
        // would go exactly as they do today, while every prompt-trim test in
        // this fixture still passed.
        //
        // This is a real observation, not a hypothetical: a run of five
        // consecutive samples spanning eight minutes showed the memory tree's
        // trim counter frozen while a sibling tree's stop counters advanced,
        // which reads as "the collector has gone quiet here" and is actually
        // "the sampling window was shorter than this tree's GC period".
        var provider = await SeededAsync();
        var sut = await CollectorAsync(provider, checkpointOffset: null, holdCeiling: null, useDefaultCeiling: true);

        // Pass 1: races the seeded writes.
        var first = await RunAsync(sut);

        // Pass 2: nothing new written. This is the pass that would have trimmed
        // the seeded entries had the first one deferred them.
        var second = await RunAsync(sut);

        // A later write, then the pass that follows it - the deferred shape.
        await provider.AppendBatchAsync(Tree, 0, new[] { Entry(3, Hlc(4)), Entry(4, Hlc(5)) }, CancellationToken.None);
        var third = await RunAsync(sut);
        var fourth = await RunAsync(sut);

        var lowest = await provider.GetLowestOffsetAsync(Tree, 0, CancellationToken.None);
        var highest = await provider.GetHighestOffsetAsync(Tree, 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(
                new[] { first.Report.EntriesTrimmed, second.Report.EntriesTrimmed, third.Report.EntriesTrimmed, fourth.Report.EntriesTrimmed },
                Is.EqualTo(new[] { 0L, 0L, 0L, 0L }),
                "The hold must survive passes that follow the write rather than race it, because that is the pass "
                + "the trim actually lands on.");
            Assert.That(Advanced(second.Stops), Is.EqualTo(new[] { "durability_hold" }),
                "A quiet pass over a tree with no durable floor is still a pass that must retain.");
            Assert.That(Advanced(fourth.Stops), Is.EqualTo(new[] { "durability_hold" }));
            Assert.That(lowest, Is.Zero,
                "Nothing may have been released from the front of the shard across four passes.");
            Assert.That(highest, Is.EqualTo(4),
                "The later writes must still be present, not merely un-trimmed at the time they were made.");
        });
    }

    [Test]
    public async Task RunOnceAsync_with_the_hold_disabled_trims_on_the_pass_after_the_write()
    {
        // The control for the test above, and the reason it is not vacuous.
        //
        // Same four-pass sequence, same deferred write, hold explicitly
        // disabled. If this also retained everything, the deferred-pass test
        // would be asserting nothing about the hold - it would merely be
        // showing that this fixture's provider does not trim, which would make
        // the guard worthless while looking green.
        var provider = await SeededAsync();
        var sut = await CollectorAsync(provider, checkpointOffset: null, holdCeiling: 0);

        var first = await RunAsync(sut);
        await provider.AppendBatchAsync(Tree, 0, new[] { Entry(3, Hlc(4)), Entry(4, Hlc(5)) }, CancellationToken.None);
        var second = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(first.Report.EntriesTrimmed, Is.EqualTo(3),
                "With the hold off this is the pre-#3300 behaviour: everything eligible goes.");
            Assert.That(second.Report.EntriesTrimmed, Is.EqualTo(2),
                "And the write deferred past its own pass is taken by the next one - which is precisely the "
                + "event the hold has to survive.");
            Assert.That(Advanced(second.Stops), Is.EqualTo(new[] { "durability_unverified" }));
        });
    }

    [Test]
    public async Task RunOnceAsync_declines_the_hold_when_a_non_materialiser_cursor_admits_the_trim()
    {
        // THE SEPARABILITY CONTROL, and the reason the predicate is keyed on
        // consumer identity rather than on floor presence.
        //
        // This tree is byte-identical to the #3300 shape under the old
        // predicate: no durable materialiser offset floor, a positive cursor
        // present, entries eligible. Keying the hold on `offsetFloor is null`
        // therefore held this tree too - permanently, because a shipper never
        // publishes an offset and so the floor never arrives. That is unbounded
        // retention imposed on a correctly-configured deployment, which is
        // issue #3094 arriving on our initiative.
        //
        // The two are separable only on what the cursor is EVIDENCE OF. A
        // shipper's cursor says the data reached a peer and it outlives this
        // process; a leaf materialiser's cursor is a claim about state in that
        // leaf's memory and dies with the process. So the hold must decline
        // here and engage in the test below, on trees that differ in nothing
        // else.
        var provider = await SeededAsync();
        var sut = await CollectorAsync(
            provider, checkpointOffset: null, holdCeiling: null,
            useDefaultCeiling: true, includeForeignConsumer: true);

        var (report, stops, forcedReasons, engagedReasons) = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.EqualTo(3),
                "A shipper cursor is durable evidence, so the collector must reclaim exactly as it did "
                + "before the hold existed. Retaining here would be permanent, because a shipper never "
                + "publishes an offset floor for the hold to be released by.");
            Assert.That(Advanced(stops), Is.EqualTo(new[] { "durability_unverified" }),
                "The naming half still applies - there genuinely is no offset floor - but naming it is all "
                + "the collector may do on this tree.");
            Assert.That(engagedReasons, Is.Empty,
                "The hold did not engage, so it must not claim to have.");
            Assert.That(forcedReasons, Is.Empty,
                "Nor may it report being forced past a ceiling it never engaged against; that would indict "
                + "a healthy deployment for a fault it does not have.");
        });
    }

    [Test]
    public async Task RunOnceAsync_holding_a_materialiser_only_tree_reports_never_pinned()
    {
        // The positive half of the pair above. Identical tree, one difference:
        // no consumer reports a cursor from outside this process. Every cursor
        // admitting the trim is a leaf materialiser with no durable offset
        // coverage, so the sole attestation for these entries is in-process
        // state that dies at the process boundary - and releasing them is the
        // data loss issue #3300 recorded.
        var provider = await SeededAsync();
        var sut = await CollectorAsync(
            provider, checkpointOffset: null, holdCeiling: null, useDefaultCeiling: true);

        var (report, stops, forcedReasons, engagedReasons) = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.EqualTo(0),
                "Nothing outside this process has attested to any of these entries, so none may be released.");
            Assert.That(Advanced(stops), Is.EqualTo(new[] { "durability_hold" }));
            Assert.That(engagedReasons, Is.EqualTo(new[] { "never_pinned" }),
                "No durable floor has ever been observed for this tree, so the hold is reporting a STALLED "
                + "tree that will not clear without someone repairing its materialiser - not a transient. "
                + "Collapsing this onto the same arm as pin_regressed would tell an operator mid-upgrade "
                + "that they had an outage.");
            Assert.That(forcedReasons, Is.Empty);
        });
    }

    [Test]
    public async Task RunOnceAsync_holding_after_a_floor_disappears_reports_pin_regressed_not_never_pinned()
    {
        // The rolling-upgrade residue, given its own arm rather than counted as
        // a stall. Both conditions hold the scan and both stop on
        // `durability_hold`, so the stop reason alone cannot tell them apart -
        // but they call for opposite operator responses. A tree that has never
        // pinned needs intervention; a tree whose floor has gone is mid-upgrade
        // or mid-leaf-churn and resolves itself when the leaves re-pin.
        //
        // Driven on ONE collector across two passes, because the distinction
        // lives in the per-tree high-water mark that survives floor loss. A
        // fresh collector would have no history and would - correctly - report
        // never_pinned, which is exactly the conflation this guards.
        var provider = await SeededAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, LeafConsumer, Hlc(20));

        var offsets = new Dictionary<string, long>(StringComparer.Ordinal) { [LeafConsumer] = 1 };
        var reportOffsets = true;

        var pinGrain = Substitute.For<IWalMaterialiserPinGrain>();
        pinGrain.GetPinsAsync().Returns(Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(
            new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal) { [LeafConsumer] = Hlc(20) }));
        pinGrain.GetPinOffsetsAsync().Returns(_ => Task.FromResult<IReadOnlyDictionary<string, long>>(
            reportOffsets ? offsets : new Dictionary<string, long>(StringComparer.Ordinal)));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pinGrain);

        var sc = new ServiceCollection();
        sc.AddSingleton<IWalStorageProvider>(provider);
        sc.AddSingleton(factory);
        var sut = new LatticeWalGc(
            sc.BuildServiceProvider(), registry, Monitor(null, useDefaultCeiling: true));

        var first = await RunAsync(sut);

        // The leaf stops publishing an offset - the upgrade rolls, the
        // activation churns, the floor vanishes.
        reportOffsets = false;
        var second = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(first.EngagedReasons, Is.Empty,
                "A floor existed on the first pass, so the hold had nothing to protect against and must "
                + "not have engaged.");
            Assert.That(Advanced(second.Stops), Is.EqualTo(new[] { "durability_hold" }),
                "With the floor gone the hold engages, because every remaining cursor is an uncovered "
                + "materialiser.");
            Assert.That(second.EngagedReasons, Is.EqualTo(new[] { "pin_regressed" }),
                "And it must say the floor REGRESSED rather than that it never existed. This collector "
                + "watched the floor advance on the previous pass; reporting never_pinned here would "
                + "send an operator to repair a materialiser that is working.");
        });
    }
}
