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

    private static IOptionsMonitor<LatticeOptions> Monitor(long? holdCeiling)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var options = new LatticeOptions
        {
            WalPartitions = 1,
            WalDurabilityHoldCeilingBytes = holdCeiling,
        };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    /// <summary>
    /// Builds a collector over a seeded WAL. Pass <see langword="null"/> for
    /// <paramref name="checkpointOffset"/> to report no durable offsets at all,
    /// which is the absent-floor state this whole fixture is about.
    /// </summary>
    private static async Task<LatticeWalGc> CollectorAsync(
        InMemoryWalStorageProvider provider,
        long? checkpointOffset,
        long? holdCeiling)
    {
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));
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

        return new LatticeWalGc(sc.BuildServiceProvider(), registry, Monitor(holdCeiling));
    }

    private static async Task<(LatticeWalGcReport Report, List<Stop> Stops, long Forced)> RunAsync(LatticeWalGc sut)
    {
        var stops = new List<Stop>();
        long forced = 0;

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
            l => l.SetMeasurementEventCallback<long>((_, measurement, _, _) =>
                Interlocked.Add(ref forced, measurement)));

        var report = await sut.RunOnceAsync(Tree);
        return (report, stops, Interlocked.Read(ref forced));
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

        var (report, stops, forced) = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.EqualTo(3),
                "With no hold configured the pass must trim exactly as it did before, so the naming change "
                + "cannot be blamed for a behaviour change.");
            Assert.That(Advanced(stops), Is.EqualTo(new[] { "durability_unverified" }),
                "A scan that released a non-empty shard having never established a durable floor must say so. "
                + "Reporting 'exhausted' here is what let issue #3300 run for eleven hours behind healthy-looking "
                + "series.");
            Assert.That(forced, Is.Zero,
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

        var (report, stops, _) = await RunAsync(sut);

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

        var (report, stops, forced) = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.Zero,
                "This is the whole point: entries nothing is known to have applied must survive the pass.");
            Assert.That(Advanced(stops), Is.EqualTo(new[] { "durability_hold" }),
                "Retaining for want of a durable floor is a different fact from releasing for want of one, and "
                + "the two must be separable without inference because they differ in whether data survived.");
            Assert.That(forced, Is.Zero,
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

        var (report, stops, forced) = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.EqualTo(3),
                "Past its ceiling the hold must yield, because an unbounded WAL is the worse of the two outages.");
            Assert.That(forced, Is.EqualTo(1),
                "Trimming records nothing is known to have applied must increment the forced-progress counter. "
                + "A silent yield here would restore the original defect with extra configuration.");
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

        var (report, stops, forced) = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.EqualTo(3));
            Assert.That(Advanced(stops), Is.EqualTo(new[] { "exhausted" }));
            Assert.That(forced, Is.Zero,
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

        var (report, stops, forced) = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(report.EntriesTrimmed, Is.Zero);
            Assert.That(Advanced(stops), Is.EqualTo(new[] { "empty" }));
            Assert.That(forced, Is.Zero);
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

        var (_, stops, _) = await RunAsync(sut);

        var reasons = stops.Select(static s => s.Reason).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(reasons, Does.Contain("durability_unverified"));
            Assert.That(reasons, Does.Contain("durability_hold"));
        });
    }
}
