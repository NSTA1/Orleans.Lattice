using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Regression tests for the durable-floor stall-age signal (issue #3300).
/// <para>
/// The acceptance requirement is literal and was written by the operator who
/// lost the data: it must be possible to see <i>"this tree's durable floor has
/// not advanced in N hours"</i> from the metric surface alone, without running a
/// census, diffing an archive, or restarting the process to find out what
/// survived. In the field the fault was invisible for eleven hours because every
/// series the collector published was either a volume count - which rises just
/// as happily when nothing is being made durable - or a state arm that read
/// healthy. None of them measured <b>elapsed time without progress</b>, which is
/// the one quantity that separates a slow tree from a stopped one.
/// </para>
/// <para>
/// The trap these tests exist to close is the absent case. A tree with no
/// durable floor at all has no advance timestamp to subtract from, so the
/// obvious implementation reports zero - putting the state in which nothing is
/// known to be durable on exactly the reading a perfectly healthy tree produces.
/// That is this repository's signature defect (a component that cannot establish
/// a fact returning the reassuring value), and it would have made the new
/// instrument useless against the very incident that motivated it.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeWalGcDurableFloorStallTests
{
    private const string Tree = "tree";
    private const string LeafConsumer = "_lattice_materialiser_tree_leaf-1";

    private sealed record Sample(string Status, long Seconds);

    /// <summary>A clock the test advances by hand.</summary>
    private sealed class ManualClock(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _now = start;

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan by) => _now += by;
    }

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

    private static IOptionsMonitor<LatticeOptions> Monitor()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var options = new LatticeOptions { WalPartitions = 1 };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    /// <summary>
    /// Builds a collector whose reported durable offset is read from
    /// <paramref name="offset"/> at the moment of each pass, so a test can move
    /// the floor between passes.
    /// </summary>
    private static async Task<(LatticeWalGc Sut, ManualClock Clock)> CollectorAsync(Func<long?> offset)
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree,
            0,
            new[] { Entry(0, Hlc(1)), Entry(1, Hlc(2)) },
            CancellationToken.None);

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
        pinGrain.GetPinOffsetsAsync().Returns(_ =>
        {
            var current = offset();
            return Task.FromResult<IReadOnlyDictionary<string, long>>(
                current is { } o
                    ? new Dictionary<string, long>(StringComparer.Ordinal) { [LeafConsumer] = o }
                    : new Dictionary<string, long>(StringComparer.Ordinal));
        });

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pinGrain);
        sc.AddSingleton(factory);

        var clock = new ManualClock(new DateTimeOffset(2026, 9, 20, 8, 0, 0, TimeSpan.Zero));
        var sut = new LatticeWalGc(sc.BuildServiceProvider(), registry, Monitor(), clock);
        return (sut, clock);
    }

    private static async Task<List<Sample>> RunAsync(LatticeWalGc sut)
    {
        var samples = new List<Sample>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.WalGcDurableFloorStallSeconds,
            l => l.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
            {
                string? status = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagStatus)
                    {
                        status = tag.Value as string;
                    }
                }

                samples.Add(new Sample(status ?? "<untagged>", measurement));
            }));

        await sut.RunOnceAsync(Tree);
        return samples;
    }

    [Test]
    public async Task RunOnceAsync_reports_a_growing_age_for_a_tree_whose_floor_never_appears()
    {
        // The #3300 signature, and the trap. The floor is absent on every pass,
        // so there is no advance timestamp to measure from; the age must be
        // taken from first observation instead. Reporting zero here would give
        // the state in which nothing is known to be durable the same reading as
        // the healthiest possible tree.
        var (sut, clock) = await CollectorAsync(static () => null);

        var first = await RunAsync(sut);
        clock.Advance(TimeSpan.FromHours(11));
        var later = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(first.Select(static s => s.Status), Is.EqualTo(new[] { "absent" }),
                "A tree with no durable floor must be reported as absent, not folded in with trees that have one.");
            Assert.That(later.Single().Status, Is.EqualTo("absent"));
            Assert.That(later.Single().Seconds, Is.EqualTo(11 * 3600),
                "This is the whole acceptance criterion: an operator must be able to read 'this tree's durable "
                + "floor has not advanced in eleven hours' off the metric surface, without a census or a restart.");
        });
    }

    [Test]
    public async Task RunOnceAsync_reports_a_growing_age_for_a_floor_that_exists_but_stops_moving()
    {
        // The other stall shape: a floor was established once and then froze.
        // Distinguished from absent because they indict different things - one
        // is a materialiser that never started, the other one that stopped.
        var (sut, clock) = await CollectorAsync(static () => 5L);

        var first = await RunAsync(sut);
        clock.Advance(TimeSpan.FromMinutes(30));
        var later = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(first.Single().Status, Is.EqualTo("advanced"),
                "The first pass to see a floor has watched it rise from nothing, which is progress.");
            Assert.That(first.Single().Seconds, Is.Zero);
            Assert.That(later.Single().Status, Is.EqualTo("stalled"));
            Assert.That(later.Single().Seconds, Is.EqualTo(30 * 60));
        });
    }

    [Test]
    public async Task RunOnceAsync_resets_the_age_when_the_floor_actually_advances()
    {
        // The control. Without this, an instrument that simply counted uptime
        // would pass every other test in this fixture.
        var floor = 5L;
        var (sut, clock) = await CollectorAsync(() => floor);

        await RunAsync(sut);
        clock.Advance(TimeSpan.FromMinutes(30));
        floor = 6L;
        var advanced = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(advanced.Single().Status, Is.EqualTo("advanced"));
            Assert.That(advanced.Single().Seconds, Is.Zero,
                "A floor that moved is progress and must reset the clock, or a healthy tree would eventually "
                + "alert purely for having been up a long time.");
        });
    }

    [Test]
    public async Task RunOnceAsync_does_not_treat_a_falling_floor_as_progress()
    {
        // The floor is a MINIMUM over reporting leaves, so it can legitimately
        // fall when a lagging leaf starts reporting. Measuring progress against
        // the previous pass rather than against a high-water mark would read
        // that fall as movement and restart the stall clock on a tree making
        // none - which is the failure this signal exists to catch.
        var floor = 5L;
        var (sut, clock) = await CollectorAsync(() => floor);

        await RunAsync(sut);
        clock.Advance(TimeSpan.FromMinutes(20));
        floor = 3L;
        var dropped = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(dropped.Single().Status, Is.EqualTo("stalled"),
                "A floor that went backwards has not made progress, whatever it did relative to the last pass.");
            Assert.That(dropped.Single().Seconds, Is.EqualTo(20 * 60));
        });
    }

    [Test]
    public async Task RunOnceAsync_does_not_reset_the_stall_clock_when_a_floor_disappears()
    {
        // Losing the floor is the opposite of progress, so it must not be
        // allowed to look like a fresh start. The age keeps running from the
        // last real advance.
        long? floor = 5L;
        var (sut, clock) = await CollectorAsync(() => floor);

        await RunAsync(sut);
        clock.Advance(TimeSpan.FromMinutes(45));
        floor = null;
        var lost = await RunAsync(sut);

        Assert.Multiple(() =>
        {
            Assert.That(lost.Single().Status, Is.EqualTo("absent"));
            Assert.That(lost.Single().Seconds, Is.EqualTo(45 * 60),
                "The age must continue from the last genuine advance rather than restarting, or a tree that "
                + "oscillates between a frozen floor and no floor would never accumulate an alertable age.");
        });
    }
}
