using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the durable pin-shed ceiling added for issue #3310.
/// <para>
/// The caller-side shed gate of issue #2014 was designed as a short, self-tuning
/// hold-off proportional to the cost of the write that opened it - its sibling
/// fixture asserts as much in
/// <c>LeafCursorReporterShedTests.Reports_resume_once_the_shed_window_lapses</c>.
/// Nothing bounded how often the window could be re-opened, and the write paths
/// that are <b>exempt</b> from the gate (the birth block-pin seed and the
/// retention flush) record their own duration into the very gate they skip. On a
/// large tree under sustained leaf-activation churn those exempt writes can hold
/// a shard's window open indefinitely, which starves the only path that restamps
/// materialiser coverage.
/// </para>
/// <para>
/// The observable consequence is the issue #3310 signature: the durable pin
/// freezes at one offset while the checkpoint it tracks advances past it, so the
/// WAL GC's trim floor never moves and retained WAL grows without bound. A live
/// estate held an offset floor at exactly 504832 across 64 samples over 40
/// minutes while its checkpoint passed 513950 and its WAL grew 1610 -> 1785 MB.
/// </para>
/// <para>
/// <b>The fix bounds the stall; it does not remove the shedding and it does not
/// weaken the seam into fail-open.</b> Issue #3300 is this same seam failing the
/// other way - releasing WAL it cannot prove durable - so the tests below assert
/// both directions: the floor must advance, and the offset it advances to must
/// never exceed what the leaf clamped as durably covered.
/// </para>
/// </summary>
[TestFixture]
public sealed class LeafCursorReporterShedCeilingTests
{
    private const string Tree = "tree-3310";
    private const string Consumer = "_lattice_materialiser_tree-3310_leaf-1";

    /// <summary>
    /// A window far longer than any arm below runs for. Every arm that wants
    /// sustained pressure forces this once: the point under test is what happens
    /// <i>while</i> a window stays open, never how a window closes.
    /// </summary>
    private const long SustainedWindowMs = 30_000;

    [SetUp]
    public void ResetPressure()
    {
        WalMaterialiserPinPressure.ResetForTests();
        _options = new LatticeOptions();
        _monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        _monitor.Get(Arg.Any<string>()).Returns(_ => _options);
    }

    [TearDown]
    public void ClearPressure() => WalMaterialiserPinPressure.ResetForTests();

    private LatticeOptions _options = new();
    private IOptionsMonitor<LatticeOptions> _monitor = null!;

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    /// <summary>
    /// The pin grain key the reporter under test will actually route to.
    /// <para>
    /// Resolved from the <b>same</b> options monitor the reporter is given, not
    /// from <c>null</c>. <c>ResolveShardCount(null)</c> returns the legacy count
    /// of 1 and yields the bare tree name as the key, while a reporter holding
    /// real options resolves 8 and routes to a hashed sub-key. Forcing a window
    /// on the legacy key would leave the reporter's real shard unpressured, so
    /// every arm here would exercise no gate at all and pass vacuously.
    /// </para>
    /// </summary>
    private string ShardKey() =>
        WalMaterialiserPinRouting.ShardKey(Tree, Consumer, WalMaterialiserPinRouting.ResolveShardCount(_monitor));

    private (LeafCursorReporter reporter, OffsetTrackingPinGrain pin) Create(TimeSpan? ceiling)
    {
        _options.WalMaterialiserPinShedCeiling = ceiling;

        var registry = Substitute.For<IWalCursorRegistry>();
        registry.SnapshotAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<WalCursorSnapshot>>(Array.Empty<WalCursorSnapshot>()));

        var pin = new OffsetTrackingPinGrain();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pin);

        return (new LeafCursorReporter(registry, factory, _monitor), pin);
    }

    /// <summary>
    /// Drives the leaf's Zero block pin through and returns once it has landed,
    /// so every arm starts from the same seeded baseline. The seed is a
    /// write-through and is never sheddable, so it lands regardless of pressure.
    /// </summary>
    private static async Task SeedBlockPinAsync(LeafCursorReporter reporter, OffsetTrackingPinGrain pin)
    {
        reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, HybridLogicalClock.Zero, -1);
        await TestPoll.UntilAsync(
            () => pin.ReportCount >= 1,
            "the Zero block-pin seed must land before pressure is applied, otherwise an arm that asserts 'the offset never advanced' passes against a pin that was never written at all",
            TimeSpan.FromSeconds(2));
    }

    /// <summary>
    /// Reports a run of advancing checkpoints, exactly as a leaf checkpointing
    /// under load does. Each shed report rolls the reporter's debounce back, so
    /// every call re-attempts rather than being coalesced away - which is what
    /// makes a held-open window a total stall rather than a slowdown.
    /// </summary>
    private static async Task ReportAdvancingCheckpointsAsync(LeafCursorReporter reporter, int count, int spacingMs)
    {
        for (var i = 1; i <= count; i++)
        {
            reporter.NoteDurableMaterialiserFrontier(Tree, Consumer, Hlc(100 * i), 100 * i);
            await Task.Delay(spacingMs);
        }
    }

    /// <summary>
    /// The defect, stated as the behaviour of the unfixed build. With the
    /// ceiling disarmed - which is the library default and was the only
    /// behaviour available before issue #3310 - a shard whose window stays open
    /// restamps coverage exactly never, however many checkpoints the leaf
    /// reports.
    /// <para>
    /// This arm is the mutation check for the fixed arm below: it pins the
    /// behaviour the fix changes, so a regression that silently disarms the
    /// ceiling turns the pair into a contradiction rather than a quiet pass.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_held_open_window_stalls_coverage_completely_when_no_ceiling_is_armed()
    {
        var (reporter, pin) = Create(ceiling: null);
        await SeedBlockPinAsync(reporter, pin);
        var seededOffset = pin.HighestOffset;

        WalMaterialiserPinPressure.ForceShedForTests(ShardKey(), SustainedWindowMs);
        await ReportAdvancingCheckpointsAsync(reporter, count: 8, spacingMs: 40);

        Assert.That(pin.HighestOffset, Is.EqualTo(seededOffset),
            "with no ceiling armed a held-open shed window drops every coalescible report, so the durable pin never leaves its seeded value while the checkpoint advances past it - the issue #3310 stall");
    }

    /// <summary>
    /// The fix. The same held-open window, with a ceiling armed, cannot prevent
    /// coverage from restamping: once the shard has shed continuously for longer
    /// than the ceiling, a report is forced through and the durable pin advances.
    /// <para>
    /// This is the property that bounds retained WAL. The WAL GC trims to the
    /// durable pin offset, so an advancing pin is a moving trim floor and a
    /// frozen pin is unbounded retention; asserting the pin advances under
    /// sustained pressure is asserting retention is bounded at its source.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_ceiling_forces_coverage_to_restamp_through_a_held_open_window()
    {
        var (reporter, pin) = Create(ceiling: TimeSpan.FromMilliseconds(100));
        await SeedBlockPinAsync(reporter, pin);
        var seededOffset = pin.HighestOffset;

        WalMaterialiserPinPressure.ForceShedForTests(ShardKey(), SustainedWindowMs);
        await ReportAdvancingCheckpointsAsync(reporter, count: 8, spacingMs: 40);

        await TestPoll.UntilAsync(
            () => pin.HighestOffset > seededOffset,
            "the ceiling must force a report through a window that has stayed open past it",
            TimeSpan.FromSeconds(2));

        Assert.That(pin.HighestOffset, Is.GreaterThan(seededOffset),
            "the shed window must bound rather than block: a shard shedding for longer than the ceiling forces one report through, so the durable pin advances and the WAL GC's trim floor moves");
    }

    /// <summary>
    /// The bound is a bound, not a bypass. Forcing restarts the run clock, so a
    /// shard under sustained pressure still sheds the overwhelming majority of
    /// its reports - the issue #2012 queue relief the gate exists for is
    /// preserved, and only enough reports are let through to keep coverage
    /// moving.
    /// </summary>
    [Test]
    public async Task Forcing_costs_at_most_one_report_per_ceiling_period()
    {
        var shardKey = ShardKey();
        WalMaterialiserPinPressure.ForceShedForTests(shardKey, SustainedWindowMs);

        var forced = 0;
        var shed = 0;
        var ceilingMs = 100L;

        // Evaluate far more often than the ceiling period, exactly as a hot
        // checkpoint path would. Over ~500ms with a 100ms ceiling only a handful
        // of evaluations may force.
        for (var i = 0; i < 100; i++)
        {
            switch (WalMaterialiserPinPressure.EvaluateShed(shardKey, ceilingMs))
            {
                case WalMaterialiserPinPressure.PinShedDecision.Forced:
                    forced++;
                    break;
                case WalMaterialiserPinPressure.PinShedDecision.Shed:
                    shed++;
                    break;
                default:
                    Assert.Fail("the window was forced open for the whole arm, so no evaluation may report Proceed");
                    break;
            }

            await Task.Delay(5);
        }

        Assert.Multiple(() =>
        {
            Assert.That(forced, Is.GreaterThan(0),
                "a run far longer than the ceiling must force at least one report through");
            Assert.That(shed, Is.GreaterThan(forced * 3),
                "forcing must remain rare relative to shedding: the ceiling bounds the stall, it does not disable the issue #2012 queue relief that the gate exists to provide");
        });
    }

    /// <summary>
    /// A disarmed ceiling must behave exactly as every pre-#3310 build did. The
    /// run clock still advances - that is what keeps the stall observable - but
    /// nothing is ever forced.
    /// </summary>
    [Test]
    public async Task A_disarmed_ceiling_never_forces_and_preserves_the_historical_behaviour()
    {
        var shardKey = ShardKey();
        WalMaterialiserPinPressure.ForceShedForTests(shardKey, SustainedWindowMs);

        for (var i = 0; i < 20; i++)
        {
            Assert.That(
                WalMaterialiserPinPressure.EvaluateShed(shardKey, ceilingMs: null),
                Is.EqualTo(WalMaterialiserPinPressure.PinShedDecision.Shed),
                "a disarmed ceiling must never force a report through");
            await Task.Delay(5);
        }
    }

    /// <summary>
    /// The stall is observable whether or not it is bounded. With the ceiling
    /// disarmed - the default - the shed-run gauge is the only thing standing
    /// between an unbounded stall and a silent one, which is half of what issue
    /// #3310 set out to fix.
    /// </summary>
    [Test]
    public async Task The_stall_gauge_reports_a_shed_run_even_when_no_ceiling_is_armed()
    {
        var shardKey = ShardKey();
        WalMaterialiserPinPressure.ForceShedForTests(shardKey, SustainedWindowMs);

        // Open the run, then let it age past a whole second so the gauge - which
        // reports seconds - has a non-zero value to publish.
        WalMaterialiserPinPressure.EvaluateShed(shardKey, ceilingMs: null);
        await Task.Delay(1100);
        WalMaterialiserPinPressure.EvaluateShed(shardKey, ceilingMs: null);

        var observed = ObserveStallGauge();

        Assert.That(observed, Is.Not.Empty,
            "a shard with an open shed run must publish a stall series; an unbounded stall that publishes nothing is exactly the silent failure issue #3310 exists to remove");

        var (value, tags) = observed[0];
        Assert.Multiple(() =>
        {
            Assert.That(value, Is.GreaterThanOrEqualTo(1),
                "the gauge reports the age of the run in seconds and must climb while the shard keeps shedding");
            Assert.That(tags[LatticeMetrics.TagTree], Is.EqualTo(Tree),
                "the stall must be attributable to a tree");
            Assert.That(tags, Does.ContainKey(LatticeMetrics.TagPinShard),
                "the stall must be attributable to a pin shard: summed to the tree, an actively-reporting majority of shards masks a stalled minority");
        });
    }

    /// <summary>
    /// The gauge is a stall gauge, not a shed gauge. A shard that lets a report
    /// through has restamped coverage, so its run is over and it must stop
    /// publishing a stall - otherwise a recovered shard reads identically to a
    /// latched one.
    /// </summary>
    [Test]
    public void The_stall_gauge_clears_once_a_report_gets_through()
    {
        var shardKey = ShardKey();
        WalMaterialiserPinPressure.ForceShedForTests(shardKey, durationMs: 1);
        WalMaterialiserPinPressure.EvaluateShed(shardKey, ceilingMs: null);

        Thread.Sleep(30);

        Assert.That(
            WalMaterialiserPinPressure.EvaluateShed(shardKey, ceilingMs: null),
            Is.EqualTo(WalMaterialiserPinPressure.PinShedDecision.Proceed),
            "the window lapsed, so the report proceeds");

        Assert.That(ObserveStallGauge(), Is.Empty,
            "a shard that has let a report through is not stalled and must publish no stall series");
    }

    /// <summary>
    /// <b>The fail-closed guard.</b> Issue #3300 is this same seam failing in the
    /// opposite direction, releasing WAL it cannot prove durable. A forced report
    /// must therefore never carry an offset the leaf did not clamp as durably
    /// covered: forcing publishes evidence that was already bounded by
    /// <c>min(checkpoint, durable coverage)</c> upstream, so it can only ever
    /// publish more durability evidence, never overstate it.
    /// </summary>
    [Test]
    public async Task A_forced_report_never_carries_an_offset_beyond_what_the_leaf_reported()
    {
        var (reporter, pin) = Create(ceiling: TimeSpan.FromMilliseconds(50));
        await SeedBlockPinAsync(reporter, pin);

        WalMaterialiserPinPressure.ForceShedForTests(ShardKey(), SustainedWindowMs);
        await ReportAdvancingCheckpointsAsync(reporter, count: 6, spacingMs: 40);

        await TestPoll.UntilAsync(
            () => pin.HighestOffset > -1,
            "a report must be forced through before the offsets it carried can be checked",
            TimeSpan.FromSeconds(2));

        Assert.That(pin.HighestOffset, Is.LessThanOrEqualTo(600),
            "forcing must not invent durability: the highest offset the pin ever sees must be one the leaf actually reported, never a synthesised advance");
    }

    /// <summary>
    /// A non-positive ceiling is a misconfiguration that would force every report
    /// through a live window, disabling the issue #2012 shedding entirely and
    /// re-saturating the pin grain's non-reentrancy queue. It degrades to the
    /// documented default instead of turning a typo into an outage.
    /// </summary>
    [Test]
    public async Task A_non_positive_ceiling_is_refused_rather_than_disabling_the_shedding()
    {
        var (reporter, pin) = Create(ceiling: TimeSpan.Zero);
        await SeedBlockPinAsync(reporter, pin);
        var seededOffset = pin.HighestOffset;

        WalMaterialiserPinPressure.ForceShedForTests(ShardKey(), SustainedWindowMs);
        await ReportAdvancingCheckpointsAsync(reporter, count: 6, spacingMs: 30);

        Assert.That(pin.HighestOffset, Is.EqualTo(seededOffset),
            "a zero ceiling must resolve to disarmed, not to 'force everything': forcing every report through a live window is the issue #2012 saturation the shed gate exists to prevent");
    }

    /// <summary>
    /// Collects the current measurements of the shed-stall gauge. Built through
    /// the shared <see cref="MeterListening"/> helper so the instrument is passed
    /// as a parameter and its owning initialiser has necessarily completed before
    /// the listener exists.
    /// </summary>
    private static List<(long Value, Dictionary<string, object?> Tags)> ObserveStallGauge()
    {
        var captured = new List<(long, Dictionary<string, object?>)>();

        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.MaterialiserPinShedStallSeconds,
            l => l.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
            {
                var bag = new Dictionary<string, object?>(StringComparer.Ordinal);
                for (var i = 0; i < tags.Length; i++)
                {
                    bag[tags[i].Key] = tags[i].Value;
                }

                captured.Add((measurement, bag));
            }));

        listener.RecordObservableInstruments();
        return captured;
    }

    /// <summary>
    /// Pin-grain fake that tracks the highest durably-reported checkpoint offset,
    /// replicating the real grain's monotonic-max merge. The offset - not the
    /// report count - is what the WAL GC trims against, so it is the quantity
    /// every assertion above is stated in.
    /// </summary>
    private sealed class OffsetTrackingPinGrain : IWalMaterialiserPinGrain
    {
        private long _highestOffset = -1;
        private int _reportCount;

        private readonly ConcurrentDictionary<string, long> _offsets = new(StringComparer.Ordinal);

        public long HighestOffset => Interlocked.Read(ref _highestOffset);

        public int ReportCount => Volatile.Read(ref _reportCount);

        public Task ReportAsync(string consumerId, HybridLogicalClock frontier) => Task.CompletedTask;

        public Task ReportManyAsync(IReadOnlyList<MaterialiserPinReport> reports)
        {
            Merge(reports);
            return Task.CompletedTask;
        }

        public Task SeedManyAsync(IReadOnlyList<MaterialiserPinReport> reports)
        {
            Merge(reports);
            return Task.CompletedTask;
        }

        private void Merge(IReadOnlyList<MaterialiserPinReport> reports)
        {
            for (var i = 0; i < reports.Count; i++)
            {
                var report = reports[i];
                _offsets.AddOrUpdate(
                    report.ConsumerId,
                    report.CheckpointOffset,
                    (_, existing) => Math.Max(existing, report.CheckpointOffset));

                long seen;
                while ((seen = Interlocked.Read(ref _highestOffset)) < report.CheckpointOffset)
                {
                    Interlocked.CompareExchange(ref _highestOffset, report.CheckpointOffset, seen);
                }

                Interlocked.Increment(ref _reportCount);
            }
        }

        public Task<IReadOnlyDictionary<string, HybridLogicalClock>> GetPinsAsync() =>
            Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(
                new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal));

        public Task<IReadOnlyDictionary<string, long>> GetPinOffsetsAsync() =>
            Task.FromResult<IReadOnlyDictionary<string, long>>(
                new Dictionary<string, long>(_offsets, StringComparer.Ordinal));

        public Task RemoveAsync(string consumerId) => Task.CompletedTask;

        public Task ClearAsync() => Task.CompletedTask;
    }
}
