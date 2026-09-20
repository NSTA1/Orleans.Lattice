using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// The instrument test for the fan-in bound: does the measurement apparatus
/// reach the regime in which the bound binds, and can it tell the bounded path
/// from the unbounded one (issue #3266)?
///
/// <para>
/// <b>What went wrong, and why it was worse than a failure.</b> The bound was
/// merged with a benchmark rig behind it, the rig was run, and it came back
/// green on three figures: an observed width of 1.9 to 3.2 against a bound of
/// 16, an admission wait of ~0.002 ms, and 0.7% of reads batched. Every one of
/// those readings is consistent with a comfortable bound, and every one of them
/// is also exactly what the apparatus emits when the gate is never entered. The
/// run did not weakly test the bound; it did not test it. A green from such a
/// run is worse than a red, because it additionally asserts there is nothing to
/// fix. See "False greens" in <c>.github/instructions/testing.instructions.md</c>.
/// </para>
///
/// <para>
/// <b>The governing principle this fixture encodes:</b> an instrument that
/// cannot reach the failing regime yields no evidence, not weak evidence. So
/// the tests here are not about the gate's behaviour - <see
/// cref="RegistryFanInGateTests"/> covers that - they are about whether a
/// reading taken from the gate is worth anything. Each one pins a property of
/// the MEASUREMENT.
/// </para>
///
/// <para>
/// <b>Why this lives in the test suite and not only in the rig.</b> The rig
/// evidence is a JSON file produced by one Docker run on one machine on one
/// day: it demonstrates the fix but cannot defend it. These arms run on every
/// CI build, so a future change that quietly returns the apparatus to the
/// unmeasurable regime - widening the batch take, moving the record site,
/// resolving from a cache before the gate - fails here instead of being
/// rediscovered by another false green.
/// </para>
/// </summary>
[TestFixture]
public class RegistryFanInRegimeTests
{
    /// <summary>
    /// Offered fan-in far above the permit count. Chosen well clear of the bound
    /// rather than just above it, so the arm is measuring the saturated regime
    /// rather than the boundary between regimes.
    /// </summary>
    private const int SaturatingWidth = 400;

    /// <summary>
    /// Offered fan-in deliberately below the permit count - the regime the
    /// original rig run was actually in, reproduced here so the false green can
    /// be demonstrated rather than described.
    /// </summary>
    /// <remarks>
    /// It is produced by STAGGERING arrivals rather than by narrowing a barrier
    /// release, because dispersal is what the original regime actually was: the
    /// rig offered ~50 ops/s spread across a window, and the birth arm's
    /// arrivals are spread by a 60-second reminder. A narrow barrier release
    /// would be a different thing - four ids hitting the lock at one instant
    /// couple incidentally and batch a third of the time at that sample size,
    /// which says nothing about a gate serving dispersed traffic. Reproducing
    /// the regime means reproducing its arrival process, not just its
    /// concurrency.
    /// </remarks>
    private const int StarvedStaggerMillis = 10;

    /// <summary>How many staggered arrivals the starved arm issues.</summary>
    private const int StarvedArrivals = 200;

    /// <summary>
    /// A hold long enough that a wave's calls genuinely overlap. Without it the
    /// first read completes before the last is issued and the offered fan-in
    /// collapses to one however wide the wave is - which is the same dispersal
    /// failure, at a microsecond scale instead of the birth arm's 60 seconds.
    /// </summary>
    private static readonly TimeSpan Hold = TimeSpan.FromMilliseconds(60);

    /// <summary>The figures a gate reading consists of.</summary>
    /// <param name="PeakRegistryCalls">
    /// High-water mark of concurrent registry GRAIN CALLS. This is the quantity
    /// the bound actually governs, and the one the production storm was about -
    /// 103 timeouts against a single registry activation, one per call in
    /// flight.
    /// </param>
    /// <param name="PeakRegistryKeys">
    /// High-water mark of concurrent KEY reads, counting a batch of B as B. This
    /// is NOT bounded at the permit count and must not be read as though it
    /// were: a permit carries up to <c>MaxBatchSize</c> ids, so the ceiling here
    /// is the product of the two constants, asserted as 1024 by
    /// <c>RegistryFanInGateTests.The_downstream_ceiling_is_the_product_of_the_two_constants</c>.
    /// Carried alongside the call count precisely so a reader cannot mistake one
    /// for the other.
    /// </param>
    private sealed record GateReading(
        int PeakRegistryCalls,
        int PeakRegistryKeys,
        int PeakGateWidth,
        int PeakOfferedDepth,
        int PeakBatchSize,
        double MaxWaitMs,
        double BatchedProportion);

    private static TreeRegistryEntry Entry() => new()
    {
        MaxLeafKeys = 128,
        MaxInternalChildren = 128,
        ShardCount = 1,
    };

    /// <summary>
    /// Builds a registry that holds each read open for <see cref="Hold"/> and
    /// records two separate high-water marks: concurrent grain CALLS (what the
    /// bound governs) and concurrent KEY reads (what actually reaches the
    /// backing tree, counting a batch of B as B). They are not the same number
    /// and the difference between them is a finding, not bookkeeping.
    /// </summary>
    private static (IGrainFactory Factory, Func<int> PeakCalls, Func<int> PeakKeys) BuildRegistry()
    {
        var callsInFlight = 0;
        var peakCalls = 0;
        var keysInFlight = 0;
        var peakKeys = 0;

        static void Raise(ref int peak, int now)
        {
            int seen;
            while (now > (seen = Volatile.Read(ref peak)) &&
                   Interlocked.CompareExchange(ref peak, now, seen) != seen)
            {
            }
        }

        void Enter(int keys)
        {
            Raise(ref peakCalls, Interlocked.Increment(ref callsInFlight));
            Raise(ref peakKeys, Interlocked.Add(ref keysInFlight, keys));
        }

        void Leave(int keys)
        {
            Interlocked.Decrement(ref callsInFlight);
            Interlocked.Add(ref keysInFlight, -keys);
        }

        var registry = Substitute.For<ILatticeRegistry>();

        registry.GetEntryAsync(Arg.Any<string>()).Returns(async _ =>
        {
            Enter(1);
            try
            {
                await Task.Delay(Hold);
                return (TreeRegistryEntry?)Entry();
            }
            finally
            {
                Leave(1);
            }
        });

        registry.GetEntriesAsync(Arg.Any<IReadOnlyList<string>>()).Returns(async call =>
        {
            var ids = (IReadOnlyList<string>)call[0];
            Enter(ids.Count);
            try
            {
                await Task.Delay(Hold);
                return ids.ToDictionary(id => id, _ => Entry(), StringComparer.Ordinal);
            }
            finally
            {
                Leave(ids.Count);
            }
        });

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        return (factory, () => Volatile.Read(ref peakCalls), () => Volatile.Read(ref peakKeys));
    }

    /// <summary>
    /// Runs one wave of <paramref name="width"/> distinct trees, released
    /// together, and reads every gate instrument across it.
    /// </summary>
    /// <param name="gated">
    /// When false, the wave addresses the registry directly and reaches no gate -
    /// the control arm. This is the in-process equivalent of the rig's
    /// <c>--fanout-ungated</c> switch, and it exists for the same reason: a rig
    /// that reaches the bounded regime but produces the same numbers with and
    /// without the bound has measured the workload, not the bound.
    /// </param>
    private static async Task<GateReading> RunWaveAsync(int width, bool gated, int staggerMillis = 0)
    {
        var widths = new List<int>();
        var depths = new List<int>();
        var batches = new List<int>();
        var waits = new List<double>();

        using var widthListener = MeterListening.StartForInstrument(
            LatticeMetrics.RegistryAdmissionInFlight,
            l => l.SetMeasurementEventCallback<int>((_, v, _, _) => { lock (widths) widths.Add(v); }));
        using var depthListener = MeterListening.StartForInstrument(
            LatticeMetrics.RegistryAdmissionQueueDepth,
            l => l.SetMeasurementEventCallback<int>((_, v, _, _) => { lock (depths) depths.Add(v); }));
        using var batchListener = MeterListening.StartForInstrument(
            LatticeMetrics.RegistryAdmissionBatchSize,
            l => l.SetMeasurementEventCallback<int>((_, v, _, _) => { lock (batches) batches.Add(v); }));
        using var waitListener = MeterListening.StartForInstrument(
            LatticeMetrics.RegistryAdmissionWait,
            l => l.SetMeasurementEventCallback<double>((_, v, _, _) => { lock (waits) waits.Add(v); }));

        var (factory, peakCalls, peakKeys) = BuildRegistry();
        var gate = new RegistryFanInGate(factory);
        var registry = factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        // Released from one barrier so simultaneity is a property of the harness
        // rather than a hoped-for coincidence of scheduling - the same reason the
        // rig's fanout arm uses a barrier instead of an open-loop pacer. A
        // positive stagger disables the barrier and issues arrivals spread in
        // time instead, which is how the STARVED regime is reproduced: dispersal,
        // not narrowness, is what kept the original run out of the bound.
        var barrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var wave = new List<Task>(width);
        for (var i = 0; i < width; i++)
        {
            var treeId = $"regime-tree-{i:D5}";
            if (staggerMillis > 0)
            {
                await Task.Delay(staggerMillis);
            }

            wave.Add(IssueAsync(treeId));
        }

        barrier.SetResult();
        await Task.WhenAll(wave);

        widthListener.Dispose();
        depthListener.Dispose();
        batchListener.Dispose();
        waitListener.Dispose();

        GateReading reading;
        lock (batches)
        {
            var total = batches.Count;
            var batched = batches.Count(b => b > 1);
            lock (widths)
            lock (depths)
            lock (waits)
            {
                reading = new GateReading(
                    PeakRegistryCalls: peakCalls(),
                    PeakRegistryKeys: peakKeys(),
                    PeakGateWidth: widths.Count == 0 ? 0 : widths.Max(),
                    PeakOfferedDepth: depths.Count == 0 ? 0 : depths.Max(),
                    PeakBatchSize: total == 0 ? 0 : batches.Max(),
                    MaxWaitMs: waits.Count == 0 ? 0 : waits.Max(),
                    BatchedProportion: total == 0 ? 0 : (double)batched / total);
            }
        }

        // Emitted so the three figures issue #3266 asks for are readable
        // straight out of a CI log, not only out of a Docker rig result that
        // one machine produced once.
        TestContext.Out.WriteLine(
            $"[regime] arm={(gated ? "gated" : "ungated")} width={width} stagger={staggerMillis}ms "
            + $"| offeredDepth={reading.PeakOfferedDepth} gateWidth={reading.PeakGateWidth}/"
            + $"{RegistryFanInGate.GlobalMaxConcurrentReads} maxWait={reading.MaxWaitMs:F3}ms "
            + $"batched={reading.BatchedProportion:P1} peakBatch={reading.PeakBatchSize} "
            + $"| registryCalls={reading.PeakRegistryCalls} registryKeys={reading.PeakRegistryKeys}");

        return reading;

        async Task IssueAsync(string treeId)
        {
            if (staggerMillis <= 0)
            {
                await barrier.Task;
            }

            if (gated)
            {
                await gate.GetEntryAsync(treeId);
            }
            else
            {
                await registry.GetEntryAsync(treeId);
            }
        }
    }

    /// <summary>
    /// <b>Criterion 1 and 2.</b> A wave far wider than the permit count must
    /// drive the gate into saturation, and all three of the figures the original
    /// run reported must leave their floors.
    /// <para>
    /// The thresholds are stated against the gate's own constant rather than
    /// against the numbers the first run happened to produce, so the arm keeps
    /// its meaning if the bound is ever retuned. The batched-proportion
    /// threshold is set an order of magnitude above the 0.7% that was reported,
    /// because a figure that merely moved would not distinguish a fixed rig from
    /// a slightly luckier run of the broken one.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_wave_wider_than_the_bound_drives_the_gate_into_saturation()
    {
        var reading = await RunWaveAsync(SaturatingWidth, gated: true);

        Assert.Multiple(() =>
        {
            Assert.That(reading.PeakOfferedDepth, Is.GreaterThan(RegistryFanInGate.GlobalMaxConcurrentReads),
                $"offered fan-in must exceed the bound of {RegistryFanInGate.GlobalMaxConcurrentReads} before "
                + "any other figure here is evidence about the bound at all - this is the check the "
                + "original run had no instrument for, and without it the three readings below are "
                + "indistinguishable from an untouched gate.");

            Assert.That(reading.PeakGateWidth, Is.EqualTo(RegistryFanInGate.GlobalMaxConcurrentReads),
                $"gate width must reach the bound itself, not a factor of five below it. Reported at "
                + $"{reading.PeakGateWidth} against a bound of {RegistryFanInGate.GlobalMaxConcurrentReads}.");

            Assert.That(reading.MaxWaitMs, Is.GreaterThan(1.0),
                "admission must genuinely queue. The ~0.002 ms the original run reported is the "
                + "instrument's STRUCTURAL FLOOR, not a small reading: an arrival that finds a free "
                + "permit is dequeued by its own thread in the same stack frame, so a few microseconds "
                + "is what a gate that never queued emits, and no workload can make it smaller.");

            Assert.That(reading.BatchedProportion, Is.GreaterThan(0.1),
                $"the batching path must actually run. Reported {reading.BatchedProportion:P1} against "
                + "the original run's 0.7%, which was not a weak batching result - a batch of two "
                + "requires two distinct ids waiting at one instant, so 0.7% was a report that nothing "
                + "ever waited.");

            Assert.That(reading.PeakBatchSize, Is.GreaterThan(1),
                "at least one dispatch must carry more than one id, or the gate is queueing without "
                + "coalescing and MaxBatchSize is dead weight");
        });
    }

    /// <summary>
    /// <b>Criterion 3, and the single most important arm in this fixture.</b> A
    /// rig that reaches the saturated regime but produces the same numbers with
    /// and without the bound has measured the workload rather than the bound,
    /// and is still not an instrument.
    /// <para>
    /// The discriminator is the number of concurrent registry GRAIN CALLS: the
    /// bound's purpose is to hold that flat as offered fan-in grows, and it is
    /// the quantity the production storm was counted in - 103 timeouts against
    /// one registry activation, one per call in flight. So the ungated arm must
    /// show calls tracking the wave width and the gated arm must show them
    /// pinned at the permit count.
    /// </para>
    /// <para>
    /// <b>The figure that does NOT separate, reported rather than tuned away.</b>
    /// Concurrent KEY reads are essentially identical on both arms at this
    /// width. That is not a defect in the apparatus, it is what the bound does:
    /// a permit carries up to <c>MaxBatchSize</c> ids, so the work admitted is
    /// bounded by the product of the two constants (1024, asserted by
    /// <c>RegistryFanInGateTests.The_downstream_ceiling_is_the_product_of_the_two_constants</c>)
    /// and a 400-wide wave passes under it untouched. Anyone reading the bound
    /// as "at most 16 reads reach the registry" is wrong by up to a factor of
    /// 64, and the arm asserts the non-separation so that misreading fails here
    /// instead of surviving as folklore.
    /// </para>
    /// </summary>
    [Test]
    public async Task The_rig_tells_the_bounded_path_apart_from_the_unbounded_one()
    {
        var gated = await RunWaveAsync(SaturatingWidth, gated: true);
        var ungated = await RunWaveAsync(SaturatingWidth, gated: false);

        Assert.Multiple(() =>
        {
            Assert.That(ungated.PeakRegistryCalls, Is.GreaterThan(RegistryFanInGate.GlobalMaxConcurrentReads * 4),
                $"the control arm must genuinely be unbounded: {SaturatingWidth} simultaneous reads that "
                + "reach no gate must pile into the registry. If this is low the control is not a control, "
                + "and the comparison below proves nothing.");

            Assert.That(gated.PeakRegistryCalls, Is.LessThanOrEqualTo(RegistryFanInGate.GlobalMaxConcurrentReads),
                $"the bounded path must be decisively separable from the unbounded one under the SAME "
                + $"offered load: gated peak {gated.PeakRegistryCalls} calls vs ungated "
                + $"{ungated.PeakRegistryCalls}. If these two are comparable, the rig cannot tell "
                + "whether the bound is present, and a green run from it means nothing whatever.");

            // Stated as an assertion, not a comment, because it is the thing
            // most likely to be misremembered about this bound.
            Assert.That(gated.PeakRegistryKeys, Is.GreaterThan(RegistryFanInGate.GlobalMaxConcurrentReads * 4),
                $"the bound does NOT hold the work reaching the registry near the permit count - "
                + $"{gated.PeakRegistryKeys} keys were in flight concurrently on the GATED path, against "
                + $"{ungated.PeakRegistryKeys} ungated, because a permit carries up to MaxBatchSize ids. "
                + "The bound limits calls, and the ceiling on admitted work is the product of the two "
                + "constants. This arm asserts the non-separation so that reading is not lost.");

            // The gate's own instruments must be silent on the ungated path, and
            // loud on the gated one. This is what makes the discrimination
            // readable from the gate block of a rig result rather than requiring
            // a side-by-side diff of two whole runs.
            Assert.That(ungated.PeakOfferedDepth, Is.Zero,
                "no gate instrument may record on a path that reaches no gate - an instrument that "
                + "reports on traffic it did not observe would let the control arm manufacture "
                + "evidence for the treatment arm");
            Assert.That(gated.PeakOfferedDepth, Is.GreaterThan(RegistryFanInGate.GlobalMaxConcurrentReads));
        });
    }

    /// <summary>
    /// <b>The false green itself, reproduced.</b> Below the bound the two paths
    /// are indistinguishable, and every figure the original run reported sits at
    /// its floor.
    /// <para>
    /// This arm is the one that would have caught the original run, and it is
    /// asserted in the POSITIVE direction on purpose: it does not merely allow
    /// the readings to be low, it requires them to be, and requires the gated
    /// and ungated paths to agree. That makes it a statement about the
    /// apparatus - "at this width the rig cannot see the bound" - rather than a
    /// weaker test of the gate. A future change that made a sub-bound wave
    /// somehow look saturated would fail here, and it should, because it would
    /// mean the instruments had stopped reporting the regime honestly.
    /// </para>
    /// </summary>
    [Test]
    public async Task Below_the_bound_the_rig_cannot_see_the_bound_at_all()
    {
        var gated = await RunWaveAsync(StarvedArrivals, gated: true, staggerMillis: StarvedStaggerMillis);
        var ungated = await RunWaveAsync(StarvedArrivals, gated: false, staggerMillis: StarvedStaggerMillis);

        Assert.Multiple(() =>
        {
            // The precondition. If dispersal failed to hold offered fan-in below
            // the bound this arm is measuring the wrong regime and every
            // assertion under it is void, so it is checked rather than assumed.
            Assert.That(gated.PeakOfferedDepth, Is.LessThan(RegistryFanInGate.GlobalMaxConcurrentReads),
                "this arm is only meaningful while dispersal genuinely holds offered fan-in below the "
                + $"bound; got a peak depth of {gated.PeakOfferedDepth}");

            Assert.That(gated.PeakRegistryCalls,
                Is.EqualTo(ungated.PeakRegistryCalls).Within(2),
                $"below the bound the gated path ({gated.PeakRegistryCalls}) and the ungated path "
                + $"({ungated.PeakRegistryCalls}) put the SAME concurrency into the registry, "
                + "because the gate admits every arrival immediately. Any rig arm operating here is "
                + "incapable of telling the two apart, whatever else it reports - which is the whole "
                + "of what the original green run established.");

            Assert.That(gated.PeakGateWidth, Is.LessThan(RegistryFanInGate.GlobalMaxConcurrentReads),
                "width cannot exceed the offered fan-in, so a dispersed arrival process caps the "
                + "reading below the bound - which reads as headroom and is actually starvation");

            // Not asserted as an exact zero, and the reason is a finding rather
            // than a tolerance. A dispatch batches whenever two ids are enqueued
            // between one Pump and the next, and arrivals enqueue under the lock
            // but pump outside it - so a coincidence of microseconds can produce
            // a batch of two even with a free permit for every arrival. That
            // incidental coupling is NOT the bound coalescing anything: it does
            // not grow with offered load, and it is present on a gate that never
            // queued once.
            //
            // It is also, in shape and magnitude, the original run's 0.7%. That
            // figure was never a weak batching result to be improved on; it was
            // the floor, and reading it as "batching is happening, just not much"
            // inverts its meaning. Hence the acceptance criterion of "materially
            // above 0.7%" rather than "above zero".
            Assert.That(gated.BatchedProportion, Is.LessThan(0.05),
                $"below the bound the batched proportion must stay at its floor; got "
                + $"{gated.BatchedProportion:P1}. A small non-zero value here is incidental enqueue "
                + "coupling, not coalescing - which is what the original run's 0.7% was, and why that "
                + "figure could never have been evidence that the batching path ran.");

            Assert.That(gated.MaxWaitMs, Is.LessThan(1.0),
                "and the admission wait floor: microseconds, unreachable from below, and identical in "
                + "shape to a bound that is comfortably wide");
        });
    }

    /// <summary>
    /// The apparatus must be MONOTONE in offered fan-in, or a single reading
    /// cannot be placed on a scale.
    /// <para>
    /// Without this, a width of 16 and a width of 400 could both report
    /// saturation and the figure would carry no information about how hard the
    /// bound was being pressed. Monotonicity is what makes it legitimate to read
    /// a rig result as "the regime was reached" rather than merely "a number was
    /// produced", and it is what lets the arm be walked down with
    /// <c>--fanout-gap-ms</c> to show the instruments follow the regime rather
    /// than reporting a constant.
    /// </para>
    /// </summary>
    [Test]
    public async Task Offered_fan_in_tracks_the_arrival_process_so_a_reading_can_be_placed_on_a_scale()
    {
        var dispersed = await RunWaveAsync(StarvedArrivals, gated: true, staggerMillis: StarvedStaggerMillis);
        var simultaneous = await RunWaveAsync(SaturatingWidth, gated: true);

        Assert.Multiple(() =>
        {
            Assert.That(simultaneous.PeakOfferedDepth, Is.GreaterThan(dispersed.PeakOfferedDepth * 4),
                $"a simultaneous release must register as a decisively deeper queue than a dispersed "
                + $"one at comparable volume ({simultaneous.PeakOfferedDepth} vs "
                + $"{dispersed.PeakOfferedDepth}), or the apparatus is reporting a constant and no "
                + "single reading from it can be placed on a scale");
            Assert.That(simultaneous.PeakGateWidth, Is.GreaterThan(dispersed.PeakGateWidth),
                "and gate width must rise with it, up to the bound");

            // The axis that matters, stated explicitly: the two arms here issue
            // comparable VOLUMES and differ almost entirely in arrival
            // simultaneity. That is the diagnosis the fix rests on - the original
            // rig could not reach the regime because its arrivals were dispersed,
            // not because its estate was small - so raising tree count or
            // operation rate on a dispersed arm would have moved neither figure.
            Assert.That(dispersed.PeakOfferedDepth, Is.LessThan(RegistryFanInGate.GlobalMaxConcurrentReads),
                $"{StarvedArrivals} dispersed arrivals must NOT reach the bound, however many there are");
        });
    }
}
