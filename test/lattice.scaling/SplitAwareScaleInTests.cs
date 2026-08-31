using NSubstitute;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// End-to-end proof that the scale-in safety gate is genuinely split-aware now
/// that a real <see cref="ISplitActivityProbe"/> is wired
/// (<see cref="LatticeSplitActivityProbe"/>, backed by
/// <see cref="ILatticeAdmin.GetSplitActivityAsync"/>): scale-in is suppressed
/// while a split is reported in flight and released once it completes.
/// <para>
/// The other split tests exercise the probe and the computer in isolation; this
/// fixture drives the whole wired path - substituted admin surface, real probe,
/// real facade, real computer - because the defect this closes was precisely
/// that the pieces were individually correct but never connected.
/// </para>
/// </summary>
[TestFixture]
public sealed class SplitAwareScaleInTests
{
    private const string AdminGrainKey = "_lattice_admin";
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>Comfortably past the two-minute default scale-in gate window.</summary>
    private static readonly TimeSpan PastTheWindow = TimeSpan.FromSeconds(300);

    private sealed class FakeCompute : IComputePressureCollector
    {
        public ComputePressure Pressure { get; set; }

        public ValueTask<ComputePressure> CollectAsync(CancellationToken cancellationToken)
            => ValueTask.FromResult(Pressure);
    }

    private sealed class FakeStorage : IStoragePressureCollector
    {
        public ValueTask<StoragePressure> CollectAsync(CancellationToken cancellationToken)
            => ValueTask.FromResult(default(StoragePressure));
    }

    private sealed class FakeReplicas(int count) : IReplicaCountProvider
    {
        public ValueTask<int> GetActiveReplicaCountAsync(CancellationToken cancellationToken)
            => ValueTask.FromResult(count);
    }

    /// <summary>
    /// A substituted admin surface whose reported in-flight split count can be
    /// changed between ticks, standing in for the cluster's split-admission gate.
    /// </summary>
    private sealed class SplitActivityStub
    {
        private int _inFlight;

        public ILatticeAdmin Admin { get; }

        public SplitActivityStub()
        {
            Admin = Substitute.For<ILatticeAdmin>();
            Admin.GetSplitActivityAsync(Arg.Any<CancellationToken>())
                .Returns(_ => Task.FromResult(new SplitActivityReport
                {
                    InFlight = _inFlight,
                    ReportingTrees = _inFlight > 0 ? 1 : 0,
                    ObservedAt = T0,
                }));
        }

        public void SplitsInFlight(int count) => _inFlight = count;
    }

    private static (LatticeScalingSignal Facade, FakeCompute Compute, MutableTimeProvider Clock, SplitActivityStub Splits) Build()
    {
        var options = Microsoft.Extensions.Options.Options.Create(new LatticeScalingSignalOptions());
        var clock = new MutableTimeProvider(T0);
        var compute = new FakeCompute();
        var splits = new SplitActivityStub();

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILatticeAdmin>(AdminGrainKey).Returns(splits.Admin);

        var facade = new LatticeScalingSignal(
            compute,
            new FakeStorage(),
            new FakeReplicas(4),
            new LatticeSplitActivityProbe(grainFactory),
            new ScalingSignalComputer(options),
            options,
            clock);

        return (facade, compute, clock, splits);
    }

    private static async Task<ScalingSignal> TickAsync(LatticeScalingSignal facade)
    {
        await facade.SampleOnceAsync(CancellationToken.None);
        return await facade.GetScalingSignalAsync();
    }

    /// <summary>
    /// Drives the facade far enough for the scale-in gate to open: one tick to
    /// arm the window (the gate starts measuring continuous eligibility from the
    /// first eligible tick, so that tick can never itself be permitted) and a
    /// second a full window later.
    /// </summary>
    private static async Task<ScalingSignal> TickPastTheGateAsync(LatticeScalingSignal facade, MutableTimeProvider clock)
    {
        clock.Advance(PastTheWindow);
        await TickAsync(facade);
        clock.Advance(PastTheWindow);
        return await TickAsync(facade);
    }

    [Test]
    public async Task Scale_in_is_suppressed_while_a_split_is_in_flight_and_released_once_it_completes()
    {
        var (facade, compute, clock, splits) = Build();

        // Peak load establishes the held scalar at 4 replicas' worth of demand.
        compute.Pressure = new ComputePressure { Activation = 1.0 };
        var peak = await TickAsync(facade);
        Assert.That(peak.ScaleValue, Is.EqualTo(4.0).Within(1e-9), "peak demand sets the held scalar");

        // Demand collapses with nothing splitting, which arms the scale-in
        // window. Arming first is what makes the rest of this test meaningful:
        // it means the split - and only the split - is what holds the gate shut
        // at the point where it would otherwise have opened.
        compute.Pressure = default;
        clock.Advance(TimeSpan.FromSeconds(1));
        var armed = await TickAsync(facade);
        Assert.That(armed.ScaleValue, Is.EqualTo(4.0).Within(1e-9), "the arming tick can never itself release");

        // A split starts, then the window elapses. Without split awareness the
        // gate would open here and the scalar would descend; it must not.
        splits.SplitsInFlight(1);
        clock.Advance(PastTheWindow);
        var duringSplit = await TickAsync(facade);

        Assert.Multiple(() =>
        {
            Assert.That(duringSplit.ScaleValue, Is.EqualTo(4.0).Within(1e-9),
                "an in-flight split must suppress scale-in even though the window has fully elapsed");
            Assert.That(duringSplit.Reason, Does.Contain("scale-in held by safety gate"));
        });

        // The split completes. Eligibility restarts from this tick, so the gate
        // is still closed until the window has elapsed again.
        splits.SplitsInFlight(0);
        clock.Advance(TimeSpan.FromSeconds(1));
        var justAfterSplit = await TickAsync(facade);
        Assert.That(justAfterSplit.ScaleValue, Is.EqualTo(4.0).Within(1e-9),
            "the window restarts when the split clears; scale-in is not immediate");

        // Once the window has elapsed with no split in flight, the gate releases
        // and the smoothed scalar is finally allowed to descend.
        clock.Advance(PastTheWindow);
        var released = await TickAsync(facade);

        Assert.Multiple(() =>
        {
            Assert.That(released.ScaleValue, Is.LessThan(4.0),
                "scale-in is released once the split completes and the window elapses");
            Assert.That(released.Reason, Does.Not.Contain("scale-in held by safety gate"));
        });
    }

    [Test]
    public async Task Without_a_split_the_same_timeline_releases_scale_in()
    {
        // The control for the test above: an identical clock timeline with the
        // probe reporting nothing in flight must reach the release. Without this
        // pairing, the suppression assertions could be satisfied by the gate
        // window alone and would prove nothing about split awareness.
        var (facade, compute, clock, _) = Build();

        compute.Pressure = new ComputePressure { Activation = 1.0 };
        await TickAsync(facade);

        compute.Pressure = default;
        clock.Advance(TimeSpan.FromSeconds(1));
        await TickAsync(facade);

        clock.Advance(PastTheWindow);
        var released = await TickAsync(facade);

        Assert.Multiple(() =>
        {
            Assert.That(released.ScaleValue, Is.LessThan(4.0),
                "with no split in flight the gate opens at exactly the tick the split held shut");
            Assert.That(released.Reason, Does.Not.Contain("scale-in held by safety gate"));
        });
    }

    [Test]
    public async Task A_split_starting_mid_descent_re_suppresses_scale_in()
    {
        var (facade, compute, clock, splits) = Build();

        compute.Pressure = new ComputePressure { Activation = 1.0 };
        await TickAsync(facade);

        // Demand collapses and the gate opens, so the scalar starts descending.
        compute.Pressure = default;
        var descending = await TickPastTheGateAsync(facade, clock);
        Assert.That(descending.ScaleValue, Is.LessThan(4.0), "with no split, scale-in proceeds");

        // A split now starts. The descent must stop where it is rather than
        // continuing to release replicas underneath an in-flight split.
        splits.SplitsInFlight(1);
        clock.Advance(PastTheWindow);
        var held = await TickAsync(facade);

        Assert.That(held.ScaleValue, Is.EqualTo(descending.ScaleValue).Within(1e-9),
            "a split starting mid-descent re-suppresses scale-in at the current level");
    }

    [Test]
    public async Task An_unreachable_admin_surface_does_not_wedge_scale_in()
    {
        var options = Microsoft.Extensions.Options.Options.Create(new LatticeScalingSignalOptions());
        var clock = new MutableTimeProvider(T0);
        var compute = new FakeCompute { Pressure = new ComputePressure { Activation = 1.0 } };

        // No grain factory at all - the package added outside a silo.
        var facade = new LatticeScalingSignal(
            compute,
            new FakeStorage(),
            new FakeReplicas(4),
            new LatticeSplitActivityProbe(grainFactory: null),
            new ScalingSignalComputer(options),
            options,
            clock);

        await TickAsync(facade);

        compute.Pressure = default;
        var signal = await TickPastTheGateAsync(facade, clock);

        Assert.That(signal.ScaleValue, Is.LessThan(4.0),
            "a degraded split-activity source must fail open rather than suppress scale-in forever");
    }
}
