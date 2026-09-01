using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Coverage for <see cref="LatticeSplitActivityProbe"/>: the mapping of the core
/// administrative split-activity snapshot
/// (<see cref="ILatticeAdmin.GetSplitActivityAsync"/>) onto the scale-in gate's
/// boolean, and the fail-open degradation when no grain factory is available or
/// the administrative call fails. Uses a substituted core seam so no cluster is
/// required.
/// </summary>
[TestFixture]
public sealed class LatticeSplitActivityProbeTests
{
    private const string AdminGrainKey = "_lattice_admin";

    private static IGrainFactory FactoryFor(ILatticeAdmin admin)
    {
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeAdmin>(AdminGrainKey).Returns(admin);
        return factory;
    }

    private static ILatticeAdmin AdminReporting(int inFlight, int reportingTrees)
    {
        var admin = Substitute.For<ILatticeAdmin>();
        admin.GetSplitActivityAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new SplitActivityReport
            {
                InFlight = inFlight,
                ReportingTrees = reportingTrees,
                ObservedAt = DateTimeOffset.UnixEpoch,
            }));
        return admin;
    }

    [Test]
    public async Task No_grain_factory_reports_no_split_in_flight()
    {
        var probe = new LatticeSplitActivityProbe(grainFactory: null);

        Assert.That(await probe.AnySplitInFlightAsync(CancellationToken.None), Is.False);
    }

    [Test]
    public async Task An_idle_cluster_reports_no_split_in_flight()
    {
        var probe = new LatticeSplitActivityProbe(FactoryFor(AdminReporting(inFlight: 0, reportingTrees: 0)));

        Assert.That(await probe.AnySplitInFlightAsync(CancellationToken.None), Is.False);
    }

    [Test]
    public async Task A_single_in_flight_split_is_reported()
    {
        var probe = new LatticeSplitActivityProbe(FactoryFor(AdminReporting(inFlight: 1, reportingTrees: 1)));

        Assert.That(await probe.AnySplitInFlightAsync(CancellationToken.None), Is.True);
    }

    [Test]
    public async Task Splits_across_several_trees_are_reported()
    {
        var probe = new LatticeSplitActivityProbe(FactoryFor(AdminReporting(inFlight: 4, reportingTrees: 3)));

        Assert.That(await probe.AnySplitInFlightAsync(CancellationToken.None), Is.True);
    }

    [Test]
    public async Task A_failing_admin_call_degrades_to_no_split_rather_than_throwing()
    {
        var admin = Substitute.For<ILatticeAdmin>();
        admin.GetSplitActivityAsync(Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("admin unreachable"));

        var probe = new LatticeSplitActivityProbe(FactoryFor(admin));

        // Fail-open: suppressing scale-in forever on a persistently unreachable
        // admin surface would cost more than the narrow risk the gate mitigates.
        Assert.That(await probe.AnySplitInFlightAsync(CancellationToken.None), Is.False);
    }

    [Test]
    public void Caller_cancellation_is_not_swallowed()
    {
        var admin = Substitute.For<ILatticeAdmin>();
        admin.GetSplitActivityAsync(Arg.Any<CancellationToken>())
            .ThrowsAsync(new OperationCanceledException());

        var probe = new LatticeSplitActivityProbe(FactoryFor(admin));

        Assert.That(
            async () => await probe.AnySplitInFlightAsync(new CancellationToken(canceled: true)),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task The_probe_makes_exactly_one_admin_call_per_tick()
    {
        var admin = AdminReporting(inFlight: 0, reportingTrees: 0);
        var probe = new LatticeSplitActivityProbe(FactoryFor(admin));

        await probe.AnySplitInFlightAsync(CancellationToken.None);

        // The gate is polled on every sample tick, so it must never fan out.
        await admin.Received(1).GetSplitActivityAsync(Arg.Any<CancellationToken>());
    }
}
