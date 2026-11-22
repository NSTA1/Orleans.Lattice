using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// The view maintainer's lag age (issue #6145): how long the oldest source entry
/// the view has not applied has been waiting, derived from the entry lag each
/// drain pass and status read already computes, against a controllable clock.
/// </summary>
[TestFixture]
public class ViewMaintainerLagAgeTests
{
    private sealed class AdvanceableClock(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _utcNow = start;

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public void Advance(TimeSpan delta) => _utcNow = _utcNow.Add(delta);
    }

    private static (ViewMaintainerGrain Grain, AdvanceableClock Clock, FakePersistentState<ViewCheckpointState> State) Create()
    {
        var factory = Substitute.For<IGrainFactory>();
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("viewmaintainer", "orders-view"));
        var viewOptions = Substitute.For<IOptionsMonitor<LatticeViewOptions>>();
        viewOptions.Get(Arg.Any<string>()).Returns(new LatticeViewOptions());
        var state = new FakePersistentState<ViewCheckpointState>();

        var grain = new ViewMaintainerGrain(
            context,
            factory,
            reminderRegistry: null!,
            NullLogger<ViewMaintainerGrain>.Instance,
            Substitute.For<IViewCatalog>(),
            commitLogReader: null!,
            subscriber: null!,
            cursorRegistry: null!,
            optionsResolver: TestOptionsResolver.ForFactory(factory),
            viewOptions,
            latticeOptions: null!,
            replicationContext: Substitute.For<ILatticeReplicationContext>(),
            saturationSignal: null,
            historyRowCodec: null!,
            state);
        var clock = new AdvanceableClock(DateTimeOffset.UnixEpoch);
        grain.SetSourceIdentityClockForTesting(clock);
        return (grain, clock, state);
    }

    [Test]
    public void A_caught_up_view_reports_zero_age()
    {
        var (grain, clock, _) = Create();

        Assert.That(grain.ObserveLagAge(0), Is.EqualTo(TimeSpan.Zero));
        clock.Advance(TimeSpan.FromMinutes(5));
        Assert.That(grain.ObserveLagAge(0), Is.EqualTo(TimeSpan.Zero));
    }

    [Test]
    public void A_backlog_that_does_not_drain_ages_with_the_clock()
    {
        var (grain, clock, state) = Create();
        state.State.AppliedOffsets[0] = 9;

        Assert.That(grain.ObserveLagAge(5), Is.EqualTo(TimeSpan.Zero));
        clock.Advance(TimeSpan.FromSeconds(40));
        Assert.That(grain.ObserveLagAge(5), Is.EqualTo(TimeSpan.FromSeconds(40)));
        clock.Advance(TimeSpan.FromHours(2));
        Assert.That(grain.ObserveLagAge(5), Is.EqualTo(TimeSpan.FromSeconds(40) + TimeSpan.FromHours(2)));
    }

    [Test]
    public void Applying_the_oldest_entries_moves_the_age_to_the_next_observation()
    {
        var (grain, clock, state) = Create();
        state.State.AppliedOffsets[0] = 9; // 10 applied

        grain.ObserveLagAge(5); // t=0, head 15
        clock.Advance(TimeSpan.FromSeconds(10));
        grain.ObserveLagAge(10); // t=10, head 20

        // Everything present at t=0 is now applied, but entries up to head 20 are not.
        state.State.AppliedOffsets[0] = 15; // 16 applied
        clock.Advance(TimeSpan.FromSeconds(10));
        Assert.That(grain.ObserveLagAge(4), Is.EqualTo(TimeSpan.FromSeconds(10)));
    }

    [Test]
    public void A_steadily_drained_stream_does_not_age_without_bound()
    {
        var (grain, clock, state) = Create();
        long applied = 0;
        var maxAge = TimeSpan.Zero;
        for (var tick = 0; tick < 200; tick++)
        {
            state.State.AppliedOffsets[0] = applied - 1;
            var age = grain.ObserveLagAge(3);
            maxAge = age > maxAge ? age : maxAge;
            clock.Advance(TimeSpan.FromSeconds(1));
            applied += 3;
        }

        Assert.That(maxAge, Is.LessThanOrEqualTo(TimeSpan.FromSeconds(2)));
    }

    [Test]
    public void Catching_up_resets_the_age()
    {
        var (grain, clock, state) = Create();
        state.State.AppliedOffsets[0] = 0;

        grain.ObserveLagAge(5);
        clock.Advance(TimeSpan.FromMinutes(3));
        Assert.That(grain.ObserveLagAge(0), Is.EqualTo(TimeSpan.Zero));
        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.That(grain.ObserveLagAge(2), Is.EqualTo(TimeSpan.Zero));
    }
}
