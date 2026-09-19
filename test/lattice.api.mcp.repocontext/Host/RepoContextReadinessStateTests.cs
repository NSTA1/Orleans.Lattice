using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for <see cref="RepoContextReadinessState"/>: the Starting to Ready
/// to Draining transitions and the invariant that a late warmup can never re-open
/// readiness once draining has begun.
/// </summary>
[TestFixture]
public sealed class RepoContextReadinessStateTests
{
    [Test]
    public void New_state_starts_not_ready_in_the_starting_phase()
    {
        var state = new RepoContextReadinessState();

        Assert.Multiple(() =>
        {
            Assert.That(state.Phase, Is.EqualTo(RepoContextLifecyclePhase.Starting));
            Assert.That(state.IsReady, Is.False);
        });
    }

    [Test]
    public void MarkReady_from_starting_transitions_to_ready()
    {
        var state = new RepoContextReadinessState();

        state.MarkReady();

        Assert.Multiple(() =>
        {
            Assert.That(state.Phase, Is.EqualTo(RepoContextLifecyclePhase.Ready));
            Assert.That(state.IsReady, Is.True);
        });
    }

    [Test]
    public void BeginDrain_flips_ready_to_draining_and_not_ready()
    {
        var state = new RepoContextReadinessState();
        state.MarkReady();

        state.BeginDrain();

        Assert.Multiple(() =>
        {
            Assert.That(state.Phase, Is.EqualTo(RepoContextLifecyclePhase.Draining));
            Assert.That(state.IsReady, Is.False);
        });
    }

    [Test]
    public void MarkReady_after_drain_cannot_re_open_readiness()
    {
        var state = new RepoContextReadinessState();
        state.BeginDrain();

        state.MarkReady();

        Assert.Multiple(() =>
        {
            Assert.That(state.Phase, Is.EqualTo(RepoContextLifecyclePhase.Draining));
            Assert.That(state.IsReady, Is.False);
        });
    }

    /// <summary>A settable clock so the recorded drain instant is deterministic.</summary>
    private sealed class MutableClock(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _now = start;

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan by) => _now += by;
    }

    [Test]
    public void DrainStartedAtUtc_is_null_before_draining()
        => Assert.That(new RepoContextReadinessState().DrainStartedAtUtc, Is.Null);

    [Test]
    public void BeginDrain_stamps_the_drain_start_instant_from_the_clock()
    {
        var clock = new MutableClock(DateTimeOffset.UnixEpoch);
        var state = new RepoContextReadinessState(clock);

        state.BeginDrain();

        Assert.That(state.DrainStartedAtUtc, Is.EqualTo(DateTimeOffset.UnixEpoch));
    }

    [Test]
    public void Repeated_BeginDrain_does_not_move_the_recorded_origin()
    {
        var clock = new MutableClock(DateTimeOffset.UnixEpoch);
        var state = new RepoContextReadinessState(clock);

        state.BeginDrain();
        clock.Advance(TimeSpan.FromMinutes(5));
        state.BeginDrain(); // both the ApplicationStopping hook and StopAsync call this

        Assert.That(state.DrainStartedAtUtc, Is.EqualTo(DateTimeOffset.UnixEpoch),
            "The drain-duration bound must measure from the FIRST drain, or a second call "
            + "would silently reset the clock and mask a hang.");
    }
}
