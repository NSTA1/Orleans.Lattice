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
}
