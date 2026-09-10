namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Tests for <see cref="RepoContextRetrievalReadinessState.Arming"/>: the second,
/// independent fact this state reports beside its readiness verdict - whether the
/// approximate plane last answered from a trained partitioning.
/// <para>
/// The load-bearing assertions here are the ones proving arming does <b>not</b>
/// move the verdict. An unarmed plane answers with complete recall by exhaustive
/// scan, so it is genuinely ready, and a corpus below the training threshold can
/// never partition; gating readiness on arming would fail such a deployment
/// permanently. The signal was added to make an invisible distinction visible, not
/// to make a healthy box unhealthy.
/// </para>
/// </summary>
public sealed partial class RepoContextRetrievalReadinessStateTests
{
    [Test]
    public void Arming_starts_unknown()
    {
        using var state = Create(out _);

        Assert.That(
            state.Arming,
            Is.EqualTo(RepoContextRetrievalArming.Unknown),
            "Before any query the plane answered, arming is genuinely unobserved. Reporting 'unarmed' "
            + "here would be a claim, not a reading, and would be indistinguishable from a plane "
            + "observed to hold no partitioning.");
    }

    [Test]
    public void An_observation_is_recorded()
    {
        using var state = Create(out _);

        state.ObserveArming(RepoContextRetrievalArming.Armed);

        Assert.That(state.Arming, Is.EqualTo(RepoContextRetrievalArming.Armed));
    }

    [Test]
    public void The_latest_observation_wins()
    {
        using var state = Create(out _);

        state.ObserveArming(RepoContextRetrievalArming.Armed);
        state.ObserveArming(RepoContextRetrievalArming.Unarmed);

        Assert.That(
            state.Arming,
            Is.EqualTo(RepoContextRetrievalArming.Unarmed),
            "Arming reports the plane's current partitioning. A rebuild can legitimately unarm a plane, "
            + "and a latch would keep asserting a partitioning that no longer exists.");
    }

    [Test]
    public void An_unknown_observation_never_erases_a_real_one()
    {
        using var state = Create(out _);

        state.ObserveArming(RepoContextRetrievalArming.Armed);
        state.ObserveArming(RepoContextRetrievalArming.Unknown);

        Assert.That(
            state.Arming,
            Is.EqualTo(RepoContextRetrievalArming.Armed),
            "Unknown carries no observation. Letting it overwrite one would turn a query the plane "
            + "declined to answer into evidence about the plane's partitioning.");
    }

    [Test]
    public void Arming_does_not_make_a_building_host_ready()
    {
        using var state = Create(out _);

        state.ObserveArming(RepoContextRetrievalArming.Armed);

        Assert.Multiple(() =>
        {
            Assert.That(state.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Building));
            Assert.That(state.IsReady, Is.False,
                "Arming is not evidence that a semantic retrieval succeeded, so it must never latch "
                + "readiness. Only MarkServing may assert demonstrated capability.");
        });
    }

    [Test]
    public void An_unarmed_plane_is_still_ready()
    {
        using var state = Create(out _);

        state.MarkServing();
        state.ObserveArming(RepoContextRetrievalArming.Unarmed);

        Assert.Multiple(() =>
        {
            Assert.That(state.IsReady, Is.True,
                "An unarmed plane answers by exhaustive scan with complete recall, so it is ready. A "
                + "corpus below the training threshold can never partition, and failing it for that "
                + "would wedge the deployment permanently.");
            Assert.That(state.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Serving));
            Assert.That(state.Arming, Is.EqualTo(RepoContextRetrievalArming.Unarmed));
        });
    }

    [Test]
    public void Readiness_and_arming_are_independent()
    {
        using var armed = Create(out _);
        using var unarmed = Create(out _);

        armed.MarkServing();
        armed.ObserveArming(RepoContextRetrievalArming.Armed);
        unarmed.MarkServing();
        unarmed.ObserveArming(RepoContextRetrievalArming.Unarmed);

        Assert.Multiple(() =>
        {
            Assert.That(armed.IsReady, Is.EqualTo(unarmed.IsReady),
                "The verdict is deliberately identical for both, which is why a second signal was "
                + "needed at all.");
            Assert.That(armed.Arming, Is.Not.EqualTo(unarmed.Arming),
                "And the second signal is deliberately not identical, which is the whole fix.");
        });
    }
}
