namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Tests for the saturated-unavailable phase (issue #3286): the readable state
/// that distinguishes a plane which is still arming from one that will not arm at
/// the present capacity.
/// <para>
/// <b>The distinction is the whole point, and nothing else on this state could
/// draw it.</b> A plane whose open is refused admission on every attempt reports
/// <c>building</c>, returns 503 from <c>/health/ready</c>, and holds the opening
/// phase's in-flight gauge at a rising number with every later phase at zero.
/// Those are, to the byte, the readings of a large and entirely healthy cold
/// open. An operator shown them cannot tell whether to wait or to add capacity,
/// and the one they supply is always "wait".
/// </para>
/// <para>
/// <b>Not sticky, and that is deliberate.</b> The open keeps retrying while this
/// phase is reported, so a plane whose saturation clears self-heals without an
/// operator. A phase that could not be left would convert a capacity episode into
/// a permanent outage needing a restart, which is strictly worse than the
/// unbounded retry it replaced.
/// </para>
/// </summary>
public sealed partial class RepoContextRetrievalReadinessStateTests
{
    [Test]
    public void A_saturation_declaration_applies_from_arming()
    {
        using var state = Create(out _);

        var entered = state.MarkSaturationUnavailable();

        Assert.Multiple(() =>
        {
            Assert.That(entered, Is.True,
                "the call reports whether it entered the episode, because the caller logs once per "
                + "episode rather than once per refusal");
            Assert.That(
                state.Phase,
                Is.EqualTo(RepoContextRetrievalReadinessPhase.SaturatedUnavailable));
            Assert.That(
                RepoContextRetrievalReadinessState.PhaseTag(state.Phase),
                Is.EqualTo(RepoContextRetrievalReadinessState.PhaseSaturatedUnavailableTag),
                "the phase has to carry a bounded tag of its own, or it is invisible to every reader "
                + "that goes through the tag rather than the enum");
        });
    }

    [Test]
    public void A_saturated_plane_is_not_ready()
    {
        using var state = Create(out _);

        state.MarkSaturationUnavailable();

        Assert.Multiple(() =>
        {
            Assert.That(state.IsReady, Is.False,
                "A PLANE THAT WILL NOT ARM IS NOT READY. The readiness test used to be the NEGATIVE "
                + "form - anything that is not Building - which would have silently reported this new "
                + "phase as ready. That is not a failure this fixture would have to argue about: it "
                + "would have declared a dead plane healthy.");
            Assert.That(
                RepoContextRetrievalReadinessState.IsReadyPhase(
                    RepoContextRetrievalReadinessPhase.SaturatedUnavailable),
                Is.False,
                "and the static predicate the HTTP endpoint and the health tool both call must agree, "
                + "or two readers of one state disagree about whether the box is serving");
        });
    }

    [Test]
    public void Every_declared_phase_has_an_explicit_ready_verdict()
    {
        // THE GUARD AGAINST THE NEXT PHASE BEING ADDED SILENTLY. The negative form
        // this replaced defaulted a brand-new phase to READY, which is the worst
        // available default. The positive form throws instead, and this fixture is
        // what proves it covers the whole enum rather than merely compiling.
        var phases = Enum.GetValues<RepoContextRetrievalReadinessPhase>();

        Assert.That(phases, Is.Not.Empty,
            "positive control: the reflection must find members, or every assertion below passes over "
            + "an empty set");

        Assert.Multiple(() =>
        {
            foreach (var phase in phases)
            {
                Assert.That(
                    () => RepoContextRetrievalReadinessState.IsReadyPhase(phase),
                    Throws.Nothing,
                    $"phase {phase} has no ready verdict, so a reader asking whether the box is serving "
                    + "gets an exception rather than an answer");
                Assert.That(
                    () => RepoContextRetrievalReadinessState.PhaseTag(phase),
                    Throws.Nothing,
                    $"phase {phase} has no bounded tag");
            }
        });
    }

    [Test]
    public void A_saturation_declaration_is_entered_once_per_episode()
    {
        using var state = Create(out _);

        var first = state.MarkSaturationUnavailable();
        var second = state.MarkSaturationUnavailable();

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.True, "positive control: the first call must enter the episode");
            Assert.That(second, Is.False,
                "A REFUSAL RUN RE-DECLARES ON EVERY TICK. Reporting entry each time would have the "
                + "handle write one warning per refusal, which at the observed 0.66 per minute is "
                + "roughly a thousand a day for as long as the capacity shortfall lasted.");
        });
    }

    [Test]
    public void A_saturation_declaration_does_not_disturb_a_serving_plane()
    {
        using var state = Create(out _);
        state.MarkServing();

        var entered = state.MarkSaturationUnavailable();

        Assert.Multiple(() =>
        {
            Assert.That(entered, Is.False);
            Assert.That(state.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Serving),
                "A SERVING PLANE HAS AN INDEX. A late refusal on some other open must not take a "
                + "plane that is answering queries out of service - the declaration is about a plane "
                + "that has never armed, and applying it to one that has would turn a diagnostic into "
                + "an outage.");
        });
    }

    [Test]
    public void A_saturation_declaration_does_not_disturb_a_keyword_only_deployment()
    {
        using var state = Create(out _);
        state.MarkKeywordOnly();

        var entered = state.MarkSaturationUnavailable();

        Assert.Multiple(() =>
        {
            Assert.That(entered, Is.False);
            Assert.That(state.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.KeywordOnly),
                "keyword-only is an INTENDED deployment with no embedding provider bound and IS ready. "
                + "Letting a vector-plane refusal overwrite it would report an outage on a box that is "
                + "configured exactly as its operator meant it to be.");
        });
    }

    [Test]
    public void A_declared_plane_returns_to_arming_when_it_clears()
    {
        using var state = Create(out _);
        state.MarkSaturationUnavailable();

        var left = state.ClearSaturationUnavailable();

        Assert.Multiple(() =>
        {
            Assert.That(left, Is.True);
            Assert.That(state.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Building),
                "IT RETURNS TO ARMING, NOT TO SERVING. Admission recovering says the open can proceed; "
                + "whether the plane then serves is a separate fact reported by whoever completes it. "
                + "Clearing straight to Serving would report an index that does not exist yet.");
            Assert.That(state.IsReady, Is.False,
                "and arming is still not ready, so the recovery does not smuggle in a readiness claim");
        });
    }

    [Test]
    public void Clearing_a_plane_that_was_never_declared_does_nothing()
    {
        using var state = Create(out _);

        var left = state.ClearSaturationUnavailable();

        Assert.Multiple(() =>
        {
            Assert.That(left, Is.False,
                "the open calls this on every attempt that banks progress, so a no-op has to be "
                + "reported as one or the recovery line is logged by every healthy open");
            Assert.That(state.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Building));
        });
    }

    [Test]
    public void Clearing_does_not_pull_a_serving_plane_back_to_arming()
    {
        using var state = Create(out _);
        state.MarkServing();

        var left = state.ClearSaturationUnavailable();

        Assert.Multiple(() =>
        {
            Assert.That(left, Is.False);
            Assert.That(state.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Serving),
                "the open clears its run on completion as well as on progress, and the completion path "
                + "runs on a plane that is about to serve. A clear that unconditionally wrote Building "
                + "would unready every plane that ever took a refusal on its way up.");
        });
    }

    [Test]
    public void A_declared_plane_can_still_be_marked_serving()
    {
        using var state = Create(out _);
        state.MarkSaturationUnavailable();

        state.MarkServing();

        Assert.Multiple(() =>
        {
            Assert.That(state.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Serving),
                "TERMINAL IS A CLAIM, NOT A BEHAVIOUR. The open never stops retrying, so the plane that "
                + "was declared unavailable is exactly the plane that may arm ten minutes later when "
                + "occupancy falls. A declaration it could not come back from would be an outage this "
                + "change CAUSED rather than reported.");
            Assert.That(state.IsReady, Is.True);
        });
    }
}
