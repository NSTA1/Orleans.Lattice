using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for <see cref="RepoContextRetrievalReadinessState"/>: the vector-plane
/// readiness state machine behind the host's readiness probe. Every test is driven by an
/// injected <see cref="TimeProvider"/> - there is no sleep, no timer, and no wall-clock
/// dependence anywhere in this fixture, so the anti-flap behaviour is asserted
/// deterministically rather than raced.
/// </summary>
[TestFixture]
public sealed class RepoContextRetrievalReadinessStateTests
{
    private static readonly TimeSpan HoldDown = TimeSpan.FromSeconds(30);

    private static RepoContextRetrievalReadinessState Create(
        out SettableTimeProvider clock, TimeSpan? holdDown = null)
    {
        clock = new SettableTimeProvider();
        return new RepoContextRetrievalReadinessState(clock, holdDown ?? HoldDown);
    }

    [Test]
    public void Rejects_a_null_time_provider()
        => Assert.That(() => new RepoContextRetrievalReadinessState(null!), Throws.ArgumentNullException);

    [Test]
    public void Defaults_to_the_shared_fault_hold_down()
    {
        using var state = new RepoContextRetrievalReadinessState(new SettableTimeProvider());

        Assert.That(state.FaultHoldDown, Is.EqualTo(RepoContextRetrievalReadinessState.DefaultFaultHoldDown));
    }

    [Test]
    public void Starts_building_and_not_ready()
    {
        using var state = Create(out _);

        Assert.Multiple(() =>
        {
            Assert.That(state.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Building));
            Assert.That(state.IsReady, Is.False, "A host configured for semantic retrieval is not ready until it has proven it can serve.");
            Assert.That(state.TimeToReady, Is.Null);
        });
    }

    [Test]
    public void Stays_not_ready_while_the_vector_plane_is_unavailable()
    {
        using var state = Create(out var clock);

        state.MarkUnavailable(RepoContextRetrievalPath.KeywordVectorPlaneUnavailable);
        clock.Advance(TimeSpan.FromMinutes(10));

        Assert.Multiple(() =>
        {
            Assert.That(state.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Building));
            Assert.That(state.IsReady, Is.False);
            Assert.That(state.TimeToReady, Is.Null);
        });
    }

    [Test]
    public void Becomes_ready_once_the_plane_serves()
    {
        using var state = Create(out var clock);
        clock.Advance(TimeSpan.FromSeconds(85));

        state.MarkServing();

        Assert.Multiple(() =>
        {
            Assert.That(state.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Serving));
            Assert.That(state.IsReady, Is.True);
            Assert.That(state.TimeToReady, Is.EqualTo(TimeSpan.FromSeconds(85)));
        });
    }

    [Test]
    public void Is_ready_in_a_keyword_only_configuration()
    {
        using var state = Create(out _);

        state.MarkKeywordOnly();

        Assert.Multiple(() =>
        {
            Assert.That(state.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.KeywordOnly));
            Assert.That(state.IsReady, Is.True, "A box with no embedder bound is legitimately ready in keyword-only mode.");
            Assert.That(state.TimeToReady, Is.EqualTo(TimeSpan.Zero));
        });
    }

    [Test]
    public void Keyword_only_ignores_a_vector_plane_fault()
    {
        using var state = Create(out var clock);
        state.MarkKeywordOnly();

        state.MarkUnavailable(RepoContextRetrievalPath.KeywordVectorPlaneUnavailable);
        clock.Advance(HoldDown + HoldDown);

        Assert.That(state.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.KeywordOnly),
            "There is no vector plane to be unavailable when no embedder is bound; readiness must not deadlock.");
    }

    [Test]
    public void Readiness_does_not_oscillate_across_a_transient_fault()
    {
        using var state = Create(out var clock);
        state.MarkServing();

        // A transient fault opens the hold-down window. Readiness must not drop while it
        // is open, no matter how often it is polled or how often the fault repeats.
        state.MarkUnavailable(RepoContextRetrievalPath.KeywordIndexDegraded);

        for (var i = 0; i < 5; i++)
        {
            clock.Advance(TimeSpan.FromSeconds(5));
            state.MarkUnavailable(RepoContextRetrievalPath.KeywordIndexDegraded);
            Assert.That(state.IsReady, Is.True, $"Readiness flapped at poll {i} inside the hold-down window.");
        }

        // The fault clears before the window elapses: readiness never changed at all.
        state.MarkServing();
        clock.Advance(HoldDown + HoldDown);

        Assert.Multiple(() =>
        {
            Assert.That(state.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Serving));
            Assert.That(state.IsReady, Is.True);
        });
    }

    [Test]
    public void Readiness_is_revoked_once_a_fault_outlives_the_hold_down()
    {
        using var state = Create(out var clock);
        state.MarkServing();

        state.MarkUnavailable(RepoContextRetrievalPath.KeywordVectorPlaneUnavailable);
        clock.Advance(HoldDown - TimeSpan.FromTicks(1));
        Assert.That(state.IsReady, Is.True, "The hold-down had not yet elapsed.");

        clock.Advance(TimeSpan.FromTicks(1));

        Assert.Multiple(() =>
        {
            Assert.That(state.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Building));
            Assert.That(state.IsReady, Is.False, "A persistent fault must eventually revoke readiness.");
        });
    }

    [Test]
    public void A_repeated_fault_does_not_extend_the_hold_down()
    {
        using var state = Create(out var clock);
        state.MarkServing();
        state.MarkUnavailable(RepoContextRetrievalPath.KeywordVectorPlaneUnavailable);

        // Re-reporting the same episode must not slide the window forward, or a
        // continuously-failing plane would stay "ready" for ever.
        clock.Advance(TimeSpan.FromSeconds(20));
        state.MarkUnavailable(RepoContextRetrievalPath.KeywordVectorPlaneUnavailable);
        clock.Advance(TimeSpan.FromSeconds(20));

        Assert.That(state.IsReady, Is.False);
    }

    [Test]
    public void Recovers_to_serving_after_readiness_was_revoked()
    {
        using var state = Create(out var clock);
        state.MarkServing();
        state.MarkUnavailable(RepoContextRetrievalPath.KeywordVectorPlaneUnavailable);
        clock.Advance(HoldDown);
        Assert.That(state.IsReady, Is.False);

        state.MarkServing();

        Assert.That(state.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Serving));
    }

    [Test]
    public void Time_to_ready_is_stamped_once_and_never_moves()
    {
        using var state = Create(out var clock);
        clock.Advance(TimeSpan.FromSeconds(5));
        state.MarkServing();

        clock.Advance(TimeSpan.FromMinutes(5));
        state.MarkUnavailable(RepoContextRetrievalPath.KeywordIndexDegraded);
        state.MarkServing();

        Assert.That(state.TimeToReady, Is.EqualTo(TimeSpan.FromSeconds(5)),
            "Time-to-retrieval-ready measures the first arrival at ready, not the latest recovery.");
    }

    [Test]
    public void Keyword_only_never_demotes_a_serving_plane()
    {
        using var state = Create(out _);
        state.MarkServing();

        state.MarkKeywordOnly();

        Assert.That(state.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Serving));
    }

    [Test]
    public void Observe_maps_every_vocabulary_value_onto_the_state_machine()
    {
        using (var semanticExact = Create(out _))
        {
            semanticExact.Observe(RepoContextRetrievalPath.SemanticExact);
            Assert.That(semanticExact.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Serving));
        }

        using (var semanticApproximate = Create(out _))
        {
            semanticApproximate.Observe(RepoContextRetrievalPath.SemanticApproximate);
            Assert.That(semanticApproximate.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Serving));
        }

        using (var noEmbedder = Create(out _))
        {
            noEmbedder.Observe(RepoContextRetrievalPath.KeywordNoEmbedder);
            Assert.That(noEmbedder.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.KeywordOnly));
        }

        using (var planeUnavailable = Create(out var clock))
        {
            planeUnavailable.MarkServing();
            planeUnavailable.Observe(RepoContextRetrievalPath.KeywordVectorPlaneUnavailable);
            clock.Advance(HoldDown);
            Assert.That(planeUnavailable.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Building));
        }

        using (var degraded = Create(out var clock))
        {
            degraded.MarkServing();
            degraded.Observe(RepoContextRetrievalPath.KeywordIndexDegraded);
            clock.Advance(HoldDown);
            Assert.That(degraded.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Building));
        }
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("keyword")]
    [TestCase("semantic")]
    public void Observe_ignores_a_value_outside_the_vocabulary(string? path)
    {
        using var state = Create(out var clock);

        state.Observe(path);
        clock.Advance(HoldDown + HoldDown);

        Assert.That(state.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Building),
            "An unrecognised path must change nothing - it can neither invent readiness nor revoke it.");
    }

    [Test]
    public void A_non_positive_hold_down_revokes_readiness_on_the_first_fault()
    {
        using var state = Create(out _, TimeSpan.Zero);
        state.MarkServing();

        state.MarkUnavailable(RepoContextRetrievalPath.KeywordVectorPlaneUnavailable);

        Assert.That(state.IsReady, Is.False);
    }

    [Test]
    public void Publishes_the_time_to_retrieval_ready_histogram()
    {
        var clock = new SettableTimeProvider();
        double? seconds = null;
        string? phaseTag = null;

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RepoContextUsageRecorder.MeterName
                && instrument.Name == RepoContextRetrievalReadinessState.ReadySecondsInstrumentName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<double>((_, measurement, tags, _) =>
        {
            seconds = measurement;
            foreach (var tag in tags)
            {
                if (tag.Key == RepoContextRetrievalReadinessState.PhaseTagKey)
                {
                    phaseTag = tag.Value as string;
                }
            }
        });
        listener.Start();

        using var state = new RepoContextRetrievalReadinessState(clock, HoldDown);
        clock.Advance(TimeSpan.FromSeconds(42));
        state.MarkServing();

        // A second arrival at ready must not re-publish: the figure is per process.
        state.MarkUnavailable(RepoContextRetrievalPath.KeywordIndexDegraded);
        state.MarkServing();

        Assert.Multiple(() =>
        {
            Assert.That(seconds, Is.EqualTo(42d).Within(0.001));
            Assert.That(phaseTag, Is.EqualTo(RepoContextRetrievalReadinessState.PhaseServingTag));
        });
    }

    [Test]
    public void Publishes_the_time_to_retrieval_ready_histogram_for_a_keyword_only_host()
    {
        var clock = new SettableTimeProvider();
        string? phaseTag = null;

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RepoContextUsageRecorder.MeterName
                && instrument.Name == RepoContextRetrievalReadinessState.ReadySecondsInstrumentName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<double>((_, _, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == RepoContextRetrievalReadinessState.PhaseTagKey)
                {
                    phaseTag = tag.Value as string;
                }
            }
        });
        listener.Start();

        using var state = new RepoContextRetrievalReadinessState(clock, HoldDown);
        state.MarkKeywordOnly();

        Assert.That(phaseTag, Is.EqualTo(RepoContextRetrievalReadinessState.PhaseKeywordOnlyTag));
    }

    [Test]
    public void Meters_one_fault_episode_with_its_cause()
    {
        var clock = new SettableTimeProvider();
        long episodes = 0;
        var causes = new List<string?>();

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RepoContextUsageRecorder.MeterName
                && instrument.Name == RepoContextRetrievalReadinessState.UnavailableInstrumentName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
        {
            episodes += measurement;
            foreach (var tag in tags)
            {
                if (tag.Key == RepoContextRetrievalReadinessState.CauseTagKey)
                {
                    causes.Add(tag.Value as string);
                }
            }
        });
        listener.Start();

        using var state = new RepoContextRetrievalReadinessState(clock, HoldDown);
        state.MarkServing();

        // One episode, reported three times: the counter must rise once.
        state.MarkUnavailable(RepoContextRetrievalPath.KeywordVectorPlaneUnavailable);
        state.MarkUnavailable(RepoContextRetrievalPath.KeywordVectorPlaneUnavailable);
        state.MarkUnavailable(RepoContextRetrievalPath.KeywordVectorPlaneUnavailable);

        // Recovery closes the episode, so the next fault is a new one.
        state.MarkServing();
        state.MarkUnavailable(RepoContextRetrievalPath.KeywordIndexDegraded);

        Assert.Multiple(() =>
        {
            Assert.That(episodes, Is.EqualTo(2));
            Assert.That(causes, Is.EqualTo(new[]
            {
                RepoContextRetrievalPath.KeywordVectorPlaneUnavailable,
                RepoContextRetrievalPath.KeywordIndexDegraded,
            }));
        });
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("repo/lattice/file/src/Secret.cs")]
    public void An_unrecognised_fault_cause_is_metered_as_unknown(string? cause)
    {
        var clock = new SettableTimeProvider();
        string? causeTag = null;

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RepoContextUsageRecorder.MeterName
                && instrument.Name == RepoContextRetrievalReadinessState.UnavailableInstrumentName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, _, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == RepoContextRetrievalReadinessState.CauseTagKey)
                {
                    causeTag = tag.Value as string;
                }
            }
        });
        listener.Start();

        using var state = new RepoContextRetrievalReadinessState(clock, HoldDown);
        state.MarkUnavailable(cause);

        Assert.That(causeTag, Is.EqualTo(RepoContextRetrievalReadinessState.UnknownCause),
            "An arbitrary cause must never reach the meter as an unbounded tag value.");
    }

    [Test]
    public void Meters_a_probe_reported_fault_under_the_probe_cause()
    {
        var clock = new SettableTimeProvider();
        string? causeTag = null;

        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RepoContextUsageRecorder.MeterName
                && instrument.Name == RepoContextRetrievalReadinessState.UnavailableInstrumentName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, _, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == RepoContextRetrievalReadinessState.CauseTagKey)
                {
                    causeTag = tag.Value as string;
                }
            }
        });
        listener.Start();

        using var state = new RepoContextRetrievalReadinessState(clock, HoldDown);
        state.MarkUnavailable(RepoContextRetrievalReadinessState.ProbeCause);

        Assert.That(causeTag, Is.EqualTo(RepoContextRetrievalReadinessState.ProbeCause));
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        var state = new RepoContextRetrievalReadinessState(new SettableTimeProvider());

        state.Dispose();

        Assert.That(() => state.Dispose(), Throws.Nothing);
    }
}
