using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Covers the forecast added for issue #2598: the relationship between the budget
/// this process derived and the drain the previous process actually measured.
/// </summary>
/// <remarks>
/// <para>
/// The defect issue #2598 records is not that 90s is the wrong number. It is that
/// the host knew the budget at startup and the drain duration at failure and never
/// put the two in the same sentence, so the mismatch was <b>only ever observable at
/// the moment it was too late to act on</b>. These tests pin the comparison that
/// closes that gap, and in particular pin that it reaches its conclusions from
/// measurements rather than from constants: with no recorded drain the forecast
/// reports that it cannot project, which is distinguishable from projecting zero.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextDrainForecastTests
{
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(90);
    private static readonly DateTimeOffset Observed = new(2026, 3, 4, 5, 6, 7, TimeSpan.Zero);

    private static RepoContextDrainObservation Drain(
        RepoContextDrainOutcome outcome,
        double? seconds,
        int? resident = 10_000)
        => new(
            Observed,
            outcome,
            Budget,
            seconds is { } value ? TimeSpan.FromSeconds(value) : null,
            resident);

    [Test]
    public void With_no_recorded_drain_the_forecast_reports_that_it_cannot_project()
    {
        // Not "fits". A first start has no evidence either way, and reporting the
        // absence of evidence as a pass is how a budget goes years without anyone
        // discovering it never covered the work.
        var forecast = RepoContextDrainForecast.Evaluate(null, Budget);

        Assert.Multiple(() =>
        {
            Assert.That(forecast.Verdict, Is.EqualTo(RepoContextDrainForecastVerdict.NoHistory));
            Assert.That(forecast.IsFailing, Is.False);
            Assert.That(forecast.ConsumedFraction, Is.Null);
            Assert.That(forecast.TryProject(5000, out _), Is.False, "a projection needs a measured cost, not a guessed one");
        });
    }

    [Test]
    public void A_start_marker_with_no_terminal_outcome_is_read_as_a_process_killed_mid_drain()
    {
        // The one genuinely new observation in this change. The premise elsewhere in
        // this component is that the container's real grace period is unobservable
        // from inside the container, and that is true WITHIN a process. It is not
        // true across a restart: a drain that recorded its start and never recorded
        // an end is a process that did not survive its own drain.
        var forecast = RepoContextDrainForecast.Evaluate(
            Drain(RepoContextDrainOutcome.Started, seconds: null),
            Budget);

        Assert.Multiple(() =>
        {
            Assert.That(forecast.Verdict, Is.EqualTo(RepoContextDrainForecastVerdict.KilledMidDrain));
            Assert.That(forecast.IsFailing, Is.True, "this is the condition the declaration is supposed to prevent");
            Assert.That(
                forecast.ConsumedFraction,
                Is.Null,
                "no duration was ever measured, so no fraction can be computed and none may be invented");
        });
    }

    [Test]
    public void An_abandoned_drain_is_reported_as_exceeding_the_budget()
    {
        var forecast = RepoContextDrainForecast.Evaluate(
            Drain(RepoContextDrainOutcome.Abandoned, 102.1),
            Budget);

        Assert.Multiple(() =>
        {
            Assert.That(forecast.Verdict, Is.EqualTo(RepoContextDrainForecastVerdict.Exceeded));
            Assert.That(forecast.IsFailing, Is.True);
            Assert.That(forecast.ConsumedFraction, Is.EqualTo(102.1 / 90.0).Within(1e-9));
        });
    }

    [Test]
    public void The_verdict_and_the_fraction_printed_beside_it_are_computed_from_the_same_budget()
    {
        // The regression pin for issue #3305, stated as the equivalence that defect
        // violated: a live container reported "took 91.9s and does not fit this
        // process's 180s shutdown budget (51% of it)". Both figures were correct;
        // only the boolean was computed against a different budget from the
        // percentage, because a recorded Abandoned outcome - a fact about the
        // PREVIOUS process's budget - was replayed unconditionally against this one.
        //
        // Asserting the two agree across a matrix is what makes them unable to
        // diverge again. Asserting the one scenario below would not: it would pass
        // against any implementation that special-cased that scenario.
        foreach (var recordedBudget in new[] { 30d, 90d, 180d })
        {
            foreach (var outcome in new[] { RepoContextDrainOutcome.Completed, RepoContextDrainOutcome.Abandoned })
            {
                foreach (var duration in new[] { 0.5d, 20d, 63d, 89.7d, 91.9d, 102.1d, 400d })
                {
                    foreach (var currentBudget in new[] { 30d, 90d, 180d, 240d })
                    {
                        var forecast = RepoContextDrainForecast.Evaluate(
                            new RepoContextDrainObservation(
                                Observed,
                                outcome,
                                TimeSpan.FromSeconds(recordedBudget),
                                TimeSpan.FromSeconds(duration),
                                10_000),
                            TimeSpan.FromSeconds(currentBudget));

                        var context = $"{outcome} drain of {duration}s recorded under {recordedBudget}s, "
                            + $"evaluated against {currentBudget}s";

                        Assert.That(forecast.ConsumedFraction, Is.Not.Null, context);

                        // The equivalence, in both directions. "Does not fit" is
                        // exactly "consumed the whole budget", so a verdict of
                        // Exceeded beside a sub-100% percentage - the shape #3305
                        // reported - is unrepresentable.
                        Assert.That(
                            forecast.Verdict == RepoContextDrainForecastVerdict.Exceeded,
                            Is.EqualTo(forecast.ConsumedFraction >= 1.0d),
                            $"the verdict must agree with the fraction printed beside it: {context}");
                    }
                }
            }
        }
    }

    [Test]
    public void The_remediation_for_an_exceeded_budget_is_never_below_the_grant_already_declared()
    {
        // The second half of the #3305 complaint: the message advised raising the
        // grant "to at least 123s" to a deployment already declaring 240s, so
        // following it would have been a reduction. That was a symptom of the verdict
        // firing when it should not have, and it cannot recur once Exceeded means
        // the drain genuinely overran - but the property is worth asserting directly,
        // because it is the one an operator acts on.
        foreach (var grantSeconds in new[] { 30d, 120d, 240d, 600d })
        {
            var grant = TimeSpan.FromSeconds(grantSeconds);
            var budget = RepoContextShutdownBudget.Derive(grant);

            // Any drain that overruns this budget, including one that only just does.
            foreach (var duration in new[] { budget, budget + TimeSpan.FromSeconds(1), budget * 3 })
            {
                var forecast = RepoContextDrainForecast.Evaluate(
                    new RepoContextDrainObservation(
                        Observed,
                        RepoContextDrainOutcome.Abandoned,
                        budget,
                        duration,
                        10_000),
                    budget);

                Assert.That(forecast.Verdict, Is.EqualTo(RepoContextDrainForecastVerdict.Exceeded));
                Assert.That(
                    forecast.RequiredStopGracePeriod!.Value,
                    Is.GreaterThanOrEqualTo(grant),
                    $"advising a grant below the {grantSeconds}s already declared would ask an operator "
                    + "to reduce it in order to fix an overrun");
            }
        }
    }

    [Test]
    public void A_drain_abandoned_under_a_smaller_budget_is_unproven_against_a_larger_one_rather_than_exceeded()
    {
        // The verbatim scenario from issue #3305. The previous process abandoned a
        // drain at 91.9s under a 90s budget; the grant was then raised to 240s, so
        // this process derives 180s. The old code replayed the recorded abandonment
        // and announced "does not fit ... (51% of it)".
        var forecast = RepoContextDrainForecast.Evaluate(
            new RepoContextDrainObservation(
                Observed,
                RepoContextDrainOutcome.Abandoned,
                TimeSpan.FromSeconds(90),
                TimeSpan.FromSeconds(91.9),
                3334),
            TimeSpan.FromSeconds(180));

        Assert.Multiple(() =>
        {
            // Not Exceeded: 91.9s fits 180s, and saying otherwise beside "51%" is the
            // defect. Not Fits either: the drain was CUT SHORT, so 91.9s is a floor
            // on what a complete drain costs and not a measurement of one. Reporting
            // it as fitting would replace a false alarm with a false all-clear.
            Assert.That(forecast.Verdict, Is.EqualTo(RepoContextDrainForecastVerdict.Unproven));
            Assert.That(
                forecast.IsFailing,
                Is.False,
                "an operator who has just performed the remediation must not still be told the next "
                + "stop will be abandoned");
            Assert.That(forecast.ConsumedFraction, Is.EqualTo(91.9 / 180.0).Within(1e-9));
            Assert.That(
                forecast.RequiredStopGracePeriod!.Value,
                Is.LessThan(TimeSpan.FromSeconds(240)),
                "the floor the truncated drain implies is already covered by the declared grant, which is "
                + "why the report must not present it as a value to raise to");
        });
    }

    [Test]
    public void The_same_abandoned_drain_against_an_unchanged_budget_is_still_reported_as_exceeding()
    {
        // The control that stops the #3305 fix from becoming a suppression. Nothing
        // about the deployment changed here, so the abandonment is still a live
        // prediction and must still be reported as one.
        var forecast = RepoContextDrainForecast.Evaluate(
            new RepoContextDrainObservation(
                Observed,
                RepoContextDrainOutcome.Abandoned,
                TimeSpan.FromSeconds(90),
                TimeSpan.FromSeconds(91.9),
                3334),
            TimeSpan.FromSeconds(90));

        Assert.Multiple(() =>
        {
            Assert.That(forecast.Verdict, Is.EqualTo(RepoContextDrainForecastVerdict.Exceeded));
            Assert.That(forecast.IsFailing, Is.True);
            Assert.That(forecast.ConsumedFraction, Is.GreaterThanOrEqualTo(1.0d));
        });
    }

    [Test]
    public void An_unproven_verdict_still_carries_the_per_activation_cost_the_truncated_drain_measured()
    {
        // The projection is the mechanism that reports growth while the container
        // runs, and it is the only live signal left once the startup verdict stops
        // claiming an overrun. Losing it here would trade #3305's false alarm for
        // silence, which is a worse outcome than the defect.
        var forecast = RepoContextDrainForecast.Evaluate(
            new RepoContextDrainObservation(
                Observed,
                RepoContextDrainOutcome.Abandoned,
                TimeSpan.FromSeconds(90),
                TimeSpan.FromSeconds(91.9),
                3334),
            TimeSpan.FromSeconds(180));

        Assert.That(forecast.PerActivationCost, Is.Not.Null);
        Assert.That(forecast.TryProject(8000, out var projection), Is.True);
        Assert.That(
            projection.ExceedsBudget,
            Is.True,
            "8000 activations at the cost this drain measured does not fit 180s, and the running poll "
            + "must still say so");
    }

    [Test]
    public void The_gate_run_two_measurement_yields_the_grant_that_would_actually_have_covered_it()
    {
        // The measurement from the gate run that motivated issue #2598: a 102.1s
        // drain against a 90s budget derived from an assumed 120s grant. The value
        // this reports is the point of the whole exercise - "raise the grace period"
        // without a number is how a value gets doubled and found wrong in the same
        // direction later.
        var forecast = RepoContextDrainForecast.Evaluate(
            Drain(RepoContextDrainOutcome.Abandoned, 102.1),
            Budget);

        Assert.That(forecast.RequiredStopGracePeriod, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(
                forecast.RequiredStopGracePeriod!.Value.TotalSeconds,
                Is.EqualTo(137d),
                "the required grant inverts the derivation and rounds up to the whole second it is "
                + "printed as, so 102.1/0.75 = 136.13 is reported as the 137 that actually covers it");
            Assert.That(
                RepoContextShutdownBudget.Derive(forecast.RequiredStopGracePeriod.Value),
                Is.GreaterThanOrEqualTo(TimeSpan.FromSeconds(102.1)),
                "the reported grant must actually derive a budget that covers the drain, or it is worthless advice");
            Assert.That(
                forecast.RequiredStopGracePeriod.Value,
                Is.GreaterThan(RepoContextShutdownBudget.DefaultStopGracePeriod),
                "declaring the value the derivation already assumed changes nothing, which is the trap #2598 names");
        });
    }

    [Test]
    public void A_drain_that_completed_but_exceeds_this_processs_budget_is_still_reported_as_exceeding()
    {
        // The case nothing else would report. A deployment that LOWERED its declared
        // grant since the last stop has a drain that fitted then and does not fit
        // now, and the recorded outcome says "Completed".
        var forecast = RepoContextDrainForecast.Evaluate(
            new RepoContextDrainObservation(
                Observed,
                RepoContextDrainOutcome.Completed,
                TimeSpan.FromSeconds(300),
                TimeSpan.FromSeconds(120),
                10_000),
            Budget);

        Assert.Multiple(() =>
        {
            Assert.That(forecast.Verdict, Is.EqualTo(RepoContextDrainForecastVerdict.Exceeded));
            Assert.That(forecast.RequiredStopGracePeriod, Is.Not.Null);
        });
    }

    [Test]
    public void A_drain_consuming_most_of_the_budget_is_reported_as_thin_rather_than_as_a_pass()
    {
        // The 67.2s-against-90s drain from issue #2397. It fits, so it is not the
        // failing case, but reporting it identically to a one-second drain is how
        // the headroom eroded unobserved in the first place.
        var forecast = RepoContextDrainForecast.Evaluate(
            Drain(RepoContextDrainOutcome.Completed, 67.2),
            Budget);

        Assert.Multiple(() =>
        {
            Assert.That(forecast.Verdict, Is.EqualTo(RepoContextDrainForecastVerdict.Thin));
            Assert.That(forecast.IsFailing, Is.False, "a thin drain is a lead indicator, not a fault");
            Assert.That(forecast.RequiredStopGracePeriod, Is.Null, "nothing needs raising yet");
        });
    }

    [Test]
    public void A_drain_with_real_headroom_is_reported_as_fitting()
    {
        // The positive control. Without it, a fixture asserting Thin would pass just
        // as happily against a forecast that reported Thin for every drain.
        var forecast = RepoContextDrainForecast.Evaluate(
            Drain(RepoContextDrainOutcome.Completed, 20),
            Budget);

        Assert.Multiple(() =>
        {
            Assert.That(forecast.Verdict, Is.EqualTo(RepoContextDrainForecastVerdict.Fits));
            Assert.That(forecast.IsFailing, Is.False);
        });
    }

    [Test]
    public void A_projection_multiplies_the_live_residency_by_the_cost_the_last_drain_measured()
    {
        // This is the member that answers the drain signal's standing remark - "drain
        // duration tracks the resident activation set, which nothing here bounds" -
        // by making that set's consequence visible while the container is running.
        var forecast = RepoContextDrainForecast.Evaluate(
            Drain(RepoContextDrainOutcome.Completed, 30, resident: 10_000),
            Budget);

        Assert.That(forecast.TryProject(20_000, out var projection), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(projection.ProjectedDrain.TotalSeconds, Is.EqualTo(60).Within(1e-6));
            Assert.That(projection.ResidentActivations, Is.EqualTo(20_000));
            Assert.That(projection.ExceedsBudget, Is.False);
            Assert.That(projection.ConsumedFraction, Is.EqualTo(60.0 / 90.0).Within(1e-9));
        });
    }

    [Test]
    public void A_projection_past_the_budget_reports_the_grant_that_would_cover_it()
    {
        var forecast = RepoContextDrainForecast.Evaluate(
            Drain(RepoContextDrainOutcome.Completed, 30, resident: 10_000),
            Budget);

        Assert.That(forecast.TryProject(40_000, out var projection), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(projection.ProjectedDrain.TotalSeconds, Is.EqualTo(120).Within(1e-6));
            Assert.That(projection.ExceedsBudget, Is.True, "120s does not fit a 90s budget");
            Assert.That(
                RepoContextShutdownBudget.Derive(projection.RequiredStopGracePeriod),
                Is.GreaterThanOrEqualTo(projection.ProjectedDrain));
        });
    }

    [Test]
    public void A_projection_that_fits_thinly_is_distinguishable_from_one_with_headroom()
    {
        var forecast = RepoContextDrainForecast.Evaluate(
            Drain(RepoContextDrainOutcome.Completed, 30, resident: 10_000),
            Budget);

        Assert.That(forecast.TryProject(25_000, out var thin), Is.True);
        Assert.That(forecast.TryProject(5_000, out var roomy), Is.True);

        Assert.Multiple(() =>
        {
            Assert.That(thin.IsThin, Is.True, "75s of a 90s budget is thin");
            Assert.That(thin.ExceedsBudget, Is.False);
            Assert.That(roomy.IsThin, Is.False);
        });
    }

    [Test]
    public void A_projection_is_refused_when_the_last_drain_recorded_no_residency_to_divide_by()
    {
        // Without a residency the recorded duration is a single number with no scale,
        // and multiplying a live count by a cost nobody measured would read as a
        // measurement while being none.
        var forecast = RepoContextDrainForecast.Evaluate(
            Drain(RepoContextDrainOutcome.Completed, 30, resident: null),
            Budget);

        Assert.Multiple(() =>
        {
            Assert.That(forecast.PerActivationCost, Is.Null);
            Assert.That(forecast.TryProject(10_000, out _), Is.False);
        });
    }

    [Test]
    public void A_negative_residency_is_refused_rather_than_projected()
    {
        var forecast = RepoContextDrainForecast.Evaluate(
            Drain(RepoContextDrainOutcome.Completed, 30),
            Budget);

        Assert.That(forecast.TryProject(-1, out _), Is.False);
    }
}
