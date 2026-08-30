using Orleans.Lattice.Explorer.Plugins.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// The bounded control choices: that they are filtered by the server's own
/// published limits and by nothing the client invented, and that a value the
/// control never rendered cannot become a selection.
/// </summary>
[TestFixture]
public sealed class TelemetryDurationChoicesTests
{
    [Test]
    public void An_unbounded_entry_offers_the_whole_range_ladder_as_one_shared_instance()
    {
        var first = TelemetryDurationChoices.RangesFor(ExplorerTelemetryBounds.Unbounded);
        var second = TelemetryDurationChoices.RangesFor(ExplorerTelemetryBounds.Unbounded);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Not.Empty);
            Assert.That(
                first,
                Is.SameAs(second),
                "the unbounded case must not rebuild the ladder, which is the common case on every render");
        });
    }

    [Test]
    public void A_max_range_removes_every_longer_candidate()
    {
        var bounds = ExplorerTelemetrySample.Bounds(maxRange: TimeSpan.FromHours(1));

        var ranges = TelemetryDurationChoices.RangesFor(bounds);

        Assert.That(
            ranges.All(choice => choice.Duration <= TimeSpan.FromHours(1)),
            Is.True,
            $"offered {string.Join(", ", ranges.Select(c => c.Label))}");
    }

    [Test]
    public void A_max_lookback_shorter_than_the_max_range_is_the_binding_limit()
    {
        var bounds = ExplorerTelemetrySample.Bounds(
            maxRange: TimeSpan.FromDays(7),
            maxLookback: TimeSpan.FromMinutes(30));

        var ranges = TelemetryDurationChoices.RangesFor(bounds);

        Assert.That(ranges.All(choice => choice.Duration <= TimeSpan.FromMinutes(30)), Is.True);
    }

    [Test]
    public void A_point_budget_removes_a_range_that_would_overrun_it_at_the_chosen_step()
    {
        // 60 points at a one-minute step is an hour; the budget admits an hour
        // and refuses everything longer, even though MaxRange alone would not.
        var bounds = ExplorerTelemetrySample.Bounds(
            minStep: TimeSpan.FromSeconds(15),
            maxRange: TimeSpan.FromDays(7),
            maxLookback: TimeSpan.FromDays(7),
            maxPoints: 61);

        var ranges = TelemetryDurationChoices.RangesFor(bounds, TimeSpan.FromMinutes(1));

        Assert.That(ranges.All(choice => choice.Duration <= TimeSpan.FromHours(1)), Is.True);
    }

    [Test]
    public void A_finer_step_admits_fewer_ranges_than_a_coarser_one()
    {
        var bounds = ExplorerTelemetrySample.Bounds(
            minStep: TimeSpan.FromSeconds(15),
            maxRange: TimeSpan.FromDays(7),
            maxLookback: TimeSpan.FromDays(7),
            maxPoints: 240);

        var fine = TelemetryDurationChoices.RangesFor(bounds, TimeSpan.FromSeconds(15));
        var coarse = TelemetryDurationChoices.RangesFor(bounds, TimeSpan.FromMinutes(5));

        Assert.That(fine.Count, Is.LessThan(coarse.Count));
    }

    [Test]
    public void Bounds_that_admit_no_candidate_offer_nothing_rather_than_a_default()
    {
        var bounds = ExplorerTelemetrySample.Bounds(maxRange: TimeSpan.FromSeconds(1));

        Assert.That(TelemetryDurationChoices.RangesFor(bounds), Is.Empty);
    }

    [Test]
    public void A_step_range_removes_candidates_on_both_sides()
    {
        var bounds = ExplorerTelemetrySample.Bounds(
            minStep: TimeSpan.FromMinutes(1),
            maxStep: TimeSpan.FromMinutes(15));

        var steps = TelemetryDurationChoices.StepsFor(bounds);

        Assert.Multiple(() =>
        {
            Assert.That(steps, Is.Not.Empty);
            Assert.That(steps.All(choice =>
                choice.Duration >= TimeSpan.FromMinutes(1)
                && choice.Duration <= TimeSpan.FromMinutes(15)), Is.True);
        });
    }

    [Test]
    public void An_entry_declaring_no_step_range_offers_the_whole_step_ladder_as_one_shared_instance()
    {
        var bounds = new ExplorerTelemetryBounds(
            TimeSpan.Zero,
            TimeSpan.Zero,
            TimeSpan.FromMinutes(1),
            TimeSpan.FromHours(1),
            TimeSpan.FromDays(1),
            MaxPoints: 0);

        Assert.That(
            TelemetryDurationChoices.StepsFor(bounds),
            Is.SameAs(TelemetryDurationChoices.StepsFor(bounds)));
    }

    [Test]
    public void Resolving_a_label_that_was_offered_yields_its_duration()
    {
        var choices = TelemetryDurationChoices.RangesFor(ExplorerTelemetryBounds.Unbounded);

        var resolved = TelemetryDurationChoices.TryResolve(choices, "1h", out var choice);

        Assert.Multiple(() =>
        {
            Assert.That(resolved, Is.True);
            Assert.That(choice.Duration, Is.EqualTo(TimeSpan.FromHours(1)));
        });
    }

    [Test]
    public void Resolving_a_label_that_was_never_offered_yields_nothing()
    {
        // The control only ever renders labels from the offered list, so a value
        // arriving that is not in it was not produced by the control - and a
        // window the entry never admitted must not be reachable by editing one
        // into the DOM.
        var choices = TelemetryDurationChoices.RangesFor(
            ExplorerTelemetrySample.Bounds(maxRange: TimeSpan.FromMinutes(15)));

        Assert.That(TelemetryDurationChoices.TryResolve(choices, "7d", out _), Is.False);
    }

    [Test]
    public void Resolving_a_null_or_empty_label_yields_nothing() =>
        Assert.Multiple(() =>
        {
            var choices = TelemetryDurationChoices.RangesFor(ExplorerTelemetryBounds.Unbounded);
            Assert.That(TelemetryDurationChoices.TryResolve(choices, null, out _), Is.False);
            Assert.That(TelemetryDurationChoices.TryResolve(choices, string.Empty, out _), Is.False);
        });

    [Test]
    public void Resolving_against_a_null_list_is_rejected() =>
        Assert.That(
            () => TelemetryDurationChoices.TryResolve(null!, "1h", out _),
            Throws.ArgumentNullException);

    [Test]
    public void An_unset_duration_labels_as_the_server_default() =>
        Assert.That(
            TelemetryDurationChoices.LabelFor(
                TelemetryDurationChoices.RangesFor(ExplorerTelemetryBounds.Unbounded),
                TimeSpan.Zero),
            Is.EqualTo(TelemetryDurationChoices.ServerDefaultLabel));

    [Test]
    public void A_chosen_duration_labels_as_the_choice_that_carries_it() =>
        Assert.That(
            TelemetryDurationChoices.LabelFor(
                TelemetryDurationChoices.RangesFor(ExplorerTelemetryBounds.Unbounded),
                TimeSpan.FromHours(3)),
            Is.EqualTo("3h"));

    [Test]
    public void A_duration_no_longer_offered_labels_as_the_server_default()
    {
        // What a control shows when a selection made against another entry's
        // bounds has just been dropped.
        var narrow = TelemetryDurationChoices.RangesFor(
            ExplorerTelemetrySample.Bounds(maxRange: TimeSpan.FromMinutes(15)));

        Assert.That(
            TelemetryDurationChoices.LabelFor(narrow, TimeSpan.FromDays(7)),
            Is.EqualTo(TelemetryDurationChoices.ServerDefaultLabel));
    }

    [Test]
    public void An_unset_duration_is_always_offered_because_the_server_default_is_always_legal() =>
        Assert.That(TelemetryDurationChoices.IsOffered([], TimeSpan.Zero), Is.True);

    [Test]
    public void A_duration_outside_the_offered_list_is_not_offered() =>
        Assert.That(
            TelemetryDurationChoices.IsOffered(
                TelemetryDurationChoices.RangesFor(
                    ExplorerTelemetrySample.Bounds(maxRange: TimeSpan.FromMinutes(15))),
                TimeSpan.FromDays(7)),
            Is.False);

    [Test]
    public void Asking_whether_a_null_list_offers_something_is_rejected() =>
        Assert.Multiple(() =>
        {
            Assert.That(
                () => TelemetryDurationChoices.IsOffered(null!, TimeSpan.FromHours(1)),
                Throws.ArgumentNullException);
            Assert.That(
                () => TelemetryDurationChoices.LabelFor(null!, TimeSpan.FromHours(1)),
                Throws.ArgumentNullException);
        });

    [Test]
    public void Every_offered_label_is_distinct_so_it_can_serve_as_the_option_value()
    {
        var ranges = TelemetryDurationChoices.RangesFor(ExplorerTelemetryBounds.Unbounded);
        var steps = TelemetryDurationChoices.StepsFor(ExplorerTelemetryBounds.Unbounded);

        Assert.Multiple(() =>
        {
            Assert.That(ranges.Select(c => c.Label).Distinct(StringComparer.Ordinal).Count(), Is.EqualTo(ranges.Count));
            Assert.That(steps.Select(c => c.Label).Distinct(StringComparer.Ordinal).Count(), Is.EqualTo(steps.Count));
            Assert.That(
                ranges.Select(c => c.Label),
                Does.Not.Contain(TelemetryDurationChoices.ServerDefaultLabel),
                "the server-default option is rendered separately and must not collide with a ladder label");
        });
    }

    [Test]
    public void Both_ladders_are_ascending_so_a_control_reads_in_order()
    {
        var ranges = TelemetryDurationChoices.RangesFor(ExplorerTelemetryBounds.Unbounded);
        var steps = TelemetryDurationChoices.StepsFor(ExplorerTelemetryBounds.Unbounded);

        Assert.Multiple(() =>
        {
            Assert.That(ranges.Select(c => c.Duration), Is.Ordered.Ascending);
            Assert.That(steps.Select(c => c.Duration), Is.Ordered.Ascending);
        });
    }
}
