namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// The bounded-parameter half of the facade's behaviour: the resolution step is
/// clamped into the entry's declared budget, and a window outside the entry's
/// bounds or the deployment-wide guardrails is rejected rather than silently
/// narrowed.
/// </summary>
public sealed partial class LatticeTelemetryTests
{
    private static TelemetryQueryRequest Windowed(TimeSpan span, TimeSpan step) => new()
    {
        QueryId = ReadRate,
        Range = TelemetryTimeRange.Between(
            FixedTimeProvider.Instant - span, FixedTimeProvider.Instant, step),
    };

    [Test]
    public async Task QueryAsync_clamps_a_step_below_the_entrys_minimum()
    {
        var harness = new TelemetryFacadeHarness();

        var response = await harness.Build().QueryAsync(Windowed(TimeSpan.FromHours(1), TimeSpan.FromSeconds(1)));

        Assert.Multiple(() =>
        {
            Assert.That(response.Range.Step, Is.EqualTo(TimeSpan.FromSeconds(15)),
                "A step finer than the entry permits clamps up to the finest it permits.");
            Assert.That(harness.Backend.LastStep, Is.EqualTo(TimeSpan.FromSeconds(15)));
        });
    }

    [Test]
    public async Task QueryAsync_clamps_a_step_above_the_entrys_maximum()
    {
        var harness = new TelemetryFacadeHarness()
            .WithOptions(options =>
            {
                options.MaxRange = TimeSpan.FromDays(3);
                options.MaxStep = TimeSpan.FromHours(6);
            });

        var response = await harness.Build().QueryAsync(Windowed(TimeSpan.FromDays(2), TimeSpan.FromHours(4)));

        Assert.That(response.Range.Step, Is.EqualTo(TimeSpan.FromHours(1)),
            "A step coarser than the entry permits clamps down to the coarsest it permits.");
    }

    [Test]
    public async Task QueryAsync_applies_the_entrys_default_step_when_the_caller_supplies_none()
    {
        var harness = new TelemetryFacadeHarness();

        var response = await harness.Build().QueryAsync(Windowed(TimeSpan.FromHours(1), TimeSpan.Zero));

        Assert.That(response.Range.Step, Is.EqualTo(TimeSpan.FromSeconds(60)));
    }

    [Test]
    public async Task QueryAsync_passes_an_in_range_step_through_unchanged()
    {
        var harness = new TelemetryFacadeHarness();

        var response = await harness.Build().QueryAsync(Windowed(TimeSpan.FromHours(1), TimeSpan.FromMinutes(5)));

        Assert.That(response.Range.Step, Is.EqualTo(TimeSpan.FromMinutes(5)));
    }

    [Test]
    public async Task The_rate_window_tracks_the_clamped_step()
    {
        var harness = new TelemetryFacadeHarness();

        await harness.Build().QueryAsync(Windowed(TimeSpan.FromDays(1), TimeSpan.FromMinutes(15)));

        Assert.That(harness.Backend.SingleQuery, Does.Contain("[1h]"),
            "A hard-coded rate window would report a fraction of each step at a coarse resolution.");
    }

    [Test]
    public void QueryAsync_rejects_a_window_longer_than_the_entry_permits()
    {
        var harness = new TelemetryFacadeHarness();

        Assert.That(
            async () => await harness.Build().QueryAsync(Windowed(TimeSpan.FromDays(10), TimeSpan.FromHours(1))),
            Throws.TypeOf<TelemetryQueryBoundsException>()
                .With.Property(nameof(TelemetryQueryBoundsException.Violation))
                .EqualTo(TelemetryBoundsViolation.RangeTooLong));
    }

    [Test]
    public void QueryAsync_rejects_a_descending_window()
    {
        var harness = new TelemetryFacadeHarness();
        var request = new TelemetryQueryRequest
        {
            QueryId = ReadRate,
            Range = TelemetryTimeRange.Between(
                FixedTimeProvider.Instant,
                FixedTimeProvider.Instant.AddHours(-1),
                TimeSpan.FromMinutes(1)),
        };

        Assert.That(
            async () => await harness.Build().QueryAsync(request),
            Throws.TypeOf<TelemetryQueryBoundsException>()
                .With.Property(nameof(TelemetryQueryBoundsException.Violation))
                .EqualTo(TelemetryBoundsViolation.RangeNotAscending),
            "A descending window is rejected outright rather than normalised, so a caller never "
            + "silently gets a window it did not ask for.");
    }

    [Test]
    public void QueryAsync_rejects_a_window_reaching_further_back_than_the_entrys_lookback()
    {
        var harness = new TelemetryFacadeHarness();
        var request = new TelemetryQueryRequest
        {
            QueryId = StorageBytes,
            Range = TelemetryTimeRange.At(FixedTimeProvider.Instant.AddDays(-60)),
        };

        Assert.That(
            async () => await harness.Build().QueryAsync(request),
            Throws.TypeOf<TelemetryQueryBoundsException>()
                .With.Property(nameof(TelemetryQueryBoundsException.Violation))
                .EqualTo(TelemetryBoundsViolation.LookbackTooOld));
    }

    [Test]
    public void QueryAsync_rejects_a_window_and_step_yielding_more_points_than_the_budget()
    {
        var harness = new TelemetryFacadeHarness();

        // Seven days at the entry's fifteen-second floor is far beyond its point
        // budget, so the request is refused rather than quietly coarsened.
        Assert.That(
            async () => await harness.Build().QueryAsync(
                Windowed(TimeSpan.FromDays(7), TimeSpan.FromSeconds(15))),
            Throws.TypeOf<TelemetryQueryBoundsException>()
                .With.Property(nameof(TelemetryQueryBoundsException.Violation))
                .EqualTo(TelemetryBoundsViolation.TooManyPoints));
    }

    [Test]
    public void QueryAsync_rejects_a_window_beyond_the_deployment_guardrail()
    {
        // The entry permits seven days, but the host caps every range query at one
        // hour, so the deployment guardrail is what refuses this.
        var harness = new TelemetryFacadeHarness()
            .WithOptions(options => options.MaxRange = TimeSpan.FromHours(1));

        Assert.That(
            async () => await harness.Build().QueryAsync(Windowed(TimeSpan.FromHours(6), TimeSpan.FromMinutes(5))),
            Throws.TypeOf<TelemetryQueryBoundsException>()
                .With.Property(nameof(TelemetryQueryBoundsException.Violation))
                .EqualTo(TelemetryBoundsViolation.RangeTooLong));
    }

    [Test]
    public void QueryAsync_rejects_a_step_beyond_the_deployment_guardrail()
    {
        var harness = new TelemetryFacadeHarness()
            .WithOptions(options => options.MaxStep = TimeSpan.FromMinutes(1));

        Assert.That(
            async () => await harness.Build().QueryAsync(Windowed(TimeSpan.FromHours(2), TimeSpan.FromMinutes(30))),
            Throws.TypeOf<TelemetryQueryBoundsException>()
                .With.Property(nameof(TelemetryQueryBoundsException.Violation))
                .EqualTo(TelemetryBoundsViolation.StepAboveMaximum));
    }

    [Test]
    public void A_rejected_request_never_reaches_the_backend()
    {
        var harness = new TelemetryFacadeHarness();

        Assert.That(
            async () => await harness.Build().QueryAsync(Windowed(TimeSpan.FromDays(10), TimeSpan.FromHours(1))),
            Throws.TypeOf<TelemetryQueryBoundsException>());
        Assert.That(harness.Backend.Queries, Is.Empty,
            "Bounds must be enforced before any work is dispatched, never after.");
    }

    [Test]
    public async Task An_instant_entry_ignores_a_supplied_start_and_step()
    {
        var harness = new TelemetryFacadeHarness();
        var request = new TelemetryQueryRequest
        {
            QueryId = StorageBytes,
            Range = TelemetryTimeRange.Between(
                FixedTimeProvider.Instant.AddDays(-1),
                FixedTimeProvider.Instant,
                TimeSpan.FromSeconds(1)),
        };

        var response = await harness.Build().QueryAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(response.Range.IsInstant, Is.True,
                "An instant entry evaluates at one instant, so a caller cannot smuggle a range past "
                + "its point budget.");
            Assert.That(response.Range.EndUtc, Is.EqualTo(FixedTimeProvider.Instant));
            Assert.That(harness.Backend.LastWasRange, Is.False);
        });
    }

    [Test]
    public async Task An_unset_window_defaults_to_a_window_ending_now()
    {
        var harness = new TelemetryFacadeHarness();
        var request = new TelemetryQueryRequest { QueryId = StorageBytes };

        var response = await harness.Build().QueryAsync(request);

        Assert.That(response.Range.EndUtc, Is.EqualTo(FixedTimeProvider.Instant));
    }

    [Test]
    public async Task Every_range_entry_serves_a_request_that_supplies_no_window_at_all()
    {
        // The most natural call a binding can make is a query id and nothing else.
        // Defaulting the window to the entry's maximum range would blow the entry's
        // own point budget at its default step, so the default must be consistent
        // with the bounds that then validate it.
        foreach (var descriptor in RangeDescriptors())
        {
            var harness = new TelemetryFacadeHarness();
            var request = new TelemetryQueryRequest { QueryId = descriptor.QueryId };

            var response = await harness.Build().QueryAsync(request);

            Assert.Multiple(() =>
            {
                Assert.That(response.Range.EndUtc, Is.EqualTo(FixedTimeProvider.Instant),
                    descriptor.QueryId);
                Assert.That(response.Range.Duration, Is.GreaterThan(TimeSpan.Zero),
                    $"'{descriptor.QueryId}' defaulted to an empty window.");
                Assert.That(
                    descriptor.Bounds.Validate(response.Range, FixedTimeProvider.Instant),
                    Is.EqualTo(TelemetryBoundsViolation.None),
                    $"'{descriptor.QueryId}' defaulted to a window its own bounds reject.");
            });
        }
    }

    [Test]
    public async Task A_defaulted_window_stays_inside_the_entrys_point_budget()
    {
        foreach (var descriptor in RangeDescriptors())
        {
            var response = await new TelemetryFacadeHarness()
                .Build()
                .QueryAsync(new TelemetryQueryRequest { QueryId = descriptor.QueryId });

            Assert.That(response.Range.PointCount,
                Is.LessThanOrEqualTo(descriptor.Bounds.MaxPoints),
                $"'{descriptor.QueryId}' defaulted past its own point budget.");
        }
    }

    [Test]
    public async Task A_defaulted_window_stays_inside_the_deployment_range_guardrail()
    {
        var harness = new TelemetryFacadeHarness()
            .WithOptions(options => options.MaxRange = TimeSpan.FromHours(2));

        var response = await harness.Build().QueryAsync(new TelemetryQueryRequest { QueryId = ReadRate });

        Assert.Multiple(() =>
        {
            Assert.That(response.Range.Duration, Is.LessThanOrEqualTo(TimeSpan.FromHours(2)),
                "A host that caps every range query must also cap the window a defaulted request "
                + "resolves to, or the default would be rejected by the host's own guardrail.");
            Assert.That(harness.Backend.Queries, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task A_request_supplying_only_an_end_instant_is_served()
    {
        var harness = new TelemetryFacadeHarness();
        var request = new TelemetryQueryRequest
        {
            QueryId = ReadRate,
            Range = new TelemetryTimeRange { EndUtc = FixedTimeProvider.Instant.AddHours(-3) },
        };

        var response = await harness.Build().QueryAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(response.Range.EndUtc, Is.EqualTo(FixedTimeProvider.Instant.AddHours(-3)));
            Assert.That(response.Range.Duration, Is.GreaterThan(TimeSpan.Zero));
        });
    }

    [Test]
    public async Task A_defaulted_window_honours_the_callers_requested_step()
    {
        var harness = new TelemetryFacadeHarness();
        var request = new TelemetryQueryRequest
        {
            QueryId = ReadRate,
            Range = new TelemetryTimeRange { Step = TimeSpan.FromMinutes(5) },
        };

        var response = await harness.Build().QueryAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(response.Range.Step, Is.EqualTo(TimeSpan.FromMinutes(5)));
            Assert.That(response.Range.PointCount, Is.LessThanOrEqualTo(1500));
        });
    }

    private static IEnumerable<TelemetryQueryDescriptor> RangeDescriptors() =>
        LatticeTelemetryQueries.Definitions
            .Select(d => d.Descriptor)
            .Where(d => d.Kind == TelemetryQueryKind.Range);

    [Test]
    public async Task The_echoed_range_reports_the_step_that_was_actually_evaluated()
    {
        var harness = new TelemetryFacadeHarness();

        var response = await harness.Build().QueryAsync(Windowed(TimeSpan.FromHours(1), TimeSpan.FromSeconds(1)));

        Assert.That(response.Range.Step, Is.EqualTo(harness.Backend.LastStep),
            "A client must render the axis it really received rather than the one it asked for.");
    }
}
