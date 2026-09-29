namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// The window a request gets when it names no start, and the deployment guardrail's
/// disagreements with the entry's own bounds. Both sit behind checks that the curated
/// catalogue's entries never trip, so they are driven against purpose-built entries.
/// </summary>
/// <remarks>
/// Two arms of the host-guardrail violation mapper are deliberately not driven here,
/// because nothing can reach them. It re-checks, in order, that the window ascends and
/// that the step is positive, mirroring the check order of the deployment guardrail it
/// reports for. Both conditions are already settled by the time it runs: the entry's own
/// bounds reject a descending window before the guardrail is consulted, and the catalogue
/// compiler refuses to register a range entry whose default step is not positive, so the
/// resolved step of a range query is always positive. They are defence in depth, kept so
/// the mapper's order matches the guardrail's, and a test that reached them would have to
/// change production behaviour to do it.
/// </remarks>
public sealed partial class LatticeTelemetryTests
{
    private static TelemetryQueryDefinition RangeEntry(string queryId, TelemetryQueryBounds bounds) => new()
    {
        Descriptor = new TelemetryQueryDescriptor
        {
            QueryId = queryId,
            Title = queryId,
            Description = "test entry",
            Unit = "1",
            Kind = TelemetryQueryKind.Range,
            Semantic = TelemetryMeasurementSemantic.Level,
            Parameters = TelemetryQueryParameters.TimeRange | TelemetryQueryParameters.Step,
            Bounds = bounds,
            Instruments = [new TelemetryInstrumentReference(
                "metric.alpha", "orleans.lattice", "1", TelemetryMeasurementSemantic.Level)],
        },
        QueryTemplate = "sum(metric_alpha{$scope$})",
    };

    /// <summary>A request naming only the query id, so the facade must choose the whole window.</summary>
    private static TelemetryQueryRequest Unwindowed(string queryId) => new() { QueryId = queryId };

    [Test]
    public async Task An_entry_declaring_no_usable_point_budget_still_serves_a_bounded_default_window()
    {
        // The default window is normally the entry's point budget at its own step. An entry
        // whose budget cannot produce one - no budget, or a budget of a single point - has no
        // such window, and must still be served a bounded one rather than an open-ended scan
        // back to the epoch.
        var harness = new TelemetryFacadeHarness()
            .WithDefinitions(RangeEntry("defaults.no_budget", new TelemetryQueryBounds
            {
                MinStep = TimeSpan.FromMinutes(1),
                DefaultStep = TimeSpan.FromMinutes(1),
            }));

        var response = await harness.Build().QueryAsync(Unwindowed("defaults.no_budget"));

        Assert.Multiple(() =>
        {
            Assert.That(response.Range.EndUtc, Is.EqualTo(FixedTimeProvider.Instant));
            Assert.That(response.Range.Duration, Is.EqualTo(TimeSpan.FromHours(1)),
                "an entry with no usable point budget falls back to the fixed default span");
            Assert.That(harness.Backend.LastWasRange, Is.True);
        });
    }

    [Test]
    public async Task A_default_window_is_derived_from_the_point_budget_when_the_entry_declares_one()
    {
        // Anti-vacuity for the case above: an entry that CAN express a window from its budget
        // must get that window, so a facade that always served the fixed fallback would fail
        // here while passing every other bounds test.
        var harness = new TelemetryFacadeHarness()
            .WithDefinitions(RangeEntry("defaults.budgeted", new TelemetryQueryBounds
            {
                MinStep = TimeSpan.FromMinutes(1),
                DefaultStep = TimeSpan.FromMinutes(1),
                MaxPoints = 11,
            }));

        var response = await harness.Build().QueryAsync(Unwindowed("defaults.budgeted"));

        Assert.That(response.Range.Duration, Is.EqualTo(TimeSpan.FromMinutes(10)),
            "ten one-minute steps yield the eleven points the budget permits");
    }

    [Test]
    public async Task A_default_window_never_exceeds_the_entrys_own_maximum_range()
    {
        // The default has to satisfy every bound the request is then validated against,
        // including the entry's maximum range - otherwise the most natural request a binding
        // can make (a query id and nothing else) would be rejected by the bounds that
        // produced it.
        var harness = new TelemetryFacadeHarness()
            .WithDefinitions(RangeEntry("defaults.capped", new TelemetryQueryBounds
            {
                MinStep = TimeSpan.FromMinutes(1),
                DefaultStep = TimeSpan.FromMinutes(1),
                MaxRange = TimeSpan.FromMinutes(10),
            }));

        var response = await harness.Build().QueryAsync(Unwindowed("defaults.capped"));

        Assert.That(response.Range.Duration, Is.EqualTo(TimeSpan.FromMinutes(10)),
            "the fallback span is clamped to the entry's maximum range rather than rejected by it");
    }

    [Test]
    public void A_cancelled_query_surfaces_the_cancellation_rather_than_a_backend_fault()
    {
        // Every other backend fault is wrapped as a TelemetryBackendException that names the
        // query. A cancellation the caller asked for is not a backend fault, and wrapping it
        // would make a caller's own cancellation indistinguishable from the backend failing.
        var harness = new TelemetryFacadeHarness();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        harness.Backend.Fault = new OperationCanceledException(cts.Token);

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await harness.Build().QueryAsync(
                    TelemetryFacadeHarness.RangeRequest(ReadRate), cts.Token),
                Throws.InstanceOf<OperationCanceledException>()
                    .And.Not.InstanceOf<TelemetryBackendException>());

            // Anti-vacuity: the same fault on an uncancelled request IS a backend fault, so
            // the discrimination is on the token and not merely on the exception type.
            var uncancelled = new TelemetryFacadeHarness();
            uncancelled.Backend.Fault = new OperationCanceledException();
            Assert.That(
                async () => await uncancelled.Build().QueryAsync(TelemetryFacadeHarness.RangeRequest(ReadRate)),
                Throws.TypeOf<TelemetryBackendException>());
        });
    }
}
