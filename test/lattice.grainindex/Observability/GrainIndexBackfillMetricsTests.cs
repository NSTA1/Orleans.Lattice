using Orleans.Lattice.GrainIndex.Observability;
using Orleans.Lattice.GrainIndex.Tests.Backfill;

namespace Orleans.Lattice.GrainIndex.Tests.Observability;

/// <summary>
/// Covers what the backfill crawl publishes: the per-pass enrolled and failure
/// counters attributed to the backfill route, and the progress gauges an
/// OpenTelemetry subscriber reads.
/// </summary>
/// <remarks>
/// Every pass is invoked explicitly, exactly as the backfill's own unit tests
/// do: the harness leaves the background driver off, so nothing here waits on a
/// timer, a reminder, or wall-clock time. The instruments and the progress
/// registry are process-wide, so the fixture is not parallelizable and clears
/// the registry around each test.
/// </remarks>
[TestFixture]
[NonParallelizable]
public sealed class GrainIndexBackfillMetricsTests
{
    private const string Index = BackfillHarness.IndexName;

    [SetUp]
    public void ClearBefore() => GrainIndexBackfillProgressRegistry.Clear();

    [TearDown]
    public void ClearAfter() => GrainIndexBackfillProgressRegistry.Clear();

    [Test]
    public async Task A_pass_counts_the_grains_it_onboarded_against_the_backfill_route()
    {
        var harness = new BackfillHarness().WithKeys("a", "b", "c", "d").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();

        using var recorder = new InstrumentRecorder();
        await grain.RunBatchAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                recorder.Sum(
                    GrainIndexMetrics.GrainsEnrolledName,
                    GrainIndexMetrics.TagPath,
                    GrainIndexMetrics.PathBackfill),
                Is.EqualTo((double)harness.Options.BackfillBatchSize));
            Assert.That(
                recorder.For(GrainIndexMetrics.GrainsEnrolledName)[0]
                    .HasTag(GrainIndexMetrics.TagIndex, Index),
                Is.True);
        });
    }

    [Test]
    public async Task A_pass_that_onboarded_nothing_counts_nothing()
    {
        var harness = new BackfillHarness().WithKeys("a", "b").WithRegistryRecord();
        harness.WithEnrolled("a");
        harness.WithEnrolled("b");
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();

        using var recorder = new InstrumentRecorder();
        await grain.RunBatchAsync();

        Assert.That(recorder.For(GrainIndexMetrics.GrainsEnrolledName), Is.Empty,
            "A skipped grain is one the index already records; it is not an enrolment.");
    }

    [Test]
    public async Task A_grain_the_crawl_could_not_onboard_is_counted_as_a_write_failure()
    {
        var harness = new BackfillHarness().WithKeys("a", "b").WithRegistryRecord();
        harness.Activator.Failing.Add("a");
        harness.Activator.Failing.Add("b");
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();

        using var recorder = new InstrumentRecorder();
        await grain.RunBatchAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                recorder.Sum(
                    GrainIndexMetrics.WriteFailuresName,
                    GrainIndexMetrics.TagPath,
                    GrainIndexMetrics.PathBackfill),
                Is.EqualTo(2d));
            Assert.That(recorder.For(GrainIndexMetrics.GrainsEnrolledName), Is.Empty);
        });
    }

    [Test]
    public async Task Starting_a_crawl_publishes_its_state_and_position()
    {
        var harness = new BackfillHarness().WithKeys("a", "b", "c", "d").WithRegistryRecord();
        var grain = harness.CreateGrain();

        using var recorder = new InstrumentRecorder();
        await grain.EnsureStartedAsync();
        recorder.Collect();

        Assert.Multiple(() =>
        {
            Assert.That(
                recorder.Latest(GrainIndexMetrics.BackfillStateName, Index)!.Value,
                Is.EqualTo((double)(int)GrainIndexBackfillState.Running));
            Assert.That(recorder.Latest(GrainIndexMetrics.BackfillProcessedName, Index)!.Value, Is.Zero);
        });
    }

    [Test]
    public async Task Each_pass_advances_the_published_progress()
    {
        var harness = new BackfillHarness().WithKeys("a", "b", "c", "d").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();

        using var recorder = new InstrumentRecorder();

        await grain.RunBatchAsync();
        recorder.Collect();
        var afterFirst = recorder.Latest(GrainIndexMetrics.BackfillProcessedName, Index)!.Value;

        recorder.Reset();
        await grain.RunBatchAsync();
        recorder.Collect();
        var afterSecond = recorder.Latest(GrainIndexMetrics.BackfillProcessedName, Index)!.Value;

        Assert.Multiple(() =>
        {
            Assert.That(afterFirst, Is.EqualTo(2d));
            Assert.That(afterSecond, Is.EqualTo(4d));
        });
    }

    [Test]
    public async Task A_key_source_that_bounds_its_population_publishes_a_total_and_a_percentage()
    {
        var harness = new BackfillHarness().WithKeys("a", "b", "c", "d").WithRegistryRecord();
        harness.KeySource!.ApproximateCount = 4;
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();

        using var recorder = new InstrumentRecorder();
        await grain.RunBatchAsync();
        recorder.Collect();

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Latest(GrainIndexMetrics.BackfillTotalName, Index)!.Value, Is.EqualTo(4d));
            Assert.That(
                recorder.Latest(GrainIndexMetrics.BackfillPercentCompleteName, Index)!.Value,
                Is.EqualTo(50d));
        });
    }

    [Test]
    public async Task A_key_source_with_no_bound_publishes_progress_but_no_percentage()
    {
        var harness = new BackfillHarness().WithKeys("a", "b", "c", "d").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();

        using var recorder = new InstrumentRecorder();
        await grain.RunBatchAsync();
        recorder.Collect();

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Latest(GrainIndexMetrics.BackfillProcessedName, Index)!.Value, Is.EqualTo(2d));
            Assert.That(recorder.For(GrainIndexMetrics.BackfillTotalName), Is.Empty);
            Assert.That(recorder.For(GrainIndexMetrics.BackfillPercentCompleteName), Is.Empty);
        });
    }

    [Test]
    public async Task A_paused_crawl_publishes_the_paused_state()
    {
        var harness = new BackfillHarness().WithKeys("a", "b", "c", "d").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();

        using var recorder = new InstrumentRecorder();
        await grain.PauseAsync();
        recorder.Collect();

        Assert.That(
            recorder.Latest(GrainIndexMetrics.BackfillStateName, Index)!.Value,
            Is.EqualTo((double)(int)GrainIndexBackfillState.Paused));
    }

    [Test]
    public async Task A_completed_crawl_publishes_a_hundred_percent_even_with_no_bound()
    {
        var harness = new BackfillHarness().WithKeys("a").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();

        using var recorder = new InstrumentRecorder();

        // Batch size is 2, so a one-key population is exhausted by the first pass.
        await grain.RunBatchAsync();
        recorder.Collect();

        Assert.Multiple(() =>
        {
            Assert.That(
                recorder.Latest(GrainIndexMetrics.BackfillStateName, Index)!.Value,
                Is.EqualTo((double)(int)GrainIndexBackfillState.Completed));
            Assert.That(
                recorder.Latest(GrainIndexMetrics.BackfillPercentCompleteName, Index)!.Value,
                Is.EqualTo(100d));
        });
    }

    [Test]
    public async Task Deactivating_stops_this_silo_reporting_for_the_index()
    {
        var harness = new BackfillHarness().WithKeys("a", "b", "c", "d").WithRegistryRecord();
        var grain = harness.CreateGrain();
        await grain.EnsureStartedAsync();

        using var recorder = new InstrumentRecorder();
        await grain.OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            CancellationToken.None);
        recorder.Collect();

        Assert.That(recorder.For(GrainIndexMetrics.BackfillStateName), Is.Empty,
            "A frozen sample left behind by a silo that no longer hosts the crawl would read as live progress.");
    }
}
