using Orleans.Lattice.GrainIndex.Observability;

namespace Orleans.Lattice.GrainIndex.Tests.Observability;

/// <summary>
/// Covers <c>GrainIndexBackfillProgressRegistry</c>: the percent-complete rule,
/// and the fact that publishing moves the four observable gauges an
/// OpenTelemetry subscriber reads.
/// </summary>
/// <remarks>
/// The registry backs process-wide gauges, so the fixture is not parallelizable
/// and clears the registry either side of every test.
/// </remarks>
[TestFixture]
[NonParallelizable]
public sealed class GrainIndexBackfillProgressRegistryTests
{
    private const string Index = "progress-tests";

    [SetUp]
    public void ClearBefore() => GrainIndexBackfillProgressRegistry.Clear();

    [TearDown]
    public void ClearAfter() => GrainIndexBackfillProgressRegistry.Clear();

    [Test]
    public void Publishing_rejects_a_null_status() =>
        Assert.That(
            () => GrainIndexBackfillProgressRegistry.Publish(null!, 10),
            Throws.ArgumentNullException);

    [Test]
    public void Removing_rejects_a_null_index_name() =>
        Assert.That(
            () => GrainIndexBackfillProgressRegistry.Remove(null!),
            Throws.ArgumentNullException);

    [Test]
    public void Removing_an_index_that_was_never_published_is_a_no_op() =>
        Assert.That(
            () => GrainIndexBackfillProgressRegistry.Remove("never-published"),
            Throws.Nothing);

    [Test]
    public void Nothing_published_means_no_measurements()
    {
        using var recorder = new InstrumentRecorder();

        recorder.Collect();

        Assert.Multiple(() =>
        {
            Assert.That(recorder.For(GrainIndexMetrics.BackfillProcessedName), Is.Empty);
            Assert.That(recorder.For(GrainIndexMetrics.BackfillTotalName), Is.Empty);
            Assert.That(recorder.For(GrainIndexMetrics.BackfillPercentCompleteName), Is.Empty);
            Assert.That(recorder.For(GrainIndexMetrics.BackfillStateName), Is.Empty);
        });
    }

    [Test]
    public void A_published_crawl_reports_processed_total_percent_and_state()
    {
        using var recorder = new InstrumentRecorder();

        GrainIndexBackfillProgressRegistry.Publish(
            Status(GrainIndexBackfillState.Running, visited: 25),
            total: 100);

        recorder.Collect();

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Latest(GrainIndexMetrics.BackfillProcessedName, Index)!.Value, Is.EqualTo(25d));
            Assert.That(recorder.Latest(GrainIndexMetrics.BackfillTotalName, Index)!.Value, Is.EqualTo(100d));
            Assert.That(
                recorder.Latest(GrainIndexMetrics.BackfillPercentCompleteName, Index)!.Value,
                Is.EqualTo(25d));
            Assert.That(
                recorder.Latest(GrainIndexMetrics.BackfillStateName, Index)!.Value,
                Is.EqualTo((double)(int)GrainIndexBackfillState.Running));
        });
    }

    [Test]
    public void A_crawl_with_no_bound_reports_progress_but_neither_total_nor_percent()
    {
        using var recorder = new InstrumentRecorder();

        GrainIndexBackfillProgressRegistry.Publish(
            Status(GrainIndexBackfillState.Running, visited: 7),
            total: null);

        recorder.Collect();

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Latest(GrainIndexMetrics.BackfillProcessedName, Index)!.Value, Is.EqualTo(7d));
            Assert.That(recorder.For(GrainIndexMetrics.BackfillTotalName), Is.Empty);
            Assert.That(recorder.For(GrainIndexMetrics.BackfillPercentCompleteName), Is.Empty);
        });
    }

    [Test]
    public void Republishing_replaces_the_previous_sample_rather_than_adding_one()
    {
        using var recorder = new InstrumentRecorder();

        GrainIndexBackfillProgressRegistry.Publish(Status(GrainIndexBackfillState.Running, 10), 100);
        GrainIndexBackfillProgressRegistry.Publish(Status(GrainIndexBackfillState.Running, 40), 100);

        recorder.Collect();

        var processed = recorder.For(GrainIndexMetrics.BackfillProcessedName);

        Assert.Multiple(() =>
        {
            Assert.That(processed, Has.Count.EqualTo(1));
            Assert.That(processed[0].Value, Is.EqualTo(40d));
        });
    }

    [Test]
    public void Two_indexes_report_side_by_side_under_their_own_tags()
    {
        using var recorder = new InstrumentRecorder();

        GrainIndexBackfillProgressRegistry.Publish(Status(GrainIndexBackfillState.Running, 5, "index-a"), 50);
        GrainIndexBackfillProgressRegistry.Publish(Status(GrainIndexBackfillState.Paused, 9, "index-b"), null);

        recorder.Collect();

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Latest(GrainIndexMetrics.BackfillProcessedName, "index-a")!.Value, Is.EqualTo(5d));
            Assert.That(recorder.Latest(GrainIndexMetrics.BackfillProcessedName, "index-b")!.Value, Is.EqualTo(9d));
            Assert.That(recorder.For(GrainIndexMetrics.BackfillTotalName), Has.Count.EqualTo(1));
            Assert.That(recorder.For(GrainIndexMetrics.BackfillPercentCompleteName), Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void Removing_an_index_stops_it_being_reported()
    {
        using var recorder = new InstrumentRecorder();

        GrainIndexBackfillProgressRegistry.Publish(Status(GrainIndexBackfillState.Running, 3), 10);
        GrainIndexBackfillProgressRegistry.Remove(Index);

        recorder.Collect();

        Assert.That(recorder.For(GrainIndexMetrics.BackfillProcessedName), Is.Empty);
    }

    [Test]
    public void Clearing_stops_every_index_being_reported()
    {
        using var recorder = new InstrumentRecorder();

        GrainIndexBackfillProgressRegistry.Publish(Status(GrainIndexBackfillState.Running, 3, "index-a"), 10);
        GrainIndexBackfillProgressRegistry.Publish(Status(GrainIndexBackfillState.Running, 4, "index-b"), 10);
        GrainIndexBackfillProgressRegistry.Clear();

        recorder.Collect();

        Assert.That(recorder.For(GrainIndexMetrics.BackfillStateName), Is.Empty);
    }

    [Test]
    public void A_completed_crawl_is_a_hundred_percent_even_with_no_bound() =>
        Assert.That(
            GrainIndexBackfillProgressRegistry.PercentComplete(
                GrainIndexBackfillState.Completed,
                processed: 3,
                total: null),
            Is.EqualTo(100d));

    [TestCase(GrainIndexBackfillState.NotStarted)]
    [TestCase(GrainIndexBackfillState.Running)]
    [TestCase(GrainIndexBackfillState.Paused)]
    [TestCase(GrainIndexBackfillState.Failed)]
    public void An_unfinished_crawl_with_no_bound_has_no_percentage(GrainIndexBackfillState state) =>
        Assert.That(
            GrainIndexBackfillProgressRegistry.PercentComplete(state, processed: 3, total: null),
            Is.Null);

    [TestCase(0L)]
    [TestCase(-1L)]
    public void A_non_positive_bound_yields_no_percentage(long total) =>
        Assert.That(
            GrainIndexBackfillProgressRegistry.PercentComplete(
                GrainIndexBackfillState.Running,
                processed: 3,
                total: total),
            Is.Null);

    [Test]
    public void A_percentage_is_clamped_when_a_bound_turns_out_low() =>
        Assert.That(
            GrainIndexBackfillProgressRegistry.PercentComplete(
                GrainIndexBackfillState.Running,
                processed: 500,
                total: 100),
            Is.EqualTo(100d));

    [Test]
    public void A_crawl_that_has_visited_nothing_is_zero_percent() =>
        Assert.That(
            GrainIndexBackfillProgressRegistry.PercentComplete(
                GrainIndexBackfillState.Running,
                processed: 0,
                total: 100),
            Is.EqualTo(0d));

    private static GrainIndexBackfillStatus Status(
        GrainIndexBackfillState state,
        long visited,
        string indexName = Index) =>
        new(
            indexName,
            state,
            resumeAfterKey: null,
            visited,
            enrolled: visited,
            skipped: 0,
            failed: 0,
            passes: 1,
            revisitsEnrolled: false,
            startedUtc: null,
            updatedUtc: null,
            completedUtc: null,
            failureMessage: null);
}
