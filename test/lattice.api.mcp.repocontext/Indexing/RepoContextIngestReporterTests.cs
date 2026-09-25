using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// Unit tests for <see cref="RepoContextIngestReporter"/> and the
/// <see cref="RepoContextIngestPass"/> it hands out - the instrument family that makes
/// a stalled repository-context ingest distinguishable from a finished one on
/// <c>/metrics</c> alone (issue #3151).
/// <para>
/// The behaviours under test are the ones the issue turns on: every series is present
/// at zero once a repository has begun a pass, so a quiet arm is a measured zero;
/// progress reports carrying run-cumulative figures are converted to deltas without
/// ever double-counting; every pass settles into exactly one outcome arm; and the age
/// gauge climbs while no pass completes and restarts when one does.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextIngestReporterTests
{
    private const string Repo = RepoIndexRunnerHarness.RepoId;

    [Test]
    public void BeginPass_mints_every_counter_series_for_a_new_repository_at_zero()
    {
        using var reporter = new RepoContextIngestReporter(new ManualTimeProvider());
        using var capture = new IngestMeasurementCapture(reporter);

        reporter.BeginPass(Repo);

        var minted = capture.Measurements
            .Select(m => (m.Instrument, Outcome: m.Tags.GetValueOrDefault(RepoContextIngestReporter.OutcomeTagKey), m.Value))
            .ToList();

        Assert.Multiple(() =>
        {
            Assert.That(
                minted.Select(m => $"{m.Instrument}|{m.Outcome}"),
                Is.EquivalentTo(new[]
                {
                    $"{RepoContextIngestReporter.FilesScannedInstrumentName}|",
                    $"{RepoContextIngestReporter.FilesInstrumentName}|{RepoContextIngestReporter.FileAddedTag}",
                    $"{RepoContextIngestReporter.FilesInstrumentName}|{RepoContextIngestReporter.FileUpdatedTag}",
                    $"{RepoContextIngestReporter.FilesInstrumentName}|{RepoContextIngestReporter.FileRemovedTag}",
                    $"{RepoContextIngestReporter.FilesInstrumentName}|{RepoContextIngestReporter.FileUnchangedTag}",
                    $"{RepoContextIngestReporter.FilesEmbeddedInstrumentName}|",
                    $"{RepoContextIngestReporter.SymbolsEmbeddedInstrumentName}|",
                    $"{RepoContextIngestReporter.FilesContentProjectedInstrumentName}|",
                    $"{RepoContextIngestReporter.PassesInstrumentName}|{RepoContextIngestReporter.PassCompletedTag}",
                    $"{RepoContextIngestReporter.PassesInstrumentName}|{RepoContextIngestReporter.PassFailedTag}",
                    $"{RepoContextIngestReporter.PassesInstrumentName}|{RepoContextIngestReporter.PassCancelledTag}",
                }));
            Assert.That(minted.Select(m => m.Value), Is.All.EqualTo(0d), "Priming must never fabricate a count.");
            Assert.That(
                capture.Measurements.Select(m => m.Tags[RepoContextIngestReporter.RepositoryTagKey]),
                Is.All.EqualTo(Repo));
            Assert.That(
                capture.Measurements.Select(m => m.Tags[LatticeTenantLabel.TagTenant]),
                Is.All.EqualTo(LatticeTenantLabel.PlatformTenant));
            Assert.That(
                capture.Count(RepoContextIngestReporter.PassDurationInstrumentName),
                Is.Zero,
                "A primed histogram would fabricate a zero-second pass.");
        });
    }

    [Test]
    public void BeginPass_mints_a_repository_only_once_but_mints_each_new_repository()
    {
        using var reporter = new RepoContextIngestReporter(new ManualTimeProvider());
        using var capture = new IngestMeasurementCapture(reporter);

        reporter.BeginPass(Repo);
        var afterFirst = capture.Measurements.Count;
        reporter.BeginPass(Repo);
        var afterSecond = capture.Measurements.Count;
        reporter.BeginPass("other");

        Assert.Multiple(() =>
        {
            Assert.That(afterFirst, Is.EqualTo(11));
            Assert.That(afterSecond, Is.EqualTo(afterFirst), "A repository already minted is not minted again.");
            Assert.That(capture.Count(RepoContextIngestReporter.FilesScannedInstrumentName, "other"), Is.EqualTo(1));
        });
    }

    [Test]
    public void BeginPass_null_repository_throws()
    {
        using var reporter = new RepoContextIngestReporter();

        Assert.That(() => reporter.BeginPass(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Observe_run_cumulative_figures_record_only_their_growth()
    {
        using var reporter = new RepoContextIngestReporter(new ManualTimeProvider());
        using var capture = new IngestMeasurementCapture(reporter);
        var pass = reporter.BeginPass(Repo);

        pass.Observe(new RepoIndexProgressUpdate { FilesScanned = 3 });
        pass.Observe(new RepoIndexProgressUpdate { FilesScanned = 5, FilesEmbedded = 2 });
        pass.Observe(new RepoIndexProgressUpdate { FilesEmbedded = 7 });

        Assert.Multiple(() =>
        {
            Assert.That(capture.Sum(RepoContextIngestReporter.FilesScannedInstrumentName), Is.EqualTo(5));
            Assert.That(capture.Sum(RepoContextIngestReporter.FilesEmbeddedInstrumentName), Is.EqualTo(7));
            Assert.That(
                capture.Measurements
                    .Where(m => m.Instrument == RepoContextIngestReporter.FilesScannedInstrumentName && m.Value != 0)
                    .Select(m => m.Value),
                Is.EqualTo(new[] { 3d, 2d }),
                "Each report records the delta over the previous high-water mark, not the cumulative figure.");
        });
    }

    [Test]
    public void Observe_a_repeated_smaller_or_absent_figure_records_nothing()
    {
        using var reporter = new RepoContextIngestReporter(new ManualTimeProvider());
        using var capture = new IngestMeasurementCapture(reporter);
        var pass = reporter.BeginPass(Repo);
        pass.Observe(new RepoIndexProgressUpdate { FilesScanned = 4, SymbolsEmbedded = 9 });
        var before = capture.Measurements.Count;

        pass.Observe(new RepoIndexProgressUpdate { FilesScanned = 4, SymbolsEmbedded = 2 });
        pass.Observe(new RepoIndexProgressUpdate { Phase = RepoIndexPhase.Vectorising });

        Assert.Multiple(() =>
        {
            Assert.That(capture.Measurements.Count, Is.EqualTo(before));
            Assert.That(capture.Sum(RepoContextIngestReporter.FilesScannedInstrumentName), Is.EqualTo(4));
            Assert.That(capture.Sum(RepoContextIngestReporter.SymbolsEmbeddedInstrumentName), Is.EqualTo(9));
        });
    }

    [Test]
    public void Observe_each_figure_lands_on_its_own_instrument_and_outcome_arm()
    {
        using var reporter = new RepoContextIngestReporter(new ManualTimeProvider());
        using var capture = new IngestMeasurementCapture(reporter);
        var pass = reporter.BeginPass(Repo);

        pass.Observe(new RepoIndexProgressUpdate
        {
            FilesScanned = 17,
            FilesAdded = 1,
            FilesUpdated = 2,
            FilesRemoved = 3,
            FilesUnchanged = 4,
            FilesEmbedded = 5,
            SymbolsEmbedded = 6,
            FilesContentProjected = 7,
        });

        Assert.Multiple(() =>
        {
            Assert.That(capture.Sum(RepoContextIngestReporter.FilesScannedInstrumentName), Is.EqualTo(17));
            Assert.That(capture.Sum(RepoContextIngestReporter.FilesInstrumentName, outcome: RepoContextIngestReporter.FileAddedTag), Is.EqualTo(1));
            Assert.That(capture.Sum(RepoContextIngestReporter.FilesInstrumentName, outcome: RepoContextIngestReporter.FileUpdatedTag), Is.EqualTo(2));
            Assert.That(capture.Sum(RepoContextIngestReporter.FilesInstrumentName, outcome: RepoContextIngestReporter.FileRemovedTag), Is.EqualTo(3));
            Assert.That(capture.Sum(RepoContextIngestReporter.FilesInstrumentName, outcome: RepoContextIngestReporter.FileUnchangedTag), Is.EqualTo(4));
            Assert.That(capture.Sum(RepoContextIngestReporter.FilesEmbeddedInstrumentName), Is.EqualTo(5));
            Assert.That(capture.Sum(RepoContextIngestReporter.SymbolsEmbeddedInstrumentName), Is.EqualTo(6));
            Assert.That(capture.Sum(RepoContextIngestReporter.FilesContentProjectedInstrumentName), Is.EqualTo(7));
        });
    }

    [Test]
    public void Observe_result_lands_the_plan_outcomes_a_no_change_pass_never_reports()
    {
        using var reporter = new RepoContextIngestReporter(new ManualTimeProvider());
        using var capture = new IngestMeasurementCapture(reporter);
        var pass = reporter.BeginPass(Repo);
        pass.Observe(new RepoIndexProgressUpdate { FilesScanned = 10 });

        pass.Observe(new RepoContextBootstrapResult
        {
            RepoId = Repo,
            FilesScanned = 10,
            FilesAdded = 0,
            FilesUpdated = 1,
            FilesRemoved = 0,
            FilesUnchanged = 9,
            SymbolsCaptured = 0,
            ElapsedMilliseconds = 5,
        });

        Assert.Multiple(() =>
        {
            Assert.That(capture.Sum(RepoContextIngestReporter.FilesScannedInstrumentName), Is.EqualTo(10), "The scan already reported is not counted again.");
            Assert.That(capture.Sum(RepoContextIngestReporter.FilesInstrumentName, outcome: RepoContextIngestReporter.FileUpdatedTag), Is.EqualTo(1));
            Assert.That(capture.Sum(RepoContextIngestReporter.FilesInstrumentName, outcome: RepoContextIngestReporter.FileUnchangedTag), Is.EqualTo(9));
        });
    }

    [Test]
    public void Observe_null_result_throws()
    {
        using var reporter = new RepoContextIngestReporter();
        var pass = reporter.BeginPass(Repo);

        Assert.That(() => pass.Observe((RepoContextBootstrapResult)null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Complete_counts_one_completed_pass_and_records_its_duration()
    {
        var time = new ManualTimeProvider();
        using var reporter = new RepoContextIngestReporter(time);
        using var capture = new IngestMeasurementCapture(reporter);
        var pass = reporter.BeginPass(Repo);
        time.Advance(TimeSpan.FromSeconds(7));

        pass.Complete();

        AssertSettledAs(capture, RepoContextIngestReporter.PassCompletedTag, expectedSeconds: 7);
    }

    [Test]
    public void Fail_counts_one_failed_pass_and_records_its_duration()
    {
        var time = new ManualTimeProvider();
        using var reporter = new RepoContextIngestReporter(time);
        using var capture = new IngestMeasurementCapture(reporter);
        var pass = reporter.BeginPass(Repo);
        time.Advance(TimeSpan.FromSeconds(3));

        pass.Fail();

        AssertSettledAs(capture, RepoContextIngestReporter.PassFailedTag, expectedSeconds: 3);
    }

    [Test]
    public void Cancel_counts_one_cancelled_pass_and_records_its_duration()
    {
        var time = new ManualTimeProvider();
        using var reporter = new RepoContextIngestReporter(time);
        using var capture = new IngestMeasurementCapture(reporter);
        var pass = reporter.BeginPass(Repo);
        time.Advance(TimeSpan.FromSeconds(11));

        pass.Cancel();

        AssertSettledAs(capture, RepoContextIngestReporter.PassCancelledTag, expectedSeconds: 11);
    }

    [Test]
    public void A_settled_pass_ignores_every_later_settle_and_progress_report()
    {
        using var reporter = new RepoContextIngestReporter(new ManualTimeProvider());
        using var capture = new IngestMeasurementCapture(reporter);
        var pass = reporter.BeginPass(Repo);
        pass.Fail();

        pass.Complete();
        pass.Cancel();
        pass.Fail();
        pass.Observe(new RepoIndexProgressUpdate { FilesScanned = 99 });

        Assert.Multiple(() =>
        {
            Assert.That(capture.Sum(RepoContextIngestReporter.PassesInstrumentName), Is.EqualTo(1));
            Assert.That(capture.Sum(RepoContextIngestReporter.PassesInstrumentName, outcome: RepoContextIngestReporter.PassFailedTag), Is.EqualTo(1));
            Assert.That(capture.Count(RepoContextIngestReporter.PassDurationInstrumentName), Is.EqualTo(1));
            Assert.That(capture.Sum(RepoContextIngestReporter.FilesScannedInstrumentName), Is.Zero);
        });
    }

    [Test]
    public void Last_completed_pass_age_climbs_until_a_pass_completes_then_restarts()
    {
        var time = new ManualTimeProvider();
        using var reporter = new RepoContextIngestReporter(time);
        using var capture = new IngestMeasurementCapture(reporter);

        capture.CollectObservables();
        var beforeAnyPass = capture.Count(RepoContextIngestReporter.LastCompletedPassAgeInstrumentName);

        var first = reporter.BeginPass(Repo);
        time.Advance(TimeSpan.FromSeconds(30));
        first.Fail();
        time.Advance(TimeSpan.FromSeconds(10));
        var whileStalled = ReadAge(capture);

        var second = reporter.BeginPass(Repo);
        time.Advance(TimeSpan.FromSeconds(5));
        second.Complete();
        var justCompleted = ReadAge(capture);

        time.Advance(TimeSpan.FromSeconds(4));
        var afterCompletion = ReadAge(capture);

        Assert.Multiple(() =>
        {
            Assert.That(beforeAnyPass, Is.Zero, "A repository with no pass on this silo has no age series.");
            Assert.That(whileStalled, Is.EqualTo(40), "Until a pass completes the age runs from the first pass beginning.");
            Assert.That(justCompleted, Is.Zero, "A completed pass restarts the age.");
            Assert.That(afterCompletion, Is.EqualTo(4));
        });
    }

    [Test]
    public void Last_completed_pass_age_reads_each_repository_separately()
    {
        var time = new ManualTimeProvider();
        using var reporter = new RepoContextIngestReporter(time);
        using var capture = new IngestMeasurementCapture(reporter);
        var healthy = reporter.BeginPass("healthy");
        reporter.BeginPass("wedged");
        time.Advance(TimeSpan.FromSeconds(20));
        healthy.Complete();
        time.Advance(TimeSpan.FromSeconds(2));

        capture.CollectObservables();
        var ages = capture.Measurements
            .Where(m => m.Instrument == RepoContextIngestReporter.LastCompletedPassAgeInstrumentName)
            .ToDictionary(m => m.Tags[RepoContextIngestReporter.RepositoryTagKey]!, m => m.Value);

        Assert.Multiple(() =>
        {
            Assert.That(ages, Has.Count.EqualTo(2));
            Assert.That(ages["healthy"], Is.EqualTo(2));
            Assert.That(ages["wedged"], Is.EqualTo(22), "A healthy repository must not hide a wedged one.");
            Assert.That(
                capture.Measurements
                    .Where(m => m.Instrument == RepoContextIngestReporter.LastCompletedPassAgeInstrumentName)
                    .Select(m => m.Tags[LatticeTenantLabel.TagTenant]),
                Is.All.EqualTo(LatticeTenantLabel.PlatformTenant));
        });
    }

    [Test]
    public void Dispose_stops_publishing_and_is_idempotent()
    {
        var reporter = new RepoContextIngestReporter(new ManualTimeProvider());
        using var capture = new IngestMeasurementCapture(reporter);

        reporter.Dispose();
        reporter.BeginPass(Repo).Complete();

        Assert.Multiple(() =>
        {
            Assert.That(capture.Measurements, Is.Empty);
            Assert.That(() => reporter.Dispose(), Throws.Nothing);
        });
    }

    [Test]
    public void Advance_absent_or_non_increasing_figures_return_zero_and_keep_the_mark()
    {
        long mark = 5;

        Assert.Multiple(() =>
        {
            Assert.That(RepoContextIngestPass.Advance(ref mark, null), Is.Zero);
            Assert.That(RepoContextIngestPass.Advance(ref mark, 5), Is.Zero);
            Assert.That(RepoContextIngestPass.Advance(ref mark, 2), Is.Zero);
            Assert.That(mark, Is.EqualTo(5));
            Assert.That(RepoContextIngestPass.Advance(ref mark, 8), Is.EqualTo(3));
            Assert.That(mark, Is.EqualTo(8));
        });
    }

    private static double ReadAge(IngestMeasurementCapture capture)
    {
        var before = capture.Measurements.Count;
        capture.CollectObservables();
        return capture.Measurements
            .Skip(before)
            .Single(m => m.Instrument == RepoContextIngestReporter.LastCompletedPassAgeInstrumentName && m.Matches(Repo, null))
            .Value;
    }

    private static void AssertSettledAs(IngestMeasurementCapture capture, string outcome, double expectedSeconds)
    {
        string[] arms =
        [
            RepoContextIngestReporter.PassCompletedTag,
            RepoContextIngestReporter.PassFailedTag,
            RepoContextIngestReporter.PassCancelledTag,
        ];

        Assert.Multiple(() =>
        {
            foreach (var arm in arms)
            {
                Assert.That(
                    capture.Sum(RepoContextIngestReporter.PassesInstrumentName, outcome: arm),
                    Is.EqualTo(arm == outcome ? 1 : 0),
                    $"passes{{outcome={arm}}}");
            }

            var durations = capture.Measurements
                .Where(m => m.Instrument == RepoContextIngestReporter.PassDurationInstrumentName)
                .ToList();
            Assert.That(durations, Has.Count.EqualTo(1));
            Assert.That(durations[0].Value, Is.EqualTo(expectedSeconds));
            Assert.That(durations[0].Tags[RepoContextIngestReporter.OutcomeTagKey], Is.EqualTo(outcome));
            Assert.That(durations[0].Tags[RepoContextIngestReporter.RepositoryTagKey], Is.EqualTo(Repo));
        });
    }
}
