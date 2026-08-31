using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.GrainIndex.Tests.Observability;

/// <summary>
/// The observability half of the grain index end to end: a real silo, a real
/// registry tree, a real index tree, dormant grains the crawl onboards, and the
/// instruments an OpenTelemetry subscriber would actually receive.
/// </summary>
/// <remarks>
/// <para>
/// These cover the acceptance shapes: accurate progress while a crawl runs and
/// <see cref="GrainIndexBackfillState.Completed"/> once it finishes; pause,
/// resume, and rebuild observably stopping and restarting the crawl; the
/// documented instruments appearing on the shared <c>orleans.lattice</c> meter
/// correctly tagged; and onboarding through either route moving the enrolled
/// counter and the entry-count gauge.
/// </para>
/// <para>
/// Nothing here waits on wall-clock time, a scheduler, or a collection. The
/// fixture switches the index's background driver off, so every pass is invoked
/// explicitly through the administrative surface and the crawl advances exactly
/// as far as a test asks it to. Progress is compared against a deterministically
/// driven crawl, never against elapsed time; the drain loop is bounded by a pass
/// count derived from the population size, not by a timeout.
/// </para>
/// </remarks>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class GrainIndexObservabilityIntegrationTests
{
    private const string Index = GrainIndexAdminClusterFixture.IndexName;
    private const int Population = GrainIndexAdminClusterFixture.PopulationSize;
    private const int BatchSize = GrainIndexAdminClusterFixture.BatchSize;
    private const int EntriesPerGrain = GrainIndexAdminClusterFixture.EntriesPerGrain;

    private GrainIndexAdminClusterFixture _fixture = null!;
    private string[] _population = [];
    private int _runId;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new GrainIndexAdminClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [SetUp]
    public async Task SetUp()
    {
        _population = await _fixture.SeedDormantPopulationAsync($"run{++_runId}");
        await _fixture.ResetIndexAsync();
    }

    private IGrainIndexAdmin Admin => _fixture.Admin;

    [Test]
    public void The_admin_is_resolvable_from_the_silo_container() =>
        Assert.That(
            _fixture.SiloServices.GetService<IGrainIndexAdmin>(),
            Is.Not.Null,
            "AddGrainIndex has to register the operator surface, or a host has to wire it by hand.");

    [Test]
    public void The_admin_lists_the_index_the_silo_declares() =>
        Assert.That(Admin.DeclaredIndexes, Is.EqualTo(new[] { Index }));

    [Test]
    public void Asking_about_an_index_the_silo_does_not_declare_fails_loudly() =>
        Assert.That(
            async () => await Admin.GetStatusAsync("no-such-index"),
            Throws.TypeOf<GrainIndexNotDeclaredException>());

    [Test]
    public async Task A_declared_index_reports_its_registered_declaration_and_no_drift()
    {
        var status = await Admin.GetStatusAsync(Index);

        Assert.Multiple(() =>
        {
            Assert.That(status.IndexName, Is.EqualTo(Index));
            Assert.That(status.Registered, Is.True,
                "Reconciliation runs at silo start, so the registry knows the index by the time a test runs.");
            Assert.That(status.Definition.TreeName, Is.EqualTo(GrainIndexTreeNames.ForIndex(Index)));
            Assert.That(status.Definition.Properties.Select(p => p.Name), Is.EqualTo(new[] { "Age", "Country" }));
            Assert.That(status.Drift.HasDrift, Is.False);
            Assert.That(status.KeyCodecId, Is.Not.Empty);
            Assert.That(status.Fingerprint.Value, Is.Not.Empty);
        });
    }

    [Test]
    public async Task Progress_tracks_the_crawl_pass_by_pass_and_settles_at_completed()
    {
        await Admin.RebuildAsync(Index);

        var afterStart = await Admin.GetStatusAsync(Index);
        await Admin.RunBackfillPassAsync(Index);
        var afterOnePass = await Admin.GetStatusAsync(Index);

        var passes = await _fixture.DrainBackfillAsync();
        var afterDrain = await Admin.GetStatusAsync(Index);

        Assert.Multiple(() =>
        {
            Assert.That(afterStart.Backfill.State, Is.EqualTo(GrainIndexBackfillState.Running));
            Assert.That(afterStart.Progress.Processed, Is.Zero);

            Assert.That(afterOnePass.Progress.Processed, Is.EqualTo(BatchSize),
                "A pass visits exactly one batch, so the processed count is the batch size and not a "
                + "function of how long the pass took.");
            Assert.That(afterOnePass.Progress.LastProcessedKey, Is.EqualTo(_population[BatchSize - 1]));
            Assert.That(afterOnePass.Progress.Total, Is.EqualTo(Population));
            Assert.That(
                afterOnePass.Progress.PercentComplete,
                Is.EqualTo(BatchSize * 100d / Population).Within(0.001));

            Assert.That(passes, Is.GreaterThan(0));
            Assert.That(afterDrain.Backfill.State, Is.EqualTo(GrainIndexBackfillState.Completed));
            Assert.That(afterDrain.Progress.Processed, Is.EqualTo(Population));
            Assert.That(afterDrain.Progress.PercentComplete, Is.EqualTo(100d));
            Assert.That(afterDrain.Progress.LastError, Is.Null);
        });
    }

    [Test]
    public async Task The_entry_count_reflects_what_the_crawl_actually_wrote()
    {
        await Admin.RebuildAsync(Index);
        await _fixture.DrainBackfillAsync();

        var status = await Admin.GetStatusAsync(Index);
        var onTree = await _fixture.IndexEntryCountAsync();

        Assert.Multiple(() =>
        {
            Assert.That(status.EntryCount, Is.EqualTo(onTree));
            Assert.That(status.EntryCount, Is.EqualTo(Population * EntriesPerGrain));
        });
    }

    [Test]
    public async Task Listing_statuses_reports_every_declared_index()
    {
        var statuses = await Admin.ListStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(statuses, Has.Count.EqualTo(1));
            Assert.That(statuses[0].IndexName, Is.EqualTo(Index));
        });
    }

    [Test]
    public async Task Pausing_holds_the_crawl_and_resuming_continues_from_its_checkpoint()
    {
        await Admin.RebuildAsync(Index);
        await Admin.RunBackfillPassAsync(Index);

        var paused = await Admin.PauseBackfillAsync(Index);
        var passWhilePaused = await Admin.RunBackfillPassAsync(Index);
        var whilePaused = await Admin.GetStatusAsync(Index);

        var resumed = await Admin.ResumeBackfillAsync(Index);
        await Admin.RunBackfillPassAsync(Index);
        var afterResume = await Admin.GetStatusAsync(Index);

        Assert.Multiple(() =>
        {
            Assert.That(paused.State, Is.EqualTo(GrainIndexBackfillState.Paused));
            Assert.That(passWhilePaused.Visited, Is.Zero,
                "A pause is only meaningful if a pass on a paused crawl does nothing at all.");
            Assert.That(whilePaused.Progress.Processed, Is.EqualTo(BatchSize));
            Assert.That(whilePaused.Progress.LastProcessedKey, Is.EqualTo(_population[BatchSize - 1]),
                "The checkpoint has to survive the pause, or a resume would restart the crawl.");

            Assert.That(resumed.State, Is.EqualTo(GrainIndexBackfillState.Running));
            Assert.That(afterResume.Progress.Processed, Is.EqualTo(BatchSize * 2),
                "A resumed crawl continues from its checkpoint rather than re-walking what it has done.");
        });
    }

    [Test]
    public async Task Rebuilding_a_completed_crawl_runs_it_again_over_the_whole_population()
    {
        await Admin.RebuildAsync(Index);
        await _fixture.DrainBackfillAsync();
        var completed = await Admin.GetStatusAsync(Index);

        var restarted = await Admin.RebuildAsync(Index);
        var afterRestart = await Admin.GetStatusAsync(Index);
        await _fixture.DrainBackfillAsync();
        var rebuilt = await Admin.GetStatusAsync(Index);

        Assert.Multiple(() =>
        {
            Assert.That(completed.Backfill.State, Is.EqualTo(GrainIndexBackfillState.Completed));

            Assert.That(restarted.State, Is.EqualTo(GrainIndexBackfillState.Running));
            Assert.That(restarted.RevisitsEnrolled, Is.True,
                "A rebuild that skipped already-indexed grains would be a no-op on a completed crawl.");
            Assert.That(afterRestart.Progress.Processed, Is.Zero);
            Assert.That(afterRestart.Progress.LastProcessedKey, Is.Null);

            Assert.That(rebuilt.Backfill.State, Is.EqualTo(GrainIndexBackfillState.Completed));
            Assert.That(rebuilt.Progress.Processed, Is.EqualTo(Population));
            Assert.That(rebuilt.Backfill.Enrolled, Is.EqualTo(Population),
                "Every grain is revisited, so none of them is skipped as already enrolled.");
        });
    }

    [Test]
    public async Task The_documented_instruments_appear_on_the_core_meter_tagged_by_index()
    {
        using var recorder = new InstrumentRecorder();

        await Admin.RebuildAsync(Index);
        await _fixture.DrainBackfillAsync();
        recorder.Collect();

        var measurements = recorder.Measurements();
        var instruments = measurements.Select(m => m.Instrument).Distinct().ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(instruments, Does.Contain(GrainIndexMetrics.GrainsEnrolledName));
            Assert.That(instruments, Does.Contain(GrainIndexMetrics.EntriesName));
            Assert.That(instruments, Does.Contain(GrainIndexMetrics.ProjectionDurationName));
            Assert.That(instruments, Does.Contain(GrainIndexMetrics.BackfillProcessedName));
            Assert.That(instruments, Does.Contain(GrainIndexMetrics.BackfillTotalName));
            Assert.That(instruments, Does.Contain(GrainIndexMetrics.BackfillPercentCompleteName));
            Assert.That(instruments, Does.Contain(GrainIndexMetrics.BackfillStateName));

            Assert.That(
                measurements.All(m => m.HasTag(GrainIndexMetrics.TagIndex, Index)),
                Is.True,
                "Every grain-index series has to name the index it belongs to, or an operator cannot "
                + "attribute it.");
        });
    }

    [Test]
    public async Task A_crawl_onboarding_a_dormant_population_counts_on_both_routes_and_moves_the_entry_gauge()
    {
        using var recorder = new InstrumentRecorder();

        await Admin.RebuildAsync(Index);
        await _fixture.DrainBackfillAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                recorder.Sum(
                    GrainIndexMetrics.GrainsEnrolledName,
                    GrainIndexMetrics.TagPath,
                    GrainIndexMetrics.PathBackfill),
                Is.EqualTo((double)Population),
                "The crawl caused every one of these enrolments.");
            Assert.That(
                recorder.Sum(
                    GrainIndexMetrics.GrainsEnrolledName,
                    GrainIndexMetrics.TagPath,
                    GrainIndexMetrics.PathActivation),
                Is.EqualTo((double)Population),
                "Each grain also performed its own enrolment when the crawl activated it.");
            Assert.That(
                recorder.Sum(GrainIndexMetrics.EntriesName),
                Is.EqualTo((double)(Population * EntriesPerGrain)),
                "The entry gauge has to end up at the number of entries the index actually holds.");
            Assert.That(recorder.For(GrainIndexMetrics.WriteFailuresName), Is.Empty);
        });
    }

    [Test]
    public async Task A_grain_onboarded_by_ordinary_traffic_counts_on_the_activation_route_alone()
    {
        using var recorder = new InstrumentRecorder();

        var key = $"traffic-{_runId}-{Guid.NewGuid():N}";
        await _fixture.Cluster.GrainFactory.GetGrain<IAdminUserGrain>(key).SetAsync(41, "GB");

        Assert.Multiple(() =>
        {
            Assert.That(
                recorder.Sum(
                    GrainIndexMetrics.GrainsEnrolledName,
                    GrainIndexMetrics.TagPath,
                    GrainIndexMetrics.PathActivation),
                Is.EqualTo(1d));
            Assert.That(
                recorder.Sum(
                    GrainIndexMetrics.GrainsEnrolledName,
                    GrainIndexMetrics.TagPath,
                    GrainIndexMetrics.PathBackfill),
                Is.Zero,
                "No crawl was involved, so nothing may be attributed to one.");
            Assert.That(
                recorder.Sum(GrainIndexMetrics.EntriesName),
                Is.EqualTo((double)EntriesPerGrain));
            Assert.That(recorder.For(GrainIndexMetrics.ProjectionDurationName), Is.Not.Empty);
        });
    }
}
