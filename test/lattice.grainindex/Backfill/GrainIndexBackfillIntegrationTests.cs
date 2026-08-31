using Microsoft.Extensions.DependencyInjection;
using Orleans.Core.Internal;
using Orleans.Lattice.GrainIndex.Backfill;
using Orleans.Lattice.GrainIndex.Registry;

namespace Orleans.Lattice.GrainIndex.Tests.Backfill;

/// <summary>
/// The background backfill end to end: a real silo, a real reminder service, a
/// real registry tree, a real index tree, and dormant grains that index
/// themselves the moment the crawl activates them.
/// </summary>
/// <remarks>
/// <para>
/// These cover the four acceptance shapes - a full backfill of a dormant
/// population, a resume after the crawl's activation is lost mid-crawl, the rate
/// limit pacing a pass, and the skip over grains the activation path has already
/// onboarded - plus the control primitives an administrative surface drives.
/// </para>
/// <para>
/// Nothing here waits on wall-clock time, a scheduler, or a collection. The
/// fixture switches the index's background driver off, so every pass is invoked
/// explicitly and the crawl advances exactly as far as a test asks it to. The
/// loop that runs a crawl to completion is bounded by a pass count derived from
/// the population size, not by a timeout.
/// </para>
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class GrainIndexBackfillIntegrationTests
{
    private GrainIndexBackfillClusterFixture _fixture = null!;
    private string[] _population = [];
    private int _runId;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new GrainIndexBackfillClusterFixture();
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

    private IGrainIndexBackfillGrain Backfill() =>
        _fixture.Cluster.GrainFactory.GetGrain<IGrainIndexBackfillGrain>(
            GrainIndexBackfillClusterFixture.IndexName);

    private IGrainIndex<IBackfillUserGrain, BackfillUserState> Index() =>
        _fixture.SiloServices
            .GetRequiredService<IGrainIndexProvider>()
            .GetIndex<IBackfillUserGrain, BackfillUserState>(GrainIndexBackfillClusterFixture.IndexName);

    private Task<IReadOnlyList<string>> MatchedKeysAsync() =>
        Index().Where(s => s.Country == GrainIndexBackfillClusterFixture.Country).ToKeyListAsync();

    /// <summary>
    /// Runs passes until the crawl settles. The bound is one pass per grain plus
    /// the pass that observes the exhausted source, so it is a function of the
    /// population rather than of how long anything takes.
    /// </summary>
    private static async Task<GrainIndexBackfillStatus> DrainAsync(IGrainIndexBackfillGrain backfill)
    {
        var maxPasses = GrainIndexBackfillClusterFixture.PopulationSize + 2;
        for (var pass = 0; pass < maxPasses; pass++)
        {
            var result = await backfill.RunBatchAsync();
            if (result.State != GrainIndexBackfillState.Running)
                break;
        }

        return await backfill.GetStatusAsync();
    }

    [Test]
    public async Task A_dormant_population_becomes_queryable_once_the_crawl_completes()
    {
        var before = await MatchedKeysAsync();

        var backfill = Backfill();
        await backfill.EnsureStartedAsync();
        var status = await DrainAsync(backfill);

        var matched = await MatchedKeysAsync();

        Assert.Multiple(() =>
        {
            Assert.That(before, Is.Empty,
                "The premise of the test is a population the index knows nothing about.");
            Assert.That(status.State, Is.EqualTo(GrainIndexBackfillState.Completed));
            Assert.That(status.Enrolled, Is.EqualTo(GrainIndexBackfillClusterFixture.PopulationSize));
            Assert.That(matched, Is.EquivalentTo(_population),
                "Every grain that existed before the index did has to be findable once the crawl "
                + "has run, without any of them being addressed by application traffic.");
        });
    }

    [Test]
    public async Task A_completed_crawl_leaves_every_grain_recorded_as_enrolled()
    {
        var backfill = Backfill();
        await backfill.EnsureStartedAsync();
        await DrainAsync(backfill);

        var enrolled = await _fixture.EnrolledKeysAsync();

        Assert.That(enrolled, Is.EquivalentTo(_population),
            "Without the markers the next crawl would revisit the whole population.");
    }

    [Test]
    public async Task A_pass_enrols_at_most_the_configured_batch_size()
    {
        var backfill = Backfill();
        await backfill.EnsureStartedAsync();

        var first = await backfill.RunBatchAsync();
        var afterOnePass = await MatchedKeysAsync();

        Assert.Multiple(() =>
        {
            Assert.That(first.Visited, Is.EqualTo(GrainIndexBackfillClusterFixture.BatchSize));
            Assert.That(first.Enrolled, Is.EqualTo(GrainIndexBackfillClusterFixture.BatchSize));
            Assert.That(first.State, Is.EqualTo(GrainIndexBackfillState.Running));
            Assert.That(afterOnePass, Has.Count.EqualTo(GrainIndexBackfillClusterFixture.BatchSize),
                "A single tick that onboarded the whole population would put the crawl's entire "
                + "cluster impact on one moment, which is what the rate limit exists to prevent.");
            Assert.That(afterOnePass, Is.EquivalentTo(new[] { _population[0], _population[1] }),
                "The crawl walks the key range in order, so the first pass is its first keys.");
        });
    }

    [Test]
    public async Task Losing_the_crawls_activation_mid_pass_resumes_from_the_checkpoint()
    {
        var backfill = Backfill();
        await backfill.EnsureStartedAsync();
        await backfill.RunBatchAsync();
        var beforeRestart = await backfill.GetStatusAsync();

        // Deactivating the crawl's grain is what a host restart leaves behind:
        // no in-memory position, one durable checkpoint in the registry tree.
        await backfill.AsReference<IGrainManagementExtension>().DeactivateOnIdle();

        var revived = Backfill();
        var resumed = await revived.EnsureStartedAsync();
        var status = await DrainAsync(revived);

        var matched = await MatchedKeysAsync();
        var entryCount = await _fixture.IndexEntryCountAsync();

        Assert.Multiple(() =>
        {
            Assert.That(beforeRestart.ResumeAfterKey, Is.EqualTo(_population[1]));
            Assert.That(resumed.ResumeAfterKey, Is.EqualTo(_population[1]),
                "A restarted host must pick the crawl up where it stopped, not at the head of the range.");
            Assert.That(resumed.Visited, Is.EqualTo(GrainIndexBackfillClusterFixture.BatchSize));
            Assert.That(status.State, Is.EqualTo(GrainIndexBackfillState.Completed));
            Assert.That(status.Visited, Is.EqualTo(GrainIndexBackfillClusterFixture.PopulationSize),
                "Resuming must neither repeat a key nor skip one.");
            Assert.That(matched, Is.EquivalentTo(_population));
            Assert.That(entryCount, Is.EqualTo(GrainIndexBackfillClusterFixture.PopulationSize * 2),
                "Two projected properties per grain and no duplicates: a resumed crawl that "
                + "re-projected a grain must produce the same entries rather than extra ones.");
        });
    }

    [Test]
    public async Task Restarting_a_completed_crawl_re_runs_the_full_range_without_corrupting_it()
    {
        var backfill = Backfill();
        await backfill.EnsureStartedAsync();
        await DrainAsync(backfill);
        var entriesAfterFirstCrawl = await _fixture.IndexEntryCountAsync();

        var restarted = await backfill.RestartAsync();
        var status = await DrainAsync(backfill);
        var entriesAfterRestart = await _fixture.IndexEntryCountAsync();
        var matched = await MatchedKeysAsync();

        Assert.Multiple(() =>
        {
            Assert.That(entriesAfterFirstCrawl, Is.EqualTo(GrainIndexBackfillClusterFixture.PopulationSize * 2));
            Assert.That(restarted.ResumeAfterKey, Is.Null);
            Assert.That(restarted.RevisitsEnrolled, Is.True);
            Assert.That(status.State, Is.EqualTo(GrainIndexBackfillState.Completed));
            Assert.That(status.Visited, Is.EqualTo(GrainIndexBackfillClusterFixture.PopulationSize));
            Assert.That(entriesAfterRestart, Is.EqualTo(entriesAfterFirstCrawl),
                "Re-projecting an unchanged grain produces an empty plan, so a replayed crawl "
                + "is idempotent rather than corrupting.");
            Assert.That(matched, Is.EquivalentTo(_population));
        });
    }

    [Test]
    public async Task Grains_the_activation_path_already_onboarded_are_skipped()
    {
        // Two of the population are addressed by ordinary application traffic
        // first, which is exactly what the activation path onboards.
        await _fixture.Cluster.GrainFactory.GetGrain<IBackfillUserGrain>(_population[0]).GetAgeAsync();
        await _fixture.Cluster.GrainFactory.GetGrain<IBackfillUserGrain>(_population[1]).GetAgeAsync();

        var backfill = Backfill();
        await backfill.EnsureStartedAsync();

        var first = await backfill.RunBatchAsync();
        var status = await DrainAsync(backfill);
        var matched = await MatchedKeysAsync();

        Assert.Multiple(() =>
        {
            Assert.That(first.Skipped, Is.EqualTo(2),
                "Re-projecting a grain the activation path already onboarded is the redundant "
                + "storm the seen markers exist to prevent.");
            Assert.That(first.Enrolled, Is.Zero);
            Assert.That(status.State, Is.EqualTo(GrainIndexBackfillState.Completed));
            Assert.That(status.Skipped, Is.EqualTo(2));
            Assert.That(
                status.Enrolled,
                Is.EqualTo(GrainIndexBackfillClusterFixture.PopulationSize - 2));
            Assert.That(matched, Is.EquivalentTo(_population),
                "Skipping is an optimisation, not a gap: the skipped grains are already indexed.");
        });
    }

    [Test]
    public async Task Pausing_stops_the_crawl_and_resuming_finishes_it()
    {
        var backfill = Backfill();
        await backfill.EnsureStartedAsync();
        await backfill.RunBatchAsync();

        var paused = await backfill.PauseAsync();
        var whilePaused = await backfill.RunBatchAsync();
        var resumed = await backfill.ResumeAsync();
        var status = await DrainAsync(backfill);
        var matched = await MatchedKeysAsync();

        Assert.Multiple(() =>
        {
            Assert.That(paused.State, Is.EqualTo(GrainIndexBackfillState.Paused));
            Assert.That(whilePaused.Visited, Is.Zero);
            Assert.That(whilePaused.State, Is.EqualTo(GrainIndexBackfillState.Paused));
            Assert.That(resumed.State, Is.EqualTo(GrainIndexBackfillState.Running));
            Assert.That(resumed.ResumeAfterKey, Is.EqualTo(_population[1]));
            Assert.That(status.State, Is.EqualTo(GrainIndexBackfillState.Completed));
            Assert.That(matched, Is.EquivalentTo(_population));
        });
    }

    [Test]
    public async Task A_completed_crawl_clears_the_registrys_needs_backfill_flag()
    {
        var backfill = Backfill();
        await backfill.EnsureStartedAsync();
        await DrainAsync(backfill);

        var record = await _fixture.SiloServices
            .GetRequiredService<IGrainIndexRegistryStore>()
            .ReadAsync(GrainIndexBackfillClusterFixture.IndexName, CancellationToken.None);

        Assert.That(record!.NeedsBackfill, Is.False,
            "The flag is what a silo start reads to decide whether the index still owes a crawl.");
    }
}
