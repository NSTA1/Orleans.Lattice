using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for <see cref="RepoContextAnnIndexRegistry"/>, the persisted
/// approximate retrieval plane. They cover the properties the whole change turns
/// on: that a still-building index declines to answer rather than answering
/// incompletely, that a built index answers from its partitioning, that a restart
/// reloads rather than rebuilds, that a local write is applied precisely, and
/// that a retired vector can never come back.
/// </summary>
[TestFixture]
public sealed class RepoContextAnnIndexRegistryTests
{
    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static float[] Query(double angle)
    {
        var vector = new float[AnnPlaneFixture.Space.Dimension];
        vector[0] = (float)Math.Cos(angle);
        vector[1] = (float)Math.Sin(angle);
        return vector;
    }

    [Test]
    public async Task Search_before_any_build_reports_bootstrapping_and_returns_nothing()
    {
        using var fixture = new AnnPlaneFixture();
        fixture.SeedRing(16);

        var outcome = await fixture.SearchAsync(Query(0d), 5, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.State, Is.EqualTo(RepoContextAnnServingState.Bootstrapping),
                "A plane with no index yet must decline, so the caller serves the exact scan.");
            Assert.That(outcome.Matches, Is.Empty,
                "A declining plane answers nothing rather than answering from a corpus it has not ingested.");
        });
    }

    [Test]
    public void Progress_is_absent_until_an_index_is_opened_for_the_pair()
    {
        using var fixture = new AnnPlaneFixture();

        var known = fixture.Registry.TryGetProgress(
            AnnPlaneFixture.RepoId, AnnPlaneFixture.Space, out _);

        Assert.That(known, Is.False,
            "Nothing has been opened, so the honest answer is that there is no index to report on.");
    }

    [Test]
    public async Task Retrieval_keeps_working_and_reports_the_build_state_while_the_index_is_building()
    {
        // A batch size below the corpus guarantees the build needs several steps, so
        // there is a real mid-build window to observe rather than an inferred one.
        using var fixture = new AnnPlaneFixture(new RepoContextAnnOptions
        {
            AutoBuild = false,
            MinimumTrainingCount = 8,
            PartitionCount = 4,
            Probes = 4,
            IngestBatchSize = 4,
            MaxItemsPerChunk = 8,
        });
        fixture.SeedRing(32);

        // Two steps: the first starts the build (it counts the source and reserves),
        // the second ingests one batch. With a batch of 4 against a corpus of 32 the
        // index is now genuinely part-way through, which is the window to observe.
        await fixture.Registry.BuildStepAsync(AnnPlaneFixture.RepoId, AnnPlaneFixture.Space, Ct);
        var afterFirstStep = await fixture.Registry.BuildStepAsync(
            AnnPlaneFixture.RepoId, AnnPlaneFixture.Space, Ct);
        var midBuild = await fixture.SearchAsync(Query(0d), 5, Ct);
        var reported = fixture.Registry.TryGetProgress(
            AnnPlaneFixture.RepoId, AnnPlaneFixture.Space, out var progress);

        await fixture.BuildAsync(Ct);
        var afterBuild = await fixture.SearchAsync(Query(0d), 5, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(afterFirstStep.Phase, Is.Not.EqualTo(VectorIndexBuildPhase.Ready),
                "The corpus is eight ingest batches wide, so two steps cannot complete the build.");
            Assert.That(midBuild.State, Is.EqualTo(RepoContextAnnServingState.Bootstrapping),
                "Mid-build the plane declines, so retrieval continues through the exact path with complete recall.");
            Assert.That(reported, Is.True,
                "An index that is building is still an index the host can ask about.");
            Assert.That(progress.IsReady, Is.False,
                "The build state is reported honestly rather than as a finished index.");
            Assert.That(progress.VectorsIndexed, Is.InRange(1, 31),
                "The mid-build report carries the work actually done - part of the corpus, not all and not none.");
            Assert.That(progress.VectorsExpected, Is.EqualTo(32),
                "And it knows how much work there is, so the fraction it reports is a fact rather than a guess.");
            Assert.That(afterBuild.State, Is.Not.EqualTo(RepoContextAnnServingState.Bootstrapping),
                "Once the build completes the plane answers for itself.");
        });
    }

    [Test]
    public async Task A_corpus_below_the_training_threshold_serves_exhaustively_rather_than_approximately()
    {
        using var fixture = new AnnPlaneFixture(new RepoContextAnnOptions
        {
            AutoBuild = false,
            // Above the corpus, so the build legitimately finishes with no partitioning.
            MinimumTrainingCount = 4_096,
        });
        fixture.SeedRing(12);
        await fixture.BuildAsync(Ct);

        var outcome = await fixture.SearchAsync(Query(0d), 3, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.State, Is.EqualTo(RepoContextAnnServingState.Exhaustive),
                "An index with no partitioning answers by exhaustive scan, which is exact and not approximate.");
            Assert.That(outcome.Matches, Has.Count.EqualTo(3),
                "Warming up is slower, never a smaller answer.");
            Assert.That(outcome.Matches[0].VectorId, Is.EqualTo(AnnPlaneFixture.Id(0)),
                "An exhaustive answer is exact, so the true nearest neighbour ranks first.");
        });
    }

    [Test]
    public async Task A_trained_index_serves_approximately_and_hydrates_the_canonical_source_key()
    {
        using var fixture = new AnnPlaneFixture();
        fixture.SeedRing(64);
        await fixture.BuildAsync(Ct);

        var outcome = await fixture.SearchAsync(Query(0d), 4, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.State, Is.EqualTo(RepoContextAnnServingState.Approximate),
                "A trained index answers from its partitioning, and says so.");
            Assert.That(outcome.Matches, Is.Not.Empty, "The trained index answers the query.");
            Assert.That(outcome.Matches[0].VectorId, Is.EqualTo(AnnPlaneFixture.Id(0)),
                "The query sits exactly on the first ring vector, so it must rank first.");
            Assert.That(
                outcome.Matches[0].SourceKey,
                Is.EqualTo(RepoContextKeys.File(AnnPlaneFixture.RepoId, "src/File0.cs")),
                "Every match carries the canonical key the caller hydrates the record from.");
        });
    }

    [Test]
    public async Task A_match_the_store_of_record_no_longer_resolves_is_dropped()
    {
        using var fixture = new AnnPlaneFixture();
        fixture.SeedRing(64);
        await fixture.BuildAsync(Ct);

        // Removed behind the plane's back, exactly as a coherence sweep or a
        // cross-cluster deletion would: the index still holds the vector, but the
        // store of record does not, and the store of record always wins.
        fixture.Source.Remove(AnnPlaneFixture.Id(0));

        var outcome = await fixture.SearchAsync(Query(0d), 4, Ct);

        Assert.That(
            outcome.Matches.Select(match => match.VectorId),
            Does.Not.Contain(AnnPlaneFixture.Id(0)),
            "A hit the store of record will not stand behind is dropped rather than returned.");
    }

    [Test]
    public async Task A_local_write_is_applied_so_the_new_vector_is_immediately_findable()
    {
        using var fixture = new AnnPlaneFixture();
        fixture.SeedRing(64);
        await fixture.BuildAsync(Ct);

        var target = Query(0.05d);
        var added = new float[AnnPlaneFixture.Space.Dimension];
        added[0] = target[0];
        added[1] = target[1];

        var sourceKey = RepoContextKeys.File(AnnPlaneFixture.RepoId, "src/Added.cs");
        fixture.Seed("vec-added", sourceKey, added);
        await fixture.Registry.ApplyWriteAsync(
            AnnPlaneFixture.RepoId,
            AnnPlaneFixture.Space,
            [new RepoContextAnnVectorUpdate("vec-added", sourceKey, added)],
            [],
            Ct);

        var outcome = await fixture.SearchAsync(target, 3, Ct);

        Assert.That(outcome.Matches[0].VectorId, Is.EqualTo("vec-added"),
            "A vector written after the build is in the index, so an exactly matching query returns it first.");
    }

    [Test]
    public async Task A_retired_vector_is_never_returned_again()
    {
        using var fixture = new AnnPlaneFixture();
        fixture.SeedRing(64);
        await fixture.BuildAsync(Ct);

        var before = await fixture.SearchAsync(Query(0d), 3, Ct);

        // The write seam retires the identifier and the store of record loses it, in
        // that order, which is the order the writer applies them in.
        fixture.Source.Remove(AnnPlaneFixture.Id(0));
        await fixture.Registry.ApplyWriteAsync(
            AnnPlaneFixture.RepoId, AnnPlaneFixture.Space, [], [AnnPlaneFixture.Id(0)], Ct);

        var after = await fixture.SearchAsync(Query(0d), 3, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(before.Matches[0].VectorId, Is.EqualTo(AnnPlaneFixture.Id(0)),
                "The vector was findable before it was retired, so the assertion below is not vacuous.");
            Assert.That(
                after.Matches.Select(match => match.VectorId),
                Does.Not.Contain(AnnPlaneFixture.Id(0)),
                "A retired vector is a ghost if it comes back, whatever the store of record says.");
        });
    }

    [Test]
    public async Task A_retirement_applies_across_every_space_the_repository_holds_an_index_for()
    {
        using var fixture = new AnnPlaneFixture();
        fixture.SeedRing(64);
        await fixture.BuildAsync(Ct);

        fixture.Source.Remove(AnnPlaneFixture.Id(0));
        await fixture.Registry.ApplyRetirementAsync(
            AnnPlaneFixture.RepoId, [AnnPlaneFixture.Id(0)], Ct);

        var after = await fixture.SearchAsync(Query(0d), 3, Ct);

        Assert.That(
            after.Matches.Select(match => match.VectorId),
            Does.Not.Contain(AnnPlaneFixture.Id(0)),
            "A whole-source retirement does not know the space it was written under, so it applies to all of them.");
    }

    [Test]
    public async Task Applying_a_write_for_a_pair_with_no_open_index_is_a_no_op()
    {
        using var fixture = new AnnPlaneFixture();
        var sourceKey = RepoContextKeys.File(AnnPlaneFixture.RepoId, "src/Added.cs");

        await fixture.Registry.ApplyWriteAsync(
            AnnPlaneFixture.RepoId,
            AnnPlaneFixture.Space,
            [new RepoContextAnnVectorUpdate("vec-added", sourceKey, new float[AnnPlaneFixture.Space.Dimension])],
            [],
            Ct);

        Assert.Multiple(() =>
        {
            Assert.That(
                fixture.Registry.TryGetProgress(AnnPlaneFixture.RepoId, AnnPlaneFixture.Space, out _),
                Is.False,
                "A write must never be the thing that starts an expensive build.");
            Assert.That(fixture.Store.RecordsWritten, Is.Zero,
                "Nothing was persisted, because there is no index for the write to maintain.");
        });
    }

    [Test]
    public async Task A_restart_reloads_the_persisted_index_rather_than_rebuilding_it()
    {
        using var fixture = new AnnPlaneFixture();
        fixture.SeedRing(64);
        await fixture.BuildAsync(Ct);
        var enumerationsAfterFirstBuild = fixture.Source.FullEnumerations;

        fixture.Restart();
        await fixture.BuildAsync(Ct);
        fixture.Registry.TryGetProgress(AnnPlaneFixture.RepoId, AnnPlaneFixture.Space, out var progress);
        var outcome = await fixture.SearchAsync(Query(0d), 3, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(progress.RestoredFromDurableState, Is.True,
                "The second start loaded the persisted index; it did not recompute it.");
            Assert.That(progress.VectorsIndexed, Is.EqualTo(64),
                "A reload brings back the whole corpus, not a prefix of it.");
            Assert.That(fixture.Source.FullEnumerations, Is.EqualTo(enumerationsAfterFirstBuild),
                "Reloading must not re-stream the store of record: that is the cold-start cost being removed.");
            Assert.That(outcome.State, Is.EqualTo(RepoContextAnnServingState.Approximate),
                "A reloaded index serves immediately, without a second build.");
        });
    }

    [Test]
    public async Task An_index_behind_the_store_of_record_catches_up_when_it_is_opened()
    {
        using var fixture = new AnnPlaneFixture(new RepoContextAnnOptions
        {
            AutoBuild = false,
            MinimumTrainingCount = 8,
            PartitionCount = 4,
            Probes = 4,
            // Nothing is flushed on the maintenance path, so a restart deliberately
            // reproduces the unclean-stop case the catch-up exists to repair.
            FlushAfterUpdates = int.MaxValue,
        });
        fixture.SeedRing(64);
        await fixture.BuildAsync(Ct);

        // Landed in the store of record but never persisted into the index.
        var late = new float[AnnPlaneFixture.Space.Dimension];
        late[0] = 1f;
        late[1] = 0.01f;
        var sourceKey = RepoContextKeys.File(AnnPlaneFixture.RepoId, "src/Late.cs");
        fixture.Seed("vec-zlate", sourceKey, late);

        fixture.Restart();
        await fixture.BuildAsync(Ct);
        fixture.Registry.TryGetProgress(AnnPlaneFixture.RepoId, AnnPlaneFixture.Space, out var progress);

        // A repair that is not persisted is repeated on every start, so the second
        // restart is the assertion that actually proves it was durable.
        var enumerationsAfterRepair = fixture.Source.FullEnumerations;
        fixture.Restart();
        await fixture.BuildAsync(Ct);
        fixture.Registry.TryGetProgress(AnnPlaneFixture.RepoId, AnnPlaneFixture.Space, out var second);

        Assert.Multiple(() =>
        {
            Assert.That(progress.VectorsIndexed, Is.EqualTo(65),
                "A persisted index behind the store of record repairs itself on open rather than serving short.");
            Assert.That(second.VectorsIndexed, Is.EqualTo(65),
                "And the repaired index is what the next start loads.");
            Assert.That(fixture.Source.FullEnumerations, Is.EqualTo(enumerationsAfterRepair),
                "The repair was made durable, so the next start does not re-stream the store of record for it.");
        });
    }

    [Test]
    public async Task Disposing_the_registry_stops_it_answering()
    {
        var fixture = new AnnPlaneFixture();
        fixture.SeedRing(64);
        await fixture.BuildAsync(Ct);
        var registry = fixture.Registry;

        registry.Dispose();
        var outcome = await registry.SearchAsync(
            AnnPlaneFixture.RepoId, Query(0d), AnnPlaneFixture.Space, 3, Ct);

        Assert.That(outcome.State, Is.EqualTo(RepoContextAnnServingState.Bootstrapping),
            "A disposed plane declines rather than touching a store it has released.");
        fixture.Dispose();
    }

    [Test]
    public async Task Flushing_persists_maintenance_the_index_was_holding()
    {
        using var fixture = new AnnPlaneFixture(new RepoContextAnnOptions
        {
            AutoBuild = false,
            MinimumTrainingCount = 8,
            PartitionCount = 4,
            Probes = 4,
            FlushAfterUpdates = int.MaxValue,
        });
        fixture.SeedRing(64);
        await fixture.BuildAsync(Ct);

        var added = new float[AnnPlaneFixture.Space.Dimension];
        added[0] = 1f;
        var sourceKey = RepoContextKeys.File(AnnPlaneFixture.RepoId, "src/Added.cs");
        fixture.Seed("vec-zadded", sourceKey, added);
        await fixture.Registry.ApplyWriteAsync(
            AnnPlaneFixture.RepoId,
            AnnPlaneFixture.Space,
            [new RepoContextAnnVectorUpdate("vec-zadded", sourceKey, added)],
            [],
            Ct);

        var beforeFlush = fixture.Store.RecordsWritten;
        await fixture.Registry.FlushAsync(AnnPlaneFixture.RepoId, AnnPlaneFixture.Space, Ct);

        Assert.That(fixture.Store.RecordsWritten, Is.GreaterThan(beforeFlush),
            "A flush is what makes a maintenance update outlive the process.");
    }

    [Test]
    public void Flushing_a_pair_with_no_open_index_is_a_no_op()
    {
        using var fixture = new AnnPlaneFixture();

        Assert.That(
            async () => await fixture.Registry.FlushAsync(AnnPlaneFixture.RepoId, AnnPlaneFixture.Space, Ct),
            Throws.Nothing,
            "Flushing a pair nothing was ever opened for is meaningless, not an error.");
    }

    [Test]
    public void Constructing_with_a_null_dependency_is_rejected()
    {
        var options = AnnPlaneFixture.DefaultOptions();
        var factory = new InMemoryAnnBackingFactory();
        var logger = NullLogger<RepoContextAnnIndexRegistry>.Instance;

        Assert.Multiple(() =>
        {
            Assert.That(() => new RepoContextAnnIndexRegistry(null!, options, logger),
                Throws.ArgumentNullException);
            Assert.That(() => new RepoContextAnnIndexRegistry(factory, null!, logger),
                Throws.ArgumentNullException);
            Assert.That(() => new RepoContextAnnIndexRegistry(factory, options, null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Search_and_maintenance_reject_invalid_arguments()
    {
        using var fixture = new AnnPlaneFixture();

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await fixture.Registry.SearchAsync(
                    null!, Query(0d), AnnPlaneFixture.Space, 3, Ct),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await fixture.Registry.SearchAsync(
                    AnnPlaneFixture.RepoId, Query(0d), AnnPlaneFixture.Space, 0, Ct),
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(
                () => fixture.Registry.TryGetProgress(null!, AnnPlaneFixture.Space, out _),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await fixture.Registry.ApplyWriteAsync(
                    AnnPlaneFixture.RepoId, AnnPlaneFixture.Space, null!, [], Ct),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await fixture.Registry.ApplyWriteAsync(
                    AnnPlaneFixture.RepoId, AnnPlaneFixture.Space, [], null!, Ct),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await fixture.Registry.ApplyRetirementAsync(null!, [], Ct),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await fixture.Registry.ApplyRetirementAsync(AnnPlaneFixture.RepoId, null!, Ct),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await fixture.Registry.EnsureBuiltAsync(null!, AnnPlaneFixture.Space, Ct),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await fixture.Registry.BuildStepAsync(null!, AnnPlaneFixture.Space, Ct),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await fixture.Registry.FlushAsync(null!, AnnPlaneFixture.Space, Ct),
                Throws.ArgumentNullException);
        });
    }
}
