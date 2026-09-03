using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Vector;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit coverage for <see cref="RepoContextAnnIndexHandle"/> driven directly, so
/// the arms a registry-level test cannot reach are exercised: the shortfall repair
/// when the source cannot be counted at all, the retrain the maintenance turn owns,
/// the write and flush short-circuits, and the post-dispose behaviour.
/// <para>
/// The handle is advanced a step at a time and nothing here starts work of its
/// own, so no assertion depends on a clock, a delay, or a race with a background
/// task.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextAnnIndexHandleTests
{
    private const string RepoId = "acme";

    private static readonly EmbeddingSpaceTag Space = new("test-model", 8, VectorNormalization.UnitL2);

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static RepoContextAnnOptions Options(double retrainFraction = 0d) => new()
    {
        MinimumTrainingCount = 8,
        PartitionCount = 4,
        Probes = 4,
        FlushAfterUpdates = 1,
        IngestBatchSize = 16,
        MaxItemsPerChunk = 8,
        RetrainAfterUpdateFraction = retrainFraction,
    };

    private sealed class Rig : IDisposable
    {
        public Rig(RepoContextAnnOptions? options = null, ILogger? logger = null)
        {
            Options = options ?? RepoContextAnnIndexHandleTests.Options();
            Source = new InMemoryRepoContextVectorSource(Space);
            Store = new InMemoryVectorIndexStore();
            Handle = NewHandle(logger);
        }

        public RepoContextAnnOptions Options { get; }

        public InMemoryRepoContextVectorSource Source { get; }

        public InMemoryVectorIndexStore Store { get; }

        public RepoContextAnnIndexHandle Handle { get; private set; }

        /// <summary>Replaces the handle over the same durable store: a process restart.</summary>
        public void Restart(ILogger? logger = null)
        {
            Handle.Dispose();
            Handle = NewHandle(logger);
        }

        public void SeedRing(int count)
        {
            for (var i = 0; i < count; i++)
            {
                var angle = 2d * Math.PI * i / count;
                var vector = new float[Space.Dimension];
                vector[0] = (float)Math.Cos(angle);
                vector[1] = (float)Math.Sin(angle);
                Source.Set($"vec-{i:D6}", RepoContextKeys.File(RepoId, $"src/File{i}.cs"), vector);
            }
        }

        public static float[] Unit(int axis = 0)
        {
            var vector = new float[Space.Dimension];
            vector[axis] = 1f;
            return vector;
        }

        public void Dispose() => Handle.Dispose();

        private RepoContextAnnIndexHandle NewHandle(ILogger? logger) => new(
            RepoId,
            Space,
            Source,
            Store,
            Options,
            RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space),
            logger ?? NullLogger.Instance);
    }

    [Test]
    public void Rejects_its_null_arguments()
    {
        var source = new InMemoryRepoContextVectorSource(Space);
        var store = new InMemoryVectorIndexStore();
        var options = Options();
        const string Prefix = "repo/acme/vidx/abc/";

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new RepoContextAnnIndexHandle(
                    null!, Space, source, store, options, Prefix, NullLogger.Instance),
                Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextAnnIndexHandle(
                    RepoId, Space, null!, store, options, Prefix, NullLogger.Instance),
                Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextAnnIndexHandle(
                    RepoId, Space, source, null!, options, Prefix, NullLogger.Instance),
                Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextAnnIndexHandle(
                    RepoId, Space, source, store, null!, Prefix, NullLogger.Instance),
                Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextAnnIndexHandle(
                    RepoId, Space, source, store, options, null!, NullLogger.Instance),
                Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextAnnIndexHandle(
                    RepoId, Space, source, store, options, Prefix, null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task A_write_with_nothing_to_apply_touches_nothing()
    {
        using var rig = new Rig();
        rig.SeedRing(16);
        await rig.Handle.EnsureBuiltAsync(Ct);
        var writes = rig.Store.RecordsWritten;

        await rig.Handle.ApplyWriteAsync([], [], Ct);

        Assert.That(rig.Store.RecordsWritten, Is.EqualTo(writes),
            "an empty write must not take the maintenance turn, let alone persist a generation");
    }

    [Test]
    public async Task A_write_whose_vectors_are_the_wrong_width_applies_nothing()
    {
        using var rig = new Rig();
        rig.SeedRing(16);
        await rig.Handle.EnsureBuiltAsync(Ct);
        var writes = rig.Store.RecordsWritten;

        // A vector from a different embedding space cannot be indexed here; taking
        // it would corrupt the partitioning with a value of the wrong dimension.
        var wrongWidth = new RepoContextAnnVectorUpdate("vec-999999", RepoContextKeys.File(RepoId, "src/Wide.cs"), new float[Space.Dimension + 1]);

        await rig.Handle.ApplyWriteAsync([wrongWidth], [], Ct);

        Assert.That(rig.Store.RecordsWritten, Is.EqualTo(writes),
            "nothing applied means nothing to flush, so no generation is written");
    }

    [Test]
    public async Task A_flush_with_nothing_pending_is_a_no_op()
    {
        using var rig = new Rig();
        rig.SeedRing(16);
        await rig.Handle.EnsureBuiltAsync(Ct);
        var writes = rig.Store.RecordsWritten;

        await rig.Handle.FlushAsync(Ct);

        Assert.That(rig.Store.RecordsWritten, Is.EqualTo(writes));
    }

    [Test]
    public async Task A_flush_before_the_index_opens_is_a_no_op()
    {
        using var rig = new Rig();
        rig.SeedRing(16);

        await rig.Handle.FlushAsync(Ct);

        Assert.That(rig.Store.RecordsWritten, Is.Zero, "no index is open, so there is nothing to persist");
    }

    [Test]
    public async Task A_disposed_handle_declines_every_operation_without_throwing()
    {
        var rig = new Rig();
        rig.SeedRing(16);
        await rig.Handle.EnsureBuiltAsync(Ct);
        Assert.That(rig.Handle.IsServing, Is.True, "precondition: the index served before shutdown");
        var writes = rig.Store.RecordsWritten;

        rig.Handle.Dispose();
        rig.Handle.Dispose();

        var outcome = await rig.Handle.SearchAsync(Rig.Unit(), 5, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.State, Is.EqualTo(RepoContextAnnServingState.Bootstrapping),
                "a shutdown must read as 'not serving', never as a capability loss");
            Assert.That(async () => await rig.Handle.ApplyWriteAsync(
                [new RepoContextAnnVectorUpdate("vec-999999", RepoContextKeys.File(RepoId, "src/Late.cs"), Rig.Unit())], [], Ct), Throws.Nothing);
            Assert.That(async () => await rig.Handle.FlushAsync(Ct), Throws.Nothing);
        });
        Assert.That(rig.Store.RecordsWritten, Is.EqualTo(writes));
    }

    [Test]
    public async Task A_source_that_cannot_be_counted_is_repaired_rather_than_abandoned()
    {
        // The count is only a hint that decides whether to SKIP the repair, so
        // failing to obtain it must mean "repair", not "give up". Letting the abort
        // propagate is what made a whole index build fail on a real deployment
        // (#1844): the probe walks the repository's entire vector prefix and on a
        // large cold tree can outrun even a generous reconnect budget.
        using var rig = new Rig();
        rig.SeedRing(16);
        await rig.Handle.EnsureBuiltAsync(Ct);

        // Restart onto the persisted index, and add a vector the persisted index
        // does not hold, so there is a genuine shortfall to repair.
        rig.Restart();
        rig.Source.Set("vec-999999", RepoContextKeys.File(RepoId, "src/Late.cs"), Rig.Unit(1));
        rig.Source.FailNextCounts(1, static () => new EnumerationAbortedException("reconnect budget exhausted"));

        Assert.That(async () => await rig.Handle.EnsureBuiltAsync(Ct), Throws.Nothing,
            "an uncountable source must not fail the build");

        var outcome = await rig.Handle.SearchAsync(Rig.Unit(1), 5, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(rig.Handle.IsServing, Is.True);
            Assert.That(outcome.State, Is.Not.EqualTo(RepoContextAnnServingState.Bootstrapping));
            Assert.That(outcome.Matches.Select(static m => m.VectorId), Does.Contain("vec-999999"),
                "treating 'unknown' as 'possibly behind' is what keeps the repair on the safe path");
        });
    }

    [Test]
    public async Task A_partitioning_the_corpus_has_drifted_away_from_is_retrained()
    {
        // Only a trained index can drift, and retraining rewrites every partition,
        // so it deliberately runs on the maintenance turn a WRITE took - never on a
        // query's turn.
        var log = new CapturingLoggerProvider();
        using var rig = new Rig(Options(retrainFraction: 0.1d), log.CreateLogger("handle"));
        rig.SeedRing(16);
        await rig.Handle.EnsureBuiltAsync(Ct);

        var upserts = new List<RepoContextAnnVectorUpdate>();
        for (var i = 0; i < 8; i++)
        {
            upserts.Add(new RepoContextAnnVectorUpdate($"vec-9{i:D5}", RepoContextKeys.File(RepoId, $"src/New{i}.cs"), Rig.Unit(i % Space.Dimension)));
        }

        await rig.Handle.ApplyWriteAsync(upserts, [], Ct);

        Assert.That(
            log.Entries.Any(e => e.Message.Contains("retraining after", StringComparison.Ordinal)),
            Is.True,
            "a corpus that has moved a quarter of the way from its partitioning must be repartitioned");
    }

    [Test]
    public async Task A_below_threshold_write_flushes_without_retraining()
    {
        var log = new CapturingLoggerProvider();
        using var rig = new Rig(Options(retrainFraction: 0.9d), log.CreateLogger("handle"));
        rig.SeedRing(16);
        await rig.Handle.EnsureBuiltAsync(Ct);

        await rig.Handle.ApplyWriteAsync(
            [new RepoContextAnnVectorUpdate("vec-999999", RepoContextKeys.File(RepoId, "src/Late.cs"), Rig.Unit(2))], [], Ct);

        Assert.That(
            log.Entries.Any(e => e.Message.Contains("retraining after", StringComparison.Ordinal)),
            Is.False,
            "retraining rewrites every partition, so a handful of updates must never trigger it");
    }

    [Test]
    public async Task A_restart_reports_the_index_as_restored_from_durable_state()
    {
        using var rig = new Rig();
        rig.SeedRing(16);
        await rig.Handle.EnsureBuiltAsync(Ct);

        Assert.That(rig.Handle.RestoredFromDurableState, Is.False,
            "this process streamed the corpus itself, so it was recomputed, not loaded in");

        var streamedBeforeRestart = rig.Source.FullEnumerations;
        rig.Restart();
        await rig.Handle.EnsureBuiltAsync(Ct);

        Assert.Multiple(() =>
        {
            Assert.That(rig.Handle.RestoredFromDurableState, Is.True,
                "the cold-start attribution signal must distinguish 'loaded in' from 'recomputed'");
            Assert.That(rig.Handle.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ready));
            Assert.That(rig.Source.FullEnumerations, Is.EqualTo(streamedBeforeRestart),
                "a restored index must not re-stream the whole corpus");
        });
    }

    [Test]
    public async Task Advancing_an_already_serving_index_stays_serving()
    {
        using var rig = new Rig();
        rig.SeedRing(16);
        await rig.Handle.EnsureBuiltAsync(Ct);

        var again = await rig.Handle.AdvanceAsync(Ct);

        Assert.Multiple(() =>
        {
            Assert.That(again.Phase, Is.EqualTo(VectorIndexBuildPhase.Ready));
            Assert.That(rig.Handle.IsServing, Is.True,
                "the serving announcement is made once; a further advance must not un-serve the index");
        });
    }

    [Test]
    public async Task A_retirement_is_applied_and_persisted()
    {
        using var rig = new Rig();
        rig.SeedRing(16);
        await rig.Handle.EnsureBuiltAsync(Ct);

        await rig.Handle.ApplyWriteAsync([], ["vec-000003"], Ct);
        var outcome = await rig.Handle.SearchAsync(Rig.Unit(), 16, Ct);

        Assert.That(outcome.Matches.Select(static m => m.VectorId), Does.Not.Contain("vec-000003"),
            "a retired vector must stop being answerable, or the index becomes a stale second copy");
    }

    [Test]
    public void A_write_rejects_its_null_arguments()
    {
        using var rig = new Rig();

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await rig.Handle.ApplyWriteAsync(null!, [], Ct), Throws.ArgumentNullException);
            Assert.That(
                async () => await rig.Handle.ApplyWriteAsync([], null!, Ct), Throws.ArgumentNullException);
        });
    }
}
