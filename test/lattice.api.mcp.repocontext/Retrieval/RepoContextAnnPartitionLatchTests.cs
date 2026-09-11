using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Vector;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Coverage for issue #2706: a plane whose training declined because the corpus
/// was below <see cref="RepoContextAnnOptions.MinimumTrainingCount"/> must be
/// re-evaluated once the corpus grows past it, rather than answering every query
/// by exhaustive scan forever.
/// <para>
/// The fixture drives the observed history of the acceptance rig: train below the
/// minimum so training declines, grow the corpus past the minimum, and assert the
/// plane partitions and begins serving approximate. The negative arms bind the
/// other half - a genuinely small corpus must still decline, and a corpus that
/// meets the minimum but cannot resolve two partitions must not be retrained on
/// every maintenance turn.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextAnnPartitionLatchTests
{
    private const string RepoId = "acme";

    private static readonly EmbeddingSpaceTag Space = new("test-model", 8, VectorNormalization.UnitL2);

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    /// <summary>
    /// Options whose <see cref="RepoContextAnnOptions.PartitionCount"/> is left
    /// unset, so the partition count is derived from the corpus the way a real
    /// deployment derives it.
    /// </summary>
    private static RepoContextAnnOptions Options(int minimumTrainingCount = 64) => new()
    {
        MinimumTrainingCount = minimumTrainingCount,
        Probes = 4,
        FlushAfterUpdates = 1,
        IngestBatchSize = 32,
        MaxItemsPerChunk = 8,

        // Zero, deliberately: the drift trigger must be unable to explain any
        // partitioning this fixture observes, so what it observes can only be the
        // threshold-crossing trigger under test.
        RetrainAfterUpdateFraction = 0d,
    };

    private sealed class Rig : IDisposable
    {
        public Rig(RepoContextAnnOptions? options = null)
        {
            Options = options ?? RepoContextAnnPartitionLatchTests.Options();
            Source = new InMemoryRepoContextVectorSource(Space);
            Store = new InMemoryVectorIndexStore();
            Partitioning = new RepoContextAnnPartitioningReporter();
            Handle = NewHandle();
        }

        public RepoContextAnnOptions Options { get; }

        public InMemoryRepoContextVectorSource Source { get; }

        public InMemoryVectorIndexStore Store { get; }

        public RepoContextAnnPartitioningReporter Partitioning { get; }

        public RepoContextAnnIndexHandle Handle { get; private set; }

        /// <summary>
        /// Replaces the handle over the same durable store and the same source: a
        /// process restart onto state an earlier process left behind. This is the
        /// shape a deployment already latched by #2706 takes when it is upgraded,
        /// so it is the shape the self-healing arm has to survive.
        /// </summary>
        public void Restart()
        {
            Handle.Dispose();
            Handle = NewHandle();
        }

        /// <summary>Seeds <paramref name="count"/> vectors around a unit ring.</summary>
        /// <param name="count">How many vectors to seed.</param>
        /// <param name="from">The first ordinal to seed, so a corpus can be grown.</param>
        public void SeedRing(int count, int from = 0)
        {
            for (var i = from; i < count; i++)
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

        public void Dispose()
        {
            Handle.Dispose();
            Partitioning.Dispose();
        }

        private RepoContextAnnIndexHandle NewHandle() => new(
            RepoId,
            Space,
            Source,
            Store,
            Options,
            RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space),
            NullLogger.Instance,
            Partitioning);
    }

    /// <summary>
    /// Advances until the handle stops changing, so an assertion binds the settled
    /// state rather than whichever step of the pipeline one advance happened to
    /// reach. Bounded, so a hot loop fails the test instead of hanging it.
    /// </summary>
    private static async Task<VectorIndexBuildProgress> SettleAsync(
        RepoContextAnnIndexHandle handle, CancellationToken cancellationToken, int maxAdvances = 32)
    {
        var progress = handle.Progress;
        for (var i = 0; i < maxAdvances; i++)
        {
            var next = await handle.AdvanceAsync(cancellationToken);
            if (next.Phase == progress.Phase
                && next.VectorsIndexed == progress.VectorsIndexed
                && next.PartitionsTotal == progress.PartitionsTotal
                && i > 0)
            {
                return next;
            }

            progress = next;
        }

        return progress;
    }

    [Test]
    public async Task A_plane_that_declined_below_the_minimum_partitions_once_the_corpus_crosses_it()
    {
        // The rig's own history. Eight vectors against a minimum of sixty-four, so
        // training declines and the plane serves exhaustively - correctly, at this
        // size.
        using var rig = new Rig();
        rig.SeedRing(8);
        await rig.Handle.EnsureBuiltAsync(Ct);
        var declined = await SettleAsync(rig.Handle, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(rig.Handle.IsServing, Is.True, "an unpartitioned plane still answers");
            Assert.That(declined.Phase, Is.EqualTo(VectorIndexBuildPhase.Ready));
            Assert.That(declined.PartitionsTotal, Is.Zero, "training declined: the corpus was below the minimum");
        });

        // The corpus grows past the minimum. Nothing else changes: no restart, no
        // configuration change, no forced retrain. This is the exact transition the
        // defect never noticed.
        rig.SeedRing(512, from: 8);
        var healed = await SettleAsync(rig.Handle, Ct);

        var search = await rig.Handle.SearchAsync(Rig.Unit(), 4, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(healed.VectorsIndexed, Is.EqualTo(512), "the whole grown corpus is indexed");
            Assert.That(healed.PartitionsTotal, Is.GreaterThan(1),
                "the corpus crossed the minimum, so the plane must now hold a partitioning");
            Assert.That(healed.IsReady, Is.True, "IsReady reports answering FROM the partitioning");
            Assert.That(search.State, Is.EqualTo(RepoContextAnnServingState.Approximate),
                "semantic retrieval is answered by the partitioning, not by an exhaustive scan");
            Assert.That(search.Matches, Is.Not.Empty);
        });
    }

    [Test]
    public async Task A_latched_plane_heals_from_a_cold_start_over_untouched_durable_state()
    {
        // The self-healing requirement, and the one arm that proves the fix reaches
        // an EXISTING deployment. The latched state is built, then the process
        // dies. The new process inherits durable state it did not write, with no
        // in-memory update counter, no operator action, and no re-index.
        using var rig = new Rig();
        rig.SeedRing(8);
        await rig.Handle.EnsureBuiltAsync(Ct);
        var declined = await SettleAsync(rig.Handle, Ct);
        Assert.That(declined.PartitionsTotal, Is.Zero, "precondition: the plane is latched unpartitioned");

        // The corpus grew while the old build was running - which is how the rig
        // reached 7,628 vectors across zero partitions - and only then does the
        // upgraded process start.
        rig.SeedRing(512, from: 8);
        rig.Restart();

        await rig.Handle.EnsureBuiltAsync(Ct);
        var healed = await SettleAsync(rig.Handle, Ct);
        var search = await rig.Handle.SearchAsync(Rig.Unit(), 4, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(healed.PartitionsTotal, Is.GreaterThan(1),
                "a deployment already in the latched state recovers on its own after the upgrade");
            Assert.That(search.State, Is.EqualTo(RepoContextAnnServingState.Approximate));
        });
    }

    [Test]
    public async Task A_genuinely_small_corpus_still_declines_and_is_not_retrained_in_a_loop()
    {
        // The negative control. Replacing a stuck state with a hot one is not a
        // fix, so a corpus that is legitimately below the minimum must be left
        // alone - not merely left unpartitioned, but left un-attempted.
        using var rig = new Rig();
        rig.SeedRing(8);
        await rig.Handle.EnsureBuiltAsync(Ct);

        for (var i = 0; i < 8; i++)
        {
            await rig.Handle.AdvanceAsync(Ct);
        }

        var snapshot = rig.Partitioning.Read();

        Assert.Multiple(() =>
        {
            Assert.That(rig.Handle.Progress.PartitionsTotal, Is.Zero,
                "eight vectors against a minimum of sixty-four is genuinely small");
            Assert.That(snapshot.Repartitioned + snapshot.RepartitionDeclined, Is.Zero,
                "no training was attempted at all: the corpus never crossed the minimum");
            Assert.That(snapshot.BelowMinimum, Is.GreaterThan(0),
                "and the state is observable as small rather than as stuck");
            Assert.That(snapshot.AboveMinimum, Is.Zero);
        });
    }

    [Test]
    public async Task A_corpus_that_meets_the_minimum_but_cannot_partition_is_attempted_on_a_widening_interval()
    {
        // The other hot-loop shape, and the one the threshold test cannot dismiss
        // by itself: the corpus DOES meet the minimum, so the trigger fires, but
        // the training still resolves fewer than two partitions and declines. Left
        // unguarded that is a full training pass on every maintenance turn,
        // forever. A minimum of one makes the case reachable - sqrt(1024) is 32, so
        // the shipped default cannot reach it, but a deployment that configures a
        // minimum this low can.
        using var rig = new Rig(Options(minimumTrainingCount: 1));
        rig.SeedRing(2);
        await rig.Handle.EnsureBuiltAsync(Ct);

        for (var i = 0; i < 16; i++)
        {
            await rig.Handle.AdvanceAsync(Ct);
        }

        var snapshot = rig.Partitioning.Read();

        Assert.Multiple(() =>
        {
            Assert.That(rig.Handle.Progress.PartitionsTotal, Is.Zero,
                "two vectors cannot resolve two partitions whatever the minimum says");
            Assert.That(snapshot.RepartitionDeclined, Is.GreaterThan(0),
                "the attempt was made, so the widening interval is bounding a real path");
            Assert.That(snapshot.RepartitionDeclined, Is.LessThan(4),
                "and it is bounded: sixteen maintenance turns must not cost sixteen training passes");
            Assert.That(snapshot.Repartitioned, Is.Zero);
            Assert.That(snapshot.AboveMinimum, Is.GreaterThan(0),
                "unpartitioned although the corpus meets the minimum is exactly the case the "
                + "instrument exists to distinguish");
        });
    }

    [Test]
    public void Every_partitioning_arm_is_primed_at_zero_before_anything_is_recorded()
    {
        // A counter exports no series until its first Add, so an un-primed zero is
        // indistinguishable from a missing instrument - and "the arm is missing"
        // and "the arm is zero" are opposite diagnoses of this defect.
        var measured = new List<(string Instrument, string Tag, long Value)>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Name is RepoContextAnnPartitioningReporter.PartitioningInstrumentName
                or RepoContextAnnPartitioningReporter.RepartitionInstrumentName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            var tag = string.Empty;
            foreach (var pair in tags)
            {
                if (pair.Key is RepoContextAnnPartitioningReporter.StateTagKey
                    or RepoContextAnnPartitioningReporter.OutcomeTagKey)
                {
                    tag = pair.Value?.ToString() ?? string.Empty;
                }
            }

            measured.Add((instrument.Name, tag, value));
        });
        listener.Start();

        using var reporter = new RepoContextAnnPartitioningReporter();

        Assert.Multiple(() =>
        {
            Assert.That(measured, Has.Count.EqualTo(5), "three partitioning arms and two repartition outcomes");
            Assert.That(measured.Select(m => m.Value), Has.All.Zero, "primed, so priming cannot be mistaken for use");
            Assert.That(
                measured.Where(m => m.Instrument == RepoContextAnnPartitioningReporter.PartitioningInstrumentName)
                    .Select(m => m.Tag),
                Is.EquivalentTo(new[]
                {
                    RepoContextAnnPartitioningReporter.StatePartitionedTag,
                    RepoContextAnnPartitioningReporter.StateUnpartitionedSmallTag,
                    RepoContextAnnPartitioningReporter.StateUnpartitionedLargeTag,
                }));
            Assert.That(
                measured.Where(m => m.Instrument == RepoContextAnnPartitioningReporter.RepartitionInstrumentName)
                    .Select(m => m.Tag),
                Is.EquivalentTo(new[]
                {
                    RepoContextAnnPartitioningReporter.OutcomePartitionedTag,
                    RepoContextAnnPartitioningReporter.OutcomeDeclinedTag,
                }));
        });
    }

    [Test]
    public void Classification_separates_a_small_corpus_from_a_latched_one()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextAnnPartitioningReporter.Classify(partitionCount: 4, vectorCount: 512, minimumTrainingCount: 64),
                Is.EqualTo(RepoContextAnnPartitioningState.Partitioned));
            Assert.That(
                RepoContextAnnPartitioningReporter.Classify(partitionCount: 0, vectorCount: 8, minimumTrainingCount: 64),
                Is.EqualTo(RepoContextAnnPartitioningState.BelowMinimum),
                "unpartitioned because the corpus is genuinely small");
            Assert.That(
                RepoContextAnnPartitioningReporter.Classify(partitionCount: 0, vectorCount: 512, minimumTrainingCount: 64),
                Is.EqualTo(RepoContextAnnPartitioningState.AboveMinimum),
                "unpartitioned ALTHOUGH the corpus is large: the #2706 state");
            Assert.That(
                RepoContextAnnPartitioningReporter.Classify(partitionCount: 0, vectorCount: 64, minimumTrainingCount: 64),
                Is.EqualTo(RepoContextAnnPartitioningState.AboveMinimum),
                "the minimum is inclusive, matching the training predicate");
        });
    }

    [Test]
    public async Task The_partitioning_state_is_reported_as_large_while_latched_and_partitioned_once_healed()
    {
        // The instrument has to move across the transition, not merely exist: a
        // reading that never leaves unpartitioned-large would report the fix as
        // having failed, and one that never entered it would not have observed the
        // defect at all.
        using var rig = new Rig();
        rig.SeedRing(8);
        await rig.Handle.EnsureBuiltAsync(Ct);
        await SettleAsync(rig.Handle, Ct);
        var small = rig.Partitioning.Read();

        rig.SeedRing(512, from: 8);
        await SettleAsync(rig.Handle, Ct);
        var healed = rig.Partitioning.Read();

        Assert.Multiple(() =>
        {
            Assert.That(small.BelowMinimum, Is.GreaterThan(0));
            Assert.That(small.Partitioned, Is.Zero);
            Assert.That(healed.Repartitioned, Is.EqualTo(1), "one training pass healed it, not a loop of them");
            Assert.That(healed.Partitioned, Is.GreaterThan(0), "and the plane now reports as partitioned");
        });
    }
}
