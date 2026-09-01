using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Vector.Persistence;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Coverage for the third index transition: <b>space to space</b>, which is what a
/// model, dimension, or normalization change produces (#1872).
/// <para>
/// The other two transitions were already correct and are covered elsewhere.
/// Adoption (no index to index) answers exactly from the fall-back scan until the
/// build completes. Retrain (generation to generation within one space) writes a
/// fresh generation and flips the manifest last, so the previous generation serves
/// until the swap and is then retired by the durable index's own prefix delete -
/// asserted in <c>DurableVectorIndexRoundTripTests</c> and
/// <c>DurableVectorIndexIncrementalTests</c>.
/// </para>
/// <para>
/// A space change is different in kind, because the two indexes deliberately live
/// under different prefixes - retirement works by prefix delete, so sharing one
/// would have them deleting each other's generations. The consequence is that
/// nothing reclaimed the abandoned prefix: invisible to queries, harmless to
/// correctness, and permanently resident at hundreds of megabytes per abandoned
/// space. This is what reclaims it, strictly after the replacement is Ready so a
/// failed re-embed can still fall back.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextAnnSupersededSpaceReclamationTests
{
    private const string RepoId = "acme";
    private const int MaxTicks = 512;

    private static readonly EmbeddingSpaceTag OldSpace = new("old-model", 8, VectorNormalization.UnitL2);
    private static readonly EmbeddingSpaceTag NewSpace = new("new-model", 8, VectorNormalization.UnitL2);

    private static RepoContextAnnOptions PlaneOptions() => new()
    {
        MinimumTrainingCount = 8,
        PartitionCount = 4,
        Probes = 4,
        FlushAfterUpdates = 1,
        IngestBatchSize = 8,
        MaxItemsPerChunk = 8,
        RetrainAfterUpdateFraction = 0d,
    };

    private sealed class Rig
    {
        public InMemoryAnnBackingFactory Backing { get; } = new();

        public RepoContextAnnOptions Plane { get; } = PlaneOptions();

        public RepoContextIndexingOptions Indexing { get; init; } = new();

        public void Seed(EmbeddingSpaceTag space, int count)
        {
            var source = Backing.For(RepoId, space).Source;
            for (var i = 0; i < count; i++)
            {
                var angle = 2d * Math.PI * i / count;
                var vector = new float[space.Dimension];
                vector[0] = (float)Math.Cos(angle);
                vector[1] = (float)Math.Sin(angle);
                source.Set($"vec-{i:D6}", RepoContextKeys.File(RepoId, $"src/File{i}.cs"), vector);
            }
        }

        /// <summary>
        /// Builds one space's index to Ready through its own coordinator, exactly as
        /// a host would, and returns the coordinator's persisted state so a caller
        /// can inspect what it recorded.
        /// </summary>
        public async Task<RepoContextAnnIndexBuildState> BuildAsync(EmbeddingSpaceTag space)
        {
            using var registry = new RepoContextAnnIndexRegistry(
                Backing, Plane, NullLogger<RepoContextAnnIndexRegistry>.Instance);

            var context = Substitute.For<IGrainContext>();
            context.GrainId.Returns(GrainId.Create(
                "repoContextAnnIndexBuild", RepoContextAnnIndexKeys.BuildGrainKey(RepoId, space)));
            var services = Substitute.For<IServiceProvider>();
            services.GetService(typeof(ITimerRegistry)).Returns(Substitute.For<ITimerRegistry>());
            context.ActivationServices.Returns(services);

            var state = new FakeBuildState();
            var grain = new RepoContextAnnIndexBuildGrain(
                context,
                Substitute.For<IReminderRegistry>(),
                registry,
                Backing,
                Indexing,
                NullLogger<RepoContextAnnIndexBuildGrain>.Instance,
                state);

            await grain.EnsureBuildingAsync(space);
            for (var tick = 1; tick <= MaxTicks; tick++)
            {
                await grain.ProcessNextPhaseAsync();
                if (await grain.IsConvergedAsync())
                {
                    break;
                }
            }

            Assert.That(state.State.Converged, Is.True, $"the {space.ModelId} index must converge");
            return state.State;
        }

        public int KeysUnder(EmbeddingSpaceTag space)
        {
            var prefix = RepoContextAnnIndexKeys.IndexPrefix(RepoId, space);
            return Backing.Shared.Keys.Count(k => k.StartsWith(prefix, StringComparison.Ordinal));
        }
    }

    private sealed class FakeBuildState : IPersistentState<RepoContextAnnIndexBuildState>
    {
        public RepoContextAnnIndexBuildState State { get; set; } = new();

        public string Etag => string.Empty;

        public bool RecordExists => true;

        public Task ClearStateAsync() => Task.CompletedTask;

        public Task ReadStateAsync() => Task.CompletedTask;

        public Task WriteStateAsync() => Task.CompletedTask;
    }

    [Test]
    public async Task Re_embedding_under_a_new_model_retires_the_abandoned_space()
    {
        var rig = new Rig();
        rig.Seed(OldSpace, 32);
        await rig.BuildAsync(OldSpace);

        var oldKeys = rig.KeysUnder(OldSpace);
        Assert.That(oldKeys, Is.GreaterThan(0),
            "the abandoned index must actually exist, or the reclamation has nothing to prove");

        // The model changes. The new space is a wholly separate index under a
        // separate prefix, which is correct - and leaves the old one orphaned.
        rig.Seed(NewSpace, 32);
        await rig.BuildAsync(NewSpace);

        Assert.Multiple(() =>
        {
            Assert.That(rig.KeysUnder(OldSpace), Is.Zero,
                "Nothing else in the system ever reclaims a superseded embedding space: the space guard hides it "
                + "from queries, so it is invisible, harmless, and permanently resident until this retires it.");
            Assert.That(rig.KeysUnder(NewSpace), Is.GreaterThan(0),
                "THE LIVE INDEX MUST SURVIVE. The reclamation range-deletes whole prefixes, so a live prefix in "
                + "reach of that delete would destroy the index that had just been built.");
        });
    }

    [Test]
    public async Task Reclamation_happens_only_after_the_replacement_is_ready()
    {
        // ORDERING IS THE SAFETY PROPERTY. Until the replacement can answer, the
        // space it replaces is the only thing a failed re-embed could fall back to.
        var rig = new Rig();
        rig.Seed(OldSpace, 32);
        await rig.BuildAsync(OldSpace);

        rig.Seed(NewSpace, 64);

        using var registry = new RepoContextAnnIndexRegistry(
            rig.Backing, rig.Plane, NullLogger<RepoContextAnnIndexRegistry>.Instance);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create(
            "repoContextAnnIndexBuild", RepoContextAnnIndexKeys.BuildGrainKey(RepoId, NewSpace)));
        var services = Substitute.For<IServiceProvider>();
        services.GetService(typeof(ITimerRegistry)).Returns(Substitute.For<ITimerRegistry>());
        context.ActivationServices.Returns(services);

        var state = new FakeBuildState();
        var grain = new RepoContextAnnIndexBuildGrain(
            context,
            Substitute.For<IReminderRegistry>(),
            registry,
            rig.Backing,
            rig.Indexing,
            NullLogger<RepoContextAnnIndexBuildGrain>.Instance,
            state);

        await grain.EnsureBuildingAsync(NewSpace);

        // Pump only part way, and assert the old space is still whole at every step
        // short of convergence.
        for (var tick = 1; tick <= MaxTicks && !await grain.IsConvergedAsync(); tick++)
        {
            var before = rig.KeysUnder(OldSpace);
            await grain.ProcessNextPhaseAsync();
            if (!await grain.IsConvergedAsync())
            {
                Assert.That(rig.KeysUnder(OldSpace), Is.EqualTo(before),
                    "A mid-build reclamation would remove the only index a failed re-embed could fall back to.");
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Converged, Is.True);
            Assert.That(state.State.Reclaimed, Is.True);
            Assert.That(rig.KeysUnder(OldSpace), Is.Zero, "and it is retired once, at the end");
        });
    }

    [Test]
    public async Task The_reclamation_switch_being_off_keeps_the_superseded_space()
    {
        // The roll-back setting: an operator who wants the previous space kept for a
        // deliberate reversion turns this off.
        var rig = new Rig
        {
            Indexing = new RepoContextIndexingOptions { AnnIndexReclamation = false },
        };
        rig.Seed(OldSpace, 32);
        await rig.BuildAsync(OldSpace);
        var oldKeys = rig.KeysUnder(OldSpace);

        rig.Seed(NewSpace, 32);
        var state = await rig.BuildAsync(NewSpace);

        Assert.Multiple(() =>
        {
            Assert.That(rig.KeysUnder(OldSpace), Is.EqualTo(oldKeys),
                "With the switch off the superseded space must be left exactly as it was.");
            Assert.That(state.Reclaimed, Is.False,
                "and no reclamation may be recorded, so turning the switch back on still retires it");
            Assert.That(rig.Backing.ReclaimCalls, Is.Zero, "the sweep must not even be attempted");
        });
    }

    [Test]
    public async Task A_repository_with_only_its_live_space_retires_nothing()
    {
        var rig = new Rig();
        rig.Seed(NewSpace, 32);
        await rig.BuildAsync(NewSpace);

        Assert.Multiple(() =>
        {
            Assert.That(rig.Backing.ReclaimCalls, Is.EqualTo(1), "the sweep still runs");
            Assert.That(rig.KeysUnder(NewSpace), Is.GreaterThan(0),
                "but finds only the live space, which it must never touch");
        });
    }

    [Test]
    public async Task Another_repository_is_never_in_reach()
    {
        var rig = new Rig();
        rig.Seed(OldSpace, 32);
        await rig.BuildAsync(OldSpace);

        // A record belonging to a different repository, sitting in the same index
        // tree exactly as it does in a host.
        const string Foreign = "repo/other/vidx/deadbeefdeadbeef/m";
        await rig.Backing.Shared.WriteAsync([new KeyValuePair<string, byte[]>(Foreign, [1, 2, 3])]);

        rig.Seed(NewSpace, 32);
        await rig.BuildAsync(NewSpace);

        Assert.Multiple(() =>
        {
            Assert.That(rig.KeysUnder(OldSpace), Is.Zero, "this repository's abandoned space is retired");
            Assert.That(rig.Backing.Shared.Keys, Does.Contain(Foreign),
                "A repository-scoped walk must never reach another repository's index, whatever its state.");
        });
    }
}
