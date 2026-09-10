using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Vector.Persistence;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Coverage for the credential the approximate-index build coordinator streams its
/// corpus under (issue #2426).
/// <para>
/// The build steps run on the coordinator's phase timer, which is deliberately
/// re-armed from the activation hook so steady-state processing is decoupled from
/// whichever call activated the grain. Nothing therefore reaches the build inside
/// the arming call's scope, and a timer turn carries no ambient caller credential
/// of its own. On a host running a default-deny access gate that does <b>not</b>
/// surface as an error: a denied range read is enforced as a reject-all key filter
/// rather than an exception, so the corpus stream is <b>empty</b>, cleanly.
/// </para>
/// <para>
/// <b>An empty corpus is refused nowhere below</b>, which is what makes the failure
/// silent rather than loud - see
/// <see cref="An_anonymous_build_converges_an_empty_index_against_a_gated_corpus"/>,
/// which is both the impact determination and the negative control for the
/// fixtures that pin the remedy.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextAnnIndexBuildGrainCredentialTests
{
    private const string RepoId = AnnPlaneFixture.RepoId;
    private const string RunSubject = "local-agent";
    private const string RunScheme = "local-trusted";

    /// <summary>
    /// A ceiling on pump ticks. Every tick is a real bounded build step, so for a
    /// given corpus and batch size the count is deterministic; reaching the ceiling
    /// means the build never converged.
    /// </summary>
    private const int MaxTicks = 512;

    private static EmbeddingSpaceTag Space => AnnPlaneFixture.Space;

    /// <summary>
    /// An authority that supplies the fixed local-agent identity, mirroring the
    /// container's <c>LocalTrustedRunAuthority</c>.
    /// </summary>
    private static IRepoIndexRunAuthority RunAuthority()
    {
        var authority = Substitute.For<IRepoIndexRunAuthority>();
        authority.Resolve().Returns(
            new LatticeCredential(RunSubject, scheme: RunScheme, principalId: RunSubject));
        return authority;
    }

    /// <summary>
    /// A store-of-record view that behaves like the vector trees behind a
    /// <b>default-deny</b> access gate: it yields its corpus only to a caller
    /// carrying the seeded subject's credential, and yields <b>nothing at all</b> -
    /// cleanly, with no exception - to an anonymous one. That is precisely what
    /// <c>LatticeAccessGateEnforcement.ResolveRangeReadFilterAsync</c> does on the
    /// range-read path when the caller is denied, and it is why this defect
    /// presents as a converged empty index rather than as a fault.
    /// </summary>
    private sealed class GatedVectorSource(InMemoryRepoContextVectorSource inner) : IRepoContextVectorSource
    {
        /// <summary>The subject observed on each gated read, or <c>null</c> for an anonymous one.</summary>
        public List<string?> Observed { get; } = [];

        /// <inheritdoc />
        public int Dimensions => inner.Dimensions;

        private bool Admitted()
        {
            // Read the ambient credential at the moment of the call, exactly where
            // the real gate resolves the caller subject.
            var subject = LatticeCredentialContext.Current?.Token;
            Observed.Add(subject);
            return string.Equals(subject, RunSubject, StringComparison.Ordinal);
        }

        /// <inheritdoc />
        public async IAsyncEnumerable<VectorSourceEntry> EnumerateAsync(
            string? afterIdExclusive, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (!Admitted())
            {
                yield break;
            }

            await foreach (var entry in inner
                .EnumerateAsync(afterIdExclusive, cancellationToken)
                .ConfigureAwait(false))
            {
                yield return entry;
            }
        }

        /// <inheritdoc />
        public Task<int> CountAsync(CancellationToken cancellationToken = default)
            => Admitted() ? inner.CountAsync(cancellationToken) : Task.FromResult(0);

        /// <inheritdoc />
        public Task<bool> ContainsAsync(string id, CancellationToken cancellationToken = default)
            => Admitted() ? inner.ContainsAsync(id, cancellationToken) : Task.FromResult(false);

        /// <inheritdoc />
        public Task<IReadOnlyDictionary<string, string>> ResolveSourceKeysAsync(
            IReadOnlyList<string> vectorIds, CancellationToken cancellationToken)
            => Admitted()
                ? inner.ResolveSourceKeysAsync(vectorIds, cancellationToken)
                : Task.FromResult<IReadOnlyDictionary<string, string>>(
                    new Dictionary<string, string>(StringComparer.Ordinal));
    }

    /// <summary>
    /// The in-memory backing factory with its store-of-record view wrapped in the
    /// gate. The durable store is left ungated: this fixture is about the corpus
    /// the build reads, and gating both would not distinguish the two.
    /// </summary>
    private sealed class GatedBackingFactory : IRepoContextAnnBackingFactory
    {
        private readonly InMemoryAnnBackingFactory _inner = new();
        private readonly Dictionary<(string RepoId, EmbeddingSpaceTag Space), GatedVectorSource> _gated = [];

        /// <summary>The gated view for one repository and embedding space.</summary>
        public GatedVectorSource Gate(string repoId, EmbeddingSpaceTag space)
        {
            var key = (repoId, space);
            if (!_gated.TryGetValue(key, out var gate))
            {
                gate = new GatedVectorSource(_inner.For(repoId, space).Source);
                _gated[key] = gate;
            }

            return gate;
        }

        /// <summary>Seeds a ring of unit vectors into the underlying store of record.</summary>
        public void SeedRing(string repoId, EmbeddingSpaceTag space, int count)
        {
            var source = _inner.For(repoId, space).Source;
            for (var i = 0; i < count; i++)
            {
                var angle = 2d * Math.PI * i / count;
                var vector = new float[space.Dimension];
                vector[0] = (float)Math.Cos(angle);
                vector[1] = (float)Math.Sin(angle);
                source.Set($"vec-{i:D6}", RepoContextKeys.File(repoId, $"src/File{i}.cs"), vector);
            }
        }

        /// <inheritdoc />
        public IRepoContextVectorSource CreateSource(string repoId, EmbeddingSpaceTag space)
            => Gate(repoId, space);

        /// <inheritdoc />
        public IVectorIndexStore CreateStore(string repoId, EmbeddingSpaceTag space)
            => _inner.CreateStore(repoId, space);

        /// <inheritdoc />
        public Task<int> ReclaimSupersededSpacesAsync(
            string repoId, EmbeddingSpaceTag liveSpace, CancellationToken cancellationToken)
            => _inner.ReclaimSupersededSpacesAsync(repoId, liveSpace, cancellationToken);
    }

    /// <summary>The coordinator's persisted state, held in memory as grain storage would.</summary>
    private sealed class FakeBuildState : IPersistentState<RepoContextAnnIndexBuildState>
    {
        public RepoContextAnnIndexBuildState State { get; set; } = new();

        public string Etag => string.Empty;

        public bool RecordExists => true;

        public Task ClearStateAsync() => Task.CompletedTask;

        public Task ReadStateAsync() => Task.CompletedTask;

        public Task WriteStateAsync() => Task.CompletedTask;
    }

    /// <summary>One activation of the coordinator over a gated store of record.</summary>
    private sealed class Rig(IRepoIndexRunAuthority authority) : IDisposable
    {
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

        public GatedBackingFactory Backing { get; } = new();

        public FakeBuildState State { get; } = new();

        public RepoContextAnnIndexRegistry Registry { get; private set; } = null!;

        public RepoContextAnnIndexBuildGrain Grain { get; private set; } = null!;

        /// <summary>Builds the activation. Call after seeding.</summary>
        public Rig Start()
        {
            Registry = new RepoContextAnnIndexRegistry(
                Backing, PlaneOptions(), NullLogger<RepoContextAnnIndexRegistry>.Instance);

            var context = Substitute.For<IGrainContext>();
            context.GrainId.Returns(GrainId.Create(
                "repoContextAnnIndexBuild", RepoContextAnnIndexKeys.BuildGrainKey(RepoId, Space)));
            var services = Substitute.For<IServiceProvider>();
            services.GetService(typeof(ITimerRegistry)).Returns(Substitute.For<ITimerRegistry>());
            context.ActivationServices.Returns(services);

            Grain = new RepoContextAnnIndexBuildGrain(
                context,
                Substitute.For<IReminderRegistry>(),
                Registry,
                Backing,
                new RepoContextIndexingOptions(),
                authority,
                NullLogger<RepoContextAnnIndexBuildGrain>.Instance,
                State);

            return this;
        }

        /// <summary>
        /// Drives the phase pump, which is what the grain timer does on every tick.
        /// The arming call is made first, exactly as the sweep makes it - and note
        /// that it starts the timer and returns, so no build step runs inside it.
        /// </summary>
        public async Task<int> PumpAsync()
        {
            await Grain.EnsureBuildingAsync(Space);
            for (var tick = 1; tick <= MaxTicks; tick++)
            {
                await Grain.ProcessNextPhaseAsync();
                if (await Grain.IsConvergedAsync())
                {
                    return tick;
                }
            }

            return MaxTicks;
        }

        public void Dispose() => Registry?.Dispose();
    }

    [Test]
    public async Task The_build_streams_the_corpus_under_the_run_authoritys_credential()
    {
        // THE REMEDY. Every read the build step drives must present the run
        // authority's subject, because a default-deny gate answers an anonymous one
        // with an empty corpus rather than with a fault.
        using var rig = new Rig(RunAuthority());
        rig.Backing.SeedRing(RepoId, Space, 64);
        rig.Start();

        var ticks = await rig.PumpAsync();
        var observed = rig.Backing.Gate(RepoId, Space).Observed;

        Assert.Multiple(() =>
        {
            Assert.That(ticks, Is.LessThan(MaxTicks), "the index must converge");
            Assert.That(observed, Is.Not.Empty,
                "positive control: the build must have read the store of record at least once, "
                + "or this fixture could not tell a credentialed read from no read at all");
            Assert.That(observed, Has.All.EqualTo(RunSubject),
                "every corpus read must carry the run authority's subject; an anonymous one is "
                + "silently filtered to nothing by a default-deny gate");
            Assert.That(rig.State.State.VectorsIndexed, Is.EqualTo(64),
                "the index must hold the corpus the store of record actually contains");
            Assert.That(rig.State.State.PartitionsTotal, Is.GreaterThan(0),
                "a build over a real corpus partitions, and the persisted state must say so - this is "
                + "the arm that distinguishes it from the denied build below, which converges with "
                + "the same Converged flag and a partition count of zero");
        });
    }

    [Test]
    public async Task An_anonymous_build_converges_an_empty_index_against_a_gated_corpus()
    {
        // THE NEGATIVE CONTROL, AND THE IMPACT DETERMINATION IN ONE.
        //
        // With no authority registered the build presents no subject, so the gated
        // corpus reads empty. Nothing below refuses an empty corpus: the count
        // probe reports zero, the ingest completes on its first step, training
        // drops the partitioning and returns false rather than throwing, and the
        // build reaches Ready. This grain then records Converged with zero vectors
        // and stands the coordinator down - so the denied read is durably
        // indistinguishable from a repository that genuinely had nothing to index,
        // and nothing re-drives it.
        //
        // That is strictly worse than a build that fails and is retried forever,
        // and it is why the remedy above is a correctness fix rather than a
        // liveness one. Should a future change refuse to converge an empty index,
        // this fixture is the one to revisit: the impact statement in #2426 rests
        // on it.
        using var rig = new Rig(new NullRepoIndexRunAuthority());
        rig.Backing.SeedRing(RepoId, Space, 64);
        rig.Start();

        var ticks = await rig.PumpAsync();
        var observed = rig.Backing.Gate(RepoId, Space).Observed;

        Assert.Multiple(() =>
        {
            Assert.That(ticks, Is.LessThan(MaxTicks),
                "the anonymous build converges rather than faulting, which is the whole hazard");
            Assert.That(observed, Has.All.Null,
                "positive control: the reads must actually have been anonymous, or this fixture "
                + "is measuring something other than the denial it claims to");
            Assert.That(rig.State.State.Converged, Is.True,
                "an empty index is recorded as a completed build, not as a failure");
            Assert.That(rig.State.State.VectorsIndexed, Is.Zero,
                "the corpus of 64 vectors was filtered to nothing and the build did not notice");
            Assert.That(rig.State.State.PartitionsTotal, Is.Zero,
                "training declined to partition an empty corpus, and issue #2439 added this to the "
                + "persisted state precisely so that after the fact - logs rotated, disk only - a "
                + "denied build is distinguishable from one that trained");
        });
    }

    [Test]
    public async Task A_host_that_registers_no_authority_leaves_the_ambient_credential_untouched()
    {
        // Acceptance criterion 2. An in-process host with no access gate registers
        // no authority, and must be behaviourally unchanged: no scope is opened, so
        // whatever credential the ambient context holds is what the build presents.
        Assert.That(LatticeCredentialContext.Current, Is.Null,
            "positive control: the ambient credential must start clear, or this fixture "
            + "could not tell a leak from its own setup");

        using var ambient = LatticeCredentialContext.With(
            new LatticeCredential("ambient-subject", scheme: RunScheme, principalId: "ambient-subject"));

        using var rig = new Rig(new NullRepoIndexRunAuthority());
        rig.Backing.SeedRing(RepoId, Space, 64);
        rig.Start();

        await rig.PumpAsync();
        var observed = rig.Backing.Gate(RepoId, Space).Observed;

        Assert.That(observed, Has.All.EqualTo("ambient-subject"),
            "a null authority must leave the ambient credential in place rather than clearing it, "
            + "so a host with no gate configured is unaffected");
    }

    [Test]
    public async Task The_build_restores_the_ambient_credential_it_found_once_the_tick_ends()
    {
        // A specificity guard rather than a detector: it holds both before and
        // after the fix, and its job is to prove the remedy does not re-globalise
        // credential state onto the calling context.
        Assert.That(LatticeCredentialContext.Current, Is.Null,
            "positive control: the ambient credential must start clear");

        using var rig = new Rig(RunAuthority());
        rig.Backing.SeedRing(RepoId, Space, 16);
        rig.Start();

        await rig.PumpAsync();

        Assert.That(LatticeCredentialContext.Current, Is.Null,
            "the build's run credential must not escape onto the calling context");
    }
}
