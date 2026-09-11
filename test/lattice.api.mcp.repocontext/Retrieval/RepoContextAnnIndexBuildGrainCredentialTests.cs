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
/// <b>An empty corpus is refused nowhere in the build pipeline</b>, which is what
/// makes the failure silent rather than loud - see
/// <see cref="An_anonymous_build_reaches_ready_holding_nothing_against_a_gated_corpus"/>,
/// which is both the impact determination and the negative control for the
/// fixtures that pin the remedy. The remedy therefore sits at the coordinator,
/// which classifies an empty build against the access gate before banking it; that
/// half is covered in the <c>CorpusSignal</c> partial.
/// </para>
/// </summary>
[TestFixture]
public sealed partial class RepoContextAnnIndexBuildGrainCredentialTests
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

        /// <summary>
        /// Refuses every read regardless of the subject presented, standing in for a
        /// host whose grant has not been seeded yet. Settable so a denial that
        /// clears can be exercised on one activation, which is the case that
        /// justifies the coordinator backing off rather than standing down.
        /// </summary>
        public bool Denied { get; set; }

        /// <inheritdoc />
        public int Dimensions => inner.Dimensions;

        private bool Admitted()
        {
            // Read the ambient credential at the moment of the call, exactly where
            // the real gate resolves the caller subject.
            var subject = LatticeCredentialContext.Current?.Token;
            Observed.Add(subject);
            return !Denied && string.Equals(subject, RunSubject, StringComparison.Ordinal);
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

    /// <summary>
    /// A corpus gate probe under the fixture's control, standing in for the real
    /// one's call to <c>ILattice.GetRangeReadGateCoverageAsync</c>. It records how
    /// many times it was asked, which is what lets a test assert that the probe is
    /// taken only on the empty path and only on the backoff schedule.
    /// </summary>
    private sealed class FakeCorpusGateProbe(RepoContextAnnBuildCorpusCoverage coverage, bool throws = false)
        : IRepoContextCorpusGateProbe
    {
        /// <summary>How many times the coordinator asked for a classification.</summary>
        public int Calls { get; private set; }

        /// <inheritdoc />
        public Task<RepoContextAnnBuildCorpusCoverage> ProbeAsync(string repoId, CancellationToken cancellationToken)
        {
            Calls++;

            // The production probe never propagates: it catches and classifies as
            // Unknown, because a diagnostic that can fail a build inverts the blast
            // radius it exists to reduce. This models that contract rather than the
            // exception, so the fixture asserts the coordinator's behaviour on
            // Unknown rather than re-testing the try/catch.
            return Task.FromResult(throws ? RepoContextAnnBuildCorpusCoverage.Unknown : coverage);
        }
    }

    /// <summary>The coordinator's persisted state, held in memory as grain storage would.</summary>
    private sealed class FakeBuildState : IPersistentState<RepoContextAnnIndexBuildState>
    {
        public RepoContextAnnIndexBuildState State { get; set; } = new();

        /// <summary>
        /// How many times the coordinator has written durable state. The record is
        /// held as a live object here and <see cref="WriteStateAsync"/> is a no-op,
        /// so a field mutation is visible to an assertion whether or not it was
        /// ever persisted. This counter is therefore the only way to assert that a
        /// settled coordinator does NOT write on every activation - see issue
        /// #2712's second acceptance criterion.
        /// </summary>
        public int Writes { get; private set; }

        public string Etag => string.Empty;

        public bool RecordExists => true;

        public Task ClearStateAsync() => Task.CompletedTask;

        public Task ReadStateAsync() => Task.CompletedTask;

        public Task WriteStateAsync()
        {
            Writes++;
            return Task.CompletedTask;
        }
    }

    /// <summary>One activation of the coordinator over a gated store of record.</summary>
    private sealed class Rig(
        IRepoIndexRunAuthority authority,
        IRepoContextCorpusGateProbe? corpusGateProbe = null)
        : IDisposable
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

        /// <summary>
        /// The classifier the coordinator consults when a build completes holding
        /// nothing. Defaults to an unrestricted answer, which is the correct one for
        /// an in-process host with no access gate at all.
        /// </summary>
        public IRepoContextCorpusGateProbe Probe { get; } =
            corpusGateProbe ?? new FakeCorpusGateProbe(RepoContextAnnBuildCorpusCoverage.Unrestricted);

        /// <summary>The reporter whose series the denial signal is asserted against.</summary>
        public RepoContextAnnBuildCorpusReporter Reporter { get; } = new();

        public RepoContextAnnIndexRegistry Registry { get; private set; } = null!;

        public RepoContextAnnIndexBuildGrain Grain { get; private set; } = null!;

        /// <summary>Builds the activation. Call after seeding.</summary>
        public Rig Start()
        {
            Registry = new RepoContextAnnIndexRegistry(
                Backing, PlaneOptions(), NullLogger<RepoContextAnnIndexRegistry>.Instance);

            Grain = CreateGrain();

            return this;
        }

        /// <summary>
        /// Replaces the coordinator with a fresh one over the same durable state,
        /// the same registry and the same store of record, which is what a
        /// reactivation is.
        /// <para>
        /// <b>Why a fixture needs this rather than simply ticking again.</b>
        /// <c>ProcessNextPhaseAsync</c> returns immediately when <c>InProgress</c>
        /// is false, and <c>InProgress</c> carries the term
        /// <c>!_advancedThisActivation</c>. Once a build has converged AND taken a
        /// step in the current activation, both terms of that disjunction are
        /// false, so every subsequent tick in the same activation is a no-op. A
        /// converged coordinator therefore performs exactly <b>one</b> build step
        /// per activation, and any behaviour that follows a second converged build
        /// is unreachable without a new activation. That is the seam issue #2711's
        /// self-heal rides on, and it is why no pre-existing fixture could observe
        /// the durable record being refreshed.
        /// </para>
        /// </summary>
        public Rig Reactivate()
        {
            Grain = CreateGrain();
            return this;
        }

        private RepoContextAnnIndexBuildGrain CreateGrain()
        {
            var context = Substitute.For<IGrainContext>();
            context.GrainId.Returns(GrainId.Create(
                "repoContextAnnIndexBuild", RepoContextAnnIndexKeys.BuildGrainKey(RepoId, Space)));
            var services = Substitute.For<IServiceProvider>();
            services.GetService(typeof(ITimerRegistry)).Returns(Substitute.For<ITimerRegistry>());
            context.ActivationServices.Returns(services);

            return new RepoContextAnnIndexBuildGrain(
                context,
                Substitute.For<IReminderRegistry>(),
                Registry,
                Backing,
                new RepoContextIndexingOptions(),
                authority,
                Probe,
                Reporter,
                NullLogger<RepoContextAnnIndexBuildGrain>.Instance,
                State);
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

        /// <summary>
        /// Drives an exact number of phase ticks whether or not the build converges,
        /// which is what a build that deliberately refuses to converge needs.
        /// </summary>
        /// <param name="ticks">The number of timer ticks to deliver.</param>
        public async Task PumpTicksAsync(int ticks)
        {
            await Grain.EnsureBuildingAsync(Space);
            for (var tick = 1; tick <= ticks; tick++)
            {
                await Grain.ProcessNextPhaseAsync();
            }
        }

        public void Dispose()
        {
            Registry?.Dispose();
            Reporter.Dispose();
        }
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
    public async Task An_anonymous_build_reaches_ready_holding_nothing_against_a_gated_corpus()
    {
        // THE NEGATIVE CONTROL, AND THE IMPACT DETERMINATION IN ONE.
        //
        // With no authority registered the build presents no subject, so the gated
        // corpus reads empty. Nothing IN THE BUILD PIPELINE refuses an empty
        // corpus: the count probe reports zero, the ingest completes on its first
        // step, training drops the partitioning and returns false rather than
        // throwing, and the build reaches Ready. That is still true and is why the
        // guard had to be added at the coordinator: there is no lower seam at which
        // a denied read announces itself.
        //
        // The probe here is deliberately told the prefix is UNRESTRICTED, which
        // isolates the pipeline's own behaviour from the remedy. Under that answer
        // the coordinator banks Converged with zero vectors and stands down - which
        // is exactly the pre-#2426 hazard, reproduced on demand. The paired test
        // A_denied_corpus_is_counted_and_refused_convergence supplies the truthful
        // answer and shows the coordinator refusing instead.
        using var rig = new Rig(
            new NullRepoIndexRunAuthority(),
            new FakeCorpusGateProbe(RepoContextAnnBuildCorpusCoverage.Unrestricted));
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
                "an empty index is recorded as a completed build, not as a failure, whenever the gate "
                + "reports the prefix unrestricted - the build pipeline itself refuses nothing");
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
