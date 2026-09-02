using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Storage;
using Orleans.Timers;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// A silo-free unit harness for <see cref="RepoContextSelfIndexGrain"/>: it builds
/// the grain over substituted collaborators and, crucially, captures the grain's own
/// scan-timer callback so a tick can be driven synchronously and deterministically.
/// <para>
/// The grain's whole self-heal loop - the periodic reconcile, the failed-run
/// re-drive, the paged gap scan, and the jittered cooldown - only ever executes from
/// a timer tick. The existing cluster-backed fixtures can call
/// <see cref="RepoContextSelfIndexGrain.EnsureRunningAsync"/> but cannot make a real
/// Orleans timer fire on demand, so that loop was unreachable in test. The
/// grain-timer extension resolves <see cref="ITimerRegistry"/> from the activation's
/// services, so substituting the registry lets the harness keep the registered
/// callback and invoke it directly: a tick becomes an ordinary in-process unit test
/// instead of a timing-dependent integration test.
/// </para>
/// <para>
/// The structural and vector-membership trees are backed by ordinary sorted
/// dictionaries behind a substituted <see cref="ILattice"/>, so the real
/// <see cref="RepoContextEmbeddingGapScanner"/> and the real
/// <see cref="RepoContextVectorWriter"/> membership probe run unmodified against the
/// same key grammar and ascending ordinal order a silo would give them. That keeps
/// the gap-scan outcomes (clean, gap found, more pages) genuine rather than stubbed.
/// </para>
/// </summary>
internal sealed class SelfIndexGrainHarness
{
    /// <summary>The repository id every harness grain is keyed by.</summary>
    internal const string RepoId = "acme";

    /// <summary>
    /// The grain's own page size. A page that fills to exactly this many keys is
    /// what makes the scan report more pages remain, so the paging test needs it.
    /// </summary>
    internal const int PageSize = 512;

    private static readonly Serializer OrleansSerializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private readonly SortedDictionary<string, byte[]> _structural = new(StringComparer.Ordinal);
    private readonly SortedDictionary<string, byte[]> _membership = new(StringComparer.Ordinal);

    /// <summary>Creates a harness whose grain plays the given indexing role.</summary>
    /// <param name="role">The indexing role the options report; a spoke is inert.</param>
    /// <param name="options">An explicit options instance, overriding <paramref name="role"/>.</param>
    internal SelfIndexGrainHarness(
        RepoContextIndexingRole role = RepoContextIndexingRole.Hub,
        RepoContextIndexingOptions? options = null)
    {
        Options = options ?? new RepoContextIndexingOptions { Role = role };

        Job = Substitute.For<IRepoIndexJobGrain>();
        Job.EnsureIndexedAsync().Returns(Task.FromResult(true));
        Job.GetProgressAsync().Returns(Task.FromResult(new RepoIndexProgress
        {
            RepoId = RepoId,
            Status = RepoIndexStatus.Completed,
            Phase = RepoIndexPhase.Pending,
        }));

        StructuralTree = BuildTree(_structural);
        MembershipTree = BuildTree(_membership);

        GrainFactory = Substitute.For<IGrainFactory>();
        GrainFactory.GetGrain<ILattice>(Arg.Any<string>()).Returns(call =>
            call.ArgAt<string>(0) == RepoContextTrees.VectorMembership ? MembershipTree : StructuralTree);
        GrainFactory.GetGrain<IRepoIndexJobGrain>(Arg.Any<string>()).Returns(_ => Job);

        Context = Substitute.For<IGrainContext>();
        Context.GrainId.Returns(GrainId.Create("repoContextSelfIndex", RepoId));

        // The grain-timer extension resolves ITimerRegistry from the activation's
        // services. Capturing the registered callback (the timer's state argument)
        // is what makes a tick drivable from a unit test.
        TimerRegistry = Substitute.For<ITimerRegistry>();
        TimerRegistry.RegisterGrainTimer(
            Arg.Any<IGrainContext>(),
            Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
            Arg.Do<Func<CancellationToken, Task>>(callback => TimerCallback = callback),
            Arg.Any<GrainTimerCreationOptions>())
            .Returns(_ => Timer);

        var activationServices = new ServiceCollection();
        activationServices.AddSingleton(TimerRegistry);
        Context.ActivationServices.Returns(activationServices.BuildServiceProvider());

        Reminders = Substitute.For<IReminderRegistry>();
        Runner = Substitute.For<IRepoIndexRunner>();
        Runner.StartIndexAsync(Arg.Any<RepoIndexJobRequest>()).Returns(Task.FromResult(new RepoIndexProgress
        {
            RepoId = RepoId,
            Status = RepoIndexStatus.Running,
            Phase = RepoIndexPhase.Walking,
        }));

        RunAuthority = Substitute.For<IRepoIndexRunAuthority>();
        State = new FakeSelfIndexState();
        Time = new MutableTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
    }

    /// <summary>The indexing options the grain reads its role and cadences from.</summary>
    internal RepoContextIndexingOptions Options { get; }

    /// <summary>The substituted job grain every re-drive and progress read resolves to.</summary>
    internal IRepoIndexJobGrain Job { get; }

    /// <summary>The substituted structural tree the gap scan walks.</summary>
    internal ILattice StructuralTree { get; }

    /// <summary>The substituted vector-membership tree the coverage probe reads.</summary>
    internal ILattice MembershipTree { get; }

    /// <summary>The substituted grain factory the grain resolves trees and the job grain through.</summary>
    internal IGrainFactory GrainFactory { get; }

    /// <summary>The substituted activation context, keyed by the repository id.</summary>
    internal IGrainContext Context { get; }

    /// <summary>The substituted timer registry the scan timer is registered with.</summary>
    internal ITimerRegistry TimerRegistry { get; }

    /// <summary>The timer handle the registry hands back; disposed by <c>StopAsync</c>.</summary>
    internal IGrainTimer Timer { get; } = Substitute.For<IGrainTimer>();

    /// <summary>The substituted reminder registry the keep-alive is registered with.</summary>
    internal IReminderRegistry Reminders { get; }

    /// <summary>The substituted index runner the onboarding pass is driven through.</summary>
    internal IRepoIndexRunner Runner { get; }

    /// <summary>The substituted run authority the tick stamps its credential from.</summary>
    internal IRepoIndexRunAuthority RunAuthority { get; }

    /// <summary>The in-memory persisted scan checkpoint.</summary>
    internal FakeSelfIndexState State { get; }

    /// <summary>The controllable clock the scan schedules its cooldowns against.</summary>
    internal MutableTimeProvider Time { get; }

    /// <summary>
    /// The scan-timer callback captured when the grain armed its timer, or
    /// <see langword="null"/> when no timer was armed (as on a spoke).
    /// </summary>
    internal Func<CancellationToken, Task>? TimerCallback { get; private set; }

    /// <summary>An embedding provider, set to make the approximate-index scheduler do real work.</summary>
    internal IEmbeddingProvider? Embedder { get; set; }

    /// <summary>Builds the grain under test over this harness's collaborators.</summary>
    /// <returns>A freshly constructed grain.</returns>
    internal RepoContextSelfIndexGrain CreateGrain()
    {
        var replication = Substitute.For<ILatticeReplicationContext>();
        var cache = new RepoContextVectorCache(TimeProvider.System, Options);
        var writer = new RepoContextVectorWriter(
            GrainFactory,
            OrleansSerializer,
            replication,
            cache,
            Harness.RepoContextVectorPlaneTestDoubles.ReDeriver(GrainFactory));

        return new RepoContextSelfIndexGrain(
            Context,
            GrainFactory,
            Reminders,
            Runner,
            new RepoContextEmbeddingGapScanner(GrainFactory, writer),
            RunAuthority,
            Time,
            Options,
            new RepoContextAnnIndexScheduler(
                GrainFactory, Options, NullLogger<RepoContextAnnIndexScheduler>.Instance, Embedder),
            NullLogger<RepoContextSelfIndexGrain>.Instance,
            State);
    }

    /// <summary>A well-formed onboarding request for this harness's repository.</summary>
    /// <returns>The request <c>EnsureRunningAsync</c> is driven with.</returns>
    internal static RepoIndexJobRequest Request() => new() { RepoRoot = "/repo", RepoId = RepoId };

    /// <summary>
    /// Seeds a structural file key so the gap scan sees it. The scan reads keys
    /// only, so any non-empty value suffices.
    /// </summary>
    /// <param name="relativePath">The repository-relative file path.</param>
    /// <returns>The structural key that was seeded.</returns>
    internal string SeedFile(string relativePath)
    {
        var key = RepoContextKeys.File(RepoId, relativePath);
        _structural[key] = [1];
        return key;
    }

    /// <summary>
    /// Records a live embedding for a seeded file, exactly as the membership tree
    /// holds it: an enabled observed-remove flag under the file's source identifier.
    /// </summary>
    /// <param name="relativePath">The repository-relative file path to mark embedded.</param>
    internal void MarkEmbedded(string relativePath)
    {
        var flag = new OrFlag();
        flag.Enable("harness", 1);
        var sourceId = VectorCodec.SourceId(RepoContextKeys.File(RepoId, relativePath));
        _membership[RepoContextKeys.VectorMembership(RepoId, sourceId)] =
            JsonLatticeSerializer<OrFlag>.Default.Serialize(flag);
    }

    /// <summary>Seeds a file and marks it embedded, so it is never a gap.</summary>
    /// <param name="relativePath">The repository-relative file path.</param>
    internal void SeedEmbeddedFile(string relativePath)
    {
        SeedFile(relativePath);
        MarkEmbedded(relativePath);
    }

    /// <summary>
    /// Drives one scan-timer tick through the callback the grain registered, so the
    /// grain's own tick body runs exactly as the Orleans timer would run it.
    /// </summary>
    /// <returns>A task that completes when the tick has run.</returns>
    /// <exception cref="InvalidOperationException">No timer was armed.</exception>
    internal Task TickAsync()
    {
        if (TimerCallback is null)
        {
            throw new InvalidOperationException(
                "No scan timer was armed, so there is no tick to drive. Arm it with EnsureRunningAsync first.");
        }

        return TimerCallback(CancellationToken.None);
    }

    private static ILattice BuildTree(SortedDictionary<string, byte[]> records)
    {
        var tree = Substitute.For<ILattice>();

        tree.KeysAsync().ReturnsForAnyArgs(call => Keys(
            records, call.ArgAt<string?>(0), call.ArgAt<string?>(1)));

        tree.GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(call =>
            {
                var found = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                foreach (var key in call.ArgAt<List<string>>(0))
                {
                    if (records.TryGetValue(key, out var value))
                    {
                        found[key] = value;
                    }
                }

                return Task.FromResult(found);
            });

        return tree;
    }

    private static async IAsyncEnumerable<string> Keys(
        SortedDictionary<string, byte[]> records, string? startInclusive, string? endExclusive)
    {
        foreach (var key in records.Keys)
        {
            if (startInclusive is not null && string.CompareOrdinal(key, startInclusive) < 0)
            {
                continue;
            }

            if (endExclusive is not null && string.CompareOrdinal(key, endExclusive) >= 0)
            {
                break;
            }

            yield return key;
            await Task.CompletedTask.ConfigureAwait(false);
        }
    }

    /// <summary>
    /// A <see cref="TimeProvider"/> whose current instant the test moves by hand, so
    /// the grain's cooldown and reconcile deadlines can be crossed without waiting.
    /// </summary>
    internal sealed class MutableTimeProvider(DateTimeOffset now) : TimeProvider
    {
        private DateTimeOffset _now = now;

        /// <inheritdoc />
        public override DateTimeOffset GetUtcNow() => _now;

        /// <summary>Moves the clock forward.</summary>
        /// <param name="delta">How far to advance.</param>
        internal void Advance(TimeSpan delta) => _now += delta;
    }

    /// <summary>
    /// A minimal in-memory <see cref="IPersistentState{T}"/> that records how often
    /// the grain wrote or cleared its checkpoint, so the scan's persistence
    /// behaviour is observable without a storage provider.
    /// </summary>
    internal sealed class FakeSelfIndexState : IPersistentState<RepoContextSelfIndexState>
    {
        /// <inheritdoc />
        public RepoContextSelfIndexState State { get; set; } = new();

        /// <inheritdoc />
        public string Etag => string.Empty;

        /// <inheritdoc />
        public bool RecordExists => true;

        /// <summary>How many times the checkpoint was written.</summary>
        internal int WriteCount { get; private set; }

        /// <summary>How many times the checkpoint was cleared.</summary>
        internal int ClearCount { get; private set; }

        /// <inheritdoc />
        public Task ClearStateAsync()
        {
            ClearCount++;
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task ReadStateAsync() => Task.CompletedTask;

        /// <inheritdoc />
        public Task WriteStateAsync()
        {
            WriteCount++;
            return Task.CompletedTask;
        }
    }
}
