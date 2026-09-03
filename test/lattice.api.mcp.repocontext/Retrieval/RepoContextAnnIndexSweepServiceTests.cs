using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Vector.Persistence;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit coverage for <see cref="RepoContextAnnIndexSweepService"/>, the startup
/// sweep that arms a build coordinator for every registered repository.
/// <para>
/// <b>This is the part that actually fixes cold start</b>, so its failure handling
/// is the point rather than a detail: a coordinator nobody arms is a build nobody
/// starts. The silo is itself a hosted service, so a sweep launched from another
/// hosted service's start can easily run before the silo is dispatch-ready - the
/// first sweep failing is the expected case, not the exceptional one, and it must
/// retry with backoff rather than settle into the long cadence with nothing
/// scheduled.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextAnnIndexSweepServiceTests
{
    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static RepoContextStore Store(IGrainFactory grainFactory)
        => new(
            grainFactory,
            Substitute.For<IRepoIndexRunner>(),
            Serializer,
            new RepoContextVectorWriter(
                grainFactory,
                Serializer,
                Substitute.For<ILatticeReplicationContext>(),
                new RepoContextVectorCache(TimeProvider.System, new RepoContextIndexingOptions()),
                RepoContextVectorPlaneTestDoubles.ReDeriver(grainFactory)),
            Substitute.For<IOptionsMonitor<RepoContextTtlOptions>>(),
            TimeProvider.System);

    private static RepoContextAnnIndexScheduler Scheduler(
        IGrainFactory grainFactory, RepoContextIndexingOptions? options = null, bool withEmbedder = true)
        => new(
            grainFactory,
            options ?? new RepoContextIndexingOptions(),
            NullLogger<RepoContextAnnIndexScheduler>.Instance,
            withEmbedder ? StubEmbedder.Instance : null);

    private static RepoContextAnnIndexSweepService Sweep(
        RepoContextStore store, RepoContextAnnIndexScheduler scheduler, RepoContextIndexingOptions? options = null)
        => new(
            store,
            scheduler,
            options ?? new RepoContextIndexingOptions(),
            NullLogger<RepoContextAnnIndexSweepService>.Instance);

    /// <summary>Spins until <paramref name="condition"/> holds or the budget runs out.</summary>
    private static async Task<bool> WaitForAsync(Func<bool> condition, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return true;
            }

            await Task.Delay(15, cancellationToken).ConfigureAwait(false);
        }

        return condition();
    }

    /// <summary>A structural tree that lists the supplied repository markers.</summary>
    private static IGrainFactory GrainFactoryListing(params string[] repoIds)
    {
        var records = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
        foreach (var repoId in repoIds)
        {
            records[RepoContextKeys.Repo(repoId)] =
                Serializer.SerializeToArray(new RepoNode { RepoId = repoId });
        }

        var tree = Substitute.For<ILattice>();
        tree.EntriesAsync().ReturnsForAnyArgs(call => Entries(
            records, call.ArgAt<string?>(0), call.ArgAt<string?>(1)));
        tree.KeysAsync().ReturnsForAnyArgs(call => Keys(
            records, call.ArgAt<string?>(0), call.ArgAt<string?>(1)));
        tree.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(call =>
                Task.FromResult(records.TryGetValue(call.ArgAt<string>(0), out var value) ? value : null));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).ReturnsForAnyArgs(tree);
        return grainFactory;
    }

    private static List<KeyValuePair<string, byte[]>> Window(
        SortedDictionary<string, byte[]> records, string? startInclusive, string? endExclusive)
    {
        var window = new List<KeyValuePair<string, byte[]>>();
        foreach (var entry in records)
        {
            if (startInclusive is not null && string.CompareOrdinal(entry.Key, startInclusive) < 0)
            {
                continue;
            }

            if (endExclusive is not null && string.CompareOrdinal(entry.Key, endExclusive) >= 0)
            {
                break;
            }

            window.Add(entry);
        }

        return window;
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Entries(
        SortedDictionary<string, byte[]> records, string? startInclusive, string? endExclusive)
    {
        foreach (var entry in Window(records, startInclusive, endExclusive))
        {
            yield return entry;
            await Task.CompletedTask.ConfigureAwait(false);
        }
    }

    private static async IAsyncEnumerable<string> Keys(
        SortedDictionary<string, byte[]> records, string? startInclusive, string? endExclusive)
    {
        foreach (var entry in Window(records, startInclusive, endExclusive))
        {
            yield return entry.Key;
            await Task.CompletedTask.ConfigureAwait(false);
        }
    }

    [Test]
    public async Task The_sweep_arms_a_build_coordinator_for_every_registered_repository()
    {
        var space = EmbeddingSpaceTag.FromSpace(StubEmbedder.Instance.Space);
        var alpha = Substitute.For<IRepoContextAnnIndexBuildGrain>();
        var beta = Substitute.For<IRepoContextAnnIndexBuildGrain>();

        var grainFactory = GrainFactoryListing("alpha", "beta");
        grainFactory.GetGrain<IRepoContextAnnIndexBuildGrain>(
            RepoContextAnnIndexKeys.BuildGrainKey("alpha", space)).Returns(alpha);
        grainFactory.GetGrain<IRepoContextAnnIndexBuildGrain>(
            RepoContextAnnIndexKeys.BuildGrainKey("beta", space)).Returns(beta);

        var sweep = Sweep(Store(grainFactory), Scheduler(grainFactory));
        await sweep.StartAsync(Ct);
        try
        {
            var armed = await WaitForAsync(
                () => alpha.ReceivedCalls().Any() && beta.ReceivedCalls().Any(), Ct);
            Assert.That(armed, Is.True,
                "a restored volume with no client at all must still converge to a serving index");
        }
        finally
        {
            await sweep.StopAsync(Ct);
        }

        await alpha.Received().EnsureBuildingAsync(space);
        await beta.Received().EnsureBuildingAsync(space);
    }

    [Test]
    public async Task A_failing_sweep_retries_with_backoff_rather_than_waiting_out_the_long_cadence()
    {
        // The silo is itself a hosted service, so a grain call from this service's
        // start can outrun dispatch readiness. Settling into the reconcile cadence
        // after one failure would leave nothing scheduled for a full interval.
        var attempts = 0;
        var tree = Substitute.For<ILattice>();
        tree.KeysAsync().ReturnsForAnyArgs(_ =>
        {
            Interlocked.Increment(ref attempts);
            throw new InvalidOperationException("silo not dispatch-ready yet");
        });
        tree.EntriesAsync().ReturnsForAnyArgs(_ =>
        {
            Interlocked.Increment(ref attempts);
            throw new InvalidOperationException("silo not dispatch-ready yet");
        });

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).ReturnsForAnyArgs(tree);

        var sweep = Sweep(Store(grainFactory), Scheduler(grainFactory));
        await sweep.StartAsync(Ct);
        try
        {
            var retried = await WaitForAsync(() => Volatile.Read(ref attempts) >= 3, Ct);
            Assert.That(retried, Is.True,
                $"a failed sweep must retry promptly with backoff (observed {Volatile.Read(ref attempts)} attempts)");
        }
        finally
        {
            await sweep.StopAsync(Ct);
        }
    }

    [Test]
    public async Task A_sweep_cancelled_by_shutdown_stops_without_faulting()
    {
        // Shutdown cancellation is an ordinary stop, not a sweep failure: reporting
        // it as one would spin the retry loop against a host that is going away.
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tree = Substitute.For<ILattice>();
        tree.KeysAsync().ReturnsForAnyArgs(call => BlockUntilCancelled(entered, Token(call.Args())));
        tree.EntriesAsync().ReturnsForAnyArgs(call => BlockEntriesUntilCancelled(entered, Token(call.Args())));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).ReturnsForAnyArgs(tree);

        var sweep = Sweep(Store(grainFactory), Scheduler(grainFactory));
        await sweep.StartAsync(Ct);

        Assert.That(await WaitForAsync(() => entered.Task.IsCompleted, Ct), Is.True,
            "precondition: the sweep must be parked inside a listing when shutdown arrives");

        Assert.That(async () => await sweep.StopAsync(Ct), Throws.Nothing);
        Assert.That(sweep.ExecuteTask, Is.Null.Or.Property(nameof(Task.IsFaulted)).False);
    }

    [Test]
    public async Task The_sweep_arms_nothing_when_scheduling_is_off()
    {
        // Exact retrieval, the switch off, or no embedding provider: there is no
        // approximate index to build, so no coordinator may be spent on one.
        var grainFactory = GrainFactoryListing("alpha");
        var sweep = Sweep(Store(grainFactory), Scheduler(grainFactory, withEmbedder: false));

        await sweep.StartAsync(Ct);
        await Task.Delay(100, Ct);
        await sweep.StopAsync(Ct);

        grainFactory.DidNotReceive().GetGrain<IRepoContextAnnIndexBuildGrain>(Arg.Any<string>());
    }

    private static CancellationToken Token(object?[] args)
        => args.OfType<CancellationToken>().FirstOrDefault();

    private static async IAsyncEnumerable<string> BlockUntilCancelled(
        TaskCompletionSource entered,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        entered.TrySetResult();
        await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
        yield break;
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> BlockEntriesUntilCancelled(
        TaskCompletionSource entered,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        entered.TrySetResult();
        await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
        yield break;
    }

    /// <summary>
    /// A minimal embedding provider that advertises a space and nothing else. The
    /// scheduler only ever reads <see cref="IEmbeddingProvider.Space"/>.
    /// </summary>
    private sealed class StubEmbedder : IEmbeddingProvider
    {
        public static StubEmbedder Instance { get; } = new();

        public EmbeddingSpace Space { get; } = new("test-model", 8, normalized: true);

        public Task<bool> IsAvailableAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(true);

        public Task<EmbeddingResult> EmbedAsync(
            IReadOnlyList<string> texts,
            EmbeddingTextType textType,
            CancellationToken cancellationToken = default)
            => throw new NotSupportedException("The sweep never embeds.");
    }
}
