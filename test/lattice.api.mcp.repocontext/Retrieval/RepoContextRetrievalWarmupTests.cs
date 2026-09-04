using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for <see cref="RepoContextRetrievalWarmup"/>: the pass that proves the
/// vector plane can serve by driving a real query through the ordinary search path.
/// </summary>
[TestFixture]
public sealed class RepoContextRetrievalWarmupTests
{
    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

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

    private static RepoContextSearchService Search(IGrainFactory grainFactory, RepoContextStore store)
        => new(
            grainFactory,
            Serializer,
            Substitute.For<IRepoContextSemanticIndex>(),
            store,
            TimeProvider.System,
            NullLogger<RepoContextSearchService>.Instance);

    [Test]
    public void Rejects_null_dependencies()
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var store = Store(grainFactory);
        var search = Search(grainFactory, store);
        using var readiness = new RepoContextRetrievalReadinessState(new SettableTimeProvider());
        var logger = NullLogger<RepoContextRetrievalWarmup>.Instance;

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new RepoContextRetrievalWarmup(null!, search, readiness, logger),
                Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextRetrievalWarmup(store, null!, readiness, logger),
                Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextRetrievalWarmup(store, search, null!, logger),
                Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextRetrievalWarmup(store, search, readiness, null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task An_empty_store_is_ready_with_nothing_to_serve()
    {
        // A fresh box must not wedge: blocking readiness before its first repository is
        // onboarded would stop the very traffic that onboards one.
        var tree = Substitute.For<ILattice>();
        tree.EntriesAsync().ReturnsForAnyArgs(_ => Empty());
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).ReturnsForAnyArgs(tree);

        var store = Store(grainFactory);
        using var readiness = new RepoContextRetrievalReadinessState(new SettableTimeProvider());
        var warmup = new RepoContextRetrievalWarmup(
            store, Search(grainFactory, store), readiness, NullLogger<RepoContextRetrievalWarmup>.Instance);

        var ready = await warmup.TryWarmAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ready, Is.True);
            Assert.That(readiness.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Serving));
        });
    }

    [Test]
    public async Task A_faulting_pass_fails_closed_and_leaves_the_plane_not_ready()
    {
        var tree = Substitute.For<ILattice>();
        tree.KeysAsync().ThrowsForAnyArgs(
            new InvalidOperationException("simulated stale leaf projection activation fault"));
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).ReturnsForAnyArgs(tree);

        var store = Store(grainFactory);
        using var readiness = new RepoContextRetrievalReadinessState(new SettableTimeProvider());
        var warmup = new RepoContextRetrievalWarmup(
            store, Search(grainFactory, store), readiness, NullLogger<RepoContextRetrievalWarmup>.Instance);

        var ready = await warmup.TryWarmAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ready, Is.False, "A warmup fault must never be reported as ready.");
            Assert.That(readiness.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Building));
        });
    }

    [Test]
    public async Task A_faulting_pass_never_revokes_readiness_already_proven()
    {
        // The pass reports the readiness state's own verdict and never demotes it: a
        // plane that has already served stays ready across a transient listing fault, so
        // the warmup can never make readiness oscillate.
        var tree = Substitute.For<ILattice>();
        tree.KeysAsync().ThrowsForAnyArgs(new InvalidOperationException("simulated listing fault"));
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).ReturnsForAnyArgs(tree);

        var store = Store(grainFactory);
        using var readiness = new RepoContextRetrievalReadinessState(new SettableTimeProvider());
        readiness.MarkServing();
        var warmup = new RepoContextRetrievalWarmup(
            store, Search(grainFactory, store), readiness, NullLogger<RepoContextRetrievalWarmup>.Instance);

        var ready = await warmup.TryWarmAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ready, Is.True);
            Assert.That(readiness.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Serving));
        });
    }

    [Test]
    public void A_cancelled_pass_propagates_cancellation()
    {
        var tree = Substitute.For<ILattice>();
        tree.KeysAsync().ThrowsForAnyArgs(new OperationCanceledException());
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).ReturnsForAnyArgs(tree);

        var store = Store(grainFactory);
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        using var readiness = new RepoContextRetrievalReadinessState(new SettableTimeProvider());
        var warmup = new RepoContextRetrievalWarmup(
            store, Search(grainFactory, store), readiness, NullLogger<RepoContextRetrievalWarmup>.Instance);

        Assert.That(
            async () => await warmup.TryWarmAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>(),
            "Cancellation must never be swallowed as a fail-closed fault.");
    }

    [Test]
    public async Task A_repository_that_answers_semantically_marks_the_plane_ready()
    {
        // The pass drives a real query per repository and stops at the first one
        // whose answer proves the vector plane can serve, so a healthy box never
        // pays a query for every indexed repository.
        var grainFactory = Substitute.For<IGrainFactory>();
        var tree = SeededTree(RepoMarkers("alpha", "beta"));
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).ReturnsForAnyArgs(tree);

        var store = Store(grainFactory);
        var index = SemanticIndex(RepoContextRetrievalPath.SemanticApproximate);
        using var readiness = new RepoContextRetrievalReadinessState(new SettableTimeProvider());
        var search = new RepoContextSearchService(
            grainFactory, Serializer, index, store, TimeProvider.System,
            NullLogger<RepoContextSearchService>.Instance, AvailableEmbedder(), readiness);
        var warmup = new RepoContextRetrievalWarmup(
            store, search, readiness, NullLogger<RepoContextRetrievalWarmup>.Instance);

        var ready = await warmup.TryWarmAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ready, Is.True);
            Assert.That(readiness.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Serving));
        });
        await index.Received(1).SearchAsync(
            "alpha", Arg.Any<ReadOnlyMemory<float>>(), Arg.Any<EmbeddingSpaceTag>(),
            Arg.Any<int>(), Arg.Any<CancellationToken>());
        await index.DidNotReceive().SearchAsync(
            "beta", Arg.Any<ReadOnlyMemory<float>>(), Arg.Any<EmbeddingSpaceTag>(),
            Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Every_repository_is_probed_when_the_vector_plane_cannot_serve()
    {
        // The plane faults on every query, so the pass walks the whole repository
        // set and still reports not-ready rather than promoting a degraded
        // keyword answer to a readiness signal.
        var grainFactory = Substitute.For<IGrainFactory>();
        var tree = SeededTree(RepoMarkers("alpha", "beta"));
        grainFactory.GetGrain<ILattice>(Arg.Any<string>()).ReturnsForAnyArgs(tree);

        var store = Store(grainFactory);
        var index = Substitute.For<IRepoContextSemanticIndex>();
        index.RetrievalPath.Returns(RepoContextRetrievalPath.SemanticApproximate);
        index.SearchAsync(
                Arg.Any<string>(), Arg.Any<ReadOnlyMemory<float>>(), Arg.Any<EmbeddingSpaceTag>(),
                Arg.Any<int>(), Arg.Any<CancellationToken>())
            .ThrowsAsyncForAnyArgs(new InvalidOperationException("simulated degraded semantic index"));

        using var readiness = new RepoContextRetrievalReadinessState(new SettableTimeProvider());
        var search = new RepoContextSearchService(
            grainFactory, Serializer, index, store, TimeProvider.System,
            NullLogger<RepoContextSearchService>.Instance, AvailableEmbedder(), readiness);
        var warmup = new RepoContextRetrievalWarmup(
            store, search, readiness, NullLogger<RepoContextRetrievalWarmup>.Instance);

        var ready = await warmup.TryWarmAsync(CancellationToken.None);

        Assert.That(ready, Is.False);
        Assert.That(readiness.Phase, Is.Not.EqualTo(RepoContextRetrievalReadinessPhase.Serving));
        await index.Received(2).SearchAsync(
            Arg.Any<string>(), Arg.Any<ReadOnlyMemory<float>>(), Arg.Any<EmbeddingSpaceTag>(),
            Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    private static IRepoContextSemanticIndex SemanticIndex(string retrievalPath)
    {
        var index = Substitute.For<IRepoContextSemanticIndex>();
        index.RetrievalPath.Returns(retrievalPath);
        index.SearchAsync(
                Arg.Any<string>(), Arg.Any<ReadOnlyMemory<float>>(), Arg.Any<EmbeddingSpaceTag>(),
                Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<RepoContextVectorMatch>>(
                [new RepoContextVectorMatch("vec-0", HydratedFileKey, 1d)]));
        return index;
    }

    /// <summary>The one source key <see cref="SeededTree"/> hydrates, so a semantic match resolves to a hit.</summary>
    private static readonly string HydratedFileKey = RepoContextKeys.File("alpha", "src/Widget.cs");

    private static IEmbeddingProvider AvailableEmbedder()
    {
        var space = new EmbeddingSpace("test-model", 3, true);
        var provider = Substitute.For<IEmbeddingProvider>();
        provider.Space.Returns(space);
        provider.IsAvailableAsync(Arg.Any<CancellationToken>()).Returns(true);
        provider.EmbedAsync(
                Arg.Any<IReadOnlyList<string>>(), Arg.Any<EmbeddingTextType>(), Arg.Any<CancellationToken>())
            .Returns(EmbeddingResult.Success(space, new[] { new ReadOnlyMemory<float>([1f, 0f, 0f]) }));
        return provider;
    }

    private static SortedDictionary<string, byte[]> RepoMarkers(params string[] repoIds)
    {
        var records = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
        var repoSerializer = new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider()
            .GetRequiredService<Serializer<RepoNode>>();
        foreach (var repoId in repoIds)
        {
            records[RepoContextKeys.Repo(repoId)] = repoSerializer.SerializeToArray(new RepoNode { RepoId = repoId });
        }

        return records;
    }

    /// <summary>
    /// An <see cref="ILattice"/> over an ordered in-memory map whose key scan
    /// HONOURS the requested window. A stub that ignores the window makes the
    /// store's per-repository cursor advance loop forever.
    /// </summary>
    private static ILattice SeededTree(SortedDictionary<string, byte[]> records)
    {
        var tree = Substitute.For<ILattice>();
        tree.KeysAsync().ReturnsForAnyArgs(call => Keys(
            records, call.ArgAt<string?>(0), call.ArgAt<string?>(1)));
        tree.EntriesAsync().ReturnsForAnyArgs(_ => Empty());
        tree.GetWithVersionAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ReturnsForAnyArgs(call => Task.FromResult(
                string.Equals(call.ArgAt<string>(0), HydratedFileKey, StringComparison.Ordinal)
                    ? new VersionedValue
                    {
                        Value = Serializer.SerializeToArray(
                            new FileNode { RepoId = "alpha", Path = "src/Widget.cs" }),
                    }
                    : new VersionedValue()));
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

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Empty()
    {
        await Task.CompletedTask;
        yield break;
    }
}
