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

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Empty()
    {
        await Task.CompletedTask;
        yield break;
    }
}
