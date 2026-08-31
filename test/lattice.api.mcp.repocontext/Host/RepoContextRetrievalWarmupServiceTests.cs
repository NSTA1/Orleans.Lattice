using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for <see cref="RepoContextRetrievalWarmupService"/>: the driver that stops
/// vector-plane readiness deadlocking on traffic an orchestrator will not route to a
/// not-ready box. The tests drive the internal pass methods directly, so no test waits on
/// a timer, a delay, or the host lifetime.
/// </summary>
[TestFixture]
public sealed class RepoContextRetrievalWarmupServiceTests
{
    private static RepoContextRetrievalWarmupService Create(
        IRepoContextRetrievalWarmup pass,
        RepoContextRetrievalReadinessState readiness,
        IEmbeddingProvider? embeddingProvider)
        => new(
            pass,
            readiness,
            Substitute.For<IHostApplicationLifetime>(),
            NullLogger<RepoContextRetrievalWarmupService>.Instance,
            embeddingProvider);

    [Test]
    public void Rejects_null_dependencies()
    {
        using var readiness = new RepoContextRetrievalReadinessState(new SettableTimeProvider());
        var pass = Substitute.For<IRepoContextRetrievalWarmup>();
        var lifetime = Substitute.For<IHostApplicationLifetime>();
        var logger = NullLogger<RepoContextRetrievalWarmupService>.Instance;

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new RepoContextRetrievalWarmupService(null!, readiness, lifetime, logger),
                Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextRetrievalWarmupService(pass, null!, lifetime, logger),
                Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextRetrievalWarmupService(pass, readiness, null!, logger),
                Throws.ArgumentNullException);
            Assert.That(
                () => new RepoContextRetrievalWarmupService(pass, readiness, lifetime, null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task Warmup_marks_keyword_only_and_issues_no_query_when_no_embedder_is_bound()
    {
        using var readiness = new RepoContextRetrievalReadinessState(new SettableTimeProvider());
        var pass = Substitute.For<IRepoContextRetrievalWarmup>();
        var service = Create(pass, readiness, embeddingProvider: null);

        await service.WarmupAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(readiness.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.KeywordOnly));
            Assert.That(readiness.IsReady, Is.True, "A keyword-only box must never wait on a vector plane it has not got.");
        });
        await pass.DidNotReceiveWithAnyArgs().TryWarmAsync(default);
    }

    [Test]
    public async Task Warmup_returns_as_soon_as_the_plane_serves()
    {
        using var readiness = new RepoContextRetrievalReadinessState(new SettableTimeProvider());
        var pass = Substitute.For<IRepoContextRetrievalWarmup>();
        pass.TryWarmAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            readiness.MarkServing();
            return true;
        });
        var service = Create(pass, readiness, Substitute.For<IEmbeddingProvider>());

        await service.WarmupAsync(CancellationToken.None);

        Assert.That(readiness.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Serving));
        await pass.Received(1).TryWarmAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Warmup_stops_without_marking_ready_when_shutdown_is_requested()
    {
        using var readiness = new RepoContextRetrievalReadinessState(new SettableTimeProvider());
        using var cts = new CancellationTokenSource();
        var pass = Substitute.For<IRepoContextRetrievalWarmup>();
        pass.TryWarmAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            cts.Cancel();
            return false;
        });
        var service = Create(pass, readiness, Substitute.For<IEmbeddingProvider>());

        await service.WarmupAsync(cts.Token);

        Assert.That(readiness.IsReady, Is.False);
    }

    [Test]
    public async Task A_pass_runs_under_the_trusted_local_agent_credential()
    {
        using var readiness = new RepoContextRetrievalReadinessState(new SettableTimeProvider());
        string? observedPrincipal = null;
        string? observedScheme = null;

        var pass = Substitute.For<IRepoContextRetrievalWarmup>();
        pass.TryWarmAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            var credential = LatticeCredentialContext.Current;
            observedPrincipal = credential?.Token;
            observedScheme = credential?.Scheme;
            return true;
        });
        var service = Create(pass, readiness, Substitute.For<IEmbeddingProvider>());

        await service.RunPassAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(observedPrincipal, Is.EqualTo(LocalTrustedAgent.SubjectId));
            Assert.That(observedScheme, Is.EqualTo(LocalTrustedAgent.Scheme));
        });
    }

    [Test]
    public async Task A_cancelled_pass_does_not_propagate_out_of_the_warmup_loop()
    {
        using var readiness = new RepoContextRetrievalReadinessState(new SettableTimeProvider());
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var pass = Substitute.For<IRepoContextRetrievalWarmup>();
        pass.TryWarmAsync(Arg.Any<CancellationToken>())
            .ThrowsAsyncForAnyArgs(new OperationCanceledException());
        var service = Create(pass, readiness, Substitute.For<IEmbeddingProvider>());

        Assert.That(async () => await service.WarmupAsync(cts.Token), Throws.Nothing);
    }

    [Test]
    public async Task Stop_is_safe_before_the_application_ever_started()
    {
        using var readiness = new RepoContextRetrievalReadinessState(new SettableTimeProvider());
        var service = Create(
            Substitute.For<IRepoContextRetrievalWarmup>(), readiness, Substitute.For<IEmbeddingProvider>());

        await service.StartAsync(CancellationToken.None);

        Assert.That(async () => await service.StopAsync(CancellationToken.None), Throws.Nothing);
    }
}
