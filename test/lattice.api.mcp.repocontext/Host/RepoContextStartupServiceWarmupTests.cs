using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit coverage for the retry / cancellation behaviour of
/// <see cref="RepoContextStartupService.WarmupAsync"/>.
///
/// Warmup doubles as the container's readiness gate: it seeds the local agent's
/// grant through the durable stores, so a success proves those stores are
/// reachable and writable. The behaviour that matters when they are *not* is
/// therefore the interesting half - the host must stay not-ready and keep
/// retrying rather than crash-looping or, far worse, flipping ready anyway and
/// attracting traffic it cannot serve. Cancellation must also unwind quietly, so
/// a shutdown during a retry backoff does not surface as a spurious fault.
/// </summary>
[TestFixture]
public sealed class RepoContextStartupServiceWarmupTests
{
    private static RepoContextStartupService CreateService(
        ILatticeAuthorizationPolicyStore store,
        RepoContextReadinessState readiness)
        => new(
            store,
            Substitute.For<ILatticeSchemaVersionAdmin>(),
            readiness,
            Substitute.For<IHostApplicationLifetime>(),
            NullLogger<RepoContextStartupService>.Instance);

    [Test]
    public async Task Warmup_does_not_mark_ready_when_the_seed_is_cancelled()
    {
        var store = Substitute.For<ILatticeAuthorizationPolicyStore>();
        store.PutRuleAsync(Arg.Any<LatticeAuthorizationRule>(), Arg.Any<CancellationToken>())
            .Throws(new OperationCanceledException());
        var readiness = new RepoContextReadinessState();
        var service = CreateService(store, readiness);

        await service.WarmupAsync(CancellationToken.None);

        Assert.That(readiness.IsReady, Is.False,
            "A cancelled seed proves nothing about store reachability, so readiness must stay closed.");
    }

    [Test]
    public async Task Warmup_stops_retrying_when_cancelled_during_the_backoff_delay()
    {
        var store = Substitute.For<ILatticeAuthorizationPolicyStore>();
        store.PutRuleAsync(Arg.Any<LatticeAuthorizationRule>(), Arg.Any<CancellationToken>())
            .Throws(new InvalidOperationException("grain storage unreachable"));
        var readiness = new RepoContextReadinessState();
        var service = CreateService(store, readiness);
        using var cts = new CancellationTokenSource();

        // The first backoff is seconds long; cancelling inside it proves the wait
        // is interruptible and unwinds without surfacing a fault to the host.
        cts.CancelAfter(TimeSpan.FromMilliseconds(150));
        await service.WarmupAsync(cts.Token);

        Assert.That(readiness.IsReady, Is.False);
    }

    [Test]
    public async Task Warmup_retries_a_transient_failure_and_then_marks_ready()
    {
        var attempts = 0;
        var store = Substitute.For<ILatticeAuthorizationPolicyStore>();
        store.PutRuleAsync(Arg.Any<LatticeAuthorizationRule>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                // Fail only the very first rule write. SeedAccessAsync aborts the
                // whole pass on its first failure, so failing more than once would
                // compound the exponential backoff across many passes.
                if (Interlocked.Increment(ref attempts) == 1)
                {
                    throw new InvalidOperationException("durable store not yet reachable");
                }

                return Task.CompletedTask;
            });
        var readiness = new RepoContextReadinessState();
        var service = CreateService(store, readiness);

        await service.WarmupAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(readiness.IsReady, Is.True,
                "Warmup must recover once the stores come up rather than stay down permanently.");
            Assert.That(attempts, Is.GreaterThan(RepoContextHostTrees.All.Count),
                "The retry pass must re-seed every tree, not resume mid-way.");
        });
    }

    [Test]
    public async Task Warmup_returns_immediately_when_already_cancelled()
    {
        var store = Substitute.For<ILatticeAuthorizationPolicyStore>();
        var readiness = new RepoContextReadinessState();
        var service = CreateService(store, readiness);

        await service.WarmupAsync(new CancellationToken(canceled: true));

        Assert.That(readiness.IsReady, Is.False);
        await store.DidNotReceive().PutRuleAsync(
            Arg.Any<LatticeAuthorizationRule>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task StopAsync_drains_readiness_even_when_warmup_never_started()
    {
        var store = Substitute.For<ILatticeAuthorizationPolicyStore>();
        var readiness = new RepoContextReadinessState();
        readiness.MarkReady();
        var service = CreateService(store, readiness);

        await service.StopAsync(CancellationToken.None);

        Assert.That(readiness.IsReady, Is.False,
            "Draining before the silo stops is what stops a load balancer routing new work mid-shutdown.");
    }

    [Test]
    public async Task StartAsync_registers_the_lifecycle_hooks_without_blocking()
    {
        var store = Substitute.For<ILatticeAuthorizationPolicyStore>();
        var lifetime = Substitute.For<IHostApplicationLifetime>();
        var service = new RepoContextStartupService(
            store,
            Substitute.For<ILatticeSchemaVersionAdmin>(),
            new RepoContextReadinessState(),
            lifetime,
            NullLogger<RepoContextStartupService>.Instance);

        await service.StartAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            _ = lifetime.Received().ApplicationStarted;
            _ = lifetime.Received().ApplicationStopping;
        });
    }
}
