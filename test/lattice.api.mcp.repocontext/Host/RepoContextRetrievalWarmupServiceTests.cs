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
    /// <summary>
    /// A <see cref="TimeProvider"/> that collapses every wait to zero and reports each
    /// one to the test before it starts, so the supervision loop can be single-stepped
    /// with no wall-clock cost and no polling.
    /// <para>
    /// The hook is what makes a regime change expressible. A fixed-regime fake can only
    /// pin steady states - readiness that is ready, or readiness that never was - and
    /// the defect under test is precisely a <em>transition</em>: readiness held, then
    /// revoked. Timer creation is the loop's only externally-observable act while it is
    /// supervising, so hooking it is the one place a test can change the world
    /// mid-iteration and see what the loop does next.
    /// </para>
    /// </summary>
    /// <param name="onWait">
    /// Invoked with the 1-based ordinal of each wait, before the wait elapses.
    /// </param>
    private sealed class SteppingTimeProvider(Action<int> onWait) : TimeProvider
    {
        private int _waits;

        /// <summary>The number of waits the loop has requested.</summary>
        public int Waits => Volatile.Read(ref _waits);

        /// <inheritdoc />
        public override ITimer CreateTimer(
            TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            onWait(Interlocked.Increment(ref _waits));

            // Zero, not the requested due time: the loop's cadence is not under test,
            // and a test that honoured a 30 s supervision interval would take 30 s.
            return base.CreateTimer(callback, state, TimeSpan.Zero, period);
        }
    }

    /// <summary>
    /// A readiness state whose fault hold-down is zero, so a single
    /// <c>MarkUnavailable</c> revokes readiness at once rather than after a grace
    /// window. The hold-down itself is pinned by the readiness fixture; here it would
    /// only add a clock to advance.
    /// </summary>
    private static RepoContextRetrievalReadinessState PromptlyRevocableReadiness()
        => new(new SettableTimeProvider(), TimeSpan.Zero);

    private static RepoContextRetrievalWarmupService Create(
        IRepoContextRetrievalWarmup pass,
        RepoContextRetrievalReadinessState readiness,
        IEmbeddingProvider? embeddingProvider,
        TimeProvider? timeProvider = null)
        => new(
            pass,
            readiness,
            Substitute.For<IHostApplicationLifetime>(),
            NullLogger<RepoContextRetrievalWarmupService>.Instance,
            embeddingProvider,
            timeProvider);

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
    public async Task Warmup_supervises_without_re_running_a_pass_while_the_plane_stays_ready()
    {
        // The steady-state arm, and the control for the revocation test below: once the
        // plane serves, supervision must be a poll of an in-memory phase and nothing
        // more. A loop that re-ran the pass on every tick would embed a query and hit
        // the vector plane every 30 s forever, which is why "it re-drives when needed"
        // is only half the contract.
        using var readiness = PromptlyRevocableReadiness();
        using var cts = new CancellationTokenSource();
        var passes = 0;
        var pass = Substitute.For<IRepoContextRetrievalWarmup>();
        pass.TryWarmAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            passes++;
            readiness.MarkServing();
            return true;
        });

        // Let the loop supervise a few times over a plane that never faults, then stop.
        var clock = new SteppingTimeProvider(wait =>
        {
            if (wait >= 3)
            {
                cts.Cancel();
            }
        });
        var service = Create(pass, readiness, Substitute.For<IEmbeddingProvider>(), clock);

        await service.WarmupAsync(cts.Token);

        Assert.Multiple(() =>
        {
            Assert.That(clock.Waits, Is.GreaterThanOrEqualTo(3),
                "the loop must keep supervising after it converges, not return");
            Assert.That(passes, Is.EqualTo(1),
                "a ready plane must be polled, not re-warmed: only the first pass may run");
            Assert.That(readiness.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Serving));
        });
    }

    [Test]
    public async Task Warmup_re_drives_a_pass_when_readiness_is_revoked_after_it_had_converged()
    {
        // The defect this fixture exists for. The loop used to return the moment a pass
        // reported ready, which is only correct if readiness is a latch - and it is not:
        // an observed fault outliving the hold-down revokes it. On an idle container
        // nothing else restores it, so a live box sat at 503 for five and a half hours
        // after a single fault episode, through its vector plane finishing its build.
        //
        // The regime therefore has to CHANGE mid-test: ready, then revoked, then
        // observed. Asserting a steady state at either end would pass against the
        // one-shot loop.
        using var readiness = PromptlyRevocableReadiness();
        using var cts = new CancellationTokenSource();
        var passes = 0;
        var pass = Substitute.For<IRepoContextRetrievalWarmup>();
        pass.TryWarmAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            passes++;
            readiness.MarkServing();
            return true;
        });

        var clock = new SteppingTimeProvider(wait =>
        {
            switch (wait)
            {
                // First supervision tick: the plane faults while the loop is waiting,
                // exactly as the container's single unavailable episode did.
                case 1:
                    readiness.MarkUnavailable("vector_plane_unavailable");
                    break;

                // By the second wait the loop has already had to notice the revocation
                // and act on it, so there is nothing left to observe.
                default:
                    cts.Cancel();
                    break;
            }
        });
        var service = Create(pass, readiness, Substitute.For<IEmbeddingProvider>(), clock);

        await service.WarmupAsync(cts.Token);

        Assert.Multiple(() =>
        {
            Assert.That(passes, Is.EqualTo(2),
                "a revocation after convergence must re-drive exactly one warmup pass");
            Assert.That(readiness.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Serving),
                "the re-driven pass must restore readiness without any client traffic");
            Assert.That(readiness.IsReady, Is.True);
        });
    }

    [Test]
    public async Task Warmup_keeps_supervising_when_a_re_driven_pass_cannot_restore_readiness()
    {
        // The unhappy half of recovery. A revocation whose cause has not cleared must
        // leave the loop retrying rather than wedged or exited - otherwise the fix would
        // merely move the one-shot from "first ready" to "first failed recovery", and a
        // plane that came back a minute later would never be noticed.
        using var readiness = PromptlyRevocableReadiness();
        using var cts = new CancellationTokenSource();
        var passes = 0;
        var pass = Substitute.For<IRepoContextRetrievalWarmup>();
        pass.TryWarmAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            passes++;

            // Serve on the first pass only; every recovery attempt then fails, so the
            // plane stays revoked and the loop has to keep coming back.
            if (passes > 1)
            {
                return false;
            }

            readiness.MarkServing();
            return true;
        });

        var clock = new SteppingTimeProvider(wait =>
        {
            if (wait == 1)
            {
                readiness.MarkUnavailable("vector_plane_unavailable");
            }
            else if (wait >= 4)
            {
                cts.Cancel();
            }
        });
        var service = Create(pass, readiness, Substitute.For<IEmbeddingProvider>(), clock);

        await service.WarmupAsync(cts.Token);

        Assert.Multiple(() =>
        {
            Assert.That(passes, Is.GreaterThanOrEqualTo(3),
                "a failing recovery must be retried, not abandoned");
            Assert.That(readiness.IsReady, Is.False,
                "readiness must stay revoked while the pass cannot restore it");
        });
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
