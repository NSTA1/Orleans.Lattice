using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// Unit tests for <see cref="RepoIndexResetLauncher"/>: the reset sweep runs bound to
/// the host lifetime rather than to the caller's wait, under the caller's credential,
/// and a fault the sweep did not record reaches the job surface as a terminal
/// failure while a host shutdown does not (issue #2642).
/// </summary>
[TestFixture]
public sealed class RepoIndexResetLauncherTests
{
    private const string RepoId = "acme";

    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private IRepoIndexJobGrain _job = null!;
    private IGrainFactory _grainFactory = null!;
    private IHostApplicationLifetime _lifetime = null!;
    private CancellationTokenSource _stopping = null!;

    [SetUp]
    public void SetUp()
    {
        _job = Substitute.For<IRepoIndexJobGrain>();
        _grainFactory = Substitute.For<IGrainFactory>();
        _grainFactory.GetGrain<IRepoIndexJobGrain>(RepoId).Returns(_job);
        _stopping = new CancellationTokenSource();
        _lifetime = Substitute.For<IHostApplicationLifetime>();
        _lifetime.ApplicationStopping.Returns(_stopping.Token);
    }

    [TearDown]
    public void TearDown() => _stopping.Dispose();

    private RepoIndexResetLauncher Create(Func<string, CancellationToken, Task<RepoContextIndexResetResult>> reset)
        => new(reset, _grainFactory, _lifetime, NullLogger<RepoIndexResetLauncher>.Instance);

    private static RepoContextIndexResetResult Result(int deleted) => new()
    {
        RepoId = RepoId,
        EntriesDeleted = deleted,
        ElapsedMilliseconds = 1,
        TreesSwept = ["structural"],
        MemoryPreserved = true,
        CensusCleared = true,
    };

    private void JobReports(RepoIndexStatus status, RepoIndexPhase phase)
        => _job.GetProgressAsync().Returns(new RepoIndexProgress { RepoId = RepoId, Status = status, Phase = phase });

    [Test]
    public void Constructor_rejects_null_arguments()
    {
        Func<string, CancellationToken, Task<RepoContextIndexResetResult>> reset = (_, _) => Task.FromResult(Result(0));
        var logger = NullLogger<RepoIndexResetLauncher>.Instance;

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => _ = new RepoIndexResetLauncher((RepoContextStore)null!, _grainFactory, _lifetime, logger));
            Assert.Throws<ArgumentNullException>(() => _ = new RepoIndexResetLauncher(
                (Func<string, CancellationToken, Task<RepoContextIndexResetResult>>)null!, _grainFactory, _lifetime, logger));
            Assert.Throws<ArgumentNullException>(() => _ = new RepoIndexResetLauncher(reset, null!, _lifetime, logger));
            Assert.Throws<ArgumentNullException>(() => _ = new RepoIndexResetLauncher(reset, _grainFactory, null!, logger));
            Assert.Throws<ArgumentNullException>(() => _ = new RepoIndexResetLauncher(reset, _grainFactory, _lifetime, null!));
        });
    }

    [Test]
    public void ResetAsync_rejects_a_null_repo_id()
    {
        var launcher = Create((_, _) => Task.FromResult(Result(0)));

        Assert.Throws<ArgumentNullException>(() => launcher.ResetAsync(null!, CancellationToken.None));
    }

    [Test]
    public async Task ResetAsync_runs_the_sweep_under_the_host_stopping_token_and_returns_its_result()
    {
        CancellationToken observed = default;
        string? observedRepo = null;
        var launcher = Create((repo, ct) =>
        {
            observedRepo = repo;
            observed = ct;
            return Task.FromResult(Result(7));
        });

        using var wait = new CancellationTokenSource();
        var result = await launcher.ResetAsync(RepoId, wait.Token).WaitAsync(Timeout);

        Assert.Multiple(() =>
        {
            Assert.That(result.EntriesDeleted, Is.EqualTo(7));
            Assert.That(observedRepo, Is.EqualTo(RepoId));
            Assert.That(observed, Is.EqualTo(_stopping.Token),
                "The sweep is bound to the host lifetime, not to the caller's wait token.");
        });
    }

    [Test]
    public async Task ResetAsync_returns_the_sweep_directly_when_the_wait_cannot_be_cancelled()
    {
        var launcher = Create((_, _) => Task.FromResult(Result(2)));

        var result = await launcher.ResetAsync(RepoId, CancellationToken.None).WaitAsync(Timeout);

        Assert.That(result.EntriesDeleted, Is.EqualTo(2));
    }

    [Test]
    public async Task ResetAsync_cancelling_the_wait_abandons_the_wait_but_not_the_sweep()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finished = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var launcher = Create(async (_, ct) =>
        {
            await release.Task;
            finished.TrySetResult(ct.IsCancellationRequested);
            return Result(1);
        });

        using var wait = new CancellationTokenSource();
        var waiting = launcher.ResetAsync(RepoId, wait.Token);
        await wait.CancelAsync();

        Assert.CatchAsync<OperationCanceledException>(async () => await waiting,
            "The caller's wait ends as soon as the caller cancels.");

        release.SetResult();
        var sweepSawCancellation = await finished.Task.WaitAsync(Timeout);

        Assert.That(sweepSawCancellation, Is.False,
            "The sweep ran to the end with an uncancelled token after the caller stopped waiting.");
    }

    [Test]
    public async Task ResetAsync_runs_the_sweep_under_the_calling_credential()
    {
        var credential = new LatticeCredential("token", "Bearer", "caller");
        LatticeCredential? observed = null;
        var launcher = Create((_, _) =>
        {
            observed = LatticeCredentialContext.Current;
            return Task.FromResult(Result(0));
        });

        Task<RepoContextIndexResetResult> running;
        using (LatticeCredentialContext.With(credential))
        {
            running = launcher.ResetAsync(RepoId, CancellationToken.None);
        }

        await running.WaitAsync(Timeout);

        Assert.That(observed, Is.EqualTo(credential),
            "The detached sweep writes under the caller's credential, so the fail-closed access gate still authorizes it.");
    }

    [Test]
    public async Task ResetAsync_records_an_unrecorded_fault_as_a_failed_job_and_rethrows_it()
    {
        JobReports(RepoIndexStatus.Running, RepoIndexPhase.Resetting);
        var launcher = Create((_, _) => Task.FromException<RepoContextIndexResetResult>(new InvalidOperationException("silo gone")));

        var thrown = Assert.CatchAsync<InvalidOperationException>(
            async () => await launcher.ResetAsync(RepoId, CancellationToken.None).WaitAsync(Timeout));

        Assert.That(thrown!.Message, Is.EqualTo("silo gone"));
        await _job.Received(1).FailAsync(Arg.Is<string>(e => e.Contains("silo gone")));
    }

    [Test]
    public async Task ResetAsync_leaves_a_job_that_is_no_longer_resetting_untouched_on_fault()
    {
        JobReports(RepoIndexStatus.Failed, RepoIndexPhase.Resetting);
        var launcher = Create((_, _) => Task.FromException<RepoContextIndexResetResult>(new InvalidOperationException("already recorded")));

        Assert.CatchAsync<InvalidOperationException>(
            async () => await launcher.ResetAsync(RepoId, CancellationToken.None).WaitAsync(Timeout));

        await _job.DidNotReceive().FailAsync(Arg.Any<string>());
    }

    [Test]
    public async Task ResetAsync_does_not_record_a_host_shutdown_as_a_failure()
    {
        JobReports(RepoIndexStatus.Running, RepoIndexPhase.Resetting);
        await _stopping.CancelAsync();
        var launcher = Create((_, ct) => Task.FromCanceled<RepoContextIndexResetResult>(ct));

        Assert.CatchAsync<OperationCanceledException>(
            async () => await launcher.ResetAsync(RepoId, CancellationToken.None).WaitAsync(Timeout));

        await _job.DidNotReceive().FailAsync(Arg.Any<string>());
    }

    [Test]
    public void ResetAsync_surfaces_the_sweep_fault_even_when_recording_it_fails()
    {
        _job.GetProgressAsync().ThrowsAsync(new InvalidOperationException("job grain unreachable"));
        var launcher = Create((_, _) => Task.FromException<RepoContextIndexResetResult>(new InvalidOperationException("sweep fault")));

        var thrown = Assert.CatchAsync<InvalidOperationException>(
            async () => await launcher.ResetAsync(RepoId, CancellationToken.None).WaitAsync(Timeout));

        Assert.That(thrown!.Message, Is.EqualTo("sweep fault"),
            "A failure to record the fault must not mask the fault itself.");
    }
}
