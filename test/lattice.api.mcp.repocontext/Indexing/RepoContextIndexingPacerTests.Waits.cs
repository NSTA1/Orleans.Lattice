using NSubstitute;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// The <see cref="RepoContextIndexingPacer.PaceAsync"/> waits (the backed-off delay,
/// the bounded saturation wait and foreground yield, the duty-cycle rest, the idle
/// reset, cancellation) and the background-deferral predicate.
/// </summary>
public sealed partial class RepoContextIndexingPacerTests
{
    [Test]
    public async Task PaceAsync_backed_off_holds_the_batch_for_the_current_delay()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock);
        Batch(clock, pacer, succeeded: false);

        var pace = pacer.PaceAsync(CancellationToken.None).AsTask();
        Assert.Multiple(() =>
        {
            Assert.That(pace.IsCompleted, Is.False, "A backed-off pacer must hold the batch.");
            Assert.That(pacer.Snapshot().State, Is.EqualTo(RepoIndexPaceState.Backoff));
        });

        var elapsed = await DriveToCompletionAsync(
            clock, pace, TimeSpan.FromMilliseconds(50), TimeSpan.FromSeconds(5), "the 250 ms backoff to elapse");

        Assert.That(elapsed, Is.EqualTo(TimeSpan.FromMilliseconds(250)));
    }

    [Test]
    public async Task PaceAsync_at_the_full_rate_releases_at_once()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock);
        Batch(clock, pacer, succeeded: true);

        var pace = pacer.PaceAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pace.IsCompleted, Is.True);
            Assert.That(pacer.Snapshot().State, Is.EqualTo(RepoIndexPaceState.Pacing));
        });
        await pace;
    }

    [Test]
    public async Task PaceAsync_saturated_vector_tree_holds_the_batch_until_it_recovers()
    {
        var clock = new ManualTimeProvider();
        var signal = Signal(RepoContextTrees.VectorPayload, WalSaturationState.Saturated);
        var pacer = Create(clock, saturation: signal);

        var pace = pacer.PaceAsync(CancellationToken.None).AsTask();
        await AdvanceStepwiseAsync(clock, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(2));

        Assert.Multiple(() =>
        {
            Assert.That(pace.IsCompleted, Is.False, "A saturated tree holds the batch.");
            Assert.That(pacer.Snapshot().State, Is.EqualTo(RepoIndexPaceState.Waiting));
            Assert.That(pacer.Snapshot().Reason, Does.Contain(RepoContextTrees.VectorPayload));
            Assert.That(pacer.ShouldDeferBackground(out _), Is.True, "A batch parked on saturation defers maintenance.");
        });

        signal.GetCurrentState(RepoContextTrees.VectorPayload).Returns(WalSaturationState.Healthy);
        await DriveToCompletionAsync(
            clock, pace, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(5), "the batch to run once the tree recovers");

        Assert.That(
            pacer.Snapshot().BatchDelayMilliseconds,
            Is.EqualTo(250),
            "Saturation leaves the first batch after it at the backed-off rate.");
    }

    [Test]
    public async Task PaceAsync_saturation_wait_is_bounded()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock, saturation: Signal(RepoContextTrees.VectorMetadata, WalSaturationState.Saturated));

        var pace = pacer.PaceAsync(CancellationToken.None).AsTask();
        var elapsed = await DriveToCompletionAsync(
            clock, pace, TimeSpan.FromMilliseconds(200), TimeSpan.FromMinutes(2), "a wedged tree to stop holding the pass");

        // The bounded wait, then the 250 ms backoff the saturation raised.
        Assert.That(
            elapsed,
            Is.EqualTo(RepoContextIndexingPacer.MaxSaturationWait + TimeSpan.FromMilliseconds(250))
                .Within(TimeSpan.FromMilliseconds(200)));
    }

    [Test]
    public async Task PaceAsync_foreground_request_yields_until_it_ends()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock);
        var lease = pacer.EnterForeground();

        var pace = pacer.PaceAsync(CancellationToken.None).AsTask();
        await AdvanceStepwiseAsync(clock, TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(500));

        Assert.Multiple(() =>
        {
            Assert.That(pace.IsCompleted, Is.False);
            Assert.That(pacer.Snapshot().State, Is.EqualTo(RepoIndexPaceState.Yielding));
            Assert.That(pacer.Snapshot().ForegroundRequests, Is.EqualTo(1));
        });

        lease.Dispose();
        var elapsed = await DriveToCompletionAsync(
            clock, pace, TimeSpan.FromMilliseconds(50), TimeSpan.FromSeconds(1), "the batch to run after the lease ends");

        Assert.That(elapsed, Is.LessThanOrEqualTo(RepoContextIndexingPacer.ForegroundPollInterval));
    }

    [Test]
    public async Task PaceAsync_foreground_yield_is_bounded()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock);
        using var lease = pacer.EnterForeground();

        var pace = pacer.PaceAsync(CancellationToken.None).AsTask();
        var elapsed = await DriveToCompletionAsync(
            clock, pace, TimeSpan.FromMilliseconds(50), TimeSpan.FromSeconds(10), "a query stream to stop holding the pass");

        Assert.That(
            elapsed,
            Is.EqualTo(RepoContextIndexingPacer.MaxForegroundYield).Within(TimeSpan.FromMilliseconds(50)));
    }

    [Test]
    public async Task PaceAsync_spent_work_slice_rests_then_starts_a_fresh_slice()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock, Options(slice: TimeSpan.FromSeconds(60), rest: TimeSpan.FromSeconds(5)));
        Batch(clock, pacer, succeeded: true);
        await pacer.PaceAsync(CancellationToken.None);

        Batch(clock, pacer, succeeded: true, TimeSpan.FromSeconds(61));
        var pace = pacer.PaceAsync(CancellationToken.None).AsTask();

        Assert.Multiple(() =>
        {
            Assert.That(pace.IsCompleted, Is.False, "A spent slice must rest.");
            Assert.That(pacer.Snapshot().State, Is.EqualTo(RepoIndexPaceState.Resting));
        });

        var elapsed = await DriveToCompletionAsync(
            clock, pace, TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(30), "the 5 s rest to elapse");
        var next = pacer.PaceAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(elapsed, Is.EqualTo(TimeSpan.FromSeconds(5)));
            Assert.That(next.IsCompleted, Is.True, "The rest starts a fresh slice, so the next batch runs at once.");
        });
        await next;
    }

    [Test]
    public async Task PaceAsync_zero_slice_switches_the_duty_cycle_off()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock, Options(slice: TimeSpan.Zero));
        Batch(clock, pacer, succeeded: true);
        await pacer.PaceAsync(CancellationToken.None);

        Batch(clock, pacer, succeeded: true, TimeSpan.FromSeconds(100));
        var pace = pacer.PaceAsync(CancellationToken.None);

        Assert.That(pace.IsCompleted, Is.True);
        await pace;
    }

    [Test]
    public async Task PaceAsync_after_an_idle_spell_resets_the_delay()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock);
        for (var i = 0; i < 4; i++)
        {
            Batch(clock, pacer, succeeded: false);
        }

        clock.Advance(RepoContextIndexingPacer.IdleAfter + TimeSpan.FromSeconds(1));
        var idle = pacer.Snapshot();
        var pace = pacer.PaceAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(idle.State, Is.EqualTo(RepoIndexPaceState.Idle));
            Assert.That(pace.IsCompleted, Is.True, "An idle pacer releases the first batch of a new pass at once.");
            Assert.That(pacer.Snapshot().BatchDelayMilliseconds, Is.Zero);
        });
        await pace;
    }

    [Test]
    public void PaceAsync_cancelled_mid_wait_throws_and_leaves_no_stale_wait()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock, saturation: Signal(RepoContextTrees.VectorPayload, WalSaturationState.Saturated));
        using var cts = new CancellationTokenSource();

        var pace = pacer.PaceAsync(cts.Token).AsTask();
        Assert.That(pacer.Snapshot().State, Is.EqualTo(RepoIndexPaceState.Waiting), "Precondition: the batch is parked.");
        cts.Cancel();

        Assert.Multiple(() =>
        {
            Assert.That(async () => await pace, Throws.InstanceOf<OperationCanceledException>());
            Assert.That(pacer.Snapshot().State, Is.Not.EqualTo(RepoIndexPaceState.Waiting));
            Assert.That(pacer.ShouldDeferBackground(out _), Is.False, "Nobody is waiting any more.");
        });
    }

    [Test]
    public void ShouldDeferBackground_defers_to_foreground_requests_and_a_recent_backoff()
    {
        var clock = new ManualTimeProvider();
        var pacer = Create(clock);

        Assert.That(pacer.ShouldDeferBackground(out _), Is.False, "Nothing in flight and nothing backed off.");

        using (pacer.EnterForeground())
        {
            Assert.Multiple(() =>
            {
                Assert.That(pacer.ShouldDeferBackground(out var reason), Is.True);
                Assert.That(reason, Does.Contain("foreground"));
            });
        }

        Assert.That(pacer.ShouldDeferBackground(out _), Is.False, "The lease ended, so maintenance may run.");

        Batch(clock, pacer, succeeded: false);
        Assert.Multiple(() =>
        {
            Assert.That(pacer.ShouldDeferBackground(out var reason), Is.True);
            Assert.That(reason, Does.Contain("backing off"));
        });

        clock.Advance(RepoContextIndexingPacer.IdleAfter + TimeSpan.FromSeconds(1));
        Assert.That(pacer.ShouldDeferBackground(out _), Is.False, "A backoff nobody is running under is stale.");
    }

    [Test]
    public void EnterForeground_lease_disposed_twice_releases_once()
    {
        var pacer = Create(new ManualTimeProvider());
        var first = pacer.EnterForeground();
        using var second = pacer.EnterForeground();

        first.Dispose();
        first.Dispose();

        Assert.That(pacer.Snapshot().ForegroundRequests, Is.EqualTo(1));
    }
}
