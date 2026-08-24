using Orleans.Lattice;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end cluster tests for <see cref="ILatticeLockGrain"/> exercising the real
/// grain over the Orleans TestingHost - a live activation, real reminder / timer
/// machinery, and real serialization of the lock value types across the grain
/// boundary. The deterministic decision logic is covered exhaustively by the pure
/// <c>LockAdmissionCore</c> unit tests and the grain's own unit tests with injected
/// clock / timer seams; these tests prove the wired-up grain honours the same
/// contract under the actual runtime.
/// </summary>
[TestFixture]
[Category("Integration")]
public class LatticeLockGrainIntegrationTests
{
    private ClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    private ILatticeLockGrain Lock(string name) =>
        _cluster.GrainFactory.GetGrain<ILatticeLockGrain>(name);

    [Test]
    public async Task AcquireAsync_grants_a_positive_fencing_token_and_marks_held()
    {
        var theLock = Lock("int-acquire-basic");

        var lease = await theLock.AcquireAsync(
            new LockAcquireRequest(TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(30)));

        Assert.That(lease.Token.FencingToken, Is.GreaterThan(0));
        Assert.That(lease.ExpiresAt, Is.GreaterThan(DateTimeOffset.UtcNow));

        var status = await theLock.GetStatusAsync();
        Assert.That(status.IsHeld, Is.True);
        Assert.That(status.CurrentFencingToken, Is.EqualTo(lease.Token.FencingToken));
        Assert.That(status.QueueDepth, Is.EqualTo(0));
    }

    [Test]
    public async Task ReleaseAsync_frees_the_lock_and_clears_status()
    {
        var theLock = Lock("int-release-frees");

        var lease = await theLock.AcquireAsync(
            new LockAcquireRequest(TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(30)));
        await theLock.ReleaseAsync(lease.Token);

        var status = await theLock.GetStatusAsync();
        Assert.That(status.IsHeld, Is.False);
        Assert.That(status.LeaseExpiresAt, Is.Null);
        Assert.That(status.QueueDepth, Is.EqualTo(0));
    }

    [Test]
    public async Task TryAcquireAsync_returns_null_under_contention_then_grants_after_release()
    {
        var theLock = Lock("int-try-contention");

        var first = await theLock.AcquireAsync(
            new LockAcquireRequest(TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(30)));

        var contended = await theLock.TryAcquireAsync(TimeSpan.FromMinutes(5));
        Assert.That(contended, Is.Null, "the lock is held, a non-queuing try must fail");

        await theLock.ReleaseAsync(first.Token);

        var second = await theLock.TryAcquireAsync(TimeSpan.FromMinutes(5));
        Assert.That(second, Is.Not.Null);
        Assert.That(second!.Value.Token.FencingToken, Is.GreaterThan(first.Token.FencingToken),
            "a fresh grant must mint a strictly greater fencing token");
    }

    [Test]
    public async Task AcquireAsync_serves_waiters_in_strict_fifo_order_with_monotonic_tokens()
    {
        var theLock = Lock("int-fifo-order");

        var holder = await theLock.AcquireAsync(
            new LockAcquireRequest(TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(60)));

        // Enqueue two waiters (generous wait) while the lock is held.
        var waiterA = theLock.AcquireAsync(
            new LockAcquireRequest(TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(60)));
        // Give A time to reach the head of the queue before B enqueues, so the FIFO
        // order under test is deterministic.
        await WaitForQueueDepthAsync(theLock, 1);
        var waiterB = theLock.AcquireAsync(
            new LockAcquireRequest(TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(60)));
        await WaitForQueueDepthAsync(theLock, 2);

        // Release to the head (A).
        await theLock.ReleaseAsync(holder.Token);
        var leaseA = await waiterA;
        Assert.That(leaseA.Token.FencingToken, Is.GreaterThan(holder.Token.FencingToken));
        Assert.That(waiterB.IsCompleted, Is.False, "B must still be queued behind A");

        // Release A -> B is granted next.
        await theLock.ReleaseAsync(leaseA.Token);
        var leaseB = await waiterB;
        Assert.That(leaseB.Token.FencingToken, Is.GreaterThan(leaseA.Token.FencingToken),
            "each successive grant strictly increases the fencing token");
    }

    [Test]
    public async Task AcquireAsync_faults_with_timeout_when_max_wait_elapses()
    {
        var theLock = Lock("int-wait-timeout");

        var holder = await theLock.AcquireAsync(
            new LockAcquireRequest(TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(30)));

        Assert.That(
            async () => await theLock.AcquireAsync(
                new LockAcquireRequest(TimeSpan.FromMinutes(5), TimeSpan.FromMilliseconds(250))),
            Throws.TypeOf<TimeoutException>());

        // The timed-out waiter is removed from the queue.
        await WaitForQueueDepthAsync(theLock, 0);
        var status = await theLock.GetStatusAsync();
        Assert.That(status.QueueDepth, Is.EqualTo(0));
        Assert.That(status.CurrentFencingToken, Is.EqualTo(holder.Token.FencingToken),
            "the holder is undisturbed by a waiter's timeout");
    }

    [Test]
    public async Task RenewAsync_with_a_stale_token_is_rejected_after_reacquire()
    {
        var theLock = Lock("int-stale-renew");

        var first = await theLock.AcquireAsync(
            new LockAcquireRequest(TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(30)));
        await theLock.ReleaseAsync(first.Token);
        var second = await theLock.AcquireAsync(
            new LockAcquireRequest(TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(30)));

        Assert.That(
            async () => await theLock.RenewAsync(first.Token, TimeSpan.FromMinutes(5)),
            Throws.TypeOf<LatticeLockConflictException>(),
            "the superseded first token can no longer renew");

        // The current holder can still renew.
        var renewed = await theLock.RenewAsync(second.Token, TimeSpan.FromMinutes(5));
        Assert.That(renewed.Token.FencingToken, Is.EqualTo(second.Token.FencingToken));
    }

    [Test]
    public async Task ReleaseAsync_with_a_stale_token_is_a_silent_no_op()
    {
        var theLock = Lock("int-stale-release");

        var first = await theLock.AcquireAsync(
            new LockAcquireRequest(TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(30)));
        await theLock.ReleaseAsync(first.Token);
        var second = await theLock.AcquireAsync(
            new LockAcquireRequest(TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(30)));

        // Releasing under the superseded token must not disturb the current holder.
        await theLock.ReleaseAsync(first.Token);

        var status = await theLock.GetStatusAsync();
        Assert.That(status.IsHeld, Is.True);
        Assert.That(status.CurrentFencingToken, Is.EqualTo(second.Token.FencingToken));
    }

    [Test]
    public async Task Expired_lease_is_reclaimed_and_the_next_waiter_is_granted()
    {
        var theLock = Lock("int-lease-reclaim");

        // Hold a short lease and never renew or release it.
        var holder = await theLock.AcquireAsync(
            new LockAcquireRequest(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(60)));

        // A patient waiter should be granted once the holder's lease lapses and is
        // reclaimed by the in-activation lease timer.
        var waiter = theLock.AcquireAsync(
            new LockAcquireRequest(TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(60)));

        var reclaimed = await waiter;
        Assert.That(reclaimed.Token.FencingToken, Is.GreaterThan(holder.Token.FencingToken),
            "the reclaimed grant mints a strictly greater fencing token");
    }

    [Test]
    public async Task GetStatusAsync_reports_free_state_for_an_untouched_lock()
    {
        var status = await Lock("int-fresh-status").GetStatusAsync();

        Assert.That(status.IsHeld, Is.False);
        Assert.That(status.CurrentFencingToken, Is.EqualTo(0));
        Assert.That(status.LeaseExpiresAt, Is.Null);
        Assert.That(status.QueueDepth, Is.EqualTo(0));
    }

    /// <summary>
    /// Polls <see cref="ILatticeLockGrain.GetStatusAsync"/> until the FIFO queue
    /// reaches <paramref name="expected"/>, so a test can order enqueue operations
    /// deterministically without a fixed sleep.
    /// </summary>
    private static async Task WaitForQueueDepthAsync(ILatticeLockGrain theLock, int expected)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            var status = await theLock.GetStatusAsync();
            if (status.QueueDepth == expected)
            {
                return;
            }

            await Task.Delay(25);
        }

        Assert.Fail($"queue depth did not reach {expected} within the timeout");
    }
}
