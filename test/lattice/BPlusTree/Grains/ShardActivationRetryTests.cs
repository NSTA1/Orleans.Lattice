using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="ShardActivationRetry"/>, the bounded
/// internal-retry envelope that absorbs
/// <see cref="ShardActivationTimeoutException"/> from operator entry points
/// whose first turn drives the shard-root activation-readiness seed.
/// </summary>
[TestFixture]
public class ShardActivationRetryTests
{
    /// <summary>
    /// Happy path: an operation that returns on the first attempt is
    /// invoked exactly once and the helper returns synchronously without
    /// imposing any wait.
    /// </summary>
    [Test]
    public async Task RunAsync_invokes_operation_once_when_first_attempt_succeeds()
    {
        var calls = 0;
        await ShardActivationRetry.RunAsync(() =>
        {
            calls++;
            return Task.CompletedTask;
        });

        Assert.That(calls, Is.EqualTo(1));
    }

    /// <summary>
    /// Single-failure path: an operation that throws
    /// <see cref="ShardActivationTimeoutException"/> on the first attempt
    /// is retried and the second attempt's success is observed by the
    /// caller without the typed exception propagating.
    /// </summary>
    [Test]
    public async Task RunAsync_retries_through_first_failure_then_succeeds()
    {
        var calls = 0;
        await ShardActivationRetry.RunAsync(() =>
        {
            calls++;
            if (calls == 1) throw new ShardActivationTimeoutException("park-1");
            return Task.CompletedTask;
        });

        Assert.That(calls, Is.EqualTo(2));
    }

    /// <summary>
    /// Two-failure path: two consecutive <see cref="ShardActivationTimeoutException"/>
    /// failures are absorbed and the third attempt's success surfaces to the
    /// caller. This is the boundary case that exercises the full retry
    /// budget without exhausting it.
    /// </summary>
    [Test]
    public async Task RunAsync_retries_through_two_failures_then_succeeds()
    {
        var calls = 0;
        await ShardActivationRetry.RunAsync(() =>
        {
            calls++;
            if (calls < 3) throw new ShardActivationTimeoutException($"park-{calls}");
            return Task.CompletedTask;
        });

        Assert.That(calls, Is.EqualTo(ShardActivationRetry.MaxAttempts));
    }

    /// <summary>
    /// Exhaustion path: when every attempt throws
    /// <see cref="ShardActivationTimeoutException"/>, the most-recent
    /// exception is rethrown so the caller sees exactly the shape they
    /// would have seen pre-envelope (typed surface preserved, no
    /// double-wrap).
    /// </summary>
    [Test]
    public void RunAsync_rethrows_most_recent_typed_exception_when_budget_exhausted()
    {
        var calls = 0;
        var ex = Assert.ThrowsAsync<ShardActivationTimeoutException>(async () =>
            await ShardActivationRetry.RunAsync(() =>
            {
                calls++;
                throw new ShardActivationTimeoutException($"park-{calls}");
            }));

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(ShardActivationRetry.MaxAttempts),
                "Envelope did not exhaust the full retry budget before propagating.");
            Assert.That(ex!.Message, Is.EqualTo($"park-{ShardActivationRetry.MaxAttempts}"),
                "Envelope rethrew the wrong attempt's exception (must be the last, not the first).");
        });
    }

    /// <summary>
    /// Non-typed exceptions are <b>not</b> absorbed - the envelope is scoped
    /// strictly to the seed-timeout case and must propagate any other
    /// exception immediately, on the first attempt, without consuming a
    /// retry slot.
    /// </summary>
    [Test]
    public void RunAsync_propagates_non_typed_exceptions_without_retry()
    {
        var calls = 0;
        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await ShardActivationRetry.RunAsync(() =>
            {
                calls++;
                throw new InvalidOperationException("not-a-seed-timeout");
            }));

        Assert.That(calls, Is.EqualTo(1),
            "Envelope must not retry on unrelated exceptions.");
    }

    /// <summary>
    /// Cancellation observed inside the operation surfaces directly to the
    /// caller without being absorbed; only the typed seed-timeout shape
    /// triggers retry.
    /// </summary>
    [Test]
    public void RunAsync_propagates_OperationCanceledException_from_operation()
    {
        var calls = 0;
        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await ShardActivationRetry.RunAsync(() =>
            {
                calls++;
                throw new OperationCanceledException();
            }));

        Assert.That(calls, Is.EqualTo(1));
    }

    /// <summary>
    /// Cancellation observed during the inter-attempt backoff surfaces as
    /// <see cref="OperationCanceledException"/> (or its <see cref="TaskCanceledException"/>
    /// subclass, as raised by <see cref="Task.Delay(TimeSpan, CancellationToken)"/>)
    /// without further attempts.
    /// </summary>
    [Test]
    public void RunAsync_honours_cancellation_during_backoff()
    {
        using var cts = new CancellationTokenSource();
        var calls = 0;
        Assert.That(async () =>
            await ShardActivationRetry.RunAsync(
                () =>
                {
                    calls++;
                    cts.Cancel(); // first attempt throws; cancellation fires before the backoff completes
                    throw new ShardActivationTimeoutException("park-1");
                },
                cts.Token),
            Throws.InstanceOf<OperationCanceledException>());

        Assert.That(calls, Is.EqualTo(1),
            "Envelope must not start a second attempt once cancellation has been observed.");
    }

    /// <summary>
    /// Null operation argument is rejected eagerly with
    /// <see cref="ArgumentNullException"/> - public-API parameter validation
    /// convention.
    /// </summary>
    [Test]
    public void RunAsync_throws_for_null_operation()
    {
        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await ShardActivationRetry.RunAsync(null!));
    }
}
