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

    // -------------------------------------------------------------------------
    // Transient silo-membership churn - the envelope also absorbs the
    // SiloUnavailableException / OrleansMessageRejectionException shapes a
    // grain RPC observes while a target activation's host is restarting,
    // draining, or has just left the cluster, identically to the seed-timeout
    // case. Detection is by type name, so the test doubles need only carry the
    // matching name (they cannot reference the internal Orleans.Runtime type).
    // -------------------------------------------------------------------------

    /// <summary>
    /// A <c>SiloUnavailableException</c>-shaped fault on the first attempt is
    /// absorbed and the second attempt's success surfaces to the caller.
    /// </summary>
    [Test]
    public async Task RunAsync_retries_through_silo_unavailable_then_succeeds()
    {
        var calls = 0;
        await ShardActivationRetry.RunAsync(() =>
        {
            calls++;
            if (calls == 1) throw new FakeSiloUnavailableException("silo-gone-1");
            return Task.CompletedTask;
        });

        Assert.That(calls, Is.EqualTo(2), "Envelope did not retry the transient silo-churn fault.");
    }

    /// <summary>
    /// An <c>OrleansMessageRejectionException</c>-shaped fault (forward to a
    /// deactivating grain) on the first attempt is absorbed and the retry
    /// succeeds.
    /// </summary>
    [Test]
    public async Task RunAsync_retries_through_message_rejection_then_succeeds()
    {
        var calls = 0;
        await ShardActivationRetry.RunAsync(() =>
        {
            calls++;
            if (calls == 1) throw new FakeOrleansMessageRejectionException("forward-rejected-1");
            return Task.CompletedTask;
        });

        Assert.That(calls, Is.EqualTo(2));
    }

    /// <summary>
    /// A churn fault wrapped as the inner exception of an outer wrapper is
    /// still detected and retried - the envelope walks the inner chain.
    /// </summary>
    [Test]
    public async Task RunAsync_retries_through_wrapped_silo_churn_then_succeeds()
    {
        var calls = 0;
        await ShardActivationRetry.RunAsync(() =>
        {
            calls++;
            if (calls == 1)
            {
                throw new InvalidOperationException(
                    "wrapper", new FakeSiloUnavailableException("inner-silo-gone"));
            }
            return Task.CompletedTask;
        });

        Assert.That(calls, Is.EqualTo(2));
    }

    /// <summary>
    /// When every attempt throws a churn fault the budget exhausts and the
    /// most-recent exception is rethrown unwrapped, matching the seed-timeout
    /// exhaustion contract.
    /// </summary>
    [Test]
    public void RunAsync_rethrows_silo_churn_after_budget_exhausted()
    {
        var calls = 0;
        var ex = Assert.ThrowsAsync<FakeSiloUnavailableException>(async () =>
            await ShardActivationRetry.RunAsync(() =>
            {
                calls++;
                throw new FakeSiloUnavailableException($"silo-gone-{calls}");
            }));

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(ShardActivationRetry.MaxAttempts),
                "Envelope did not exhaust the full retry budget on the churn path.");
            Assert.That(ex!.Message, Is.EqualTo($"silo-gone-{ShardActivationRetry.MaxAttempts}"),
                "Envelope rethrew the wrong attempt's exception (must be the last).");
        });
    }

    /// <summary>
    /// The generic <see cref="ShardActivationRetry.RunAsync{T}"/> overload
    /// absorbs a churn fault and returns the retry's value, identically to the
    /// void overload.
    /// </summary>
    [Test]
    public async Task RunAsync_generic_retries_through_silo_churn_then_returns_value()
    {
        var calls = 0;
        var result = await ShardActivationRetry.RunAsync(() =>
        {
            calls++;
            if (calls == 1) throw new FakeOrleansMessageRejectionException("forward-rejected-1");
            return Task.FromResult(7);
        });

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(2));
            Assert.That(result, Is.EqualTo(7));
        });
    }

    /// <summary>
    /// <see cref="ShardActivationRetry.IsTransientSiloChurn"/> matches both
    /// churn shapes (including when nested as an inner exception) and rejects
    /// unrelated exceptions.
    /// </summary>
    [Test]
    public void IsTransientSiloChurn_matches_churn_shapes_only()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ShardActivationRetry.IsTransientSiloChurn(
                new FakeSiloUnavailableException("x")), Is.True);
            Assert.That(ShardActivationRetry.IsTransientSiloChurn(
                new FakeOrleansMessageRejectionException("x")), Is.True);
            Assert.That(ShardActivationRetry.IsTransientSiloChurn(
                new InvalidOperationException("outer", new FakeSiloUnavailableException("inner"))),
                Is.True, "Predicate must walk the inner-exception chain.");
            Assert.That(ShardActivationRetry.IsTransientSiloChurn(
                new InvalidOperationException("unrelated")), Is.False);
            Assert.That(ShardActivationRetry.IsTransientSiloChurn(
                new ShardActivationTimeoutException("seed")), Is.False,
                "Seed-timeout is handled by its own catch arm, not the churn predicate.");
        });
    }

    /// <summary>
    /// Fake <see cref="Exception"/> whose simple type name contains
    /// <c>SiloUnavailableException</c>, so the type-name detection in
    /// <see cref="ShardActivationRetry.IsTransientSiloChurn"/> classifies it as
    /// transient silo churn without taking a dependency on Orleans.Runtime.
    /// </summary>
    private sealed class FakeSiloUnavailableException : Exception
    {
        public FakeSiloUnavailableException(string message) : base(message) { }
    }

    /// <summary>
    /// Fake <see cref="Exception"/> whose simple type name contains
    /// <c>OrleansMessageRejectionException</c>, mirroring the forward-rejected
    /// shape the runtime raises against a deactivating grain.
    /// </summary>
    private sealed class FakeOrleansMessageRejectionException : Exception
    {
        public FakeOrleansMessageRejectionException(string message) : base(message) { }
    }
}
