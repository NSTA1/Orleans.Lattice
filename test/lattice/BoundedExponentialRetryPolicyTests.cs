namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the shipped
/// <see cref="BoundedExponentialRetryPolicy"/>.
/// </summary>
[TestFixture]
public class BoundedExponentialRetryPolicyTests
{
    [Test]
    public void Ctor_rejects_zero_max_attempts()
    {
        Assert.That(
            () => new BoundedExponentialRetryPolicy(maxAttempts: 0),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Ctor_rejects_negative_initial_delay()
    {
        Assert.That(
            () => new BoundedExponentialRetryPolicy(initialDelay: TimeSpan.FromMilliseconds(-1)),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Ctor_rejects_max_delay_less_than_initial_delay()
    {
        Assert.That(
            () => new BoundedExponentialRetryPolicy(
                initialDelay: TimeSpan.FromMilliseconds(100),
                maxDelay: TimeSpan.FromMilliseconds(10)),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Options_ctor_rejects_null()
    {
        Assert.That(
            () => new BoundedExponentialRetryPolicy(options: null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void ExecuteAsync_untyped_rejects_null_operation()
    {
        var policy = NewFastPolicy();
        Assert.That(
            async () => await policy.ExecuteAsync((Func<CancellationToken, Task>)null!, default),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void ExecuteAsync_typed_rejects_null_operation()
    {
        var policy = NewFastPolicy();
        Assert.That(
            async () => await policy.ExecuteAsync<int>(null!, default),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task ExecuteAsync_succeeds_on_first_attempt_when_operation_does_not_throw()
    {
        var calls = 0;
        var policy = NewFastPolicy(maxAttempts: 3);
        await policy.ExecuteAsync(_ =>
        {
            calls++;
            return Task.CompletedTask;
        }, default);
        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public async Task ExecuteAsync_retries_until_success()
    {
        var calls = 0;
        var policy = NewFastPolicy(maxAttempts: 4);
        await policy.ExecuteAsync(_ =>
        {
            calls++;
            if (calls < 3) throw new InvalidOperationException("transient");
            return Task.CompletedTask;
        }, default);
        Assert.That(calls, Is.EqualTo(3));
    }

    [Test]
    public void ExecuteAsync_rethrows_original_exception_when_budget_exhausted()
    {
        var calls = 0;
        var policy = NewFastPolicy(maxAttempts: 2);
        var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await policy.ExecuteAsync(_ =>
            {
                calls++;
                throw new InvalidOperationException("boom");
            }, default));
        Assert.That(calls, Is.EqualTo(2));
        Assert.That(ex!.Message, Is.EqualTo("boom"));
    }

    [Test]
    public void ExecuteAsync_typed_returns_value_on_success()
    {
        var policy = NewFastPolicy(maxAttempts: 3);
        var result = policy.ExecuteAsync(_ => Task.FromResult(42), default).GetAwaiter().GetResult();
        Assert.That(result, Is.EqualTo(42));
    }

    [Test]
    public async Task ExecuteAsync_typed_retries_until_success_and_returns_value()
    {
        var calls = 0;
        var policy = NewFastPolicy(maxAttempts: 4);
        var result = await policy.ExecuteAsync(_ =>
        {
            calls++;
            if (calls < 2) throw new InvalidOperationException("transient");
            return Task.FromResult("ok");
        }, default);
        Assert.That(result, Is.EqualTo("ok"));
        Assert.That(calls, Is.EqualTo(2));
    }

    [Test]
    public void ExecuteAsync_does_not_retry_when_classifier_rejects_exception()
    {
        var calls = 0;
        var policy = new BoundedExponentialRetryPolicy(
            maxAttempts: 5,
            initialDelay: TimeSpan.Zero,
            maxDelay: TimeSpan.Zero,
            retryableExceptionClassifier: ex => ex is TimeoutException);
        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await policy.ExecuteAsync(_ =>
            {
                calls++;
                throw new InvalidOperationException("not transient");
            }, default));
        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public async Task ExecuteAsync_retries_only_when_classifier_accepts_exception()
    {
        var calls = 0;
        var policy = new BoundedExponentialRetryPolicy(
            maxAttempts: 5,
            initialDelay: TimeSpan.Zero,
            maxDelay: TimeSpan.Zero,
            retryableExceptionClassifier: ex => ex is TimeoutException);
        await policy.ExecuteAsync(_ =>
        {
            calls++;
            if (calls < 3) throw new TimeoutException();
            return Task.CompletedTask;
        }, default);
        Assert.That(calls, Is.EqualTo(3));
    }

    [Test]
    public void ExecuteAsync_surfaces_cancellation_immediately_when_already_cancelled()
    {
        var policy = NewFastPolicy(maxAttempts: 3);
        var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await policy.ExecuteAsync(_ => Task.CompletedTask, cts.Token));
    }

    private static BoundedExponentialRetryPolicy NewFastPolicy(int maxAttempts = 3) =>
        new(
            maxAttempts: maxAttempts,
            initialDelay: TimeSpan.Zero,
            maxDelay: TimeSpan.Zero);
}
