using System.Data.Common;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Tests for <see cref="RepoContextResetFaults"/>: the reset sweep retries a
/// retired-leaf refusal and a transient store lock instead of aborting the whole
/// reset (#5182), and still surfaces every other fault unchanged.
/// </summary>
[TestFixture]
public sealed class RepoContextResetFaultsTests
{
    private sealed class TransientDbException(bool transient) : DbException("SQLite Error 5: busy")
    {
        public override bool IsTransient => transient;
    }

    [Test]
    public void IsTransient_classifies_retired_leaf_and_store_lock_faults()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextResetFaults.IsTransient(new LeafRetiredException("leaf-1")), Is.True);
            Assert.That(RepoContextResetFaults.IsTransient(new TransientDbException(transient: true)), Is.True);
            Assert.That(
                RepoContextResetFaults.IsTransient(new InvalidOperationException("SQLite Error 5: 'database is locked'.")),
                Is.True);
            Assert.That(
                RepoContextResetFaults.IsTransient(new InvalidOperationException("wrap", new LeafRetiredException("leaf-1"))),
                Is.True);
            Assert.That(
                RepoContextResetFaults.IsTransient(new AggregateException(new ArgumentException("x"), new LeafRetiredException("leaf-1"))),
                Is.True);
        });
    }

    [Test]
    public void IsTransient_rejects_deterministic_faults()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextResetFaults.IsTransient(new InvalidOperationException("boom")), Is.False);
            Assert.That(RepoContextResetFaults.IsTransient(new TransientDbException(transient: false)), Is.False);
            Assert.That(RepoContextResetFaults.IsTransient(new AggregateException(new ArgumentException("x"))), Is.False);
        });
    }

    [Test]
    public void BackoffFor_doubles_and_caps()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextResetFaults.BackoffFor(1), Is.EqualTo(RepoContextResetFaults.InitialBackoff));
            Assert.That(RepoContextResetFaults.BackoffFor(2), Is.EqualTo(RepoContextResetFaults.InitialBackoff * 2));
            Assert.That(RepoContextResetFaults.BackoffFor(60), Is.EqualTo(RepoContextResetFaults.MaxBackoff));
        });
    }

    [Test]
    public async Task RetryTransientAsync_retries_retired_leaf_then_returns_the_successful_count()
    {
        var calls = 0;
        var delays = new List<TimeSpan>();

        var result = await RepoContextResetFaults.RetryTransientAsync(
            _ => ++calls < 3 ? throw new LeafRetiredException("leaf-1") : Task.FromResult(42L),
            (delay, _) => { delays.Add(delay); return Task.CompletedTask; },
            maxAttempts: 5,
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo(42L));
            Assert.That(calls, Is.EqualTo(3));
            Assert.That(delays, Is.EqualTo(new[] { RepoContextResetFaults.BackoffFor(1), RepoContextResetFaults.BackoffFor(2) }));
        });
    }

    [Test]
    public void RetryTransientAsync_surfaces_a_transient_fault_once_the_budget_is_spent()
    {
        var calls = 0;

        Assert.ThrowsAsync<LeafRetiredException>(() => RepoContextResetFaults.RetryTransientAsync<long>(
            _ => { calls++; throw new LeafRetiredException("leaf-1"); },
            (_, _) => Task.CompletedTask,
            maxAttempts: 3,
            CancellationToken.None));
        Assert.That(calls, Is.EqualTo(3));
    }

    [Test]
    public void RetryTransientAsync_does_not_retry_a_deterministic_fault()
    {
        var calls = 0;

        Assert.ThrowsAsync<InvalidOperationException>(() => RepoContextResetFaults.RetryTransientAsync<long>(
            _ => { calls++; throw new InvalidOperationException("boom"); },
            (_, _) => Task.CompletedTask,
            maxAttempts: 5,
            CancellationToken.None));
        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public void RetryTransientAsync_rejects_null_arguments_and_a_zero_budget()
    {
        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ArgumentNullException>(() => RepoContextResetFaults.RetryTransientAsync<long>(
                null!, (_, _) => Task.CompletedTask, 1, CancellationToken.None));
            Assert.ThrowsAsync<ArgumentNullException>(() => RepoContextResetFaults.RetryTransientAsync(
                _ => Task.FromResult(0L), null!, 1, CancellationToken.None));
            Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => RepoContextResetFaults.RetryTransientAsync(
                _ => Task.FromResult(0L), (_, _) => Task.CompletedTask, 0, CancellationToken.None));
        });
    }
}
