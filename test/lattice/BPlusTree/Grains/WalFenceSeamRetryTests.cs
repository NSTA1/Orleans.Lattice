using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The seam retry of the WAL append fence (epic #5605): a rejection converges on
/// the live activation, and when the operation's deadline passes the caller sees
/// exactly today's deadline outcome - never the internal exception, and never as
/// an inner exception.
/// </summary>
[TestFixture]
public sealed class WalFenceSeamRetryTests
{
    [Test]
    public async Task A_rejected_write_converges_once_the_live_activation_answers()
    {
        var calls = 0;

        var result = await WalFenceSeamRetry.RunAsync(
            () =>
            {
                calls++;
                return calls < 3
                    ? Task.FromException<int>(LeafRetiredException.ForSupersededActivation(Guid.NewGuid(), calls))
                    : Task.FromResult(42);
            },
            WalFenceSeamRetry.CallerShardRoot,
            "tree");

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.EqualTo(42));
            Assert.That(calls, Is.EqualTo(3));
        });
    }

    [Test]
    public void A_reclaimed_leaf_refusal_is_not_retried_at_the_seam()
    {
        // #5605 review, S1: supersession shares LeafRetiredException with reclaim, so
        // the seam must retry only the fence's refusal and leave a genuine retirement
        // to re-route exactly as before.
        var calls = 0;
        Assert.ThrowsAsync<LeafRetiredException>(() => WalFenceSeamRetry.RunAsync<int>(
            () =>
            {
                calls++;
                return Task.FromException<int>(new LeafRetiredException("leaf"));
            },
            WalFenceSeamRetry.CallerLeaf,
            "tree"));
        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public void Other_failures_are_not_retried()
    {
        var calls = 0;

        Assert.ThrowsAsync<InvalidOperationException>(() => WalFenceSeamRetry.RunAsync(
            () =>
            {
                calls++;
                return Task.FromException(new InvalidOperationException("not a fence rejection"));
            },
            WalFenceSeamRetry.CallerLeaf,
            "tree"));
        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public async Task An_expired_deadline_surfaces_a_plain_timeout_with_no_inner_exception()
    {
        WalFenceSeamRetry? retry = null;
        TimeoutException? timeout = null;
        for (var i = 0; i < 1000 && timeout is null; i++)
        {
            try
            {
                retry = await WalFenceSeamRetry.NextAsync(
                    retry, WalFenceSeamRetry.CallerShardRoot, "tree", CancellationToken.None, TimeSpan.FromMilliseconds(100));
            }
            catch (TimeoutException ex)
            {
                timeout = ex;
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(timeout, Is.Not.Null, "unbounded churn runs to the deadline, as today");
            Assert.That(timeout!.InnerException, Is.Null, "the internal rejection never becomes an inner exception");
            Assert.That(retry!.Attempts, Is.GreaterThan(0));
        });
    }

    [Test]
    public async Task A_sub_millisecond_remainder_never_spins()
    {
        // Task.Delay truncates a sub-millisecond span to zero, so a retry whose
        // remaining budget is under a millisecond must sleep past the deadline, not
        // re-issue the call in a tight loop until it passes.
        WalFenceSeamRetry? retry = null;
        TimeoutException? timeout = null;
        for (var i = 0; i < 1000 && timeout is null; i++)
        {
            try
            {
                retry = await WalFenceSeamRetry.NextAsync(
                    retry, WalFenceSeamRetry.CallerShardRoot, "tree", CancellationToken.None, TimeSpan.FromMilliseconds(0.5));
            }
            catch (TimeoutException ex)
            {
                timeout = ex;
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(timeout, Is.Not.Null);
            Assert.That(retry?.Attempts ?? 0, Is.LessThanOrEqualTo(2), "one sleep covers the whole sub-millisecond budget");
        });
    }

    [Test]
    public void A_cancelled_operation_surfaces_its_cancellation()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await WalFenceSeamRetry.NextAsync(null, WalFenceSeamRetry.CallerShardRoot, "tree", cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    [NonParallelizable]
    public void The_deadline_is_the_silo_configured_response_timeout()
    {
        var configured = TimeSpan.FromSeconds(47);
        try
        {
            var participant = new WalFenceSeamRetryBudget(Options.Create(new SiloMessagingOptions { ResponseTimeout = configured }));
            participant.Participate(Substitute.For<ISiloLifecycle>());

            Assert.That(WalFenceSeamRetry.Budget, Is.EqualTo(configured),
                "a fence rejection is retried for as long as the carrying call can answer, not a fixed constant");

            WalFenceSeamRetry.Budget = TimeSpan.Zero;
            Assert.That(WalFenceSeamRetry.Budget, Is.EqualTo(WalFenceSeamRetry.DefaultBudget),
                "a non-positive timeout falls back to the Orleans default");
        }
        finally
        {
            WalFenceSeamRetry.Budget = WalFenceSeamRetry.DefaultBudget;
        }
    }

    [Test]
    public void A_fence_rejection_never_counts_as_a_provider_failure()
    {
        Assert.Multiple(() =>
        {
            Assert.That(WalCommitLogWriter.IsExcludedFromProviderFailureCount(LeafRetiredException.ForSupersededActivation(Guid.NewGuid(), 1)), Is.True,
                "a superseded activation's rejection must not feed the saturation signal");
            Assert.That(WalCommitLogWriter.IsExcludedFromProviderFailureCount(new InvalidOperationException()), Is.False);
        });
    }
}
