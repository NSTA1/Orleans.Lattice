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
                    ? Task.FromException<int>(new WalAppendEpochStaleException(Guid.NewGuid(), calls))
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
    public void A_cancelled_operation_surfaces_its_cancellation()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await WalFenceSeamRetry.NextAsync(null, WalFenceSeamRetry.CallerShardRoot, "tree", cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
