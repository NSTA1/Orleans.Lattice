using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #5812: a raw bulk load that outruns the response timeout is waited for on the
/// shard root, not re-issued to queue behind itself. A load the shard no longer runs is
/// issued again a bounded number of times; a shard that cannot report its status (a
/// lattice 10.2.0 silo) and a load that outlives the hang guard fail with the timeout.
/// </summary>
[TestFixture]
public sealed class ShardBulkLoadCompletionTests
{
    private static readonly TimeSpan[] NoDelay = [TimeSpan.Zero];
    private static readonly List<LwwEntry> Entries = [];

    private static IShardRootGrain ShardTimingOutOnce(out Func<int> loads)
    {
        var shard = Substitute.For<IShardRootGrain>();
        var calls = 0;
        shard.BulkLoadRawAsync("op", Entries).Returns(_ =>
            ++calls == 1 ? Task.FromException(new TimeoutException("response timeout")) : Task.CompletedTask);
        loads = () => calls;
        return shard;
    }

    [Test]
    public async Task A_load_that_completes_in_time_reads_no_status()
    {
        var shard = Substitute.For<IShardRootGrain>();
        shard.BulkLoadRawAsync("op", Entries).Returns(Task.CompletedTask);

        await ShardBulkLoadCompletion.BulkLoadRawAsync(shard, "op", Entries, logger: null, pollBackoff: NoDelay);

        await shard.DidNotReceive().GetBulkOperationStatusAsync(Arg.Any<string>());
    }

    [Test]
    public async Task A_timed_out_load_still_running_is_waited_for_and_not_reissued()
    {
        var shard = ShardTimingOutOnce(out var loads);
        shard.GetBulkOperationStatusAsync("op").Returns(
            BulkOperationStatus.Running, BulkOperationStatus.Running, BulkOperationStatus.Completed);

        await ShardBulkLoadCompletion.BulkLoadRawAsync(shard, "op", Entries, logger: null, pollBackoff: NoDelay);

        Assert.That(loads(), Is.EqualTo(1), "the running load is not issued again behind itself");
        await shard.Received(3).GetBulkOperationStatusAsync("op");
    }

    [Test]
    public async Task A_timed_out_load_the_shard_no_longer_runs_is_issued_again()
    {
        var shard = ShardTimingOutOnce(out var loads);
        shard.GetBulkOperationStatusAsync("op").Returns(BulkOperationStatus.NotRunning);

        await ShardBulkLoadCompletion.BulkLoadRawAsync(shard, "op", Entries, logger: null, pollBackoff: NoDelay);

        Assert.That(loads(), Is.EqualTo(2));
    }

    [Test]
    public void Reissues_are_bounded()
    {
        var shard = Substitute.For<IShardRootGrain>();
        shard.BulkLoadRawAsync("op", Entries).Returns(Task.FromException(new TimeoutException("response timeout")));
        shard.GetBulkOperationStatusAsync("op").Returns(BulkOperationStatus.NotRunning);

        var thrown = Assert.ThrowsAsync<TimeoutException>(() =>
            ShardBulkLoadCompletion.BulkLoadRawAsync(shard, "op", Entries, logger: null, pollBackoff: NoDelay));

        Assert.Multiple(() =>
        {
            Assert.That(thrown!.InnerException, Is.InstanceOf<TimeoutException>());
            Assert.That(shard.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(IShardRootGrain.BulkLoadRawAsync)),
                Is.EqualTo(ShardBulkLoadCompletion.MaxReissues + 1));
        });
    }

    [Test]
    public void A_shard_that_cannot_report_its_status_fails_with_the_original_timeout()
    {
        var shard = ShardTimingOutOnce(out var loads);
        shard.GetBulkOperationStatusAsync("op").ThrowsAsync(MethodUnknown());

        var thrown = Assert.ThrowsAsync<TimeoutException>(() =>
            ShardBulkLoadCompletion.BulkLoadRawAsync(shard, "op", Entries, logger: null, pollBackoff: NoDelay));

        Assert.Multiple(() =>
        {
            Assert.That(thrown!.InnerException!.Message, Is.EqualTo("response timeout"));
            Assert.That(loads(), Is.EqualTo(1), "a 10.2.0 shard root is not re-issued to, as before");
        });
    }

    [Test]
    public async Task A_transient_status_read_failure_is_read_again_and_the_load_is_not_reissued()
    {
        // Review S2 of #5815: only a 10.2.0 shard root's definite "method unknown" ends
        // the wait; a read that itself timed out says nothing about the load.
        var shard = ShardTimingOutOnce(out var loads);
        shard.GetBulkOperationStatusAsync("op").Returns(
            Task.FromException<BulkOperationStatus>(new TimeoutException("status read timed out")),
            Task.FromResult(BulkOperationStatus.Completed));

        await ShardBulkLoadCompletion.BulkLoadRawAsync(shard, "op", Entries, logger: null, pollBackoff: NoDelay);

        Assert.That(loads(), Is.EqualTo(1));
        await shard.Received(2).GetBulkOperationStatusAsync("op");
    }

    [Test]
    public void Cancelling_the_restore_ends_the_wait()
    {
        // Review N1 of #5815: the restore's token reaches the wait.
        var shard = ShardTimingOutOnce(out _);
        shard.GetBulkOperationStatusAsync("op").Returns(BulkOperationStatus.Running);
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        Assert.That(
            () => ShardBulkLoadCompletion.BulkLoadRawAsync(
                shard, "op", Entries, logger: null, cancelled.Token, pollBackoff: [TimeSpan.FromMinutes(1)]),
            Throws.InstanceOf<OperationCanceledException>());
    }

    /// <summary>The definite failure a lattice 10.2.0 silo answers an unknown method with.</summary>
    internal static TypeLoadException MethodUnknown() =>
        new(Orleans.Lattice.BPlusTree.Grains.WalAppendFenceMethodProbe.UnresolvedAliasPrefix + " 'ol.xbs'");

    [Test]
    public void A_load_still_running_past_the_hang_guard_fails()
    {
        var shard = ShardTimingOutOnce(out _);
        shard.GetBulkOperationStatusAsync("op").Returns(BulkOperationStatus.Running);

        var thrown = Assert.ThrowsAsync<TimeoutException>(() =>
            ShardBulkLoadCompletion.BulkLoadRawAsync(
                shard, "op", Entries, logger: null, pollBackoff: [TimeSpan.FromMilliseconds(20)],
                waitCeiling: TimeSpan.FromMilliseconds(100)));

        Assert.That(thrown!.Message, Does.Contain("still running"));
    }

    [Test]
    public void The_defaults_are_bounded()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ShardBulkLoadCompletion.DefaultWaitCeiling, Is.EqualTo(TimeSpan.FromMinutes(30)));
            Assert.That(ShardBulkLoadCompletion.DefaultPollBackoff, Is.All.LessThanOrEqualTo(TimeSpan.FromSeconds(5)));
            Assert.That(ShardBulkLoadCompletion.MaxReissues, Is.EqualTo(2));
        });
    }
}
