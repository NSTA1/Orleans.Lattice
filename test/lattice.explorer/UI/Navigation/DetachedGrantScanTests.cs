using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Navigation;

/// <summary>
/// #6114: a denied caller's grant scan outlives the directory's time-box, so a
/// recheck joins it instead of restarting it.
/// </summary>
[TestFixture]
public sealed class DetachedGrantScanTests
{
    [Test]
    public async Task A_waiter_that_stops_waiting_leaves_the_scan_for_the_next_waiter_to_join()
    {
        using var grantScan = new DetachedGrantScan();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var starts = 0;
        Func<CancellationToken, Task<bool?>> scan = async token =>
        {
            Interlocked.Increment(ref starts);
            await release.Task.WaitAsync(token);
            return true;
        };

        using var stopWaiting = new CancellationTokenSource();
        var first = grantScan.JoinAsync("priya", scan, stopWaiting.Token);
        stopWaiting.Cancel();
        Assert.That(async () => await first, Throws.InstanceOf<OperationCanceledException>());
        var second = grantScan.JoinAsync("priya", scan, CancellationToken.None);
        release.SetResult();

        Assert.Multiple(async () =>
        {
            Assert.That(await second.WaitAsync(TimeSpan.FromSeconds(10)), Is.True);
            Assert.That(starts, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_completed_scan_is_asked_again()
    {
        using var grantScan = new DetachedGrantScan();
        var starts = 0;
        Func<CancellationToken, Task<bool?>> scan = _ =>
        {
            Interlocked.Increment(ref starts);
            return Task.FromResult<bool?>(null);
        };

        await grantScan.JoinAsync("priya", scan, CancellationToken.None);
        await grantScan.JoinAsync("priya", scan, CancellationToken.None);

        Assert.That(starts, Is.EqualTo(2), "an unanswered scan must be able to answer next time");
    }

    [Test]
    public async Task Reset_and_a_new_caller_cancel_the_running_scan_as_unanswered()
    {
        using var grantScan = new DetachedGrantScan();
        Func<CancellationToken, Task<bool?>> hang = async token =>
        {
            await Task.Delay(Timeout.Infinite, token);
            return true;
        };

        var reset = grantScan.JoinAsync("priya", hang, CancellationToken.None);
        grantScan.Reset();
        var replaced = grantScan.JoinAsync("priya", hang, CancellationToken.None);
        _ = grantScan.JoinAsync("omar", _ => Task.FromResult<bool?>(false), CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await reset.WaitAsync(TimeSpan.FromSeconds(10)), Is.Null);
            Assert.That(await replaced.WaitAsync(TimeSpan.FromSeconds(10)), Is.Null);
        });
    }
}
