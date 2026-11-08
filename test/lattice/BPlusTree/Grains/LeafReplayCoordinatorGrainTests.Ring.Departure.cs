using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LeafReplayCoordinatorGrainTests
{
    [Test]
    public async Task Callers_that_leave_a_pending_fill_never_cancel_it_and_a_later_caller_joins_it()
    {
        // Issue #6007, D2. A starvation drive gives up a slice read at its
        // deadline and leaves; the fill it was waiting on must carry on, cache its
        // page and serve the next caller, so the retry costs no storage read.
        var (grain, log) = RingGrain();
        for (var i = 0; i < 5; i++)
            log.Append(i, "k" + i, (byte)i);
        log.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var first = new CancellationTokenSource();
        using var second = new CancellationTokenSource();
        var leaving1 = grain.ReadSliceAsync(-1, 4, 100, first.Token);
        var leaving2 = grain.ReadSliceAsync(-1, 4, 100, second.Token);

        var waited = System.Diagnostics.Stopwatch.StartNew();
        while (Volatile.Read(ref log.Reads) == 0 && waited.Elapsed < TimeSpan.FromSeconds(10))
            await Task.Delay(10);
        Assert.That(Volatile.Read(ref log.Reads), Is.EqualTo(1), "instrument validation: the shared fill must be in flight");

        first.Cancel();
        second.Cancel();
        Assert.CatchAsync<OperationCanceledException>(async () => await leaving1);
        Assert.CatchAsync<OperationCanceledException>(async () => await leaving2);

        var joining = grain.ReadSliceAsync(-1, 4, 100);
        log.Gate.SetResult();
        var joined = await joining;
        var cached = await grain.ReadSliceAsync(-1, 4, 100);

        Assert.Multiple(() =>
        {
            Assert.That(joined.Select(e => e.Offset), Is.EqualTo(new long[] { 0, 1, 2, 3, 4 }));
            Assert.That(cached.Select(e => e.Offset), Is.EqualTo(new long[] { 0, 1, 2, 3, 4 }));
            Assert.That(log.Reads, Is.EqualTo(1),
                "two departing callers and one joining caller must share exactly one storage read");
        });
    }
}
