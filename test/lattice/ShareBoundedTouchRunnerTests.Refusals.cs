namespace Orleans.Lattice.Tests;

public partial class ShareBoundedTouchRunnerTests
{
    [TestCase(1, false)]
    [TestCase(3, false)]
    [TestCase(3, true)]
    public async Task RunAsync_refused_siblings_do_not_trigger_retries_without_an_admitted_completion(
        int concurrency, bool leadAlone)
    {
        var starts = new List<int>();

        await ShareBoundedTouchRunner.RunAsync(
            8,
            concurrency,
            leadAlone,
            starts,
            static (s, i) =>
            {
                s.Add(i);
                return Task.FromResult(true);
            },
            static _ => true);

        Assert.That(starts, Is.EqualTo(Enumerable.Range(0, 8)),
            "a refusal never held a replay slot, so it cannot release one for a sibling's retry.");
    }

    [Test]
    public async Task RunAsync_an_async_refusal_does_not_wake_a_refused_sibling_for_retry()
    {
        var starts = new List<int>();
        await ShareBoundedTouchRunner.RunAsync(
            2, 1, true, starts,
            static async (s, i) =>
            {
                s.Add(i);
                await Task.Yield();
                return true;
            },
            static _ => true);
        Assert.That(starts, Is.EqualTo(new[] { 0, 1 }));
    }

    [Test]
    public async Task RunAsync_an_admitted_completion_retries_refused_holders_in_floor_order()
    {
        var script = new Script();
        var run = RunAsync(script, count: 3, concurrency: 1, leadAlone: true);

        Assert.That(await PendingAsync(script, 1), Is.EqualTo(new[] { 0 }));
        script.Settle(0, refused: true);
        Assert.That(await PendingAsync(script, 1), Is.EqualTo(new[] { 1 }));
        script.Settle(1, refused: true);
        Assert.That(await PendingAsync(script, 1), Is.EqualTo(new[] { 2 }));
        script.Settle(2, refused: false);
        Assert.That(await PendingAsync(script, 1), Is.EqualTo(new[] { 0 }));
        script.Settle(0, refused: false);
        Assert.That(await PendingAsync(script, 1), Is.EqualTo(new[] { 1 }));
        script.Settle(1, refused: false);

        await run;
        Assert.That(script.Starts, Is.EqualTo(new[] { 0, 1, 2, 0, 1 }),
            "actual slot-release evidence must still retry every refused holder, with the floor first.");
    }
}
