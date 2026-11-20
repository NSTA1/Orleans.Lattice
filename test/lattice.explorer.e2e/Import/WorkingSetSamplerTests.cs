namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Import;

/// <summary>
/// The settle that ends an import's memory proof (#6115): a process still draining work the
/// import queued is read once it has drained, and a process that keeps state is still read
/// as keeping it.
/// </summary>
[TestFixture]
[Category("EndToEnd")]
public sealed class WorkingSetSamplerTests
{
    private const long Mb = 1024L * 1024;

    [Test]
    public async Task A_draining_heap_is_read_once_it_stops_falling()
    {
        var readings = new Queue<long[]>([[250 * Mb, 180 * Mb], [200 * Mb, 180 * Mb], [160 * Mb, 179 * Mb], [158 * Mb, 179 * Mb], [158 * Mb, 179 * Mb], [157 * Mb, 179 * Mb], [100 * Mb, 100 * Mb]]);

        var settled = await WorkingSetSampler.SettleUntilDrainedAsync(() => Task.FromResult(readings.Dequeue()), () => Task.CompletedTask, rounds: 12);

        Assert.Multiple(() =>
        {
            Assert.That(settled, Is.EqualTo(new[] { 157 * Mb, 179 * Mb }));
            Assert.That(readings, Has.Count.EqualTo(1), "the drain stops after three settles in a row that did not fall");
        });
    }

    [Test]
    public async Task A_heap_that_keeps_state_is_read_after_three_more_settles()
    {
        var settles = 0;

        var settled = await WorkingSetSampler.SettleUntilDrainedAsync(
            () => Task.FromResult<long[]>([250 * Mb - (++settles * Mb)]), () => Task.CompletedTask, rounds: 12);

        Assert.Multiple(() =>
        {
            Assert.That(settled, Is.EqualTo(new[] { 246 * Mb }));
            Assert.That(settles, Is.EqualTo(4));
        });
    }

    [Test]
    public async Task A_heap_that_holds_level_between_batches_is_read_once_it_has_drained()
    {
        var readings = new Queue<long[]>([[250 * Mb], [249 * Mb], [248 * Mb], [200 * Mb], [199 * Mb], [199 * Mb], [199 * Mb], [100 * Mb]]);

        var settled = await WorkingSetSampler.SettleUntilDrainedAsync(() => Task.FromResult(readings.Dequeue()), () => Task.CompletedTask, rounds: 12);

        Assert.Multiple(() =>
        {
            Assert.That(settled, Is.EqualTo(new[] { 199 * Mb }));
            Assert.That(readings, Has.Count.EqualTo(1), "a fall after a level settle restarts the quiet window");
        });
    }

    [Test]
    public async Task The_drain_is_bounded()
    {
        var settles = 0;

        var settled = await WorkingSetSampler.SettleUntilDrainedAsync(
            () => Task.FromResult<long[]>([500 * Mb - (++settles * 10 * Mb)]), () => Task.CompletedTask, rounds: 3);

        Assert.Multiple(() =>
        {
            Assert.That(settles, Is.EqualTo(4));
            Assert.That(settled, Is.EqualTo(new[] { 460 * Mb }));
        });
    }
}
