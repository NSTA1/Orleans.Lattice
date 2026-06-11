namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end coverage for <see cref="ILatticeQueue{T}"/> over a real
/// in-memory Orleans cluster, exercising the byte-based coordinator grain,
/// the typed facade round-trip, and the system-tree backing store.
/// </summary>
[TestFixture]
[Category("Integration")]
public class LatticeQueueIntegrationTests
{
    private ClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private ILatticeQueue<string> Queue() =>
        _fixture.Cluster.GrainFactory.GetLatticeQueue<string>($"q-{Guid.NewGuid():N}");

    [Test]
    public async Task Enqueue_then_dequeue_returns_values_in_fifo_order()
    {
        var queue = Queue();

        await queue.EnqueueAsync("a");
        await queue.EnqueueAsync("b");
        await queue.EnqueueAsync("c");

        var first = await queue.TryDequeueAsync();
        var second = await queue.TryDequeueAsync();
        var third = await queue.TryDequeueAsync();
        var empty = await queue.TryDequeueAsync();

        Assert.Multiple(() =>
        {
            Assert.That(first!.Value.Value, Is.EqualTo("a"));
            Assert.That(second!.Value.Value, Is.EqualTo("b"));
            Assert.That(third!.Value.Value, Is.EqualTo("c"));
            Assert.That(empty, Is.Null);
        });
    }

    [Test]
    public async Task Count_and_peek_reflect_queue_state_without_consuming()
    {
        var queue = Queue();
        await queue.EnqueueAsync("x");
        await queue.EnqueueAsync("y");

        var peeked = await queue.PeekAsync();
        var count = await queue.CountAsync();

        Assert.Multiple(() =>
        {
            Assert.That(peeked!.Value.Value, Is.EqualTo("x"));
            Assert.That(count, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task List_returns_every_entry_in_ascending_id_order()
    {
        var queue = Queue();
        await queue.EnqueueAsync("one");
        await queue.EnqueueAsync("two");

        var entries = await queue.ListAsync();

        Assert.That(entries.Select(e => e.Value), Is.EqualTo(new[] { "one", "two" }));
    }
}
