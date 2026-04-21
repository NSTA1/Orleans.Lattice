using System.Collections.Concurrent;
using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Streams;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end event stream tests using a cluster fixture with Orleans
/// memory streams registered and <c>PublishEvents = true</c>.
/// </summary>
[TestFixture]
public sealed class EventStreamIntegrationTests
{
    private EventStreamClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new EventStreamClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    /// <summary>
    /// Waits up to <paramref name="timeout"/> for at least one event matching
    /// <paramref name="predicate"/> to arrive, returning the first match or
    /// throwing via <c>Assert.Fail</c> on timeout.
    /// </summary>
    private static async Task<LatticeTreeEvent> WaitForEventAsync(
        ConcurrentQueue<LatticeTreeEvent> sink,
        Func<LatticeTreeEvent, bool> predicate,
        TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(10));
        while (DateTime.UtcNow < deadline)
        {
            foreach (var evt in sink)
            {
                if (predicate(evt)) return evt;
            }
            await Task.Delay(50);
        }
        Assert.Fail("Timed out waiting for matching event. Observed: " + string.Join(", ", sink.Select(e => e.Kind)));
        throw new InvalidOperationException("unreachable");
    }

    private async Task<(ILattice tree, ConcurrentQueue<LatticeTreeEvent> sink, StreamSubscriptionHandle<LatticeTreeEvent> handle)>
        SubscribeAsync(string treeId)
    {
        var tree = await _fixture.CreateTreeAsync(treeId);
        var sink = new ConcurrentQueue<LatticeTreeEvent>();
        var handle = await tree.SubscribeToEventsAsync(_fixture.Cluster.Client, evt =>
        {
            sink.Enqueue(evt);
            return Task.CompletedTask;
        });
        // Give the subscription time to propagate through the memory stream provider.
        await Task.Delay(200);
        return (tree, sink, handle);
    }

    [Test]
    public async Task SetAsync_publishes_Set_event()
    {
        var (tree, sink, handle) = await SubscribeAsync($"evt-set-{Guid.NewGuid():N}");
        try
        {
            await tree.SetAsync("k1", new byte[] { 1 });
            var evt = await WaitForEventAsync(sink, e => e.Kind == LatticeTreeEventKind.Set && e.Key == "k1");
            Assert.That(evt.TreeId, Is.EqualTo(tree.GetPrimaryKeyString()));
            Assert.That(evt.OperationId, Is.Null);
        }
        finally { await handle.UnsubscribeAsync(); }
    }

    [Test]
    public async Task DeleteAsync_publishes_Delete_event_only_when_existed()
    {
        var (tree, sink, handle) = await SubscribeAsync($"evt-del-{Guid.NewGuid():N}");
        try
        {
            await tree.SetAsync("kdel", new byte[] { 9 });
            await WaitForEventAsync(sink, e => e.Kind == LatticeTreeEventKind.Set);

            await tree.DeleteAsync("kdel");
            await WaitForEventAsync(sink, e => e.Kind == LatticeTreeEventKind.Delete && e.Key == "kdel");

            // Deleting a non-existent key should not produce another Delete event.
            var before = sink.Count(e => e.Kind == LatticeTreeEventKind.Delete);
            await tree.DeleteAsync("never-existed");
            await Task.Delay(300);
            var after = sink.Count(e => e.Kind == LatticeTreeEventKind.Delete);
            Assert.That(after, Is.EqualTo(before));
        }
        finally { await handle.UnsubscribeAsync(); }
    }

    [Test]
    public async Task DeleteRangeAsync_publishes_single_DeleteRange_event_with_range_key()
    {
        var (tree, sink, handle) = await SubscribeAsync($"evt-range-{Guid.NewGuid():N}");
        try
        {
            await tree.SetAsync("a", new byte[] { 1 });
            await tree.SetAsync("b", new byte[] { 2 });
            await tree.SetAsync("c", new byte[] { 3 });

            var deleted = await tree.DeleteRangeAsync("a", "c");
            Assert.That(deleted, Is.EqualTo(2));

            var evt = await WaitForEventAsync(sink, e => e.Kind == LatticeTreeEventKind.DeleteRange);
            Assert.That(evt.Key, Is.EqualTo("a..c"));
        }
        finally { await handle.UnsubscribeAsync(); }
    }

    [Test]
    public async Task SetManyAtomicAsync_publishes_per_key_events_with_OperationId()
    {
        var (tree, sink, handle) = await SubscribeAsync($"evt-atomic-{Guid.NewGuid():N}");
        try
        {
            var opId = Guid.NewGuid().ToString("N");
            var entries = new List<KeyValuePair<string, byte[]>>
            {
                new("x", new byte[] { 1 }),
                new("y", new byte[] { 2 }),
            };
            await tree.SetManyAtomicAsync(entries, opId);

            // Both per-key Set events should carry the operationId.
            var evX = await WaitForEventAsync(sink, e => e.Kind == LatticeTreeEventKind.Set && e.Key == "x");
            var evY = await WaitForEventAsync(sink, e => e.Kind == LatticeTreeEventKind.Set && e.Key == "y");
            Assert.That(evX.OperationId, Is.EqualTo(opId));
            Assert.That(evY.OperationId, Is.EqualTo(opId));

            // Terminal AtomicWriteCompleted stamped with the same id.
            var complete = await WaitForEventAsync(sink, e => e.Kind == LatticeTreeEventKind.AtomicWriteCompleted);
            Assert.That(complete.OperationId, Is.EqualTo(opId));
        }
        finally { await handle.UnsubscribeAsync(); }
    }

    [Test]
    public async Task SetIfVersionAsync_publishes_Set_only_when_applied()
    {
        var (tree, sink, handle) = await SubscribeAsync($"evt-cas-{Guid.NewGuid():N}");
        try
        {
            await tree.SetAsync("cas", new byte[] { 1 });
            var versioned = await tree.GetWithVersionAsync("cas");
            await WaitForEventAsync(sink, e => e.Kind == LatticeTreeEventKind.Set && e.Key == "cas");

            // Stale version should not produce a Set event.
            var stale = new Orleans.Lattice.Primitives.HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
            var appliedStale = await tree.SetIfVersionAsync("cas", new byte[] { 9 }, stale);
            Assert.That(appliedStale, Is.False);

            var setCountBefore = sink.Count(e => e.Kind == LatticeTreeEventKind.Set && e.Key == "cas");

            // Fresh version should.
            var applied = await tree.SetIfVersionAsync("cas", new byte[] { 2 }, versioned.Version);
            Assert.That(applied, Is.True);
            await Task.Delay(300);
            var setCountAfter = sink.Count(e => e.Kind == LatticeTreeEventKind.Set && e.Key == "cas");
            Assert.That(setCountAfter, Is.EqualTo(setCountBefore + 1));
        }
        finally { await handle.UnsubscribeAsync(); }
    }
}
