using System.Collections.Concurrent;
using Orleans.Lattice;
using Orleans.Streams;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration tests for <see cref="ILattice.SetPublishEventsEnabledAsync"/>.
/// The underlying cluster has silo-wide <c>PublishEvents = false</c>, so events
/// only arrive on trees that explicitly opt in via the per-tree override.
/// </summary>
[TestFixture]
public sealed class PublishEventsOverrideIntegrationTests
{
    private PublishEventsOverrideClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new PublishEventsOverrideClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private async Task<(ConcurrentQueue<LatticeTreeEvent> sink, StreamSubscriptionHandle<LatticeTreeEvent> handle)>
        SubscribeAsync(ILattice tree)
    {
        var sink = new ConcurrentQueue<LatticeTreeEvent>();
        var handle = await tree.SubscribeToEventsAsync(_fixture.Cluster.Client, evt =>
        {
            sink.Enqueue(evt);
            return Task.CompletedTask;
        });
        await Task.Delay(200);
        return (sink, handle);
    }

    [Test]
    public async Task Override_enabled_on_tree_with_silo_default_disabled_emits_events()
    {
        var tree = await _fixture.CreateTreeAsync($"override-on-{Guid.NewGuid():N}");
        await tree.SetPublishEventsEnabledAsync(true);

        var (sink, handle) = await SubscribeAsync(tree);
        try
        {
            await tree.SetAsync("k1", new byte[] { 1 });

            // Wait briefly for the event to propagate through memory streams.
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
            while (DateTime.UtcNow < deadline && !sink.Any(e => e.Kind == LatticeTreeEventKind.Set))
                await Task.Delay(50);

            Assert.That(sink.Any(e => e.Kind == LatticeTreeEventKind.Set && e.Key == "k1"), Is.True,
                "Expected Set event when per-tree override forces publication on.");
        }
        finally { await handle.UnsubscribeAsync(); }
    }

    [Test]
    public async Task Override_null_with_silo_default_disabled_emits_no_events()
    {
        var tree = await _fixture.CreateTreeAsync($"override-null-{Guid.NewGuid():N}");
        // No SetPublishEventsEnabledAsync call — registry entry has PublishEvents = null.

        var (sink, handle) = await SubscribeAsync(tree);
        try
        {
            await tree.SetAsync("k1", new byte[] { 1 });
            await tree.DeleteAsync("k1");
            await Task.Delay(500);

            Assert.That(sink, Is.Empty,
                "Inherited silo default is false, so no events should be published.");
        }
        finally { await handle.UnsubscribeAsync(); }
    }

    [Test]
    public async Task Override_disabled_on_tree_suppresses_events_even_after_previous_opt_in()
    {
        var tree = await _fixture.CreateTreeAsync($"override-flip-{Guid.NewGuid():N}");
        await tree.SetPublishEventsEnabledAsync(true);

        var (sink, handle) = await SubscribeAsync(tree);
        try
        {
            await tree.SetAsync("before", new byte[] { 1 });

            // Wait for the first event to confirm publication is initially on.
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
            while (DateTime.UtcNow < deadline && !sink.Any(e => e.Key == "before"))
                await Task.Delay(50);
            Assert.That(sink.Any(e => e.Key == "before"), Is.True, "Override=true should have produced the 'before' event.");

            // Flip override off. Same activation handling the call must invalidate its gate cache immediately.
            await tree.SetPublishEventsEnabledAsync(false);

            var eventsBefore = sink.Count;
            await tree.SetAsync("after", new byte[] { 2 });
            await Task.Delay(500);

            // No new events for "after" on the handling activation.
            Assert.That(sink.Any(e => e.Key == "after"), Is.False,
                "Override=false should suppress subsequent events on the handling activation.");
            Assert.That(sink.Count, Is.EqualTo(eventsBefore),
                "Sink count should not grow after override is flipped off.");
        }
        finally { await handle.UnsubscribeAsync(); }
    }

    [Test]
    public async Task Override_null_after_true_clears_override_and_inherits_silo_default()
    {
        var tree = await _fixture.CreateTreeAsync($"override-clear-{Guid.NewGuid():N}");
        await tree.SetPublishEventsEnabledAsync(true);

        var (sink, handle) = await SubscribeAsync(tree);
        try
        {
            await tree.SetAsync("seed", new byte[] { 1 });
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
            while (DateTime.UtcNow < deadline && !sink.Any(e => e.Key == "seed"))
                await Task.Delay(50);
            Assert.That(sink.Any(e => e.Key == "seed"), Is.True);

            // Clear override — falls back to silo default (false).
            await tree.SetPublishEventsEnabledAsync(null);

            var eventsBefore = sink.Count;
            await tree.SetAsync("post-clear", new byte[] { 2 });
            await Task.Delay(500);

            Assert.That(sink.Any(e => e.Key == "post-clear"), Is.False,
                "After clearing the override the tree should inherit the silo default (false) and suppress events.");
            Assert.That(sink.Count, Is.EqualTo(eventsBefore));
        }
        finally { await handle.UnsubscribeAsync(); }
    }
}
