using System.Collections.Concurrent;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

public partial class PublicApiContractTests
{
    // ── SubscribeToEventsAsync (extension) ──────────────────────────────

    [Test]
    public async Task SubscribeToEventsAsync_receives_Set_event_on_write()
    {
        var treeId = "pac-events-subscribe-set-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var received = new ConcurrentBag<LatticeTreeEvent>();
        var subscription = await tree.SubscribeToEventsAsync(
            Cluster.Client,
            evt => { received.Add(evt); return Task.CompletedTask; });

        try
        {
            await tree.SetAsync("k", Bytes("v"));

            // Wait briefly for the event to arrive on the memory stream.
            await PollUntilAsync(() => Task.FromResult(received.Any(e => e.Kind == LatticeTreeEventKind.Set && e.Key == "k")), TimeSpan.FromSeconds(5));

            Assert.That(received.Any(e => e.Kind == LatticeTreeEventKind.Set && e.TreeId == treeId && e.Key == "k"), Is.True);
        }
        finally
        {
            await subscription.UnsubscribeAsync();
        }
    }

    [Test]
    public async Task SubscribeToEventsAsync_receives_Delete_event_on_delete()
    {
        var treeId = "pac-events-subscribe-delete-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("k", Bytes("v"));

        var received = new ConcurrentBag<LatticeTreeEvent>();
        var subscription = await tree.SubscribeToEventsAsync(
            Cluster.Client,
            evt => { received.Add(evt); return Task.CompletedTask; });

        try
        {
            await tree.DeleteAsync("k");

            await PollUntilAsync(() => Task.FromResult(received.Any(e => e.Kind == LatticeTreeEventKind.Delete)), TimeSpan.FromSeconds(5));

            Assert.That(received.Any(e => e.Kind == LatticeTreeEventKind.Delete && e.Key == "k"), Is.True);
        }
        finally
        {
            await subscription.UnsubscribeAsync();
        }
    }

    [Test]
    public async Task SubscribeToEventsAsync_with_unregistered_provider_throws()
    {
        var treeId = "pac-events-subscribe-badprov-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        Assert.That(
            async () => await tree.SubscribeToEventsAsync(
                Cluster.Client,
                _ => Task.CompletedTask,
                providerName: "DoesNotExist"),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task SubscribeToEventsAsync_with_null_callback_throws()
    {
        var treeId = "pac-events-subscribe-nullcb-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        Assert.That(
            async () => await tree.SubscribeToEventsAsync(Cluster.Client, null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    // ── SetPublishEventsEnabledAsync ────────────────────────────────────

    [Test]
    public async Task SetPublishEventsEnabledAsync_to_false_suppresses_events()
    {
        var treeId = "pac-events-toggle-off-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        // Disable per-tree.
        await tree.SetPublishEventsEnabledAsync(enabled: false);

        var received = new ConcurrentBag<LatticeTreeEvent>();
        var subscription = await tree.SubscribeToEventsAsync(
            Cluster.Client,
            evt => { received.Add(evt); return Task.CompletedTask; });

        try
        {
            await tree.SetAsync("k", Bytes("v"));

            // Allow time for any (suppressed) event to flow.
            await Task.Delay(500);

            Assert.That(received.Where(e => e.Kind == LatticeTreeEventKind.Set && e.Key == "k").ToList(), Is.Empty);
        }
        finally
        {
            await subscription.UnsubscribeAsync();
        }
    }

    [Test]
    public async Task SetPublishEventsEnabledAsync_to_null_clears_override()
    {
        var treeId = "pac-events-toggle-clear-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        // Set an override, then clear it.
        await tree.SetPublishEventsEnabledAsync(enabled: false);
        await tree.SetPublishEventsEnabledAsync(enabled: null);

        // After clear, the silo-wide default (true on this fixture) applies.
        // Subsequent writes should publish events again.
        var received = new ConcurrentBag<LatticeTreeEvent>();
        var subscription = await tree.SubscribeToEventsAsync(
            Cluster.Client,
            evt => { received.Add(evt); return Task.CompletedTask; });

        try
        {
            await tree.SetAsync("k", Bytes("v"));

            await PollUntilAsync(() => Task.FromResult(received.Any(e => e.Kind == LatticeTreeEventKind.Set && e.Key == "k")), TimeSpan.FromSeconds(5));

            Assert.That(received.Any(e => e.Kind == LatticeTreeEventKind.Set && e.Key == "k"), Is.True);
        }
        finally
        {
            await subscription.UnsubscribeAsync();
        }
    }

    [Test]
    public async Task SetPublishEventsEnabledAsync_to_true_explicitly_enables()
    {
        var treeId = "pac-events-toggle-on-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        await tree.SetPublishEventsEnabledAsync(enabled: true);

        var received = new ConcurrentBag<LatticeTreeEvent>();
        var subscription = await tree.SubscribeToEventsAsync(
            Cluster.Client,
            evt => { received.Add(evt); return Task.CompletedTask; });

        try
        {
            await tree.SetAsync("k", Bytes("v"));
            await PollUntilAsync(() => Task.FromResult(received.Any(e => e.Kind == LatticeTreeEventKind.Set)), TimeSpan.FromSeconds(5));

            Assert.That(received.Any(e => e.Kind == LatticeTreeEventKind.Set), Is.True);
        }
        finally
        {
            await subscription.UnsubscribeAsync();
        }
    }
}
