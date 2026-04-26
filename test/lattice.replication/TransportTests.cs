using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class NoOpReplicationTransportTests
{
    private static ReplicationBatch MakeBatch(
        string target = "peer",
        string tree = "tree",
        string origin = "self",
        byte[]? payload = null)
        => new()
        {
            TargetClusterId = target,
            TreeName = tree,
            OriginClusterId = origin,
            Payload = payload ?? Array.Empty<byte>(),
        };

    [Test]
    public async Task SendAsync_returns_default_unaccepted_ack()
    {
        var transport = new NoOpReplicationTransport();

        var ack = await transport.SendAsync(MakeBatch(payload: new byte[] { 1, 2, 3 }), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ack.Accepted, Is.False);
            Assert.That(ack.HighestAppliedHlc, Is.EqualTo(default(HybridLogicalClock)));
        });
    }

    [Test]
    public async Task SendAsync_completes_synchronously()
    {
        var transport = new NoOpReplicationTransport();

        var task = transport.SendAsync(MakeBatch(), CancellationToken.None);

        Assert.That(task.IsCompletedSuccessfully, Is.True);
        await task;
    }

    [Test]
    public void SendAsync_throws_when_target_cluster_id_is_null()
    {
        var transport = new NoOpReplicationTransport();
        var batch = MakeBatch(target: null!);

        Assert.That(
            async () => await transport.SendAsync(batch, CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public void SendAsync_throws_when_target_cluster_id_is_empty()
    {
        var transport = new NoOpReplicationTransport();
        var batch = MakeBatch(target: string.Empty);

        Assert.That(
            async () => await transport.SendAsync(batch, CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public void SendAsync_throws_when_tree_name_is_empty()
    {
        var transport = new NoOpReplicationTransport();
        var batch = MakeBatch(tree: string.Empty);

        Assert.That(
            async () => await transport.SendAsync(batch, CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public void SendAsync_throws_when_origin_cluster_id_is_empty()
    {
        var transport = new NoOpReplicationTransport();
        var batch = MakeBatch(origin: string.Empty);

        Assert.That(
            async () => await transport.SendAsync(batch, CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public async Task SendAsync_accepts_empty_payload()
    {
        var transport = new NoOpReplicationTransport();
        await transport.SendAsync(MakeBatch(), CancellationToken.None);
        Assert.Pass();
    }
}

[TestFixture]
public class LoopbackTransportTests
{
    private static ReplicationBatch MakeBatch(
        string target = "dest",
        string tree = "tree",
        string origin = "self",
        byte[]? payload = null)
        => new()
        {
            TargetClusterId = target,
            TreeName = tree,
            OriginClusterId = origin,
            Payload = payload ?? Array.Empty<byte>(),
        };

    [Test]
    public async Task SendAsync_records_batch()
    {
        var transport = new LoopbackTransport();

        await transport.SendAsync(MakeBatch(payload: new byte[] { 9, 8, 7 }), CancellationToken.None);

        Assert.That(transport.Sent, Has.Count.EqualTo(1));
        var recorded = transport.Sent.Single();
        Assert.Multiple(() =>
        {
            Assert.That(recorded.TargetClusterId, Is.EqualTo("dest"));
            Assert.That(recorded.TreeName, Is.EqualTo("tree"));
            Assert.That(recorded.OriginClusterId, Is.EqualTo("self"));
            Assert.That(recorded.Payload.ToArray(), Is.EqualTo(new byte[] { 9, 8, 7 }));
        });
    }

    [Test]
    public async Task SendAsync_returns_accepted_ack_by_default()
    {
        var transport = new LoopbackTransport();

        var ack = await transport.SendAsync(MakeBatch(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ack.Accepted, Is.True);
            Assert.That(ack.HighestAppliedHlc, Is.EqualTo(default(HybridLogicalClock)));
        });
    }

    [Test]
    public async Task SendAsync_uses_ack_factory_when_set()
    {
        var transport = new LoopbackTransport
        {
            AckFactory = b => new ReplicationAck
            {
                Accepted = false,
                HighestAppliedHlc = new HybridLogicalClock { WallClockTicks = 42, Counter = 0 },
            },
        };

        var ack = await transport.SendAsync(MakeBatch(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ack.Accepted, Is.False);
            Assert.That(ack.HighestAppliedHlc.WallClockTicks, Is.EqualTo(42));
        });
    }

    [Test]
    public async Task SendAsync_preserves_arrival_order()
    {
        var transport = new LoopbackTransport();

        await transport.SendAsync(MakeBatch(target: "a"), CancellationToken.None);
        await transport.SendAsync(MakeBatch(target: "b"), CancellationToken.None);
        await transport.SendAsync(MakeBatch(target: "c"), CancellationToken.None);

        Assert.That(
            transport.Sent.Select(e => e.TargetClusterId),
            Is.EqualTo(new[] { "a", "b", "c" }));
    }

    [Test]
    public void SendAsync_throws_when_target_cluster_id_is_empty()
    {
        var transport = new LoopbackTransport();
        var batch = MakeBatch(target: string.Empty);

        Assert.That(
            async () => await transport.SendAsync(batch, CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public void SendAsync_throws_when_tree_name_is_empty()
    {
        var transport = new LoopbackTransport();
        var batch = MakeBatch(tree: string.Empty);

        Assert.That(
            async () => await transport.SendAsync(batch, CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public void SendAsync_throws_when_origin_cluster_id_is_empty()
    {
        var transport = new LoopbackTransport();
        var batch = MakeBatch(origin: string.Empty);

        Assert.That(
            async () => await transport.SendAsync(batch, CancellationToken.None),
            Throws.ArgumentException);
    }
}
