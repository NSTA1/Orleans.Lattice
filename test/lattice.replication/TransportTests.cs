using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class NoOpReplicationTransportTests
{
    [Test]
    public async Task SendAsync_completes_synchronously()
    {
        var transport = new NoOpReplicationTransport();

        var task = transport.SendAsync("peer", new byte[] { 1, 2, 3 }, CancellationToken.None);

        Assert.That(task.IsCompletedSuccessfully, Is.True);
        await task;
    }

    [Test]
    public void SendAsync_throws_when_target_cluster_id_is_null()
    {
        var transport = new NoOpReplicationTransport();

        Assert.That(
            async () => await transport.SendAsync(null!, ReadOnlyMemory<byte>.Empty, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task SendAsync_accepts_empty_payload()
    {
        var transport = new NoOpReplicationTransport();
        await transport.SendAsync("peer", ReadOnlyMemory<byte>.Empty, CancellationToken.None);
        Assert.Pass();
    }
}

[TestFixture]
public class LoopbackTransportTests
{
    [Test]
    public async Task SendAsync_records_payload_with_target_cluster_id()
    {
        var transport = new LoopbackTransport();

        await transport.SendAsync("dest", new byte[] { 9, 8, 7 }, CancellationToken.None);

        Assert.That(transport.Sent, Has.Count.EqualTo(1));
        var envelope = transport.Sent.Single();
        Assert.Multiple(() =>
        {
            Assert.That(envelope.TargetClusterId, Is.EqualTo("dest"));
            Assert.That(envelope.Payload, Is.EqualTo(new byte[] { 9, 8, 7 }));
        });
    }

    [Test]
    public async Task SendAsync_preserves_arrival_order()
    {
        var transport = new LoopbackTransport();

        await transport.SendAsync("a", new byte[] { 1 }, CancellationToken.None);
        await transport.SendAsync("b", new byte[] { 2 }, CancellationToken.None);
        await transport.SendAsync("c", new byte[] { 3 }, CancellationToken.None);

        Assert.That(
            transport.Sent.Select(e => e.TargetClusterId),
            Is.EqualTo(new[] { "a", "b", "c" }));
    }

    [Test]
    public void SendAsync_throws_when_target_cluster_id_is_null()
    {
        var transport = new LoopbackTransport();

        Assert.That(
            async () => await transport.SendAsync(null!, ReadOnlyMemory<byte>.Empty, CancellationToken.None),
            Throws.ArgumentNullException);
    }
}
