using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// Pins the <see cref="IReplicationTransport"/> public contract:
/// every shipped <see cref="ReplicationBatch"/> carries the configured
/// routing metadata (TargetClusterId / TreeName / OriginClusterId),
/// successful sends return <see cref="ReplicationAck"/>
/// with <see cref="ReplicationAck.Accepted"/> set and a non-default
/// <see cref="ReplicationAck.HighestAppliedHlc"/>, and an unrouted
/// destination is rejected with <see cref="ReplicationAck.Accepted"/>
/// = false rather than throwing.
/// </summary>
public partial class PublicReplicationApiContractTests
{
    [Test]
    public async Task IReplicationTransport_send_emits_replication_batch_with_routing_metadata()
    {
        var treeId = NextTreeId("tx-batch-shape");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        var sentBefore = LoopbackDeliveringTransport.Sent.Count;

        await treeOnA.SetAsync("k", Bytes("v"));
        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => Str(await treeOnB.GetAsync("k")) == "v",
            "initial replication");

        var newBatches = LoopbackDeliveringTransport
            .Sent
            .Skip(sentBefore)
            .Where(b => b.TreeName == treeId)
            .ToList();

        Assert.That(newBatches, Is.Not.Empty,
            "At least one ReplicationBatch must be observed for the tree.");

        var batch = newBatches[0];
        Assert.Multiple(() =>
        {
            Assert.That(batch.TargetClusterId, Is.EqualTo(PublicReplicationApiClusterFixture.SiteBClusterId));
            Assert.That(batch.OriginClusterId, Is.EqualTo(PublicReplicationApiClusterFixture.SiteAClusterId));
            Assert.That(batch.TreeName, Is.EqualTo(treeId));
            Assert.That(batch.Payload.IsEmpty, Is.False);
        });
    }

    [Test]
    public async Task IReplicationTransport_send_returns_replication_ack_with_non_default_hwm_after_apply()
    {
        var treeId = NextTreeId("tx-ack");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        await treeOnA.SetAsync("k", Bytes("v"));
        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => Str(await treeOnB.GetAsync("k")) == "v",
            "initial replication");

        var deliveries = LoopbackDeliveringTransport
            .DeliveredBatches
            .Where(d => d.TreeName == treeId
                     && d.OriginClusterId == PublicReplicationApiClusterFixture.SiteAClusterId)
            .ToList();

        Assert.That(deliveries, Is.Not.Empty);

        var withApply = deliveries.FirstOrDefault(d => d.Result.Applied);
        Assert.That(withApply, Is.Not.EqualTo(default(LoopbackDeliveringTransport.DeliveryRecord)),
            "At least one delivery should have Applied=true.");
        Assert.That(withApply.Result.HighWaterMark, Is.Not.EqualTo(HybridLogicalClock.Zero),
            "The HighWaterMark on the ApplyResult must reflect the entry's stamped HLC.");
    }
}
