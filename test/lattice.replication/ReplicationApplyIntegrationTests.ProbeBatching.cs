using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

public partial class ReplicationApplyIntegrationTests
{
    [Test]
    public async Task Held_source_batch_probes_roundtrip_through_Orleans_and_keep_origins_isolated()
    {
        const string tree = "ri-held-batch-probes";
        const string origin = "held-batch-origin";
        var client = _fixture.SiteB.Client;
        var buffer = client.GetGrain<ICausalApplyBufferGrain>(tree);
        var deadLetters = client.GetGrain<IReplicationDeadLetterGrain>(tree);
        var hwm = client.GetGrain<IReplicationHighWaterMarkGrain>(tree);
        var entry = LwwSet(tree, "held", [1], Hlc(100)) with
        {
            OriginClusterId = origin,
            VectorClock = DependsOnSiteC(50),
        };
        Assert.That(await buffer.ParkAsync(entry), Is.EqualTo(1));
        await deadLetters.EnqueueAsync(entry, "batch probe", 0, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);
        await hwm.AdvanceAppliedAsync(origin, Hlc(50), [Hlc(50)], true, CancellationToken.None);
        var requested = new[] { Hlc(100), Hlc(50), Hlc(100) };

        Assert.That(await buffer.IsHoldingManyAsync(origin, requested), Is.EqualTo(new[] { true, false, true }));
        Assert.That(await deadLetters.IsHoldingManyAsync(origin, requested, CancellationToken.None),
            Is.EqualTo(new[] { true, false, true }));
        Assert.That(await hwm.HasAppliedManyAsync(origin, requested), Is.EqualTo(new[] { false, true, false }));
        Assert.That(await buffer.IsHoldingManyAsync("other", requested), Is.EqualTo(new[] { false, false, false }));
        Assert.That(await deadLetters.IsHoldingManyAsync("other", requested, CancellationToken.None),
            Is.EqualTo(new[] { false, false, false }));
        Assert.That(await hwm.HasAppliedManyAsync("other", requested), Is.EqualTo(new[] { false, false, false }));

        var frontier = client.GetGrain<IReplicationOriginFrontierGrain>(origin);
        await frontier.RecordLowWatermarkAsync(Hlc(1000), 0, CancellationToken.None);
        var verdicts = await frontier.CheckAsync([Hlc(100), Hlc(101), Hlc(100)], CancellationToken.None);
        Assert.That(verdicts, Is.EqualTo(new[] {
            CausalDependencyVerdict.Unmet, CausalDependencyVerdict.Met, CausalDependencyVerdict.Unmet }));
    }
}
