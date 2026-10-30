namespace Orleans.Lattice.Replication.Tests.Grains;

public partial class ReplicationDeadLetterGrainTests
{
    [Test]
    public async Task IsHoldingManyAsync_matches_single_probes_across_removal_and_origins()
    {
        var (grain, _, _) = await CreateGrainAsync();
        var entry = MakeEntry();
        var id = await grain.EnqueueAsync(entry, "test", 0, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);
        var requested = new[] { entry.Timestamp, HybridLogicalClock.Zero, entry.Timestamp };

        Assert.That(await grain.IsHoldingManyAsync("site-b", requested, CancellationToken.None),
            Is.EqualTo(new[] { true, false, true }));
        Assert.That(await grain.IsHoldingManyAsync("other", requested, CancellationToken.None),
            Is.EqualTo(new[] { false, false, false }));
        Assert.That(await grain.IsHoldingManyAsync("site-b", [], CancellationToken.None), Is.Empty);

        await grain.DiscardAsync(id, CancellationToken.None);
        Assert.That(await grain.IsHoldingManyAsync("site-b", requested, CancellationToken.None),
            Is.EqualTo(new[] { false, false, false }));
    }

    [Test]
    public async Task IsHoldingManyAsync_validates_arguments_and_cancellation()
    {
        var (grain, _, _) = await CreateGrainAsync();
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentException>(() => grain.IsHoldingManyAsync("", [], CancellationToken.None));
            Assert.Throws<ArgumentNullException>(() => grain.IsHoldingManyAsync("o", null!, CancellationToken.None));
            Assert.Throws<OperationCanceledException>(() => grain.IsHoldingManyAsync("o", [], new CancellationToken(true)));
        });
    }
}
