namespace Orleans.Lattice.Replication.Tests.Grains;

public partial class ReplicationHighWaterMarkGrainTests
{
    [Test]
    public async Task HasAppliedManyAsync_matches_single_probes_not_high_watermarks_and_resets()
    {
        var grain = HighWaterMarkTestGrains.Real();
        var known = new HybridLogicalClock { WallClockTicks = 10 };
        var missing = new HybridLogicalClock { WallClockTicks = 5 };
        await grain.AdvanceAppliedAsync("origin", known, [known], true, CancellationToken.None);
        var requested = new[] { known, missing, known };
        Assert.That(await grain.HasAppliedManyAsync("origin", requested), Is.EqualTo(new[] { true, false, true }));
        Assert.That(await grain.HasAppliedManyAsync("other", requested), Is.EqualTo(new[] { false, false, false }));
        Assert.That(await grain.HasAppliedManyAsync("origin", []), Is.Empty);

        await grain.ResetAppliedIdentitiesAsync(CancellationToken.None);
        Assert.That(await grain.HasAppliedManyAsync("origin", requested), Is.EqualTo(new[] { false, false, false }));
        Assert.Throws<ArgumentException>(() => grain.HasAppliedManyAsync("", []));
        Assert.Throws<ArgumentNullException>(() => grain.HasAppliedManyAsync("origin", null!));
    }
}
