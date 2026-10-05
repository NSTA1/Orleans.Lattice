using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

[TestFixture]
public class BootstrapForeignDeleteReconcileTests
{
    private const string Origin = "site-c";
    private static readonly Guid Lineage = Guid.NewGuid();

    private static readonly SnapshotSourceGeneration Stable = new()
    {
        PhysicalTreeId = "tree-1",
        ShardMapVersion = 3,
        Lineage = Lineage,
        DeleteEpoch = 0,
        IsDeleted = false,
    };

    private static HybridLogicalClock At(long ticks) => new() { WallClockTicks = ticks };

    private static SnapshotSourceFrontier Frontier(Guid? lineage = null, params HybridLogicalClock[] held) => new()
    {
        Lineage = lineage ?? Lineage,
        LowWatermarks = new Dictionary<string, HybridLogicalClock> { [Origin] = At(100) },
        Held = new Dictionary<string, HybridLogicalClock[]> { [Origin] = held },
    };

    [Test]
    public void FrontierMatchesOpen_requires_the_opening_lineage()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BootstrapForeignDeleteReconcile.FrontierMatchesOpen(Frontier(), Stable), Is.True);
            Assert.That(BootstrapForeignDeleteReconcile.FrontierMatchesOpen(Frontier(Guid.NewGuid()), Stable), Is.False);
            Assert.That(BootstrapForeignDeleteReconcile.FrontierMatchesOpen(null, Stable), Is.False);
            Assert.That(BootstrapForeignDeleteReconcile.FrontierMatchesOpen(Frontier(), null), Is.False);
            Assert.That(BootstrapForeignDeleteReconcile.FrontierMatchesOpen(Frontier(), Stable with { Lineage = null }), Is.False);
        });
    }

    [Test]
    public void IsEligible_needs_a_stable_live_known_generation_and_a_last_writer_wins_tree()
    {
        var frontier = Frontier();
        Assert.Multiple(() =>
        {
            Assert.That(BootstrapForeignDeleteReconcile.IsEligible(frontier, Stable, Stable, LatticeMergeMode.LwwRegister), Is.True);
            Assert.That(BootstrapForeignDeleteReconcile.IsEligible(frontier, Stable, Stable, LatticeMergeMode.GCounter), Is.False);
            Assert.That(BootstrapForeignDeleteReconcile.IsEligible(frontier, Stable, null, LatticeMergeMode.LwwRegister), Is.False);
            Assert.That(BootstrapForeignDeleteReconcile.IsEligible(frontier, Stable, Stable with { DeleteEpoch = 1 }, LatticeMergeMode.LwwRegister), Is.False);
            Assert.That(BootstrapForeignDeleteReconcile.IsEligible(frontier, Stable, Stable with { ShardMapVersion = 4 }, LatticeMergeMode.LwwRegister), Is.False);
            Assert.That(BootstrapForeignDeleteReconcile.IsEligible(frontier, Stable, Stable with { PhysicalTreeId = "tree-2" }, LatticeMergeMode.LwwRegister), Is.False);
            Assert.That(BootstrapForeignDeleteReconcile.IsEligible(frontier, Stable, Stable with { Lineage = Guid.NewGuid() }, LatticeMergeMode.LwwRegister), Is.False);
            Assert.That(BootstrapForeignDeleteReconcile.IsEligible(frontier, Stable, Stable with { IsDeleted = true }, LatticeMergeMode.LwwRegister), Is.False);
            Assert.That(BootstrapForeignDeleteReconcile.IsEligible(frontier, Stable, Stable with { IsDeleted = null }, LatticeMergeMode.LwwRegister), Is.False);
            Assert.That(BootstrapForeignDeleteReconcile.IsEligible(Frontier(Guid.NewGuid()), Stable, Stable, LatticeMergeMode.LwwRegister), Is.False);
        });
    }

    [Test]
    public void IsStable_rejects_an_unknown_field()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BootstrapForeignDeleteReconcile.IsStable(Stable, Stable), Is.True);
            Assert.That(BootstrapForeignDeleteReconcile.IsStable(Stable with { PhysicalTreeId = null }, Stable with { PhysicalTreeId = null }), Is.False);
            Assert.That(BootstrapForeignDeleteReconcile.IsStable(Stable with { ShardMapVersion = null }, Stable with { ShardMapVersion = null }), Is.False);
            Assert.That(BootstrapForeignDeleteReconcile.IsStable(Stable with { DeleteEpoch = null }, Stable with { DeleteEpoch = null }), Is.False);
        });
    }

    [Test]
    public void ShouldDelete_only_a_write_below_the_low_watermark_that_is_not_held()
    {
        var frontier = Frontier(held: At(50));
        Assert.Multiple(() =>
        {
            Assert.That(BootstrapForeignDeleteReconcile.ShouldDelete(frontier, Origin, At(99)), Is.True);
            Assert.That(BootstrapForeignDeleteReconcile.ShouldDelete(frontier, Origin, At(100)), Is.False, "at the watermark");
            Assert.That(BootstrapForeignDeleteReconcile.ShouldDelete(frontier, Origin, At(101)), Is.False, "above the watermark");
            Assert.That(BootstrapForeignDeleteReconcile.ShouldDelete(frontier, Origin, At(50)), Is.False, "held at the source");
            Assert.That(BootstrapForeignDeleteReconcile.ShouldDelete(frontier, "site-d", At(1)), Is.False, "an origin the frontier lacks");
            Assert.That(BootstrapForeignDeleteReconcile.ShouldDelete(frontier, "", At(1)), Is.False);
        });
    }

    [Test]
    public void ShouldDelete_ignores_a_zero_low_watermark()
    {
        var frontier = new SnapshotSourceFrontier
        {
            Lineage = Lineage,
            LowWatermarks = new Dictionary<string, HybridLogicalClock> { [Origin] = HybridLogicalClock.Zero },
        };

        Assert.That(BootstrapForeignDeleteReconcile.ShouldDelete(frontier, Origin, HybridLogicalClock.Zero), Is.False);
    }
}
