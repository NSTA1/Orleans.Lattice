using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [TestCase("absent")]
    [TestCase("newer")]
    [TestCase("live")]
    public void Apply_noop_reap_preserves_projection_clock(string row)
    {
        var (grain, _, state) = CreateGrainWithReporter();
        var projection = AsProjection(grain);
        if (row == "newer")
            projection.Apply(BuildDelete("k", hlcPhysical: 700));
        else if (row == "live")
            projection.Apply(BuildSet("k", new byte[] { 1 }, hlcPhysical: 100));

        var original = new HybridLogicalClock { WallClockTicks = 50 };
        state.State.Clock = original;
        var reap = BuildTombstoneReap("k", hlcPhysical: 600);
        projection.Apply(reap);

        Assert.That(state.State.Clock, Is.EqualTo(original));
        Assert.That(reap.Timestamp.WallClockTicks, Is.EqualTo(600));
    }

}
