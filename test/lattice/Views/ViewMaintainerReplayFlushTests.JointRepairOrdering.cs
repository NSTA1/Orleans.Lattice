using NSubstitute;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// A joint (cross-tree) repair of an aggregation view whose captured slice holds
/// two or more keys orders them by their source timestamps before projecting.
/// That sort compared <see cref="HybridLogicalClock"/> values through
/// <see cref="Comparer{T}.Default"/>, which threw because the clock implemented
/// no comparison interface, so every drain pass resumed the captured repair and
/// failed again (issue #6068).
/// </summary>
public partial class ViewMaintainerReplayFlushTests
{
    [Test]
    public async Task Aggregation_joint_repair_over_several_keys_projects_and_applies()
    {
        var view = new FakeViewTree();
        var coordinator = new MemoizingCoordinator(view).Arm();
        var checkpoint = new FakePersistentState<ViewCheckpointState>();
        var h = Create(view, checkpoint,
            new ViewRegistration(PeerViewName, PeerSourceTreeId, new IdentityProjection()),
            coordinator.Grain,
            registration: new ViewRegistration(ViewName, SourceTreeId, Projection: null,
                AggregationProjection: new PerKeySumProjection()));
        var tx = Guid.NewGuid();
        // The later-stamped key first, so the repair must reorder its slice.
        ArmLog(h, [Prepared(tx, Key, 5, 30, 2, 0), Prepared(tx, OtherKey, 7, 20, 2, 1), CrossTreeCommit(tx, 31)]);

        for (var pass = 0; pass < 4; pass++)
        {
            await h.Grain.DrainAsync();
        }

        await coordinator.Grain.Received().RegisterRepairReadyAsync(Arg.Is<ViewCrossTreeReadiness>(
            ready => ready.RepairProtocolVersion == 1));
        Assert.Multiple(() =>
        {
            Assert.That(checkpoint.State.CrossTreeRepairHolds, Is.Empty, "the repair completed and released its hold");
            Assert.That(LatticeAggregationValue.DecodeDouble(view.Rows[Key]), Is.EqualTo(5));
            Assert.That(LatticeAggregationValue.DecodeDouble(view.Rows[OtherKey]), Is.EqualTo(7));
        });
    }
}
