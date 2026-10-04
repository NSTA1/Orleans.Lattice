using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// #4464 mechanisms 3 and 4 at the bootstrap pin: the pin takes the pointwise
/// maximum with the vector already held (it never replaces it), and it
/// re-arms a drain of the causal-apply buffer after the merge.
/// </summary>
public partial class LatticeBootstrapCoordinatorGrainTests
{
    [Test]
    public async Task ProcessNextPhase_pin_merges_the_frontier_then_drains_the_causal_buffer()
    {
        var fake = new FakePersistentState<BootstrapCoordinatorState>();
        Seed(fake, LatticeBootstrapState.IncrementalHandoff);
        var frontier = new VersionVector();
        frontier.Entries["us"] = Hlc(99);
        fake.State.SnapshotAsOfHlc = Hlc(99);
        fake.State.CausalStableFrontier = frontier;
        var (grain, _, factory, _, reminders, _, hwm, _) = Create(fake);
        var buffer = Substitute.For<ICausalApplyBufferGrain>();
        factory.GetGrain<ICausalApplyBufferGrain>(Tree).Returns(buffer);
        reminders.GetReminder(Arg.Any<GrainId>(), "bootstrap-keepalive")
            .Returns(Task.FromResult<IGrainReminder?>(null));

        await grain.ProcessNextPhaseAsync();

        Received.InOrder(() =>
        {
            hwm.MergeBootstrapFrontierAsync(Arg.Any<HybridLogicalClock>(), Arg.Any<VersionVector>(), Arg.Any<CancellationToken>());
            buffer.DrainAsync();
        });
        await hwm.DidNotReceiveWithAnyArgs().PinSnapshotAsync(default, default!, default);
        Assert.That(fake.State.Phase, Is.EqualTo(LatticeBootstrapState.LiveIncremental));
    }
}
