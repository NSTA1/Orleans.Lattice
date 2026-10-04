using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// The maintenance tick re-arms the durable causal-apply buffer (#4464): it
/// drains every phase tick, so a parked entry whose dependencies are met is
/// applied even after a restart, under quiescence, or when the satisfying
/// apply ran on a silo that did not know the buffer held entries.
/// </summary>
public partial class ReplicationMaintenanceGrainTests
{
    [Test]
    public async Task ProcessNextPhaseAsync_drains_the_causal_apply_buffer_every_tick()
    {
        var (grain, _, _, _, _, _, _, factory) = Create();
        var buffer = Substitute.For<ICausalApplyBufferGrain>();
        factory.GetGrain<ICausalApplyBufferGrain>(Tree).Returns(buffer);

        await grain.ProcessNextPhaseAsync();
        await grain.ProcessNextPhaseAsync();

        await buffer.Received(2).DrainAsync();
    }

    [Test]
    public async Task ProcessNextPhaseAsync_a_failed_drain_does_not_block_the_rest_of_the_tick()
    {
        var (grain, _, _, gc, _, _, _, factory) = Create();
        var buffer = Substitute.For<ICausalApplyBufferGrain>();
        buffer.DrainAsync().ThrowsAsync(new TimeoutException("drain stuck"));
        factory.GetGrain<ICausalApplyBufferGrain>(Tree).Returns(buffer);

        await grain.ProcessNextPhaseAsync();

        await gc.Received(1).RunOnceAsync(Tree, Arg.Any<CancellationToken>());
    }
}
