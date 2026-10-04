using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Wires a real <see cref="CausalApplyBufferGrain"/> (the durable per-tree
/// causal-apply buffer, #4464) into a substitute <see cref="IGrainFactory"/>,
/// so applier fixtures exercise the production park and drain logic. The
/// returned state is the grain's "storage": constructing a second grain over
/// the same state models a deactivation / restart that reloads it.
/// </summary>
internal static class CausalBufferTestWiring
{
    public static (CausalApplyBufferGrain Grain, FakePersistentState<CausalApplyBufferState> State) Wire(
        IGrainFactory factory,
        ReplicationApplier applier,
        IOptionsMonitor<LatticeReplicationOptions> monitor,
        string treeId,
        FakePersistentState<CausalApplyBufferState>? state = null)
    {
        state ??= new FakePersistentState<CausalApplyBufferState>();
        var grain = Create(factory, applier, monitor, treeId, state);
        factory.GetGrain<ICausalApplyBufferGrain>(treeId).Returns(grain);
        return (grain, state);
    }

    public static CausalApplyBufferGrain Create(
        IGrainFactory factory,
        ReplicationApplier applier,
        IOptionsMonitor<LatticeReplicationOptions> monitor,
        string treeId,
        FakePersistentState<CausalApplyBufferState> state)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("causal-buffer", treeId));
        return new CausalApplyBufferGrain(
            context,
            factory,
            monitor,
            applier,
            NullLogger<CausalApplyBufferGrain>.Instance,
            state);
    }
}
