using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the grain-call budget of one sampling pass: maintenance is probed on the
/// four coordinators without an <see cref="ILattice"/> hop, and each shard is
/// sampled with one combined status call.
/// </summary>
public partial class HotShardMonitorGrainTests
{
    [Test]
    public async Task RunSamplingPass_probes_maintenance_on_the_coordinators_without_the_lattice_hop()
    {
        var (grain, grainFactory, _, _, _, _, _) = CreateGrain();

        await grain.RunSamplingPassAsync();

        grainFactory.DidNotReceive().GetGrain<ILattice>(Arg.Any<string>(), Arg.Any<string?>());
        await grainFactory.GetGrain<ITreeResizeGrain>(TreeId).Received(1).IsIdleAsync();
        await grainFactory.GetGrain<ITreeReshardGrain>(TreeId).Received(1).IsIdleAsync();
        await grainFactory.GetGrain<ITreeMergeGrain>(TreeId).Received(1).IsCompleteAsync();
        await grainFactory.GetGrain<ITreeSnapshotGrain>(TreeId).Received(1).IsIdleAsync();
    }

    [Test]
    public async Task RunSamplingPass_maintenance_probe_short_circuits_on_a_busy_resize()
    {
        var (grain, grainFactory, lattice, splitGrain, _, _, _) = CreateGrain();
        lattice.IsResizeCompleteAsync().Returns(false);

        await grain.RunSamplingPassAsync();

        await grainFactory.GetGrain<ITreeReshardGrain>(TreeId).DidNotReceive().IsIdleAsync();
        await grainFactory.GetGrain<ITreeMergeGrain>(TreeId).DidNotReceive().IsCompleteAsync();
        await grainFactory.GetGrain<ITreeSnapshotGrain>(TreeId).DidNotReceive().IsIdleAsync();
        await splitGrain.DidNotReceive().SplitAsync(Arg.Any<int>());
    }

    [Test]
    public async Task RunSamplingPass_probes_each_shard_once_through_the_combined_status_call()
    {
        var (grain, _, _, _, _, shardOf, _) = CreateGrain();

        await grain.RunSamplingPassAsync();

        for (var i = 0; i < 2; i++)
        {
            await shardOf(i).Received(1).GetSamplingStatusAsync();
        }
    }
}
