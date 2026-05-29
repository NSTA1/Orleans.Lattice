using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the etag race between <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusInternalGrain.AcceptSplitAsync"/>
/// (marked <c>[AlwaysInterleave]</c> so its <c>state.WriteStateAsync</c> await
/// window releases the activation turn) and concurrent non-interleaved write
/// paths that also persist state on the same activation. Without serialisation
/// through the per-activation <c>_splitGate</c>, the two pending
/// <c>WriteStateAsync</c> calls race the underlying grain-storage etag and
/// whichever returns second throws <c>InconsistentStateException</c>.
/// </summary>
public partial class BPlusInternalGrainTests
{
    [Test]
    public async Task OnChildDigestPublishedAsync_does_not_race_etag_when_concurrent_with_AcceptSplitAsync()
    {
        var state = new FakePersistentState<InternalNodeState>
        {
            SimulateEtagChecks = true,
            EtagRendezvousTimeout = TimeSpan.FromMilliseconds(250),
        };
        var grain = CreateGrain(state);
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        var snapshot = new ChildDigestSnapshot
        {
            Hash = new byte[16],
            EntryCount = 42,
            CheckpointOffset = 17,
        };

        var publishTask = Task.Run(() => grain.OnChildDigestPublishedAsync(Child0, snapshot));
        var splitTask = Task.Run(() => grain.AcceptSplitAsync("monkey", Child2));

        await Task.WhenAll(publishTask, splitTask);

        Assert.That(state.EtagConflictCount, Is.EqualTo(0),
            "Per-activation _splitGate must serialise OnChildDigestPublishedAsync against AcceptSplitAsync's interleaved WriteStateAsync window.");
    }

    [Test]
    public async Task SetParentAsync_does_not_race_etag_when_concurrent_with_AcceptSplitAsync()
    {
        var state = new FakePersistentState<InternalNodeState>
        {
            SimulateEtagChecks = true,
            EtagRendezvousTimeout = TimeSpan.FromMilliseconds(250),
        };
        var grain = CreateGrain(state);
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: true);

        var parentTask = Task.Run(() => grain.SetParentAsync(Child3));
        var splitTask = Task.Run(() => grain.AcceptSplitAsync("monkey", Child2));

        await Task.WhenAll(parentTask, splitTask);

        Assert.That(state.EtagConflictCount, Is.EqualTo(0),
            "Per-activation _splitGate must serialise SetParentAsync against AcceptSplitAsync's interleaved WriteStateAsync window.");
    }
}
