using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the etag race between concurrent public state-
/// writing methods on <c>BPlusLeafGrain</c>. The leaf's mutation surface
/// (<c>SetAsync</c> / <c>SetManyAsync</c> / <c>DeleteAsync</c>) is marked
/// <c>[AlwaysInterleave]</c>, so any other public method that calls
/// <c>PersistAsync</c> (which awaits <c>state.WriteStateAsync</c>) can
/// have its persist window overlap a concurrent foreground commit's
/// split-time persist, or another concurrent topology-update RPC's
/// persist. Without serialisation through the per-activation
/// <c>_splitGate</c>, the two pending <c>WriteStateAsync</c> calls race
/// the underlying grain-storage etag and the loser throws
/// <c>InconsistentStateException</c>.
/// <para>
/// This regression mirrors the c2-vi-followup fix on
/// <c>BPlusInternalGrain</c> (see <c>BPlusInternalGrainTests.EtagRace</c>)
/// at the leaf layer. Surfaced by the durable Azure Tables benchmark at
/// 25000:5 (U9p step c2-iv-redux probe-0) where the donor side of a
/// split sequentially calls <c>SetTreeIdAsync</c> /
/// <c>SetShardIndexAsync</c> / <c>SetKeyRangeAsync</c> /
/// <c>SetNextSiblingAsync</c> / <c>SetPrevSiblingAsync</c> on the new
/// sibling activation, while the parent internal grain concurrently
/// calls <c>SetParentAsync</c> on the same sibling.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly GrainId ParentId = GrainId.Create("internal", "parent-0");
    private static readonly GrainId NeighbourId = GrainId.Create("leaf", "neighbour-0");

    [Test]
    public async Task SetParentAsync_does_not_race_etag_when_concurrent_with_SetNextSiblingAsync()
    {
        var state = new FakePersistentState<LeafNodeState>
        {
            SimulateEtagChecks = true,
            EtagRendezvousTimeout = TimeSpan.FromMilliseconds(250),
        };
        var grain = CreateGrain(state);
        // Seed an initial persist so subsequent writes hit a non-empty
        // etag (the very first write would otherwise establish the etag
        // without a CAS, masking the race).
        await grain.SetTreeIdAsync("fox");

        var parentTask = Task.Run(() => grain.SetParentAsync(ParentId));
        var nextTask = Task.Run(() => grain.SetNextSiblingAsync(NeighbourId));

        await Task.WhenAll(parentTask, nextTask);

        Assert.That(state.EtagConflictCount, Is.EqualTo(0),
            "Per-activation _splitGate must serialise SetParentAsync against SetNextSiblingAsync's PersistAsync window.");
    }

    [Test]
    public async Task SetParentAsync_does_not_race_etag_when_concurrent_with_SetPrevSiblingAsync()
    {
        var state = new FakePersistentState<LeafNodeState>
        {
            SimulateEtagChecks = true,
            EtagRendezvousTimeout = TimeSpan.FromMilliseconds(250),
        };
        var grain = CreateGrain(state);
        await grain.SetTreeIdAsync("fox");

        var parentTask = Task.Run(() => grain.SetParentAsync(ParentId));
        var prevTask = Task.Run(() => grain.SetPrevSiblingAsync(NeighbourId));

        await Task.WhenAll(parentTask, prevTask);

        Assert.That(state.EtagConflictCount, Is.EqualTo(0),
            "Per-activation _splitGate must serialise SetParentAsync against SetPrevSiblingAsync's PersistAsync window.");
    }

    [Test]
    public async Task SetNextSiblingAsync_does_not_race_etag_when_concurrent_with_SetPrevSiblingAsync()
    {
        var state = new FakePersistentState<LeafNodeState>
        {
            SimulateEtagChecks = true,
            EtagRendezvousTimeout = TimeSpan.FromMilliseconds(250),
        };
        var grain = CreateGrain(state);
        await grain.SetTreeIdAsync("fox");

        var nextTask = Task.Run(() => grain.SetNextSiblingAsync(NeighbourId));
        var prevTask = Task.Run(() => grain.SetPrevSiblingAsync(NeighbourId));

        await Task.WhenAll(nextTask, prevTask);

        Assert.That(state.EtagConflictCount, Is.EqualTo(0),
            "Per-activation _splitGate must serialise SetNextSiblingAsync against SetPrevSiblingAsync's PersistAsync window.");
    }
}
