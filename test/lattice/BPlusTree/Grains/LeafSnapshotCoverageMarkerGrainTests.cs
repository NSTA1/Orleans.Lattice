using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit coverage for <see cref="LeafSnapshotCoverageMarkerGrain"/> (issue
/// #4634): the marker only rises, per partition, until it is cleared.
/// </summary>
[TestFixture]
public class LeafSnapshotCoverageMarkerGrainTests
{
    [Test]
    public void Raise_takes_the_per_partition_maximum()
    {
        Assert.That(LeafSnapshotCoverageMarkerGrain.Raise([3, 7], [5, 2]), Is.EqualTo(new long[] { 5, 7 }));
    }

    [Test]
    public void Raise_widens_to_the_longer_array()
    {
        Assert.That(LeafSnapshotCoverageMarkerGrain.Raise([3], [-1, 4]), Is.EqualTo(new long[] { 3, 4 }));
    }

    [Test]
    public void Raise_from_nothing_records_the_covered_offsets()
    {
        Assert.That(LeafSnapshotCoverageMarkerGrain.Raise(null, [0]), Is.EqualTo(new long[] { 0 }));
    }

    [Test]
    public void Raise_that_lifts_nothing_returns_null()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LeafSnapshotCoverageMarkerGrain.Raise([5, 7], [5, 6]), Is.Null);
            Assert.That(LeafSnapshotCoverageMarkerGrain.Raise(null, [-1]), Is.Null);
        });
    }

    [Test]
    public void Raise_never_mutates_its_inputs()
    {
        long[] current = [3, 7];
        long[] covered = [5, 2];
        LeafSnapshotCoverageMarkerGrain.Raise(current, covered);
        Assert.Multiple(() =>
        {
            Assert.That(current, Is.EqualTo(new long[] { 3, 7 }));
            Assert.That(covered, Is.EqualTo(new long[] { 5, 2 }));
        });
    }

    [Test]
    public async Task RaiseAsync_persists_a_rise_and_GetAsync_returns_it()
    {
        var state = new FakePersistentState<LeafSnapshotCoverageMarkerState>();
        var grain = new LeafSnapshotCoverageMarkerGrain(state);

        await grain.RaiseAsync([4]);

        Assert.Multiple(async () =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(1));
            Assert.That(await grain.GetAsync(), Is.EqualTo(new long[] { 4 }));
        });
    }

    [Test]
    public async Task RaiseAsync_that_lifts_nothing_does_not_write()
    {
        var state = new FakePersistentState<LeafSnapshotCoverageMarkerState>();
        state.State.CoveredOffsetsByPartition = [9];
        var grain = new LeafSnapshotCoverageMarkerGrain(state);

        await grain.RaiseAsync([4]);

        Assert.Multiple(async () =>
        {
            Assert.That(state.WriteCount, Is.Zero);
            Assert.That(await grain.GetAsync(), Is.EqualTo(new long[] { 9 }));
        });
    }

    [Test]
    public void RaiseAsync_that_fails_to_write_leaves_the_marker_where_it_was()
    {
        var state = new FakePersistentState<LeafSnapshotCoverageMarkerState>();
        state.State.CoveredOffsetsByPartition = [2];
        state.ThrowOnWrite = new InvalidOperationException("storage down");
        var grain = new LeafSnapshotCoverageMarkerGrain(state);

        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.RaiseAsync([4]));
        Assert.That(state.State.CoveredOffsetsByPartition, Is.EqualTo(new long[] { 2 }),
            "A raise the store did not accept must not be reported back as durable.");
    }

    [Test]
    public async Task GetAsync_returns_null_when_nothing_was_kept()
    {
        var grain = new LeafSnapshotCoverageMarkerGrain(new FakePersistentState<LeafSnapshotCoverageMarkerState>());
        Assert.That(await grain.GetAsync(), Is.Null);
    }

    [Test]
    public async Task GetAsync_returns_a_copy()
    {
        var state = new FakePersistentState<LeafSnapshotCoverageMarkerState>();
        state.State.CoveredOffsetsByPartition = [4];
        var grain = new LeafSnapshotCoverageMarkerGrain(state);

        var read = await grain.GetAsync();
        read![0] = 99;

        Assert.That(state.State.CoveredOffsetsByPartition, Is.EqualTo(new long[] { 4 }));
    }

    [Test]
    public async Task ClearAsync_removes_the_marker()
    {
        var state = new FakePersistentState<LeafSnapshotCoverageMarkerState>();
        state.State.CoveredOffsetsByPartition = [4];
        var grain = new LeafSnapshotCoverageMarkerGrain(state);

        await grain.ClearAsync();

        Assert.That(await grain.GetAsync(), Is.Null);
    }
}
