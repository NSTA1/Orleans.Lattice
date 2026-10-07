namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The fall-off boundary at a checkpoint of <c>0</c> (issue #4433, review finding
/// F17). A durably recorded checkpoint of <c>0</c> means offset <c>0</c> was read
/// and offset <c>1</c> is still needed, so a WAL trimmed past offset <c>1</c> has
/// lost it. An unassigned scalar <c>0</c> (issue #2703) is the "nothing read"
/// sentinel and loses nothing.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public void Cold_rebuild_over_a_durable_zero_checkpoint_whose_next_offset_was_trimmed_throws()
    {
        var store = new InMemorySnapshotStore();
        var coord = BuildChunkingCoordinator(
            head: 4, sliceSize: 8, tail: 2,
            ColdRebuildSet(2, "k2"), ColdRebuildSet(3, "k3"));
        var (grain, state, _) = BuildColdRebuildLeaf(coord, store.Stub, persistedCheckpoint: 0);
        state.State.ProjectionCheckpointOffsetAssigned = true;

        Assert.ThrowsAsync<LeafProjectionStaleException>(
            async () => await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None),
            "offset 1, the first one a leaf checkpointed at 0 still needs, fell off the log");
        Assert.That(store.SaveCount, Is.EqualTo(0));
    }

    [Test]
    public async Task Cold_rebuild_over_a_durable_zero_checkpoint_with_only_its_read_entry_trimmed_replays()
    {
        var store = new InMemorySnapshotStore();
        var coord = BuildChunkingCoordinator(
            head: 4, sliceSize: 8, tail: 1,
            ColdRebuildSet(1, "k1"), ColdRebuildSet(2, "k2"), ColdRebuildSet(3, "k3"));
        var (grain, state, _) = BuildColdRebuildLeaf(coord, store.Stub, persistedCheckpoint: 0);
        state.State.ProjectionCheckpointOffsetAssigned = true;

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(3L),
            "only the already-read offset 0 was trimmed, so the whole needed window replays");
    }

    [Test]
    public async Task An_unassigned_zero_checkpoint_is_the_nothing_read_sentinel_and_loses_nothing()
    {
        var store = new InMemorySnapshotStore();
        var coord = BuildChunkingCoordinator(
            head: 4, sliceSize: 8, tail: 2,
            ColdRebuildSet(2, "k2"), ColdRebuildSet(3, "k3"));
        var (grain, state, _) = BuildColdRebuildLeaf(coord, store.Stub, persistedCheckpoint: 0);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(3L),
            "a leaf that never recorded a checkpoint has read nothing, so a trimmed prefix is not its loss");
    }
}
