namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5851 (O3a): a catch-up replay's incremental banks persist on
/// <see cref="LatticeOptions.MaterialiserCheckpointInterval"/> alone, not on
/// <see cref="LatticeOptions.MaterialiserCheckpointEntries"/>, and the catch-up banks what
/// they held once at its end under the ordinary predicate. Before this, a catch-up across a
/// long WAL span persisted - and published its digest upward - once per entry threshold
/// scanned.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static async Task<List<long>> CatchUpPersistedOffsetsAsync(TimeSpan interval, int entries)
    {
        var replayed = Enumerable.Range(1, 12)
            .Select(i => Set(i, $"k{i:D2}"))
            .ToArray();
        var coord = BuildChunkingCoordinator(head: 13, sliceSize: 4, tail: 0, replayed);
        var store = new InMemorySnapshotStore();
        var state = NewResumableState();
        var persistedOffsets = new List<long>();
        state.OnWriteState = CheckpointAdvancingWrites(persistedOffsets);

        var (grain, _) = BuildResumableLeaf(state, coord, store.Stub, reclassifyEveryN: 1,
            configureOptions: o =>
            {
                o.MaterialiserCheckpointInterval = interval;
                o.MaterialiserCheckpointEntries = entries;
            });

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(12L), "the catch-up banked its whole span");
        for (var i = 1; i <= 12; i++)
        {
            Assert.That(await grain.GetAsync($"k{i:D2}"), Is.Not.Null, $"key k{i:D2} missing after replay");
        }

        return persistedOffsets;
    }

    [Test]
    public async Task A_catch_up_banks_once_at_its_end_rather_than_once_per_entry_threshold()
    {
        // Each four-entry slice crosses the four-entry threshold, so before #5851 the
        // catch-up persisted at 4, 8 and 12. The interval never elapses here.
        var persisted = await CatchUpPersistedOffsetsAsync(Timeout.InfiniteTimeSpan, entries: 4);

        Assert.That(persisted, Is.EqualTo(new long[] { 12 }));
    }

    [Test]
    public async Task A_catch_up_still_banks_incrementally_once_the_interval_elapses()
    {
        // The interval is what keeps a long catch-up resumable across a mid-replay
        // teardown (#1513): with it elapsed at every slice, every slice still banks.
        var persisted = await CatchUpPersistedOffsetsAsync(TimeSpan.FromTicks(1), entries: 1_000_000);

        Assert.That(persisted, Is.EqualTo(new long[] { 4, 8, 12 }));
    }
}
