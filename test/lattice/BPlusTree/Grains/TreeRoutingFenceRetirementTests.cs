using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the purge's per-tree fence-row retirement (epic #5605, #5668), run
/// against storage directly: the shard markers and the generation rows are deleted
/// only under a floor that covers them, a re-drive after a partial retirement
/// finishes it, and a WAL partition on a build that predates the retirement call is
/// retired straight from storage, while any other failure propagates.
/// </summary>
[TestFixture]
public sealed class TreeRoutingFenceRetirementTests
{
    private const string Tree = "fence-retire";

    private static Task WriteGenerationAsync(IGrainStorage storage, int partition, WalRoutingInitiator initiator, long generation) =>
        RoutingGenerationSlots.WriteAsync(storage, Tree, partition, initiator, generation);

    private static Task WriteMarkerAsync(IGrainStorage storage, int shard, long generation) =>
        HandOffMarkerRows.UpdateShardAsync(storage, Tree, shard, m =>
        {
            m.RoutingGeneration = generation;
            return true;
        });

    private static Task<long> FloorAsync(IGrainStorage storage, WalRoutingInitiator initiator) =>
        WalAppendFenceRows.ReadRoutingFloorAsync(storage, Tree, initiator);

    [Test]
    public void Initiators_are_every_shard_root_and_the_range_delete_fence()
    {
        Assert.That(
            TreeRoutingFenceRetirement.Initiators(2),
            Is.EqualTo(new[] { WalRoutingInitiator.ShardRoot(0), WalRoutingInitiator.ShardRoot(1), WalRoutingInitiator.TreeRangeDelete }));
    }

    [TestCase(false, 0)]
    [TestCase(false, 37)]
    [TestCase(true, 0)]
    public void An_initiator_tag_round_trips(bool rangeDelete, int index)
    {
        var initiator = rangeDelete ? WalRoutingInitiator.TreeRangeDelete : WalRoutingInitiator.ShardRoot(index);

        var parsed = WalAppendFenceRows.TryParseInitiatorTag(WalAppendFenceRows.InitiatorTag(initiator), out var back);

        Assert.Multiple(() =>
        {
            Assert.That(parsed, Is.True);
            Assert.That(back, Is.EqualTo(initiator));
            Assert.That(WalAppendFenceRows.TryParseInitiatorTag("x1", out _), Is.False, "an unknown kind is skipped, not mistaken for another");
            Assert.That(WalAppendFenceRows.TryParseInitiatorTag("s", out _), Is.False);
        });
    }

    [Test]
    public async Task A_partition_on_an_older_build_is_retired_straight_from_storage()
    {
        var storage = new EnumerableMemoryGrainStorage();
        await WriteGenerationAsync(storage, 0, WalRoutingInitiator.ShardRoot(0), 3);
        await WriteGenerationAsync(storage, 0, WalRoutingInitiator.TreeRangeDelete, 7);
        await WriteGenerationAsync(storage, 1, WalRoutingInitiator.ShardRoot(0), 9);

        await TreeRoutingFenceRetirement.RetirePartitionAsync(
            () => throw new TypeLoadException(WalAppendFenceMethodProbe.UnresolvedAliasPrefix + " 'ol.retire'"),
            storage,
            Tree,
            partition: 0,
            shardCount: 1);

        var left = (await RoutingGenerationSlots.ReadAsync(storage, Tree, 1, WalRoutingInitiator.ShardRoot(0))).Generation;
        var shardFloor = await FloorAsync(storage, WalRoutingInitiator.ShardRoot(0));
        var fenceFloor = await FloorAsync(storage, WalRoutingInitiator.TreeRangeDelete);
        Assert.Multiple(() =>
        {
            Assert.That(storage.GrainIds(WalAppendFenceRows.GenerationGrainType), Has.Count.EqualTo(1),
                "partition 0's table row is deleted from storage; partition 1 is retired by its own call");
            Assert.That(left, Is.EqualTo(9));
            Assert.That(shardFloor, Is.EqualTo(3), "each initiator's floor covers the rows of it the fallback deleted");
            Assert.That(fenceFloor, Is.EqualTo(7));
        });
    }

    [Test]
    public async Task Any_other_failure_of_the_partition_call_propagates_and_deletes_nothing()
    {
        var storage = new EnumerableMemoryGrainStorage();
        await WriteGenerationAsync(storage, 0, WalRoutingInitiator.ShardRoot(0), 3);

        Assert.That(
            async () => await TreeRoutingFenceRetirement.RetirePartitionAsync(
                () => throw new TimeoutException(), storage, Tree, partition: 0, shardCount: 1),
            Throws.InstanceOf<TimeoutException>());
        Assert.That(storage.GrainIds(WalAppendFenceRows.GenerationGrainType), Has.Count.EqualTo(1),
            "an ambiguous failure is re-driven by the purge, never answered by a storage delete");
    }

    [Test]
    public async Task Shard_markers_are_deleted_under_a_floor_that_covers_every_one()
    {
        var storage = new EnumerableMemoryGrainStorage();
        await WriteMarkerAsync(storage, 0, 2);
        await WriteMarkerAsync(storage, 1, 9);
        await WriteMarkerAsync(storage, 2, 4);

        var floors = await TreeRoutingFenceRetirement.RetireMarkersAsync(storage, Tree, shardCount: 3);

        Assert.Multiple(() =>
        {
            Assert.That(WalAppendFenceRows.FloorOf(floors, WalRoutingInitiator.ShardRoot(0)), Is.EqualTo(2));
            Assert.That(WalAppendFenceRows.FloorOf(floors, WalRoutingInitiator.ShardRoot(1)), Is.EqualTo(9));
            Assert.That(WalAppendFenceRows.FloorOf(floors, WalRoutingInitiator.ShardRoot(2)), Is.EqualTo(4));
            Assert.That(storage.GrainIds(HandOffMarkerRows.ShardGrainType), Is.Empty);
        });
    }

    [Test]
    public async Task A_re_drive_after_a_partial_retirement_finishes_it_and_the_floor_never_drops()
    {
        var inner = new EnumerableMemoryGrainStorage();
        var faulting = new ClearFaultingGrainStorage(inner);
        await WriteMarkerAsync(inner, 0, 5);
        await WriteMarkerAsync(inner, 1, 3);

        // The first delete crashes; whichever marker it was, the floor already covers both.
        faulting.FailClears = 1;
        Assert.That(
            async () => await TreeRoutingFenceRetirement.RetireMarkersAsync(faulting, Tree, shardCount: 2),
            Throws.InstanceOf<IOException>());
        var floorAfterCrash = await FloorAsync(inner, WalRoutingInitiator.ShardRoot(0));

        var floors = await TreeRoutingFenceRetirement.RetireMarkersAsync(faulting, Tree, shardCount: 2);

        Assert.Multiple(() =>
        {
            Assert.That(floorAfterCrash, Is.EqualTo(5), "the floor is durable before the first delete");
            Assert.That(WalAppendFenceRows.FloorOf(floors, WalRoutingInitiator.ShardRoot(0)), Is.EqualTo(5),
                "the re-drive finds nothing higher and leaves the floor where it was");
            Assert.That(inner.GrainIds(HandOffMarkerRows.ShardGrainType), Is.Empty, "the re-drive deletes what the crash left");
        });
    }

    [Test]
    public async Task A_row_rewritten_after_the_read_is_covered_by_the_floor_before_it_is_deleted()
    {
        var storage = new RewriteOnFirstClearStorage(new EnumerableMemoryGrainStorage());
        await WriteMarkerAsync(storage, 0, 2);
        storage.Arm(() => WriteMarkerAsync(storage.Inner, 0, 11));

        var floors = await TreeRoutingFenceRetirement.RetireMarkersAsync(storage, Tree, shardCount: 1);

        Assert.Multiple(() =>
        {
            Assert.That(WalAppendFenceRows.FloorOf(floors, WalRoutingInitiator.ShardRoot(0)), Is.EqualTo(11), "the conflicting rewrite is re-read and the floor raised over it");
            Assert.That(storage.Inner.GrainIds(HandOffMarkerRows.ShardGrainType), Is.Empty);
        });
    }

    /// <summary>Rewrites a row once, just before the first delete, so the delete's ETag is stale.</summary>
    private sealed class RewriteOnFirstClearStorage(EnumerableMemoryGrainStorage inner) : IGrainStorage
    {
        private Func<Task>? _rewrite;

        public EnumerableMemoryGrainStorage Inner => inner;

        public void Arm(Func<Task> rewrite) => _rewrite = rewrite;

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            inner.ReadStateAsync(stateName, grainId, grainState);

        public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            inner.WriteStateAsync(stateName, grainId, grainState);

        public async Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            if (Interlocked.Exchange(ref _rewrite, null) is { } rewrite)
            {
                await rewrite();
            }

            // A real provider deletes conditionally on the ETag; the in-memory one does not.
            var current = new GrainState<T>();
            await inner.ReadStateAsync(stateName, grainId, current);
            if (current.RecordExists && !string.Equals(current.ETag, grainState.ETag, StringComparison.Ordinal))
            {
                throw new InconsistentStateException("ETag mismatch.", current.ETag, grainState.ETag);
            }

            await inner.ClearStateAsync(stateName, grainId, grainState);
        }
    }
}
