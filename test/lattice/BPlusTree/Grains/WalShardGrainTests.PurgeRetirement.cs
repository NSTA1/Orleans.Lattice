using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the WAL shard's side of a purge's fence-row retirement (epic #5605,
/// #5668): every routing-generation entry, durable or loaded, is retired under the
/// tree id's routing-generation floor; the floor is durable before any row is
/// deleted, so a crash between the two leaves rows a re-drive deletes and never a
/// generation that can recur; and a WAL shard activated after the purge refuses a
/// claim at or below the floor, whichever path initialised the generation.
/// </summary>
public partial class WalShardGrainTests
{
    [Test]
    public async Task Retiring_routing_generations_deletes_every_entry_under_a_floor_that_covers_them()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);
        await grain.ClaimRoutingGenerationAsync(WalRoutingInitiator.ShardRoot(0), 3, CancellationToken.None);
        await grain.ClaimRoutingGenerationAsync(WalRoutingInitiator.ShardRoot(1), 2, CancellationToken.None);
        await grain.ClaimRoutingGenerationAsync(WalRoutingInitiator.TreeRangeDelete, 5, CancellationToken.None);

        // Shard 1 is outside the count passed: a loaded initiator is retired all the same.
        var highest = await grain.RetireRoutingGenerationsAsync(shardCount: 1, CancellationToken.None);
        var floors = (await WalAppendFenceRows.ReadRoutingFloorAsync(storage, TreeId)).State;

        Assert.Multiple(() =>
        {
            Assert.That(highest, Is.EqualTo(5), "the highest floor is the highest generation retired");
            Assert.That(WalAppendFenceRows.FloorOf(floors, WalRoutingInitiator.ShardRoot(0)), Is.EqualTo(3), "each initiator's floor covers its own entry");
            Assert.That(WalAppendFenceRows.FloorOf(floors, WalRoutingInitiator.ShardRoot(1)), Is.EqualTo(2), "a loaded initiator outside the count included");
            Assert.That(WalAppendFenceRows.FloorOf(floors, WalRoutingInitiator.TreeRangeDelete), Is.EqualTo(5));
            Assert.That(storage.GrainIds(WalAppendFenceRows.GenerationGrainType), Is.Empty, "every generation row is deleted");
            Assert.That(grain.HasGenerationEntryForTesting(WalRoutingInitiator.ShardRoot(0)), Is.False, "the loaded entries are dropped");
            Assert.That(grain.HasGenerationEntryForTesting(WalRoutingInitiator.ShardRoot(1)), Is.False);
            Assert.That(grain.HasGenerationEntryForTesting(WalRoutingInitiator.TreeRangeDelete), Is.False);
            Assert.That(grain.GenerationFloorForTesting(WalRoutingInitiator.TreeRangeDelete), Is.EqualTo(5));
            Assert.That(grain.GenerationFloorForTesting(WalRoutingInitiator.ShardRoot(0)), Is.EqualTo(3));
        });
    }

    [Test]
    public async Task A_straddling_initiator_of_the_purged_incarnation_is_refused_after_re_creation()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);
        var shardRoot = WalRoutingInitiator.ShardRoot(0);
        await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);
        await grain.ClaimRoutingGenerationAsync(shardRoot, 2, CancellationToken.None);
        var stale = new WalAppendStamp(FenceLeaf, 1, shardRoot, 2);
        Assert.That((await grain.AppendStampedAsync(MakeEntry("before"), stale, CancellationToken.None)).Outcome,
            Is.EqualTo(WalAppendFenceOutcome.Admitted));

        await grain.RetireRoutingGenerationsAsync(shardCount: 1, CancellationToken.None);

        // The stale shard root's write: its entry is gone, and the cold path's read-only
        // load (#5833 C1) installs the floor, floor-only, so it is refused, as an explicit re-claim is.
        var unloaded = await grain.AppendStampedAsync(MakeEntry("stale"), stale, CancellationToken.None);
        var represent = await grain.ClaimRoutingGenerationAsync(shardRoot, 2, CancellationToken.None);

        // The re-created incarnation claims above the floor, then the stale one is stale.
        var recreated = await grain.ClaimRoutingGenerationAsync(shardRoot, 3, CancellationToken.None);
        var afterRecreation = await grain.AppendStampedAsync(MakeEntry("stale-again"), stale, CancellationToken.None);
        var current = await grain.AppendStampedAsync(MakeEntry("new"), new WalAppendStamp(FenceLeaf, 1, shardRoot, 3), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(unloaded.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleGeneration), "the straddler is refused, never admitted");
            Assert.That(represent.Accepted, Is.False, "a generation of the purged incarnation cannot be re-claimed");
            Assert.That(represent.Refusal, Is.EqualTo(2), "it is refused at the floor, which the shard root raises above");
            Assert.That(recreated.Accepted, Is.True);
            Assert.That(afterRecreation.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleGeneration));
            Assert.That(current.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted));
        });
    }

    [Test]
    public async Task The_floor_is_durable_before_any_generation_row_is_deleted_and_a_re_drive_finishes()
    {
        var inner = new EnumerableMemoryGrainStorage();
        var faulting = new ClearFaultingGrainStorage(inner);
        var grain = await CreateFencedGrainAsync(faulting);
        await grain.ClaimRoutingGenerationAsync(WalRoutingInitiator.ShardRoot(0), 4, CancellationToken.None);
        await grain.ClaimRoutingGenerationAsync(WalRoutingInitiator.TreeRangeDelete, 2, CancellationToken.None);

        // A crash between the floor write and the deletes: every delete fails.
        faulting.FailClears = int.MaxValue;
        Assert.That(
            async () => await grain.RetireRoutingGenerationsAsync(shardCount: 1, CancellationToken.None),
            Throws.InstanceOf<IOException>());

        var floorAfterCrash = await WalAppendFenceRows.ReadRoutingFloorAsync(inner, TreeId, WalRoutingInitiator.ShardRoot(0));
        var rowsAfterCrash = inner.GrainIds(WalAppendFenceRows.GenerationGrainType).Count;

        // The re-drive, on a fresh activation, as the purge's keepalive would run it.
        faulting.FailClears = 0;
        var restarted = await CreateFencedGrainAsync(faulting);
        var refusedBeforeRedrive = await restarted.ClaimRoutingGenerationAsync(WalRoutingInitiator.ShardRoot(0), 4, CancellationToken.None);
        await restarted.RetireRoutingGenerationsAsync(shardCount: 1, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(floorAfterCrash, Is.EqualTo(4), "the floor covered every row before the first delete was attempted");
            Assert.That(rowsAfterCrash, Is.EqualTo(2), "the crash left the rows");
            Assert.That(refusedBeforeRedrive.Accepted, Is.False, "a left-over row's generation is refused at the floor even before the re-drive");
            Assert.That(inner.GrainIds(WalAppendFenceRows.GenerationGrainType), Is.Empty, "the re-drive deletes what the crash left");
        });
    }

    [Test]
    public async Task A_generation_initialised_without_reading_the_floor_is_refused_at_its_first_claim()
    {
        // Gate timing (#5668): an initiator that started its generation at 1 - with the
        // gate closed, or before it read the floor - meets the floor at its first claim,
        // and no stamped append is admitted under a generation that was never claimed.
        var storage = new EnumerableMemoryGrainStorage();
        await WalAppendFenceRows.RaiseRoutingFloorsAsync(storage, TreeId, new Dictionary<WalRoutingInitiator, long>
        {
            [WalRoutingInitiator.ShardRoot(0)] = 6,
            [WalRoutingInitiator.TreeRangeDelete] = 6,
        });
        var grain = await CreateFencedGrainAsync(storage);
        var shardRoot = WalRoutingInitiator.ShardRoot(0);
        await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);

        var unclaimed = await grain.AppendStampedAsync(MakeEntry("a"), new WalAppendStamp(FenceLeaf, 1, shardRoot, 1), CancellationToken.None);
        var first = await grain.ClaimRoutingGenerationAsync(shardRoot, 1, CancellationToken.None);
        var atFloor = await grain.ClaimRoutingGenerationAsync(WalRoutingInitiator.TreeRangeDelete, 6, CancellationToken.None);
        var raised = await grain.ClaimRoutingGenerationAsync(shardRoot, first.Refusal + 1, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(unclaimed.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleGeneration),
                "nothing is admitted under an unclaimed generation: the cold path's load (#5833 C1) installs the floor, floor-only");
            Assert.That(first.Accepted, Is.False);
            Assert.That(first.Refusal, Is.EqualTo(6), "a fresh activation reads the floor before its first generation claim");
            Assert.That(atFloor.Accepted, Is.False, "a claim at the floor is refused too");
            Assert.That(raised.Accepted, Is.True, "the generation above the floor is claimed");
        });
    }

    [Test]
    public async Task Retiring_routing_generations_leaves_append_epochs_untouched()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);
        await grain.ClaimAppendEpochAsync(FenceLeaf, 3, CancellationToken.None);
        await grain.ClaimRoutingGenerationAsync(WalRoutingInitiator.ShardRoot(0), 1, CancellationToken.None);

        await grain.RetireRoutingGenerationsAsync(shardCount: 1, CancellationToken.None);
        var epochEntry = await DurableEntryAsync(storage, FenceLeaf);

        Assert.Multiple(() =>
        {
            Assert.That(epochEntry, Is.EqualTo(3), "a leaf's epoch entry is retired by its own clear, not by this");
            Assert.That(grain.EpochFloorForTesting, Is.Zero, "the retired-epoch floor is a separate floor");
        });
    }
}
