using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the WAL shard's side of the append fence (epic #5605): stale
/// epochs rejected, the retired floor checked at claim write, raise and append,
/// unloaded entries rejected until re-presented, the durable table surviving a
/// reactivation or a move to another provider, and unstamped appends always
/// admitted and reported.
/// </summary>
public partial class WalShardGrainTests
{
    private static readonly Guid FenceLeaf = new("0f0f0f0f-0000-0000-0000-000000000001");
    private static readonly Guid OtherFenceLeaf = new("0f0f0f0f-0000-0000-0000-000000000002");

    private static async Task<WalShardGrain> CreateFencedGrainAsync(IGrainStorage storage, IWalStorageProvider? provider = null)
    {
        var grain = await CreateGrainAsync(provider);
        grain.UseFenceStorageForTesting(storage);
        return grain;
    }

    private static async Task<long> DurableEntryAsync(IGrainStorage storage, Guid leaf) =>
        (await WalAppendFenceRows.ReadEntryAsync(storage, TreeId, ShardIndex, leaf)).State.Epoch;

    [Test]
    public async Task A_stale_epoch_append_is_rejected_and_never_allocated()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);

        var first = await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);
        var second = await grain.ClaimAppendEpochAsync(FenceLeaf, 2, CancellationToken.None);
        var before = await grain.GetNextSequenceAsync(CancellationToken.None);

        var stale = await grain.AppendBatchStampedAsync(
            new[] { MakeEntry("a"), MakeEntry("b") }, WalAppendStamp.ForLeaf(FenceLeaf, 1), CancellationToken.None);
        var staleSingle = await grain.AppendStampedAsync(MakeEntry("c"), WalAppendStamp.ForLeaf(FenceLeaf, 1), CancellationToken.None);
        var afterStale = await grain.GetNextSequenceAsync(CancellationToken.None);
        var live = await grain.AppendBatchStampedAsync(
            new[] { MakeEntry("d") }, WalAppendStamp.ForLeaf(FenceLeaf, 2), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first.Accepted && second.Accepted, Is.True);
            Assert.That(stale.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleEpoch));
            Assert.That(stale.AdmittedCount, Is.Zero);
            Assert.That(stale.Offsets, Is.All.EqualTo(WalStampedBatchResult.Rejected));
            Assert.That(staleSingle.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleEpoch));
            Assert.That(staleSingle.Offset, Is.EqualTo(WalStampedBatchResult.Rejected));
            Assert.That(afterStale, Is.EqualTo(before), "a rejected stamped append must not allocate an offset");
            Assert.That(live.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted));
            Assert.That(live.Offsets, Is.EqualTo(new[] { before }));
        });
    }

    [Test]
    public async Task The_claim_head_is_the_allocator_next_offset()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);

        var claim = await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);

        Assert.That(claim.Head, Is.EqualTo(2), "every append allocated before the raise lies below the claim head");
    }

    [Test]
    public async Task The_durable_entry_is_a_monotone_max_merge()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);

        await grain.ClaimAppendEpochAsync(FenceLeaf, 3, CancellationToken.None);
        var lower = await grain.ClaimAppendEpochAsync(FenceLeaf, 2, CancellationToken.None);
        var restarted = await CreateFencedGrainAsync(storage);
        var lowerAfterRestart = await restarted.ClaimAppendEpochAsync(FenceLeaf, 2, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(lower.Accepted, Is.False);
            Assert.That(lower.Refusal, Is.EqualTo(3));
            Assert.That(lowerAfterRestart.Accepted, Is.False,
                "StaleEpochNeverAppendsClaimEntryOverwrite: a restart reloads the higher durable entry");
            Assert.That(lowerAfterRestart.Refusal, Is.EqualTo(3));
        });
        Assert.That(await DurableEntryAsync(storage, FenceLeaf), Is.EqualTo(3));
    }

    [Test]
    public async Task An_unloaded_entry_rejects_until_the_claim_is_re_presented()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);
        var neverClaimed = await grain.AppendStampedAsync(MakeEntry("a"), WalAppendStamp.ForLeaf(FenceLeaf, 1), CancellationToken.None);
        await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);

        // A reactivation (a restart, or a move's cutover) loses only the in-memory entries.
        var restarted = await CreateFencedGrainAsync(storage);
        var unloaded = await restarted.AppendBatchStampedAsync(
            new[] { MakeEntry("b") }, WalAppendStamp.ForLeaf(FenceLeaf, 1), CancellationToken.None);
        var represented = await restarted.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);
        var admitted = await restarted.AppendBatchStampedAsync(
            new[] { MakeEntry("c") }, WalAppendStamp.ForLeaf(FenceLeaf, 1), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(neverClaimed.Outcome, Is.EqualTo(WalAppendFenceOutcome.UnloadedEntry));
            Assert.That(unloaded.Outcome, Is.EqualTo(WalAppendFenceOutcome.UnloadedEntry),
                "StaleEpochNeverAppendsUnloadedEntryAccepted: nothing is admitted from an unloaded entry");
            Assert.That(unloaded.AdmittedCount, Is.Zero);
            Assert.That(represented.Accepted, Is.True, "the same epoch, already durable, reloads the entry");
            Assert.That(admitted.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted));
        });
    }

    [Test]
    public async Task Retirement_raises_the_floor_before_deleting_the_entry()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);
        await grain.ClaimAppendEpochAsync(FenceLeaf, 2, CancellationToken.None);

        var floor = await grain.RetireAppendEpochAsync(FenceLeaf, CancellationToken.None);
        var survivor = await grain.AppendStampedAsync(MakeEntry("a"), WalAppendStamp.ForLeaf(FenceLeaf, 2), CancellationToken.None);
        var recreatedAtOne = await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);
        var recreatedAtTwo = await grain.ClaimAppendEpochAsync(FenceLeaf, 2, CancellationToken.None);
        var recreatedAboveFloor = await grain.ClaimAppendEpochAsync(FenceLeaf, 3, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(floor, Is.EqualTo(2));
            Assert.That(survivor.Outcome, Is.Not.EqualTo(WalAppendFenceOutcome.Admitted),
                "a surviving duplicate of the retired leaf must not append");
            Assert.That(recreatedAtOne.Accepted, Is.False, "ReadPositionHonestRecreateDropsFloor: a re-created leaf claims above the floor");
            Assert.That(recreatedAtOne.Refusal, Is.EqualTo(2));
            Assert.That(recreatedAtTwo.Accepted, Is.False, "the floor is checked at the claim write");
            Assert.That(recreatedAboveFloor.Accepted, Is.True);
        });

        var restarted = await CreateFencedGrainAsync(storage);
        var afterRestart = await restarted.ClaimAppendEpochAsync(OtherFenceLeaf, 2, CancellationToken.None);
        Assert.That(afterRestart.Accepted, Is.False, "the floor row is durable: a reactivation reloads it");
    }

    [Test]
    public async Task The_retired_floor_is_checked_at_every_append()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);
        await grain.ClaimAppendEpochAsync(FenceLeaf, 3, CancellationToken.None);
        await grain.ClaimAppendEpochAsync(OtherFenceLeaf, 5, CancellationToken.None);

        // Retiring the other leaf raises the partition's floor above FenceLeaf's epoch.
        await grain.RetireAppendEpochAsync(OtherFenceLeaf, CancellationToken.None);
        var append = await grain.AppendStampedAsync(MakeEntry("a"), WalAppendStamp.ForLeaf(FenceLeaf, 3), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(grain.EpochFloorForTesting, Is.EqualTo(5));
            Assert.That(append.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleEpoch),
                "StaleEpochNeverAppendsAppendIgnoresRetiredFloor: an epoch at or below the floor is rejected at the append");
        });
    }

    [Test]
    public async Task A_move_to_another_provider_carries_the_fence_table()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var source = await CreateFencedGrainAsync(storage, new InMemoryWalStorageProvider());
        await source.ClaimAppendEpochAsync(FenceLeaf, 2, CancellationToken.None);
        await source.ClaimAppendEpochAsync(OtherFenceLeaf, 4, CancellationToken.None);
        await source.RetireAppendEpochAsync(OtherFenceLeaf, CancellationToken.None);

        // The move's cutover reactivates the partition against a different provider;
        // the fence rows live in grain storage, keyed by tree and partition.
        var target = await CreateFencedGrainAsync(storage, new InMemoryWalStorageProvider());
        var staleDuplicate = await target.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);
        var belowFloor = await target.ClaimAppendEpochAsync(OtherFenceLeaf, 4, CancellationToken.None);
        var represented = await target.ClaimAppendEpochAsync(FenceLeaf, 2, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(staleDuplicate.Accepted, Is.False, "the epoch entry survived the move");
            Assert.That(belowFloor.Accepted, Is.False, "the retired floor survived the move");
            Assert.That(represented.Accepted, Is.False,
                "the live activation's epoch is at or below the floor here, so even it must claim above");
        });

        var live = await target.ClaimAppendEpochAsync(FenceLeaf, 5, CancellationToken.None);
        var append = await target.AppendStampedAsync(MakeEntry("a"), WalAppendStamp.ForLeaf(FenceLeaf, 5), CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(live.Accepted, Is.True);
            Assert.That(append.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted));
        });
    }

    [Test]
    public async Task Unstamped_appends_are_always_admitted_and_reported()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);
        await grain.ClaimAppendEpochAsync(FenceLeaf, 7, CancellationToken.None);

        // An older build, or any silo whose view of the gate is closed, appends unstamped.
        var unstamped = await grain.AppendAsync(MakeEntry("old"), CancellationToken.None);
        var unstampedBatch = await grain.AppendBatchAsync(new[] { MakeEntry("old-1"), MakeEntry("old-2") }, CancellationToken.None);
        var stamped = await grain.AppendStampedAsync(MakeEntry("new"), WalAppendStamp.ForLeaf(FenceLeaf, 7), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(unstamped, Is.GreaterThanOrEqualTo(0), "OldNeverRejected: an unstamped append is never rejected");
            Assert.That(unstampedBatch, Has.Count.EqualTo(2));
            Assert.That(stamped.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted));
            Assert.That(stamped.UnstampedWatermark, Is.EqualTo(unstampedBatch[1]),
                "ReadPositionHonestUnstampedNotReported: every stamped response reports the highest unstamped offset");
            Assert.That(grain.UnstampedWatermarkForTesting, Is.EqualTo(unstampedBatch[1]));
        });
    }

    [Test]
    public async Task The_unstamped_watermark_restarts_at_the_recovered_head()
    {
        var provider = new InMemoryWalStorageProvider();
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage, provider);
        await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);
        await grain.AppendStampedAsync(MakeEntry("a"), WalAppendStamp.ForLeaf(FenceLeaf, 1), CancellationToken.None);
        await grain.AppendStampedAsync(MakeEntry("b"), WalAppendStamp.ForLeaf(FenceLeaf, 1), CancellationToken.None);

        var restarted = await CreateFencedGrainAsync(storage, provider);

        Assert.That(restarted.UnstampedWatermarkForTesting, Is.EqualTo(1),
            "ReadPositionHonestRestartForgetsUnstamped: everything before a restart is treated as possibly unstamped");
    }

    [Test]
    public async Task Routing_generations_are_fenced_and_durable()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);
        var shard1 = WalRoutingInitiator.ShardRoot(1);
        await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);
        await grain.ClaimRoutingGenerationAsync(shard1, 1, CancellationToken.None);
        var reclaim = await grain.ClaimRoutingGenerationAsync(shard1, 2, CancellationToken.None);

        var stale = await grain.AppendStampedAsync(
            MakeEntry("a"), new WalAppendStamp(FenceLeaf, 1, shard1, 1), CancellationToken.None);
        var current = await grain.AppendStampedAsync(
            MakeEntry("b"), new WalAppendStamp(FenceLeaf, 1, shard1, 2), CancellationToken.None);
        var unloaded = await grain.AppendStampedAsync(
            MakeEntry("c"), new WalAppendStamp(FenceLeaf, 1, WalRoutingInitiator.ShardRoot(2), 1), CancellationToken.None);
        var restarted = await CreateFencedGrainAsync(storage);
        var staleAfterRestart = await restarted.ClaimRoutingGenerationAsync(shard1, 1, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(reclaim.Accepted, Is.True);
            Assert.That(stale.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleGeneration));
            Assert.That(current.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted));
            Assert.That(unloaded.Outcome, Is.EqualTo(WalAppendFenceOutcome.UnloadedGeneration));
            Assert.That(staleAfterRestart.Accepted, Is.False,
                "ReadPositionHonestGenerationNotDurable: a restart reloads the higher durable generation");
        });
    }

    [Test]
    public void A_routing_generation_claim_names_an_initiator()
    {
        Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
            await grain.ClaimRoutingGenerationAsync(WalRoutingInitiator.None, 1, CancellationToken.None);
        });
    }

    [Test]
    public async Task Retiring_a_leaf_with_no_entry_is_a_no_op()
    {
        var storage = new EnumerableMemoryGrainStorage();
        var grain = await CreateFencedGrainAsync(storage);

        var floor = await grain.RetireAppendEpochAsync(FenceLeaf, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(floor, Is.Zero);
            Assert.That(storage.GrainIds(WalAppendFenceRows.FloorGrainType), Is.Empty);
        });
    }
}
