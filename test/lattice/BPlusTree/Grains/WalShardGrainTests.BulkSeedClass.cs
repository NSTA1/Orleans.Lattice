using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the bulk-seed stamp class on the WAL shard (#5812, member B; review
/// C2): a bulk seed names no leaf and is admitted only on a positive match against the
/// shard root's routing generation as the partition has loaded it. It fails closed on
/// an unknown generation - never admitted on an absent entry - re-presents to load a
/// current one, is stale once a close has re-claimed above it, and never raises the
/// unstamped watermark.
/// </summary>
public partial class WalShardGrainTests
{
    private static readonly WalRoutingInitiator SeedShard = WalRoutingInitiator.ShardRoot(0);

    [Test]
    public async Task A_bulk_seed_fails_closed_on_a_generation_the_partition_has_not_loaded()
    {
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());

        var single = await grain.AppendStampedAsync(MakeEntry("seed"), WalAppendStamp.BulkSeed(SeedShard, 1), CancellationToken.None);
        var batch = await grain.AppendBatchStampedAsync(
            new[] { MakeEntry("seed-a"), MakeEntry("seed-b") }, WalAppendStamp.BulkSeed(SeedShard, 1), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(single.Outcome, Is.EqualTo(WalAppendFenceOutcome.UnloadedGeneration),
                "review C2: a seed is never admitted on an absent generation entry");
            Assert.That(batch.Outcome, Is.EqualTo(WalAppendFenceOutcome.UnloadedGeneration));
            Assert.That(batch.AdmittedCount, Is.Zero, "nothing is allocated");
        });
    }

    [Test]
    public async Task A_bulk_seed_is_admitted_only_at_the_loaded_generation_and_is_not_reported()
    {
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        await grain.ClaimRoutingGenerationAsync(SeedShard, 2, CancellationToken.None);

        var below = await grain.AppendStampedAsync(MakeEntry("old"), WalAppendStamp.BulkSeed(SeedShard, 1), CancellationToken.None);
        var above = await grain.AppendStampedAsync(MakeEntry("new"), WalAppendStamp.BulkSeed(SeedShard, 3), CancellationToken.None);
        var match = await grain.AppendBatchStampedAsync(
            new[] { MakeEntry("seed-a"), MakeEntry("seed-b") }, WalAppendStamp.BulkSeed(SeedShard, 2), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(below.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleGeneration),
                "a seed below the loaded generation was admitted before a close that has since re-claimed");
            Assert.That(above.Outcome, Is.EqualTo(WalAppendFenceOutcome.UnloadedGeneration),
                "a seed above the loaded generation is not a positive match; it re-presents to load it");
            Assert.That(match.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted));
            Assert.That(match.AdmittedCount, Is.EqualTo(2), "a multi-entry seed batch registers against its generation alone");
            Assert.That(grain.UnstampedWatermarkForTesting, Is.EqualTo(-1), "a bulk seed never raises the watermark");
        });
    }

    [Test]
    public async Task A_close_re_claim_waits_for_an_admitted_seed_and_then_refuses_later_seeds()
    {
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        await grain.ClaimRoutingGenerationAsync(SeedShard, 1, CancellationToken.None);
        var seeded = await grain.AppendBatchStampedAsync(
            new[] { MakeEntry("seed-a"), MakeEntry("seed-b") }, WalAppendStamp.BulkSeed(SeedShard, 1), CancellationToken.None);

        var close = await grain.ClaimRoutingGenerationAsync(SeedShard, 2, CancellationToken.None);
        var late = await grain.AppendStampedAsync(MakeEntry("late"), WalAppendStamp.BulkSeed(SeedShard, 1), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(close.Accepted, Is.True);
            Assert.That(seeded.Offsets, Is.All.LessThan(close.Head), "every admitted seed lies below the close head");
            Assert.That(late.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleGeneration),
                "review C1: a seed after the close's re-claim is refused");
        });
    }
}
