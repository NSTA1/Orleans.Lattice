using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the per-partition routing-generation table (#5987): one row per WAL
/// partition with a slot per initiator. Every write is a conditional (ETag)
/// read-modify-write that max-merges its own slot and leaves every other slot as read,
/// so concurrent raises of two initiators' slots both survive; one cold-load read
/// serves every initiator's slot; a purge's floor covers every slot of the row it
/// retires; and a write that bypasses the partition's grain (the retirement storage
/// fallback) cannot make the grain's cached slots admit what it was meant to refuse.
/// </summary>
public partial class WalShardGrainTests
{
    [Test]
    public async Task Concurrent_raises_of_two_initiators_slots_in_one_partition_s_row_both_survive()
    {
        // 676f389a's detector (1): two activations of the partition (a duplicate, or the
        // storage fallback's writer) raise different slots of the one row. The first
        // read is held until the second raise has landed, so the first write's ETag is
        // stale: it must re-read and max-merge, never overwrite the other slot.
        var inner = new EnumerableMemoryGrainStorage();
        var held = new HeldFirstReadStorage(inner, WalAppendFenceRows.GenerationGrainType);
        var first = await CreateFencedGrainAsync(held);
        var second = await CreateFencedGrainAsync(inner);

        var firstClaim = first.ClaimRoutingGenerationAsync(WalRoutingInitiator.ShardRoot(0), 3, CancellationToken.None);
        await held.FirstReadDone.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var secondClaim = await second.ClaimRoutingGenerationAsync(WalRoutingInitiator.ShardRoot(1), 5, CancellationToken.None);
        held.Release.TrySetResult();
        var firstResult = await firstClaim;
        var (exists, slots) = await RoutingGenerationSlots.ReadAllAsync(inner, TreeId, ShardIndex);

        Assert.Multiple(() =>
        {
            Assert.That(firstResult.Accepted, Is.True);
            Assert.That(secondClaim.Accepted, Is.True);
            Assert.That(exists, Is.True);
            Assert.That(slots, Is.EquivalentTo(new Dictionary<WalRoutingInitiator, long>
            {
                [WalRoutingInitiator.ShardRoot(0)] = 3,
                [WalRoutingInitiator.ShardRoot(1)] = 5,
            }), "a raise overwrote the row, losing the other initiator's slot");
        });
    }

    [Test]
    public async Task One_cold_load_read_serves_every_initiator_s_slot_and_a_later_claim_max_merges_its_own()
    {
        // 676f389a's detector (3): the cold load reads the partition's table once and
        // installs every initiator's slot from it; a later claim raises its own slot by a
        // conditional read-modify-write and leaves every other slot as it was.
        var inner = new EnumerableMemoryGrainStorage();
        await RoutingGenerationSlots.WriteAsync(inner, TreeId, ShardIndex, WalRoutingInitiator.ShardRoot(0), 2);
        await RoutingGenerationSlots.WriteAsync(inner, TreeId, ShardIndex, WalRoutingInitiator.ShardRoot(1), 4);
        await RoutingGenerationSlots.WriteAsync(inner, TreeId, ShardIndex, WalRoutingInitiator.TreeRangeDelete, 3);
        var storage = new ReadCountingStorage(inner);
        var grain = await CreateFencedGrainAsync(storage);
        await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);
        storage.Reset();

        var s0 = await grain.AppendStampedAsync(MakeEntry("a"), new WalAppendStamp(FenceLeaf, 1, WalRoutingInitiator.ShardRoot(0), 2), CancellationToken.None);
        var s1 = await grain.AppendStampedAsync(MakeEntry("b"), new WalAppendStamp(FenceLeaf, 1, WalRoutingInitiator.ShardRoot(1), 4), CancellationToken.None);
        var t0 = await grain.AppendStampedAsync(MakeEntry("c"), new WalAppendStamp(FenceLeaf, 1, WalRoutingInitiator.TreeRangeDelete, 3), CancellationToken.None);
        var stale = await grain.AppendStampedAsync(MakeEntry("d"), new WalAppendStamp(FenceLeaf, 1, WalRoutingInitiator.ShardRoot(1), 3), CancellationToken.None);
        var tableReads = storage.Reads(WalAppendFenceRows.GenerationGrainType);

        var raise = await grain.ClaimRoutingGenerationAsync(WalRoutingInitiator.ShardRoot(1), 6, CancellationToken.None);
        var (_, slots) = await RoutingGenerationSlots.ReadAllAsync(inner, TreeId, ShardIndex);
        var fencedByRaise = await grain.AppendStampedAsync(MakeEntry("e"), new WalAppendStamp(FenceLeaf, 1, WalRoutingInitiator.ShardRoot(1), 4), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(new[] { s0.Outcome, s1.Outcome, t0.Outcome }, Is.All.EqualTo(WalAppendFenceOutcome.Admitted),
                "each initiator's durable generation is admitted after the cold load");
            Assert.That(stale.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleGeneration), "a generation below the slot is refused");
            Assert.That(tableReads, Is.EqualTo(1), "the cold loads of three initiators read the partition's table other than once");
            Assert.That(raise.Accepted, Is.True);
            Assert.That(slots, Is.EquivalentTo(new Dictionary<WalRoutingInitiator, long>
            {
                [WalRoutingInitiator.ShardRoot(0)] = 2,
                [WalRoutingInitiator.ShardRoot(1)] = 6,
                [WalRoutingInitiator.TreeRangeDelete] = 3,
            }), "the claim's write did not max-merge its own slot and leave the others");
            Assert.That(fencedByRaise.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleGeneration));
        });
    }

    [Test]
    public async Task A_purge_s_floor_covers_every_slot_of_the_row_it_retires_loaded_here_or_not()
    {
        // 676f389a's detector (2): the retirement raises each initiator's floor over its
        // slot in the row read for the delete, not only over the entries this activation
        // loaded. Shard root 7's slot was written by another activation and never loaded
        // here, and is outside the shard count passed.
        var storage = new EnumerableMemoryGrainStorage();
        var other = await CreateFencedGrainAsync(storage);
        await other.ClaimRoutingGenerationAsync(WalRoutingInitiator.ShardRoot(7), 9, CancellationToken.None);
        var grain = await CreateFencedGrainAsync(storage);
        await grain.ClaimRoutingGenerationAsync(WalRoutingInitiator.ShardRoot(0), 3, CancellationToken.None);

        var highest = await grain.RetireRoutingGenerationsAsync(shardCount: 1, CancellationToken.None);
        var floors = (await WalAppendFenceRows.ReadRoutingFloorAsync(storage, TreeId)).State;

        Assert.Multiple(() =>
        {
            Assert.That(highest, Is.EqualTo(9), "the highest floor is the row's highest slot");
            Assert.That(WalAppendFenceRows.FloorOf(floors, WalRoutingInitiator.ShardRoot(0)), Is.EqualTo(3));
            Assert.That(WalAppendFenceRows.FloorOf(floors, WalRoutingInitiator.ShardRoot(7)), Is.EqualTo(9),
                "a slot the retiring activation never loaded was deleted under a floor that does not cover it");
            Assert.That(storage.GrainIds(WalAppendFenceRows.GenerationGrainType), Is.Empty);
            Assert.That(grain.GenerationFloorForTesting(WalRoutingInitiator.ShardRoot(7)), Is.EqualTo(9),
                "the activation's own floor covers the slot too");
        });
    }

    [Test]
    public async Task A_fallback_retirement_while_the_partition_holds_its_table_cached_never_loosens_its_next_decisions()
    {
        // #5987, D2: the retirement storage fallback is the one writer that bypasses the
        // partition's grain. It raises floors and deletes the row; it never raises a slot.
        // So a slot the grain cached from its one read can only be stale-high: the next
        // admission of an initiator loaded from the cache refuses below its retired slot,
        // and the next claim re-reads the row under its ETag condition rather than writing
        // from the cache, so it neither fails on the deleted row nor resurrects its slots.
        var storage = new EnumerableMemoryGrainStorage();
        await RoutingGenerationSlots.WriteAsync(storage, TreeId, ShardIndex, WalRoutingInitiator.ShardRoot(0), 2);
        await RoutingGenerationSlots.WriteAsync(storage, TreeId, ShardIndex, WalRoutingInitiator.ShardRoot(1), 4);
        var grain = await CreateFencedGrainAsync(storage);
        await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);
        var cached = await grain.AppendStampedAsync(
            MakeEntry("a"), new WalAppendStamp(FenceLeaf, 1, WalRoutingInitiator.ShardRoot(0), 2), CancellationToken.None);
        Assert.That(cached.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted), "precondition: the cold load cached the table");

        await TreeRoutingFenceRetirement.RetirePartitionAsync(
            () => throw new TypeLoadException(WalAppendFenceMethodProbe.UnresolvedAliasPrefix + " 'ol.retire'"),
            storage,
            TreeId,
            ShardIndex,
            shardCount: 2);
        Assert.That(storage.GrainIds(WalAppendFenceRows.GenerationGrainType), Is.Empty, "precondition: the fallback deleted the row");

        var belowRetired = await grain.AppendStampedAsync(
            MakeEntry("b"), new WalAppendStamp(FenceLeaf, 1, WalRoutingInitiator.ShardRoot(1), 3), CancellationToken.None);
        var claim = await grain.ClaimRoutingGenerationAsync(WalRoutingInitiator.ShardRoot(1), 5, CancellationToken.None);
        var (exists, slots) = await RoutingGenerationSlots.ReadAllAsync(storage, TreeId, ShardIndex);

        Assert.Multiple(() =>
        {
            Assert.That(belowRetired.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleGeneration),
                "a stamp below the slot the fallback retired was admitted from the cache");
            Assert.That(claim.Accepted, Is.True, "the claim after the delete must re-read the row, not fail on the cached ETag");
            Assert.That(exists, Is.True);
            Assert.That(slots, Is.EquivalentTo(new Dictionary<WalRoutingInitiator, long> { [WalRoutingInitiator.ShardRoot(1)] = 5 }),
                "the claim wrote from the cached table, resurrecting slots the fallback retired");
        });
    }

    [Test]
    public async Task A_duplicate_activation_s_raise_after_the_table_was_cached_reaches_the_next_cold_load_through_a_claim_s_read()
    {
        // #5988 review, B1: a duplicate activation B raises shard root 1's slot after A
        // cached the table. A's next claim (of another initiator) reads the row fresh and
        // merges it into the cache, so A's later cold load of shard root 1 loads B's raise.
        var storage = new EnumerableMemoryGrainStorage();
        var (a, clock) = await CachedTableActivationAsync(storage);
        var b = await CreateFencedGrainAsync(storage);
        Assert.That((await b.ClaimRoutingGenerationAsync(WalRoutingInitiator.ShardRoot(1), 6, CancellationToken.None)).Accepted, Is.True,
            "precondition: the duplicate raised the slot after A cached the table");

        var claim = await a.ClaimRoutingGenerationAsync(WalRoutingInitiator.ShardRoot(0), 3, CancellationToken.None);
        var stale = await a.AppendStampedAsync(
            MakeEntry("b"), new WalAppendStamp(FenceLeaf, 1, WalRoutingInitiator.ShardRoot(1), 4), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(claim.Accepted, Is.True);
            Assert.That(clock.Advanced, Is.EqualTo(TimeSpan.Zero), "the cache's age bound did not lapse");
            Assert.That(stale.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleGeneration),
                "the cold load used the table cached before the duplicate's raise, though a claim read the row since");
        });
    }

    [Test]
    public async Task A_duplicate_activation_s_raise_after_the_table_was_cached_reaches_a_cold_load_once_the_cache_ages_out()
    {
        // #5988 review, B1: with no claim in between, the cache is re-read once older than
        // its bound, so a slot a duplicate raised is stale here for a bounded time only.
        var storage = new EnumerableMemoryGrainStorage();
        var (a, clock) = await CachedTableActivationAsync(storage);
        var b = await CreateFencedGrainAsync(storage);
        Assert.That((await b.ClaimRoutingGenerationAsync(WalRoutingInitiator.ShardRoot(1), 6, CancellationToken.None)).Accepted, Is.True,
            "precondition: the duplicate raised the slot after A cached the table");

        clock.Advance(WalShardGrain.GenerationRowCacheMaxAge + TimeSpan.FromTicks(1));
        var stale = await a.AppendStampedAsync(
            MakeEntry("b"), new WalAppendStamp(FenceLeaf, 1, WalRoutingInitiator.ShardRoot(1), 4), CancellationToken.None);

        Assert.That(stale.Outcome, Is.EqualTo(WalAppendFenceOutcome.StaleGeneration),
            "a cold load past the cache's age bound used the table cached before the duplicate's raise");
    }

    /// <summary>
    /// An activation that has cached the partition's table, holding shard roots 0 (2) and
    /// 1 (4), from shard root 0's cold load, on a clock the test controls.
    /// </summary>
    private static async Task<(WalShardGrain Grain, ManualClock Clock)> CachedTableActivationAsync(EnumerableMemoryGrainStorage storage)
    {
        await RoutingGenerationSlots.WriteAsync(storage, TreeId, ShardIndex, WalRoutingInitiator.ShardRoot(0), 2);
        await RoutingGenerationSlots.WriteAsync(storage, TreeId, ShardIndex, WalRoutingInitiator.ShardRoot(1), 4);
        var grain = await CreateFencedGrainAsync(storage);
        var clock = new ManualClock();
        grain.GenerationRowCacheClock = clock;
        await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);
        var cached = await grain.AppendStampedAsync(
            MakeEntry("a"), new WalAppendStamp(FenceLeaf, 1, WalRoutingInitiator.ShardRoot(0), 2), CancellationToken.None);
        Assert.That(cached.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted), "precondition: the cold load cached the table");
        return (grain, clock);
    }

    /// <summary>A clock that moves only when the test advances it.</summary>
    private sealed class ManualClock : TimeProvider
    {
        private readonly DateTimeOffset _start = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        public TimeSpan Advanced { get; private set; }

        public void Advance(TimeSpan by) => Advanced += by;

        public override DateTimeOffset GetUtcNow() => _start + Advanced;
    }
}
