using System.Text;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the leaf-walk seams that return a leaf's payload together with
/// its next sibling, so a chain walk costs one grain call per leaf instead of two.
/// Each must answer exactly what the separate payload call plus
/// <see cref="IBPlusLeafGrain.GetNextSiblingAsync"/> answered.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static readonly GrainId CombinedPageSibling = GrainId.Create("leaf", "combined-page-next");

    [Test]
    public async Task GetLiveRawEntriesAndNextSiblingAsync_matches_the_separate_calls()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", [1]);
        await grain.SetAsync("b", [2]);
        await grain.DeleteAsync("b");
        await grain.SetNextSiblingAsync(CombinedPageSibling);

        var page = await grain.GetLiveRawEntriesAndNextSiblingAsync();
        var separate = await grain.GetLiveRawEntriesAsync();

        Assert.Multiple(async () =>
        {
            Assert.That(page.Entries.Select(e => e.Key), Is.EqualTo(separate.Select(e => e.Key)));
            Assert.That(page.Entries.Select(e => e.Key), Is.EqualTo(new[] { "a" }));
            Assert.That(page.NextSibling, Is.EqualTo(await grain.GetNextSiblingAsync()));
            Assert.That(page.NextSibling, Is.EqualTo(CombinedPageSibling));
        });
    }

    [Test]
    public async Task GetLiveRawEntriesAndNextSiblingAsync_reports_null_at_the_chain_tail()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", [1]);

        var page = await grain.GetLiveRawEntriesAndNextSiblingAsync();

        Assert.That(page.NextSibling, Is.Null);
    }

    [Test]
    public async Task GetPendingMutationsForSlotsAndNextSiblingAsync_matches_the_separate_calls()
    {
        const int virtualShardCount = 4096;
        var grain = CreateGrain();
        await grain.SetNextSiblingAsync(CombinedPageSibling);
        var txid = Guid.NewGuid();
        LatticeTransactionContext.Set(txid);
        using (LatticePreparedContext.BeginScope())
        {
            await grain.SetAsync("alpha", [1]);
        }
        LatticeTransactionContext.Set(Guid.Empty);
        var slots = new[] { ShardMap.GetVirtualSlot("alpha", virtualShardCount) };

        var page = await grain.GetPendingMutationsForSlotsAndNextSiblingAsync(slots, virtualShardCount);
        var separate = await grain.GetPendingMutationsForSlotsAsync(slots, virtualShardCount);

        Assert.Multiple(() =>
        {
            Assert.That(page.Mutations.Select(m => (m.TransactionId, m.Key)),
                Is.EqualTo(separate.Select(m => (m.TransactionId, m.Key))));
            Assert.That(page.Mutations.Select(m => m.Key), Does.Contain("alpha"));
            Assert.That(page.NextSibling, Is.EqualTo(CombinedPageSibling));
        });
    }

    [Test]
    public void GetPendingMutationsForSlotsAndNextSiblingAsync_validates_like_the_separate_call()
    {
        var grain = CreateGrain();

        Assert.ThrowsAsync<ArgumentNullException>(
            () => grain.GetPendingMutationsForSlotsAndNextSiblingAsync(null!, 4096));
    }

    [Test]
    public async Task GetKeyRangeAndNextSiblingAsync_matches_the_separate_calls()
    {
        var grain = CreateGrain();
        await grain.SetNextSiblingAsync(CombinedPageSibling);

        var link = await grain.GetKeyRangeAndNextSiblingAsync();

        Assert.Multiple(async () =>
        {
            Assert.That(link.KeyRange, Is.EqualTo((await grain.GetKeyRangeAndNextSiblingAsync()).KeyRange));
            Assert.That(link.NextSibling, Is.EqualTo(await grain.GetNextSiblingAsync()));
            Assert.That(link.NextSibling, Is.EqualTo(CombinedPageSibling));
        });
    }

    [Test]
    public async Task GetKeyRangeAndNextSiblingAsync_reports_null_at_the_chain_tail()
    {
        var grain = CreateGrain();

        var link = await grain.GetKeyRangeAndNextSiblingAsync();

        Assert.That(link.NextSibling, Is.Null);
    }

    [Test]
    public async Task GetDeltaSinceAndNextSiblingAsync_matches_the_separate_calls()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", [1]);
        await grain.SetAsync("b", [2]);
        await grain.DeleteAsync("b");
        await grain.SetNextSiblingAsync(CombinedPageSibling);

        var page = await grain.GetDeltaSinceAndNextSiblingAsync(new VersionVector());
        var separate = await grain.GetDeltaSinceAsync(new VersionVector());

        Assert.Multiple(() =>
        {
            Assert.That(page.Delta.Entries.Keys, Is.EquivalentTo(separate.Entries.Keys));
            Assert.That(page.Delta.Entries.Keys, Is.EquivalentTo(new[] { "a", "b" }));
            Assert.That(page.Delta.Entries["b"].IsTombstone, Is.True);
            Assert.That(page.NextSibling, Is.EqualTo(CombinedPageSibling));
        });
    }

    [Test]
    public async Task GetDeltaSinceAndNextSiblingAsync_reports_null_at_the_chain_tail()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", [1]);

        var page = await grain.GetDeltaSinceAndNextSiblingAsync(new VersionVector());

        Assert.Multiple(() =>
        {
            Assert.That(page.Delta.Entries.Keys, Is.EquivalentTo(new[] { "a" }));
            Assert.That(page.NextSibling, Is.Null);
        });
    }

    [Test]
    public async Task GetKeyRangeAndPrevSiblingAsync_matches_the_separate_calls()
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.LowKeyInclusive = "f";
        state.State.HighKeyExclusive = "m";
        var grain = CreateGrain(state);
        await grain.SetPrevSiblingAsync(CombinedPageSibling);

        var link = await grain.GetKeyRangeAndPrevSiblingAsync();

        Assert.Multiple(async () =>
        {
            Assert.That(link.KeyRange, Is.EqualTo((await grain.GetKeyRangeAndNextSiblingAsync()).KeyRange));
            Assert.That(link.KeyRange.LowKeyInclusive, Is.EqualTo("f"));
            Assert.That(link.KeyRange.HighKeyExclusive, Is.EqualTo("m"));
            Assert.That(link.PrevSibling, Is.EqualTo(await grain.GetPrevSiblingAsync()));
            Assert.That(link.PrevSibling, Is.EqualTo(CombinedPageSibling));
        });
    }

    [Test]
    public async Task GetKeyRangeAndPrevSiblingAsync_reports_null_at_the_chain_head()
    {
        var grain = CreateGrain();

        var link = await grain.GetKeyRangeAndPrevSiblingAsync();

        Assert.That(link.PrevSibling, Is.Null);
    }

    [Test]
    public async Task Payload_pages_carry_the_leaf_key_range_and_expose_it_as_a_chain_link()
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.LowKeyInclusive = "a";
        state.State.HighKeyExclusive = "k";
        var grain = CreateGrain(state);
        await grain.SetAsync("b", [1]);
        await grain.SetNextSiblingAsync(CombinedPageSibling);
        var expected = await grain.GetKeyRangeAndNextSiblingAsync();

        var delta = await grain.GetDeltaSinceAndNextSiblingAsync(new VersionVector());
        var live = await grain.GetLiveRawEntriesAndNextSiblingAsync();

        Assert.Multiple(() =>
        {
            Assert.That(delta.KeyRange, Is.EqualTo(expected.KeyRange));
            Assert.That(delta.Link, Is.EqualTo(expected));
            Assert.That(live.KeyRange, Is.EqualTo(expected.KeyRange));
            Assert.That(live.Link, Is.EqualTo(expected));
            Assert.That(live.Link.KeyRange.HighKeyExclusive, Is.EqualTo("k"));
        });
    }

    [Test]
    public async Task GetDeltaSinceForSlotsAndNextSiblingAsync_matches_the_separate_calls()
    {
        const int virtualShardCount = 4096;
        var state = new FakePersistentState<LeafNodeState>();
        state.State.LowKeyInclusive = "a";
        state.State.HighKeyExclusive = "z";
        var grain = CreateGrain(state);
        await grain.SetAsync("alpha", [1]);
        await grain.SetAsync("beta", [2]);
        await grain.SetNextSiblingAsync(CombinedPageSibling);
        var slots = new[] { ShardMap.GetVirtualSlot("alpha", virtualShardCount) };

        var page = await grain.GetDeltaSinceForSlotsAndNextSiblingAsync(new VersionVector(), slots, virtualShardCount);
        var separate = await grain.GetDeltaSinceForSlotsAsync(new VersionVector(), slots, virtualShardCount);
        var link = await grain.GetKeyRangeAndNextSiblingAsync();

        Assert.Multiple(() =>
        {
            Assert.That(page.Delta.Entries.Keys, Is.EquivalentTo(separate.Entries.Keys));
            Assert.That(page.Delta.Entries.Keys, Does.Contain("alpha"));
            Assert.That(page.Link, Is.EqualTo(link));
            Assert.That(page.NextSibling, Is.EqualTo(CombinedPageSibling));
            Assert.That(page.KeyRange.HighKeyExclusive, Is.EqualTo("z"));
        });
    }

    [Test]
    public async Task GetDeltaSinceForSlotsAndNextSiblingAsync_still_carries_the_link_when_no_slot_moves()
    {
        var grain = CreateGrain();
        await grain.SetAsync("alpha", [1]);
        await grain.SetNextSiblingAsync(CombinedPageSibling);

        var page = await grain.GetDeltaSinceForSlotsAndNextSiblingAsync(new VersionVector(), [], 4096);

        Assert.Multiple(() =>
        {
            Assert.That(page.Delta.Entries, Is.Empty);
            Assert.That(page.NextSibling, Is.EqualTo(CombinedPageSibling));
        });
    }

    [Test]
    public void GetDeltaSinceForSlotsAndNextSiblingAsync_validates_like_the_separate_call()
    {
        var grain = CreateGrain();

        Assert.ThrowsAsync<ArgumentNullException>(
            () => grain.GetDeltaSinceForSlotsAndNextSiblingAsync(new VersionVector(), null!, 4096));
    }

    [Test]
    public async Task CompactTombstonesAndNextSiblingAsync_reaps_like_the_separate_call_and_carries_the_link()
    {
        var state = new FakePersistentState<LeafNodeState>();
        state.State.LowKeyInclusive = "a";
        state.State.HighKeyExclusive = "k";
        var grain = CreateGrain(state);
        grain.EntriesForTest["dead"] = LwwValue<byte[]>.Tombstone(new HybridLogicalClock { WallClockTicks = 10, Counter = 0 });
        state.State.Version.Tick("test");
        await grain.SetNextSiblingAsync(CombinedPageSibling);
        var link = await grain.GetKeyRangeAndNextSiblingAsync();

        var page = await grain.CompactTombstonesAndNextSiblingAsync(TimeSpan.FromHours(1), reapCeiling: null);

        Assert.Multiple(() =>
        {
            Assert.That(page.Result.EntriesRemoved, Is.EqualTo(1));
            Assert.That(grain.EntriesForTest.ContainsKey("dead"), Is.False);
            Assert.That(page.Link, Is.EqualTo(link));
            Assert.That(page.KeyRange.HighKeyExclusive, Is.EqualTo("k"));
        });
    }

    [Test]
    public async Task CompactTombstonesAndNextSiblingAsync_honours_the_reap_ceiling()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var stamp = new HybridLogicalClock { WallClockTicks = 20, Counter = 0 };
        grain.EntriesForTest["kept"] = LwwValue<byte[]>.Tombstone(stamp);
        grain.EntriesForTest["reaped"] = LwwValue<byte[]>.Tombstone(new HybridLogicalClock { WallClockTicks = 10, Counter = 0 });
        state.State.Version.Tick("test");

        var page = await grain.CompactTombstonesAndNextSiblingAsync(TimeSpan.FromHours(1), stamp);

        Assert.Multiple(() =>
        {
            Assert.That(page.Result.EntriesRemoved, Is.EqualTo(1));
            Assert.That(grain.EntriesForTest.ContainsKey("kept"), Is.True, "a tombstone at the ceiling is kept");
            Assert.That(grain.EntriesForTest.ContainsKey("reaped"), Is.False);
            Assert.That(page.NextSibling, Is.Null, "chain tail");
        });
    }

    [Test]
    public async Task DiscardPendingTransactionsAndGetNextSiblingAsync_discards_every_saga_and_persists_once()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        var projection = AsProjection(grain);
        var txA = Guid.NewGuid();
        var txB = Guid.NewGuid();
        using (LatticeApplyOffsetContext.BeginScope(partition: 0, offset: 5))
        {
            projection.Apply(BuildPreparedSet(txA, "a", Encoding.UTF8.GetBytes("va"), treeId: "discard-tree"));
        }
        using (LatticeApplyOffsetContext.BeginScope(partition: 0, offset: 6))
        {
            projection.Apply(BuildPreparedSet(txB, "b", Encoding.UTF8.GetBytes("vb"), treeId: "discard-tree"));
        }
        await grain.SetNextSiblingAsync(CombinedPageSibling);
        Assert.That(await grain.GetPendingKeysAsync(), Is.EquivalentTo(new[] { "a", "b" }),
            "precondition: both replayed prepares staged a pending bucket");
        var writesBefore = state.WriteCount;

        var next = await grain.DiscardPendingTransactionsAndGetNextSiblingAsync([txA, Guid.Empty, txB]);

        Assert.Multiple(async () =>
        {
            Assert.That(next, Is.EqualTo(CombinedPageSibling));
            Assert.That(await grain.GetPendingKeysAsync(), Is.Empty);
            Assert.That(state.State.DiscardedSagaPrepares?.Select(e => e.TransactionId),
                Is.EquivalentTo(new[] { txA, txB }),
                "every discarded saga leaves its durable replay backstop");
            Assert.That(state.WriteCount - writesBefore, Is.EqualTo(1),
                "the markers of every discarded saga are persisted in one write, not one per saga");
        });
    }

    [Test]
    public async Task DiscardPendingTransactionsAndGetNextSiblingAsync_does_not_persist_when_nothing_was_pending()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state);
        await grain.SetNextSiblingAsync(CombinedPageSibling);
        var writesBefore = state.WriteCount;

        var next = await grain.DiscardPendingTransactionsAndGetNextSiblingAsync([Guid.NewGuid(), Guid.NewGuid()]);

        Assert.Multiple(() =>
        {
            Assert.That(next, Is.EqualTo(CombinedPageSibling));
            Assert.That(state.WriteCount, Is.EqualTo(writesBefore));
        });
    }

    [Test]
    public void DiscardPendingTransactionsAndGetNextSiblingAsync_rejects_a_null_id_array()
    {
        var grain = CreateGrain();

        Assert.ThrowsAsync<ArgumentNullException>(
            () => grain.DiscardPendingTransactionsAndGetNextSiblingAsync(null!));
    }
}
