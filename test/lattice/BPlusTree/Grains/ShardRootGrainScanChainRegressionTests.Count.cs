using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Count walks over a chain that threads an orphaned leaf (issue 6199). The
/// scan walks de-duplicate an orphan's shadow rows through the chain watermark
/// (issue 3271), but the shard count walks summed every leaf the chain
/// reached, so a post-topology-change <c>CountAsync</c> over-counted by exactly
/// the orphan's row count while <c>ScanKeysAsync</c> on the same tree was
/// correct.
/// </summary>
public partial class ShardRootGrainScanChainRegressionTests
{
    private const int CountVirtualShardCount = 4096;

    /// <summary>
    /// Marks one virtual slot that none of the chain's keys hash to as moved
    /// away, which forces a count walk onto its key-materialising path without
    /// filtering any key the test expects to be counted.
    /// </summary>
    private static Action<ShardRootState> WithUnrelatedMovedAwaySlot(LeafSpec[] specs)
    {
        var used = specs
            .SelectMany(s => s.Keys)
            .Select(k => ShardMap.GetVirtualSlot(k, CountVirtualShardCount))
            .ToHashSet();
        var slot = Enumerable.Range(0, CountVirtualShardCount).First(s => !used.Contains(s));
        return s =>
        {
            s.MovedAwaySlots[slot] = 1;
            s.MovedAwayVirtualShardCount = CountVirtualShardCount;
        };
    }

    private static async Task<int> DrainCountAsync(Func<string?, Task<ShardCountPage>> page)
    {
        var total = 0;
        string? from = null;
        while (true)
        {
            var p = await page(from);
            total += p.Count;
            if (p.ResumeFromInclusive is not { } next) return total;
            from = next;
        }
    }

    [Test]
    public async Task CountBoundedAsync_orphan_duplicating_a_live_leaf_range_counts_each_key_once()
    {
        var h = BuildChain(ForwardChainWithOrphan());

        var count = await DrainCountAsync(from => h.Grain.CountBoundedAsync(from, null));

        await h.Leaves[2].Received(1).CountAndNextSiblingAsync(Arg.Any<string?>(), Arg.Any<string?>());
        Assert.That(count, Is.EqualTo(4));
    }

    [Test]
    public async Task CountBoundedAsync_with_moved_away_slots_orphan_counts_each_key_once()
    {
        var specs = ForwardChainWithOrphan();
        var h = BuildChain(specs, configureState: WithUnrelatedMovedAwaySlot(specs));

        var count = await DrainCountAsync(from => h.Grain.CountBoundedAsync(from, null));

        await AssertKeyReadsAsync(h.Leaves[1], 1);
        await AssertKeyReadsAsync(h.Leaves[2], 1);
        Assert.That(count, Is.EqualTo(4));
    }

    [Test]
    public async Task CountForSlotsBoundedAsync_orphan_duplicating_a_live_leaf_range_counts_each_key_once()
    {
        var h = BuildChain(ForwardChainWithOrphan());

        var count = await DrainCountAsync(from => h.Grain.CountForSlotsBoundedAsync([0], 1, from, null));

        await AssertKeyReadsAsync(h.Leaves[1], 1);
        await AssertKeyReadsAsync(h.Leaves[2], 1);
        Assert.That(count, Is.EqualTo(4));
    }

    [Test]
    public async Task CountWithMovedAwayBoundedAsync_orphan_duplicating_a_live_leaf_range_counts_each_key_once()
    {
        var h = BuildChain(ForwardChainWithOrphan());

        var page = await h.Grain.CountWithMovedAwayBoundedAsync(null);

        await h.Leaves[2].Received(1).CountAndNextSiblingAsync(Arg.Any<string?>(), Arg.Any<string?>());
        Assert.That(page.Count, Is.EqualTo(4));
        Assert.That(page.ResumeFromInclusive, Is.Null);
    }

    [Test]
    public async Task CountWithMovedAwayBoundedAsync_with_moved_away_slots_orphan_counts_each_key_once()
    {
        var specs = ForwardChainWithOrphan();
        var h = BuildChain(specs, configureState: WithUnrelatedMovedAwaySlot(specs));

        var page = await h.Grain.CountWithMovedAwayBoundedAsync(null);

        await AssertKeyReadsAsync(h.Leaves[2], 1);
        Assert.That(page.Count, Is.EqualTo(4));
        Assert.That(page.MovedAwaySlots, Is.Null);
    }

    [Test]
    public async Task Count_walks_on_a_healthy_chain_count_every_key()
    {
        var specs = ForwardChainHealthy();
        var plain = BuildChain(specs);
        var moved = BuildChain(specs, configureState: WithUnrelatedMovedAwaySlot(specs));
        var slots = BuildChain(specs);

        Assert.Multiple(async () =>
        {
            Assert.That(await DrainCountAsync(from => plain.Grain.CountBoundedAsync(from, null)), Is.EqualTo(6));
            Assert.That(await DrainCountAsync(from => moved.Grain.CountBoundedAsync(from, null)), Is.EqualTo(6));
            Assert.That(await DrainCountAsync(from => slots.Grain.CountForSlotsBoundedAsync([0], 1, from, null)), Is.EqualTo(6));
            Assert.That((await plain.Grain.CountWithMovedAwayBoundedAsync(null)).Count, Is.EqualTo(6));
            Assert.That((await moved.Grain.CountWithMovedAwayBoundedAsync(null)).Count, Is.EqualTo(6));
        });
    }

    [Test]
    public async Task CountBoundedAsync_orphan_does_not_end_a_bounded_count_early()
    {
        // The orphan claims [null, "b") again after the walk has left it. Its
        // declared bounds must not steer the past-range early exit, or the
        // live "b" leaf beyond it would be dropped.
        var h = BuildChain(ForwardChainWithOrphan());

        var count = await DrainCountAsync(from => h.Grain.CountBoundedAsync(from, "c"));

        Assert.That(count, Is.EqualTo(4));
    }
}
