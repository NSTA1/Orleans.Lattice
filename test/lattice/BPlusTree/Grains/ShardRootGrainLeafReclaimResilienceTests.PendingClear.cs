using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #2207: a folded leaf's grain-state clear is the step that removes its
/// storage row, replay barrier and WAL materialiser pin. It runs after the fold
/// has committed, when the leaf is already off the chain and out of routing, so
/// no later walk can rediscover it. A clear that fails there must be recorded
/// durably and retried, not logged and forgotten.
/// </summary>
public sealed partial class ShardRootGrainLeafReclaimResilienceTests
{
    [Test]
    public async Task A_folded_leaf_whose_clear_failed_is_recorded_as_owed()
    {
        var h = CreateHarness();
        h.B.ClearGrainStateAsync().Returns(
            _ => Task.FromException(new InvalidOperationException("storage unavailable")),
            _ => Task.CompletedTask);

        var reclaimed = await h.Grain.ReclaimEmptyLeavesAsync(4);

        Assert.That(reclaimed, Is.EqualTo(1));
        Assert.That(h.State.State.PendingLeafClears, Is.EqualTo(new[] { h.LeafB }),
            "the only surviving reference to an unlinked, unrouted leaf must be durable");
    }

    [Test]
    public async Task A_failed_clear_of_a_folded_leaf_is_retried_by_the_next_pass()
    {
        var h = CreateHarness();
        h.B.ClearGrainStateAsync().Returns(
            _ => Task.FromException(new InvalidOperationException("storage unavailable")),
            _ => Task.CompletedTask);

        await h.Grain.ReclaimEmptyLeavesAsync(4);
        await h.B.Received(1).ClearGrainStateAsync();

        // B is now off the chain (A -> C), so only the record can bring it back.
        Assert.That(h.Probes[h.LeafA].NextSibling, Is.EqualTo(h.LeafC));

        var writesBefore = h.State.WriteCount;
        var second = await h.Grain.ReclaimEmptyLeavesAsync(4);

        Assert.That(second, Is.Zero, "the retry is not a fold and must not be counted as one");
        await h.B.Received(2).ClearGrainStateAsync();
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.PendingLeafClears, Is.Empty,
                "a clear that landed must be retired from the record");
            Assert.That(h.State.WriteCount, Is.GreaterThan(writesBefore),
                "the retirement must reach storage, not just memory");
        });

        // A third pass has nothing owed and must not re-clear.
        await h.Grain.ReclaimEmptyLeavesAsync(4);
        await h.B.Received(2).ClearGrainStateAsync();
    }

    [Test]
    public async Task A_clear_that_keeps_failing_stays_owed_across_passes()
    {
        var h = CreateHarness();
        h.B.ClearGrainStateAsync()
            .Returns(_ => Task.FromException(new InvalidOperationException("storage unavailable")));

        await h.Grain.ReclaimEmptyLeavesAsync(4);
        await h.Grain.ReclaimEmptyLeavesAsync(4);
        await h.Grain.ReclaimEmptyLeavesAsync(4);

        await h.B.Received(3).ClearGrainStateAsync();
        Assert.That(h.State.State.PendingLeafClears, Is.EqualTo(new[] { h.LeafB }));
    }

    [Test]
    public async Task A_successful_fold_leaves_nothing_owed()
    {
        var h = CreateHarness();

        await h.Grain.ReclaimEmptyLeavesAsync(4);

        await h.B.Received(1).ClearGrainStateAsync();
        Assert.That(h.State.State.PendingLeafClears, Is.Empty);
    }

    [Test]
    public async Task A_fold_whose_owed_record_cannot_be_written_still_attempts_the_clear()
    {
        // The record write precedes the clear. Failing it must not skip the
        // clear: the removal has committed, and clearing now is the cheapest
        // way to make the record unnecessary.
        var h = CreateHarness();
        var failedOnce = false;
        h.State.OnWriteState = s =>
        {
            if (s.PendingLeafClears.Count > 0 && !failedOnce)
            {
                failedOnce = true;
                throw new InvalidOperationException("shard state unavailable");
            }
        };

        var reclaimed = await h.Grain.ReclaimEmptyLeavesAsync(4);

        Assert.That(reclaimed, Is.EqualTo(1));
        await h.B.Received(1).ClearGrainStateAsync();
        Assert.That(h.State.State.PendingLeafClears, Is.Empty);
    }

    [Test]
    public async Task A_single_leaf_shard_still_retries_a_clear_it_owes()
    {
        // The fold that emptied the shard down to one leaf can itself be the one
        // whose clear failed. The single-leaf early exit must not strand it.
        var h = CreateHarness();
        var orphaned = GrainId.Create("leaf", "reclaim-leaf-owed");
        var owed = Substitute.For<IBPlusLeafGrain>();
        owed.ClearGrainStateAsync().Returns(Task.CompletedTask);
        h.Leaves[orphaned] = owed;

        h.State.State.RootNodeId = h.LeafA;
        h.State.State.RootIsLeaf = true;
        h.State.State.PendingLeafClears.Add(orphaned);

        var reclaimed = await h.Grain.ReclaimEmptyLeavesAsync(4);

        Assert.That(reclaimed, Is.Zero);
        await owed.Received(1).ClearGrainStateAsync();
        Assert.That(h.State.State.PendingLeafClears, Is.Empty);
        await h.A.DidNotReceive().GetReclaimProbeAsync();
    }

    [Test]
    public async Task A_pass_retries_at_most_the_capped_number_of_owed_clears_oldest_first()
    {
        var h = CreateHarness();
        var owed = new List<(GrainId Id, IBPlusLeafGrain Leaf)>();
        for (var i = 0; i < ShardRootGrain.MaxPendingLeafClearRetriesPerPass + 3; i++)
        {
            var id = GrainId.Create("leaf", $"reclaim-leaf-owed-{i:D3}");
            var leaf = Substitute.For<IBPlusLeafGrain>();
            leaf.ClearGrainStateAsync().Returns(Task.CompletedTask);
            h.Leaves[id] = leaf;
            h.State.State.PendingLeafClears.Add(id);
            owed.Add((id, leaf));
        }

        await h.Grain.ReclaimEmptyLeavesAsync(4);

        var cap = ShardRootGrain.MaxPendingLeafClearRetriesPerPass;
        for (var i = 0; i < cap; i++)
        {
            await owed[i].Leaf.Received(1).ClearGrainStateAsync();
        }

        for (var i = cap; i < owed.Count; i++)
        {
            await owed[i].Leaf.DidNotReceive().ClearGrainStateAsync();
        }

        Assert.That(h.State.State.PendingLeafClears,
            Is.EqualTo(owed.Skip(cap).Select(o => o.Id).ToArray()),
            "the entries past the cap are deferred to the next pass, not dropped");

        await h.Grain.ReclaimEmptyLeavesAsync(4);
        Assert.That(h.State.State.PendingLeafClears, Is.Empty);
        for (var i = cap; i < owed.Count; i++)
        {
            await owed[i].Leaf.Received(1).ClearGrainStateAsync();
        }
    }
}
