using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit coverage for <see cref="BoundedLeafWalk"/>, the single implementation of
/// the budget / cursor / stop-condition logic the background coordinators' leaf
/// walks share (issue 1973).
/// <para>
/// The properties pinned here are the ones every call site depends on and none
/// of them re-implements: a pass visits at most its budgeted number of leaves;
/// it yields only where it can name a resume key; it reports completion only at
/// the true end of the chain; it resumes by re-descending the key rather than by
/// trusting a leaf id; and a structural change to the chain between two passes
/// cannot truncate the walk.
/// </para>
/// </summary>
[TestFixture]
public class BoundedLeafWalkTests
{
    private static string Key(int index) => $"k{index:D4}";

    /// <summary>
    /// Builds a shard whose leaf chain is <paramref name="leafCount"/> leaves
    /// long, where leaf <c>i</c> owns <c>[k{i}, k{i+1})</c> and the shard root
    /// resolves a resume key onto the leaf that owns it.
    /// </summary>
    private static (IGrainFactory Factory, IShardRootGrain Shard, GrainId[] LeafIds) Chain(int leafCount)
    {
        var factory = Substitute.For<IGrainFactory>();
        var shard = Substitute.For<IShardRootGrain>();
        var leafIds = new GrainId[leafCount];
        for (var i = 0; i < leafCount; i++)
            leafIds[i] = GrainId.Create("leaf", $"leaf-{i}");

        if (leafCount == 0)
        {
            shard.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(null));
            shard.GetLeafIdForKeyAsync(Arg.Any<string?>()).Returns(Task.FromResult<GrainId?>(null));
            return (factory, shard, leafIds);
        }

        shard.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(leafIds[0]));
        shard.GetLeafIdForKeyAsync(null).Returns(Task.FromResult<GrainId?>(leafIds[0]));

        for (var i = 0; i < leafCount; i++)
        {
            var leaf = Substitute.For<IBPlusLeafGrain>();
            factory.GetGrain<IBPlusLeafGrain>(leafIds[i]).Returns(leaf);
            leaf.GetNextSiblingAsync().Returns(Task.FromResult(
                i + 1 < leafCount ? (GrainId?)leafIds[i + 1] : null));
            leaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
            {
                LowKeyInclusive = Key(i),
                HighKeyExclusive = i + 1 < leafCount ? Key(i + 1) : null,
            }));
            shard.GetLeafIdForKeyAsync(Key(i)).Returns(Task.FromResult<GrainId?>(leafIds[i]));
        }

        return (factory, shard, leafIds);
    }

    /// <summary>Runs one pass to exhaustion, returning the leaves it visited.</summary>
    private static async Task<(List<GrainId> Visited, bool Completed, string? Resume, int LeavesVisited)> RunPassAsync(
        IGrainFactory factory, IShardRootGrain shard, string? resumeFrom, LeafWalkBudget budget)
    {
        var walk = await BoundedLeafWalk.StartAsync(factory, shard, resumeFrom, budget);
        var visited = new List<GrainId>();
        while (walk.HasLeaf)
        {
            visited.Add(walk.CurrentLeafId!.Value);
            if (!await walk.MoveNextAsync()) break;
        }
        return (visited, walk.Completed, walk.ResumeFromInclusive, walk.LeavesVisited);
    }

    [Test]
    public async Task StartAsync_on_an_empty_shard_reports_complete_with_no_leaf()
    {
        var (factory, shard, _) = Chain(0);

        var walk = await BoundedLeafWalk.StartAsync(factory, shard, null, new LeafWalkBudget(4, null));

        Assert.Multiple(() =>
        {
            Assert.That(walk.HasLeaf, Is.False);
            Assert.That(walk.CurrentLeafId, Is.Null);
            Assert.That(walk.Completed, Is.True);
            Assert.That(walk.ResumeFromInclusive, Is.Null);
            Assert.That(walk.LeavesVisited, Is.Zero);
        });
    }

    [Test]
    public async Task StartAsync_with_no_resume_key_enters_at_the_leftmost_leaf()
    {
        var (factory, shard, leafIds) = Chain(3);

        var walk = await BoundedLeafWalk.StartAsync(factory, shard, null, new LeafWalkBudget(0, null));

        Assert.That(walk.CurrentLeafId, Is.EqualTo(leafIds[0]));
        await shard.Received(1).GetLeftmostLeafIdAsync();
        await shard.DidNotReceive().GetLeafIdForKeyAsync(Arg.Any<string>());
    }

    [Test]
    public async Task StartAsync_with_a_resume_key_re_descends_through_the_shard_root()
    {
        var (factory, shard, leafIds) = Chain(3);

        var walk = await BoundedLeafWalk.StartAsync(factory, shard, Key(2), new LeafWalkBudget(0, null));

        Assert.That(walk.CurrentLeafId, Is.EqualTo(leafIds[2]));
        await shard.Received(1).GetLeafIdForKeyAsync(Key(2));
        await shard.DidNotReceive().GetLeftmostLeafIdAsync();
    }

    [Test]
    public async Task CurrentLeaf_throws_when_the_walk_has_no_leaf()
    {
        var (factory, shard, _) = Chain(0);
        var walk = await BoundedLeafWalk.StartAsync(factory, shard, null, new LeafWalkBudget(4, null));

        Assert.Throws<InvalidOperationException>(() => _ = walk.CurrentLeaf);
    }

    [Test]
    public async Task StartAsync_rejects_a_null_grain_factory()
    {
        var (_, shard, _) = Chain(1);
        Assert.That(
            async () => await BoundedLeafWalk.StartAsync(null!, shard, null, new LeafWalkBudget(1, null)),
            Throws.ArgumentNullException);
        await Task.CompletedTask;
    }

    [Test]
    public async Task StartAsync_rejects_a_null_shard()
    {
        var (factory, _, _) = Chain(1);
        Assert.That(
            async () => await BoundedLeafWalk.StartAsync(factory, null!, null, new LeafWalkBudget(1, null)),
            Throws.ArgumentNullException);
        await Task.CompletedTask;
    }

    [Test]
    public async Task An_unbounded_pass_sweeps_the_whole_chain_and_reports_complete()
    {
        var (factory, shard, leafIds) = Chain(5);

        var (visited, completed, resume, leavesVisited) =
            await RunPassAsync(factory, shard, null, LeafWalkBudget.Unbounded());

        Assert.Multiple(() =>
        {
            Assert.That(visited, Is.EqualTo(leafIds));
            Assert.That(completed, Is.True);
            Assert.That(resume, Is.Null, "a completed walk has nothing to resume");
            Assert.That(leavesVisited, Is.EqualTo(5));
        });
    }

    [Test]
    public async Task A_bounded_pass_visits_at_most_its_budgeted_leaves_and_names_where_to_resume()
    {
        var (factory, shard, leafIds) = Chain(5);

        var (visited, completed, resume, _) =
            await RunPassAsync(factory, shard, null, new LeafWalkBudget(2, null));

        Assert.Multiple(() =>
        {
            Assert.That(visited, Is.EqualTo(new[] { leafIds[0], leafIds[1] }));
            Assert.That(completed, Is.False);
            Assert.That(resume, Is.EqualTo(Key(2)),
                "the resume key is the visited leaf's exclusive high bound, which is where the next leaf begins");
        });
    }

    [Test]
    public async Task Successive_bounded_passes_visit_every_leaf_exactly_once()
    {
        var (factory, shard, leafIds) = Chain(5);

        var all = new List<GrainId>();
        string? cursor = null;
        bool completed;
        do
        {
            var pass = await RunPassAsync(factory, shard, cursor, new LeafWalkBudget(2, null));
            all.AddRange(pass.Visited);
            completed = pass.Completed;
            cursor = pass.Resume;
        }
        while (!completed);

        Assert.That(all, Is.EqualTo(leafIds));
    }

    /// <summary>
    /// The "only stop where you can resume" rule. The last leaf declares no high
    /// bound, so a budget that expires on it is not a stopping point - the walk
    /// must continue to the end of the chain rather than yield a position it
    /// cannot resume from, which would silently truncate the sweep.
    /// </summary>
    [Test]
    public async Task A_pass_does_not_stop_on_a_leaf_that_declares_no_resume_key()
    {
        var (factory, shard, leafIds) = Chain(2);

        var (visited, completed, resume, _) =
            await RunPassAsync(factory, shard, Key(1), new LeafWalkBudget(1, null));

        Assert.Multiple(() =>
        {
            Assert.That(visited, Is.EqualTo(new[] { leafIds[1] }));
            Assert.That(completed, Is.True);
            Assert.That(resume, Is.Null);
        });
    }

    /// <summary>
    /// A pass that spends its budget on the chain's final leaf must report the
    /// sweep complete, not park a cursor one position past the end.
    /// </summary>
    [Test]
    public async Task A_pass_that_exhausts_its_budget_on_the_last_leaf_reports_complete()
    {
        var (factory, shard, _) = Chain(2);

        var (_, completed, resume, _) =
            await RunPassAsync(factory, shard, null, new LeafWalkBudget(2, null));

        Assert.Multiple(() =>
        {
            Assert.That(completed, Is.True);
            Assert.That(resume, Is.Null);
        });
    }

    /// <summary>
    /// The reason the cursor is a key. Between two passes the leaf the first
    /// pass parked before is reclaimed, so its identity now activates empty with
    /// a null sibling. Re-descending the key lands on whichever leaf now owns it,
    /// so the resumed pass finishes the chain instead of mistaking a dead
    /// activation for the end of it.
    /// </summary>
    [Test]
    public async Task A_resumed_pass_whose_cursor_leaf_was_reclaimed_still_completes_the_chain()
    {
        var (factory, shard, leafIds) = Chain(4);

        var first = await RunPassAsync(factory, shard, null, new LeafWalkBudget(2, null));
        Assert.That(first.Completed, Is.False);
        Assert.That(first.Resume, Is.EqualTo(Key(2)));

        // Leaf 2 is reclaimed: its identity is now an empty virtual activation.
        // The key cursor never names it, so the walk never sees it.
        var reclaimed = Substitute.For<IBPlusLeafGrain>();
        reclaimed.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));
        reclaimed.GetKeyRangeAsync().Returns(Task.FromResult(default(LeafKeyRange)));
        factory.GetGrain<IBPlusLeafGrain>(leafIds[2]).Returns(reclaimed);

        var replacement = GrainId.Create("leaf", "leaf-2-replacement");
        var replacementLeaf = Substitute.For<IBPlusLeafGrain>();
        factory.GetGrain<IBPlusLeafGrain>(replacement).Returns(replacementLeaf);
        replacementLeaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(leafIds[3]));
        replacementLeaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
        {
            LowKeyInclusive = Key(2),
            HighKeyExclusive = Key(3),
        }));
        shard.GetLeafIdForKeyAsync(Key(2)).Returns(Task.FromResult<GrainId?>(replacement));

        var second = await RunPassAsync(factory, shard, first.Resume, LeafWalkBudget.Unbounded());

        Assert.Multiple(() =>
        {
            Assert.That(second.Visited, Is.EqualTo(new[] { replacement, leafIds[3] }));
            Assert.That(second.Completed, Is.True,
                "the resumed walk must reach the true end of the chain, not a reclaimed leaf's null sibling");
        });
    }

    /// <summary>
    /// The definition-of-done case: the chain is structurally changed between
    /// two passes. The leaf the cursor points into splits, so the keys the first
    /// pass had not reached now live across two leaves. Re-descending the key
    /// lands on the left half, and the walk follows the new chain to its end
    /// without truncating.
    /// </summary>
    [Test]
    public async Task A_resumed_pass_whose_cursor_leaf_has_split_visits_both_halves()
    {
        var (factory, shard, leafIds) = Chain(3);

        var first = await RunPassAsync(factory, shard, null, new LeafWalkBudget(1, null));
        Assert.That(first.Resume, Is.EqualTo(Key(1)));

        // Leaf 1, which owned [k0001, k0002), splits into [k0001, k0001m) and
        // [k0001m, k0002). The right half is grafted in before the old leaf 2.
        var rightHalf = GrainId.Create("leaf", "leaf-1-right");
        var rightHalfLeaf = Substitute.For<IBPlusLeafGrain>();
        factory.GetGrain<IBPlusLeafGrain>(rightHalf).Returns(rightHalfLeaf);
        rightHalfLeaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(leafIds[2]));
        rightHalfLeaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
        {
            LowKeyInclusive = "k0001m",
            HighKeyExclusive = Key(2),
        }));

        var leftHalf = factory.GetGrain<IBPlusLeafGrain>(leafIds[1]);
        leftHalf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(rightHalf));
        leftHalf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
        {
            LowKeyInclusive = Key(1),
            HighKeyExclusive = "k0001m",
        }));

        var second = await RunPassAsync(factory, shard, first.Resume, LeafWalkBudget.Unbounded());

        Assert.Multiple(() =>
        {
            Assert.That(second.Visited, Is.EqualTo(new[] { leafIds[1], rightHalf, leafIds[2] }),
                "both halves of the split leaf must be visited, so no key is skipped");
            Assert.That(second.Completed, Is.True);
        });
    }

    /// <summary>
    /// Forward progress. A resumed pass's own resume key must lie strictly
    /// beyond the position it started from, or a coordinator would re-issue the
    /// same pass forever.
    /// </summary>
    [Test]
    public async Task Every_bounded_pass_advances_the_cursor()
    {
        var (factory, shard, _) = Chain(6);

        string? cursor = null;
        for (var pass = 0; pass < 3; pass++)
        {
            var result = await RunPassAsync(factory, shard, cursor, new LeafWalkBudget(1, null));
            Assert.That(result.Resume, Is.Not.EqualTo(cursor),
                $"pass {pass} must move the cursor forward");
            cursor = result.Resume;
        }
    }

    /// <summary>
    /// Forward progress must not depend on the tree's bounds being consistent
    /// with its separators. A leaf whose persisted high bound has drifted back
    /// to the key the pass resumed at would otherwise hand that key straight
    /// back, and a coordinator that drives passes until the sweep completes
    /// would re-issue an identical pass forever. The walk must refuse that
    /// candidate and keep walking instead.
    /// </summary>
    [Test]
    public async Task A_pass_refuses_a_resume_key_that_would_not_advance()
    {
        var (factory, shard, leafIds) = Chain(3);

        // Leaf 1's bounds drift: it now reports its own low bound as its
        // exclusive high bound, so yielding on it would emit the key the pass
        // started from.
        var drifted = factory.GetGrain<IBPlusLeafGrain>(leafIds[1]);
        drifted.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
        {
            LowKeyInclusive = Key(1),
            HighKeyExclusive = Key(1),
        }));

        var (visited, completed, resume, _) =
            await RunPassAsync(factory, shard, Key(1), new LeafWalkBudget(1, null));

        Assert.Multiple(() =>
        {
            Assert.That(resume, Is.Not.EqualTo(Key(1)),
                "the walk must never hand back the position it started from");
            Assert.That(visited, Is.EqualTo(new[] { leafIds[1], leafIds[2] }),
                "refusing the candidate falls through to keep walking, not to stopping");
            Assert.That(completed, Is.True);
        });
    }

    /// <summary>
    /// A shard cannot use <see cref="BoundedLeafWalk.StartAsync"/> on itself:
    /// resolving the start leaf calls back into <c>IShardRootGrain</c>, and a
    /// non-reentrant grain self-calling deadlocks. <c>FromResolvedStart</c> is
    /// the in-shard entry point that takes the already-resolved start leaf, so
    /// the shard's own bounded walks share this implementation rather than
    /// hand-rolling the same rules (issue 1972).
    /// </summary>
    [Test]
    public async Task FromResolvedStart_walks_the_chain_without_calling_back_into_the_shard()
    {
        var (factory, shard, leafIds) = Chain(3);

        var walk = BoundedLeafWalk.FromResolvedStart(
            factory, leafIds[0], null, new LeafWalkBudget(0, null));

        var visited = new List<GrainId>();
        while (walk.HasLeaf)
        {
            visited.Add(walk.CurrentLeafId!.Value);
            if (!await walk.MoveNextAsync()) break;
        }

        Assert.Multiple(async () =>
        {
            Assert.That(visited, Is.EqualTo(leafIds));
            Assert.That(walk.Completed, Is.True);
            Assert.That(walk.ResumeFromInclusive, Is.Null);
            await shard.DidNotReceive().GetLeftmostLeafIdAsync();
            await shard.DidNotReceive().GetLeafIdForKeyAsync(Arg.Any<string?>());
        });
    }

    /// <summary>
    /// The start leaf is already resolved, but the resume key the pass began
    /// from is still needed: it is what the forward-progress guard compares a
    /// candidate resume position against.
    /// </summary>
    [Test]
    public async Task FromResolvedStart_still_refuses_a_resume_key_that_would_not_advance()
    {
        var (factory, shard, leafIds) = Chain(3);
        _ = shard;

        var drifted = factory.GetGrain<IBPlusLeafGrain>(leafIds[1]);
        drifted.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
        {
            LowKeyInclusive = Key(1),
            HighKeyExclusive = Key(1),
        }));

        var walk = BoundedLeafWalk.FromResolvedStart(
            factory, leafIds[1], Key(1), new LeafWalkBudget(1, null));

        var visited = new List<GrainId>();
        while (walk.HasLeaf)
        {
            visited.Add(walk.CurrentLeafId!.Value);
            if (!await walk.MoveNextAsync()) break;
        }

        Assert.Multiple(() =>
        {
            Assert.That(walk.ResumeFromInclusive, Is.Not.EqualTo(Key(1)),
                "the walk must never hand back the position it started from");
            Assert.That(visited, Is.EqualTo(new[] { leafIds[1], leafIds[2] }));
            Assert.That(walk.Completed, Is.True);
        });
    }

    /// <summary>
    /// A shard whose root has no leaves resolves a null start leaf. The walk
    /// must report an immediately complete, leafless pass rather than fault.
    /// </summary>
    [Test]
    public void FromResolvedStart_with_no_start_leaf_reports_complete_with_no_leaf()
    {
        var (factory, _, _) = Chain(0);

        var walk = BoundedLeafWalk.FromResolvedStart(
            factory, null, null, new LeafWalkBudget(4, null));

        Assert.Multiple(() =>
        {
            Assert.That(walk.HasLeaf, Is.False);
            Assert.That(walk.Completed, Is.True);
            Assert.That(walk.ResumeFromInclusive, Is.Null);
            Assert.That(walk.LeavesVisited, Is.Zero);
        });
    }
}
