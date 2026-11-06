using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// #5876, D1 (a5e2e2ef's detectors 1, 2 and 4): the shard root's descent verdict is
/// undecided while a link batch is in flight or when a routing-structure mutation completes
/// during the descent, and decided as before once the structure has settled.
/// </summary>
public sealed partial class ShardRootGrainDescentVerdictTests
{
    private static readonly GrainId RightLeafId = DescentVerdictHarness.RightLeafId;
    private static readonly GrainId LeftLeafId = DescentVerdictHarness.LeftLeafId;
    private static readonly GrainId SiblingId = DescentVerdictHarness.SiblingId;

    private static DescentVerdictHarness CreateHarness() => DescentVerdictHarness.Create();

    [Test]
    public async Task A_parent_lagging_a_completed_internal_split_yields_undecided_never_a_non_owner()
    {
        // (a), V1: the left parent divided at m - settled, its bound narrowed - but the root
        // does not yet route to its right half. A decided descent for n would name the left
        // leaf, which does not own it; the bound makes it undecided instead.
        var h = DescentVerdictHarness.CreateLaggingInternalSplit();

        var forMoved = await h.Grain.GetDescentVerdictAsync(LeftLeafId, "n");
        var forMovedOwner = await h.Grain.GetDescentVerdictAsync(RightLeafId, "n");
        var positive = await h.Grain.IsReachableByDescentAsync(LeftLeafId, "n");
        var view = await h.Grain.GetLiveAdvanceViewAsync(RightLeafId, "n", null);
        var inBounds = await h.Grain.GetDescentVerdictAsync(LeftLeafId, "a");

        Assert.Multiple(() =>
        {
            Assert.That(forMoved, Is.EqualTo(DescentVerdict.Undecided), "a lagging parent named a non-owner as reachable or unreachable");
            Assert.That(forMovedOwner, Is.EqualTo(DescentVerdict.Undecided), "the owner was judged on a lagging parent's route");
            Assert.That(positive, Is.False, "the boolean read trusted a lagging parent's route");
            Assert.That(view.ReachabilityUndecided, Is.True);
            Assert.That(inBounds, Is.EqualTo(DescentVerdict.Reachable), "a key inside every bound was not decided");
        });
    }

    [Test]
    public async Task A_link_batch_in_another_subtree_leaves_the_descent_decided()
    {
        // (b), liveness: a split's link batch holds the left parent's accept open; the right
        // leaf's path is settled, so its descent - and the view on it - decides as usual.
        var h = DescentVerdictHarness.CreateTwoLevel();
        var batch = await h.HoldLinkBatchAsync();
        DescentVerdict verdict;
        LeafLiveAdvanceView view;
        try
        {
            verdict = await h.Grain.GetDescentVerdictAsync(RightLeafId, "m");
            view = await h.Grain.GetLiveAdvanceViewAsync(RightLeafId, "m", null);
        }
        finally
        {
            await batch.ReleaseAsync();
        }

        Assert.Multiple(() =>
        {
            Assert.That(verdict, Is.EqualTo(DescentVerdict.Reachable), "a link batch elsewhere made a settled path undecided");
            Assert.That(view.ReachabilityUndecided, Is.False);
            Assert.That(view.Reachable, Is.True);
        });
    }

    [Test]
    public async Task A_routing_mutation_completing_mid_descent_is_undecided()
    {
        // Detector 2: a routing-structure mutation opens and closes while the descent is
        // in flight - settled before and after, but the version moved.
        var h = CreateHarness();
        h.Grain.DuringDescentForTesting = () =>
        {
            using (h.Grain.EnterStructuralRouting())
            {
            }
        };

        var verdict = await h.Grain.GetDescentVerdictAsync(RightLeafId, "m");

        Assert.That(verdict, Is.EqualTo(DescentVerdict.Undecided), "a descent across a completed routing mutation gave a verdict");
    }

    [Test]
    public async Task After_the_batch_a_descent_is_decided_as_before()
    {
        // Detector 4: once the link batch has completed, the descent decides again.
        var h = CreateHarness();
        h.Left.SetAsync("b", Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(new SplitResult
        {
            PromotedKey = "c",
            NewSiblingId = SiblingId,
            ChildIsLeaf = true,
        }));
        await h.Grain.SetAsync("b", [1]);

        var reached = await h.Grain.GetDescentVerdictAsync(RightLeafId, "m");
        var missed = await h.Grain.GetDescentVerdictAsync(LeftLeafId, "m");
        var reachable = await h.Grain.IsReachableByDescentAsync(RightLeafId, "m");

        Assert.Multiple(() =>
        {
            Assert.That(reached, Is.EqualTo(DescentVerdict.Reachable), "a settled descent did not reach the owning leaf");
            Assert.That(missed, Is.EqualTo(DescentVerdict.Unreachable), "a settled descent reached a leaf that does not own the key");
            Assert.That(reachable, Is.True);
        });
    }
}
