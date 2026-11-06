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
    public async Task A_descent_during_a_link_batch_is_undecided()
    {
        // Detector 1: a split's link batch holds the root's accept open. A descent for the
        // right leaf - which a decided descent reaches - is undecided, as are the boolean
        // read and the live-advance view built on it.
        var h = CreateHarness();
        var accepting = new TaskCompletionSource<SplitResult?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var acceptEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        h.Root.AcceptSplitAsync("c", SiblingId).Returns(_ =>
        {
            acceptEntered.TrySetResult();
            return accepting.Task;
        });
        h.Left.SetAsync("b", Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(new SplitResult
        {
            PromotedKey = "c",
            NewSiblingId = SiblingId,
            ChildIsLeaf = true,
        }));

        var write = h.Grain.SetAsync("b", [1]);
        await acceptEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var verdict = await h.Grain.GetDescentVerdictAsync(RightLeafId, "m");
        var reachable = await h.Grain.IsReachableByDescentAsync(RightLeafId, "m");
        var view = await h.Grain.GetLiveAdvanceViewAsync(RightLeafId, "m", null);
        accepting.SetResult(null);
        await write.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Multiple(() =>
        {
            Assert.That(verdict, Is.EqualTo(DescentVerdict.Undecided), "a descent during a link batch gave a verdict");
            Assert.That(reachable, Is.False, "the boolean read trusted a descent during a link batch");
            Assert.That(view.ReachabilityUndecided, Is.True, "the live-advance view carried a descent during a link batch as a verdict");
            Assert.That(view.Reachable, Is.False);
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
