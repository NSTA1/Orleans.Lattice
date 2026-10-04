using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Fast, dependency-free unit tests for <see cref="CrossTreeReceiverBarrier"/> -
/// the receiver-side cross-tree barrier's completeness rule, single verdict and
/// wait-set comparison, which <c>LatticeCrossTreeReceiverGrain.NotifyTerminalAsync</c>
/// routes through.
/// </summary>
[TestFixture]
public sealed class CrossTreeReceiverBarrierTests
{
    private static CrossTreeReceiverTerminal Terminal(string tree, bool committed) => new()
    {
        OriginClusterId = "origin",
        OperationId = "op",
        TreeId = tree,
        TransactionId = Guid.NewGuid(),
        Committed = committed,
        WaitSet = ["a", "b"],
        ObservedSourceShards = [0],
        TerminalHlc = HybridLogicalClock.Zero,
    };

    [Test]
    public void The_barrier_is_incomplete_until_every_wait_set_tree_has_arrived()
    {
        var waitSet = new List<string> { "a", "b" };
        var arrived = new Dictionary<string, CrossTreeReceiverTerminal> { ["a"] = Terminal("a", true) };

        Assert.That(CrossTreeReceiverBarrier.IsComplete(waitSet, arrived), Is.False);

        arrived["b"] = Terminal("b", true);

        Assert.That(CrossTreeReceiverBarrier.IsComplete(waitSet, arrived), Is.True);
    }

    [Test]
    public void An_arrival_outside_the_wait_set_does_not_complete_the_barrier()
    {
        var waitSet = new List<string> { "a", "b" };
        var arrived = new Dictionary<string, CrossTreeReceiverTerminal>
        {
            ["a"] = Terminal("a", true),
            ["c"] = Terminal("c", true),
        };

        Assert.That(CrossTreeReceiverBarrier.IsComplete(waitSet, arrived), Is.False);
    }

    [Test]
    public void The_verdict_commits_only_when_every_arrival_committed()
    {
        var allCommitted = new Dictionary<string, CrossTreeReceiverTerminal>
        {
            ["a"] = Terminal("a", true),
            ["b"] = Terminal("b", true),
        };
        var oneAborted = new Dictionary<string, CrossTreeReceiverTerminal>
        {
            ["a"] = Terminal("a", true),
            ["b"] = Terminal("b", false),
        };

        Assert.Multiple(() =>
        {
            Assert.That(CrossTreeReceiverBarrier.CommitsAll(allCommitted), Is.True);
            Assert.That(CrossTreeReceiverBarrier.CommitsAll(oneAborted), Is.False);
        });
    }

    [Test]
    public void Wait_sets_match_ignoring_order_and_duplicates()
    {
        var frozen = new List<string> { "a", "b" };

        Assert.Multiple(() =>
        {
            Assert.That(CrossTreeReceiverBarrier.WaitSetMatches(frozen, ["b", "a"]), Is.True);
            Assert.That(CrossTreeReceiverBarrier.WaitSetMatches(frozen, ["a", "b", "a"]), Is.True);
            Assert.That(CrossTreeReceiverBarrier.WaitSetMatches(frozen, ["a"]), Is.False);
            Assert.That(CrossTreeReceiverBarrier.WaitSetMatches(frozen, ["a", "b", "c"]), Is.False);
        });
    }

    [Test]
    public void The_barrier_validates_its_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() =>
                CrossTreeReceiverBarrier.IsComplete<bool>(null!, new Dictionary<string, bool>()));
            Assert.Throws<ArgumentNullException>(() =>
                CrossTreeReceiverBarrier.IsComplete<bool>(new List<string>(), null!));
            Assert.Throws<ArgumentNullException>(() => CrossTreeReceiverBarrier.CommitsAll(null!));
            Assert.Throws<ArgumentNullException>(() => CrossTreeReceiverBarrier.WaitSetMatches(null!, []));
            Assert.Throws<ArgumentNullException>(() => CrossTreeReceiverBarrier.WaitSetMatches([], null!));
        });
    }
}
