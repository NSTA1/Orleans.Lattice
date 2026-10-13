using System.Reflection;
using Orleans.Concurrency;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins issue #4919: the sibling-pointer setters on <see cref="IBPlusLeafGrain"/>
/// must be <see cref="AlwaysInterleaveAttribute"/>. A splitting leaf repoints its
/// right neighbour's back-link while holding its own split gate. If that
/// neighbour's turn is suspended on the shard root linking a split that overflows
/// the parent internal node, the parent split re-parents the caller, and the
/// re-parent waits on the caller's gate. Without interleave the setter queues
/// behind the neighbour's turn and the cycle is broken only by the call timeout.
/// This is the sibling-pointer counterpart of issue #4914.
/// </summary>
[TestFixture]
public sealed class IBPlusLeafGrainSiblingSetterInterleaveTests
{
    [TestCase(nameof(IBPlusLeafGrain.SetNextSiblingAsync))]
    [TestCase(nameof(IBPlusLeafGrain.SetPrevSiblingAsync))]
    public void Sibling_setter_is_marked_AlwaysInterleave(string methodName)
    {
        var method = typeof(IBPlusLeafGrain).GetMethod(methodName);

        Assert.That(method, Is.Not.Null, $"Expected to find method '{methodName}' on IBPlusLeafGrain.");
        Assert.That(method!.GetCustomAttribute<AlwaysInterleaveAttribute>(inherit: false), Is.Not.Null,
            $"IBPlusLeafGrain.{methodName} MUST be [AlwaysInterleave] (issue #4919): a split calls it on a " +
            "neighbour while holding its own split gate, and queueing behind a neighbour turn suspended on the " +
            "shard root deadlocks until the call timeout.");
    }
}
