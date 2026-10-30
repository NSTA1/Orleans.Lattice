using System.Reflection;
using Orleans.Concurrency;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins issue #4914: the re-parenting callbacks on <see cref="IBPlusLeafGrain"/>
/// must be <see cref="AlwaysInterleaveAttribute"/>. A leaf capture turn awaits
/// the shard root inline to link its split; that link can overflow the parent
/// internal, whose new sibling then seeds its moved children by calling
/// <c>SetParentAsync</c> and <c>GetChildDigestSnapshotAsync</c> on every one of
/// them - including the leaf whose turn is still suspended on the root. Without
/// interleave that call queues behind the suspended turn, which waits on the
/// root, which waits on the seeding: a cross-grain wait cycle that only the
/// call timeout breaks, after the root's link gate has been held throughout.
/// This mirrors the internal-grain fix for issue #3523.
/// </summary>
[TestFixture]
public sealed class IBPlusLeafGrainReparentInterleaveTests
{
    [TestCase(nameof(IBPlusLeafGrain.SetParentAsync))]
    [TestCase(nameof(IBPlusLeafGrain.SetParentAndGetChildDigestSnapshotAsync))]
    [TestCase(nameof(IBPlusLeafGrain.GetChildDigestSnapshotAsync))]
    public void Reparent_callback_is_marked_AlwaysInterleave(string methodName)
    {
        var method = typeof(IBPlusLeafGrain).GetMethod(methodName);

        Assert.That(method, Is.Not.Null, $"Expected to find method '{methodName}' on IBPlusLeafGrain.");
        Assert.That(method!.GetCustomAttribute<AlwaysInterleaveAttribute>(inherit: false), Is.Not.Null,
            $"IBPlusLeafGrain.{methodName} MUST be [AlwaysInterleave] (issue #4914): an internal split seeds its " +
            "moved leaves through it while one of them can be suspended on the shard root linking its own split, " +
            "and queueing behind that turn deadlocks until the call timeout.");
    }
}
