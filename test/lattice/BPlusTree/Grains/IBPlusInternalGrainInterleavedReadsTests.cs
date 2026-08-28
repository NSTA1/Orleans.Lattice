using System.Reflection;
using Orleans.Concurrency;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins U9p step 8c-c-iv-c2-vi: the routing surface on
/// <see cref="IBPlusInternalGrain"/> must be annotated
/// <see cref="AlwaysInterleaveAttribute"/> so multiple concurrent
/// shard-root traversals can run on the same internal-node
/// activation. Without the attribute, every <c>RouteWithMetadataAsync</c>
/// / <c>GetRoutingTableAsync</c> caller queues on the activation turn,
/// re-introducing the per-internal-node serial-turn ceiling that
/// c2-vi removed.
/// <para>
/// Safety argument:
/// every read method on <see cref="IBPlusInternalGrain"/> is a single
/// synchronous <c>Task.FromResult(state.State.X)</c> expression with
/// no awaits and no multi-step cross-state-field traversal, so the
/// U9h-C "interleaved read across multi-step traversal" hazard does
/// not apply. The one mutation method on the hot path
/// (<see cref="Orleans.Lattice.BPlusTree.IBPlusInternalGrain.AcceptSplitAsync"/>) is
/// <c>[AlwaysInterleave]</c> for routing concurrency but its body is
/// serialised by a per-activation <c>_splitGate</c>
/// <see cref="System.Threading.SemaphoreSlim"/> on
/// <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusInternalGrain"/>.
/// </para>
/// <para>
/// This is a contract assertion: the attribute is enforced by the
/// Orleans runtime scheduler, which is not part of the unit test
/// environment. The reflection-based assertion guards against any
/// future refactor silently stripping the attribute; that regression
/// would otherwise only be observable on a real cluster under live
/// load (as a regression in the c2-vi benchmark numbers in
/// <c></c>).
/// </para>
/// </summary>
[TestFixture]
public sealed class IBPlusInternalGrainInterleavedReadsTests
{
    [TestCase(nameof(IBPlusInternalGrain.RouteWithMetadataAsync))]
    [TestCase(nameof(IBPlusInternalGrain.GetRoutingTableAsync))]
    [TestCase(nameof(IBPlusInternalGrain.GetLeftmostChildAsync))]
    [TestCase(nameof(IBPlusInternalGrain.GetRightmostChildAsync))]
    [TestCase(nameof(IBPlusInternalGrain.GetLeftmostChildWithMetadataAsync))]
    [TestCase(nameof(IBPlusInternalGrain.GetRightmostChildWithMetadataAsync))]
    [TestCase(nameof(IBPlusInternalGrain.AreChildrenLeavesAsync))]
    [TestCase(nameof(IBPlusInternalGrain.AcceptSplitAsync))]
    public void Routing_method_is_marked_AlwaysInterleave(string methodName)
    {
        var methods = typeof(IBPlusInternalGrain)
            .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
            .Where(m => m.Name == methodName)
            .ToArray();

        Assert.That(methods, Is.Not.Empty,
            $"Expected to find method '{methodName}' on IBPlusInternalGrain.");

        foreach (var method in methods)
        {
            var attr = method.GetCustomAttribute<AlwaysInterleaveAttribute>(inherit: false);
            Assert.That(attr, Is.Not.Null,
                $"IBPlusInternalGrain.{methodName}({string.Join(", ", method.GetParameters().Select(p => p.ParameterType.Name))}) " +
                "MUST be annotated [AlwaysInterleave] per U9p step 8c-c-iv-c2-vi. Removing the attribute " +
                "reintroduces the per-internal-node serial-turn ceiling that c2-vi removed..");
        }
    }
}
