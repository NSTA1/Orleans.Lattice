using System.Reflection;
using Orleans.Concurrency;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the post-U9h-C correction: the pure read methods on
/// <see cref="IShardRootGrain"/> must NOT be annotated
/// <see cref="AlwaysInterleaveAttribute"/>. The original U9h-C ship
/// marked <c>GetAsync</c>, <c>ExistsAsync</c>, and <c>GetManyAsync</c>
/// <c>[AlwaysInterleave]</c> on the assumption that reads were safe to
/// pipeline alongside in-flight <c>SetManyAsync</c> turns because they
/// only read shard-root state. That assumption was wrong: the read
/// path performs multiple non-atomic reads of
/// <c>state.State.RootNodeId</c>, <c>state.State.RootIsLeaf</c>, and
/// <c>state.State.MovedAwaySlots</c> across awaits, and an interleaved
/// promotion or move-away publish landing between those reads
/// surfaces as a null return for a key the chaos-reshard invariant
/// guarantees must be present. The
/// <c>Chaos_reshard_under_concurrent_load_preserves_all_data</c>
/// suite caught the regression as "key missing mid-chaos" violations.
/// <para>
/// This is a contract assertion: the attribute is enforced by the
/// Orleans runtime scheduler, which is not part of the unit test
/// environment. The reflection-based assertion guards against any
/// future refactor silently reintroducing the attribute; that
/// regression would otherwise only be observable on a real cluster
/// under live load.
/// </para>
/// </summary>
[TestFixture]
public sealed class ShardRootGrainInterleavedReadsTests
{
    [TestCase(nameof(IShardRootGrain.GetAsync))]
    [TestCase(nameof(IShardRootGrain.ExistsAsync))]
    [TestCase(nameof(IShardRootGrain.GetManyAsync))]
    public void Pure_read_method_is_not_marked_AlwaysInterleave(string methodName)
    {
        var methods = typeof(IShardRootGrain)
            .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
            .Where(m => m.Name == methodName)
            .ToArray();

        Assert.That(methods, Is.Not.Empty,
            $"Expected to find method '{methodName}' on IShardRootGrain.");

        foreach (var method in methods)
        {
            var attr = method.GetCustomAttribute<AlwaysInterleaveAttribute>(inherit: false);
            Assert.That(attr, Is.Null,
                $"IShardRootGrain.{methodName}({string.Join(", ", method.GetParameters().Select(p => p.ParameterType.Name))}) " +
                "must NOT be annotated [AlwaysInterleave]. Interleaving the read path against in-flight " +
                "SetManyAsync turns races shard-root routing state and reintroduces the chaos-reshard " +
                "'key missing mid-chaos' regression.");
        }
    }
}

