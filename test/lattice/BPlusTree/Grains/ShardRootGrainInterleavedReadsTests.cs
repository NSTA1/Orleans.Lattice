using System.Reflection;
using Orleans.Concurrency;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the U9h-C interface contract: the pure read methods on
/// <see cref="IShardRootGrain"/> must be annotated
/// <see cref="AlwaysInterleaveAttribute"/> so concurrent reads on the
/// same shard activation pipeline alongside in-flight interleaved
/// <c>SetManyAsync</c> turns rather than queueing behind them.
/// <para>
/// This is a contract assertion, not a behavioural one: the
/// <see cref="AlwaysInterleaveAttribute"/> is enforced by the Orleans
/// runtime scheduler, which is not part of the unit test environment.
/// A reflection-based assertion is the correct guard against a future
/// refactor silently stripping the attribute - the regression would
/// only otherwise be observable on a real cluster under load (the
/// scenario the U9h ladder was designed to detect).
/// </para>
/// </summary>
[TestFixture]
public sealed class ShardRootGrainInterleavedReadsTests
{
    [TestCase(nameof(IShardRootGrain.GetAsync))]
    [TestCase(nameof(IShardRootGrain.ExistsAsync))]
    [TestCase(nameof(IShardRootGrain.GetManyAsync))]
    public void Pure_read_method_is_marked_AlwaysInterleave(string methodName)
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
            Assert.That(attr, Is.Not.Null,
                $"IShardRootGrain.{methodName}({string.Join(", ", method.GetParameters().Select(p => p.ParameterType.Name))}) " +
                "must be annotated [AlwaysInterleave] - U9h-C contract.");
        }
    }
}
