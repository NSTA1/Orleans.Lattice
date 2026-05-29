using System.Reflection;
using Orleans.Concurrency;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins U9p step 8c-c-iv-c2-iii: the mutation surface on
/// <see cref="IBPlusLeafGrain"/> must be annotated
/// <see cref="AlwaysInterleaveAttribute"/> so multiple producer turns
/// can run concurrently on the same leaf activation. Without the
/// attribute, every <c>SetAsync</c> / <c>SetManyAsync</c> /
/// <c>DeleteAsync</c> caller queues behind the prior call's full
/// commit (HLC tick + WAL append + apply + observer + digest), which
/// is the binding ~1.8 s in-leaf queue residual measured at the
/// c2-ii baseline.
/// <para>
/// Safety argument (recorded in the c2-iii pre-implementation re-read
///): Orleans serialises synchronous code between
/// awaits regardless of <see cref="AlwaysInterleaveAttribute"/>, so the
/// per-key LWW merge into <c>Cache</c>, the HLC tick, the
/// <c>PublishVersionAdvance</c> / <c>BumpLocalRevision</c> bumps, and
/// the <c>_digestDirty</c> flag-flip are all race-free under interleave
/// (the operations between awaits run atomically; LWW merge is
/// convergent across orderings). The single remaining hazard - two
/// interleaved turns both observing <c>Cache.Count &gt; MaxLeafKeys</c>
/// and both entering the split state machine - is serialised by the
/// per-activation <c>_splitGate</c> in <c>BPlusLeafGrain.Metrics.cs</c>
/// with a re-check inside the gate.
/// </para>
/// <para>
/// This is a contract assertion: the attribute is enforced by the
/// Orleans runtime scheduler, which is not part of the unit test
/// environment. The reflection-based assertion guards against any
/// future refactor silently stripping the attribute; that regression
/// would otherwise only be observable on a real cluster under live
/// load (as a regression in <c>lattice.set_many.duration_ms</c> P99
/// and a collapse of <c>leaf.commit.in_flight</c> back to 0).
/// </para>
/// </summary>
[TestFixture]
public sealed class IBPlusLeafGrainInterleavedWritesTests
{
    [TestCase(nameof(IBPlusLeafGrain.SetAsync))]
    [TestCase(nameof(IBPlusLeafGrain.SetManyAsync))]
    [TestCase(nameof(IBPlusLeafGrain.DeleteAsync))]
    public void Mutation_method_is_marked_AlwaysInterleave(string methodName)
    {
        var methods = typeof(IBPlusLeafGrain)
            .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
            .Where(m => m.Name == methodName)
            .ToArray();

        Assert.That(methods, Is.Not.Empty,
            $"Expected to find method '{methodName}' on IBPlusLeafGrain.");

        foreach (var method in methods)
        {
            var attr = method.GetCustomAttribute<AlwaysInterleaveAttribute>(inherit: false);
            Assert.That(attr, Is.Not.Null,
                $"IBPlusLeafGrain.{methodName}({string.Join(", ", method.GetParameters().Select(p => p.ParameterType.Name))}) " +
                "MUST be annotated [AlwaysInterleave] per U9p step 8c-c-iv-c2-iii. Removing the attribute " +
                "reintroduces the ~1.8 s per-leaf queue residual measured at the c2-ii baseline and " +
                "regresses lattice.set_many.duration_ms P99.");
        }
    }
}
