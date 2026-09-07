using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression guard for issue #2112. <see cref="ShardRootGrain"/> carried two
/// behaviourally identical private <c>ThrowIfRejectedForAnyKey</c> overloads -
/// one taking <see cref="IEnumerable{T}"/> of <see cref="string"/> and one
/// taking the entry list. Every call site bound the entry-list overload, so
/// the <see cref="IEnumerable{T}"/> variant was unreachable dead code that
/// could silently drift out of step with the live gate. This fixture pins the
/// removal: only the entry-list overload may exist, so the dead one cannot be
/// reintroduced without failing here.
/// </summary>
[TestFixture]
public class ShardRootGrainDeadOverloadRegressionTests
{
    private static IReadOnlyList<MethodInfo> RejectGateOverloads() =>
        typeof(ShardRootGrain)
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(m => m.Name == "ThrowIfRejectedForAnyKey")
            .ToList();

    [Test]
    public void ThrowIfRejectedForAnyKey_HasNoEnumerableOfStringOverload()
    {
        var firstParameterTypes = RejectGateOverloads()
            .Select(m => m.GetParameters()[0].ParameterType);

        Assert.That(firstParameterTypes, Has.None.EqualTo(typeof(IEnumerable<string>)));
    }

    [Test]
    public void ThrowIfRejectedForAnyKey_HasExactlyOneOverload()
    {
        Assert.That(RejectGateOverloads(), Has.Count.EqualTo(1));
    }
}
