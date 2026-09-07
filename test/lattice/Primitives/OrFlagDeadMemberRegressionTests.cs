using System.Linq;
using System.Reflection;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Regression guard for a dead-code defect in <see cref="OrFlag"/>. The private
/// <c>LiveEnableCount</c> helper had no call sites - <see cref="OrFlag.IsEnabled"/>
/// uses the cheaper short-circuiting live check, not a full count - so it was
/// unreachable code that only advertised a compaction cost the flag never paid.
/// This fixture pins the removal so the helper cannot silently return.
/// </summary>
[TestFixture]
public class OrFlagDeadMemberRegressionTests
{
    [Test]
    public void OrFlag_HasNoLiveEnableCountMember()
    {
        var members = typeof(OrFlag)
            .GetMembers(BindingFlags.NonPublic | BindingFlags.Public
                | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Where(m => m.Name == "LiveEnableCount");

        Assert.That(members, Is.Empty);
    }
}
