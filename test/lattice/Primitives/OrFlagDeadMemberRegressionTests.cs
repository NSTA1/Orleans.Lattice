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
        var declared = typeof(OrFlag)
            .GetMembers(BindingFlags.NonPublic | BindingFlags.Public
                | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly);

        // The claim is an absence, so the lookup needs a positive control: a
        // BindingFlags drift that returned no member at all would satisfy the
        // absence assertion while proving nothing about the removed helper.
        Assert.Multiple(() =>
        {
            Assert.That(declared, Is.Not.Empty,
                "the member lookup must see OrFlag's own members, or the absence check below "
                + "passes for the wrong reason.");
            Assert.That(declared.Where(m => m.Name == "LiveEnableCount"), Is.Empty);
        });
    }
}
