using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Tests.Session;

/// <summary>
/// The declared preference key: its canonical-name rule, its scope, and the
/// description the shell puts in a fallback message.
/// </summary>
[TestFixture]
public sealed class ExplorerPreferenceKeyTests
{
    [Test]
    public void Constructor_DefaultsToTheUserAndClusterScope()
    {
        var key = new ExplorerPreferenceKey("feature.thing", "the thing you chose");

        Assert.Multiple(() =>
        {
            Assert.That(key.Name, Is.EqualTo("feature.thing"));
            Assert.That(key.Description, Is.EqualTo("the thing you chose"));
            Assert.That(key.Scope, Is.EqualTo(ExplorerPreferenceScope.UserAndCluster));
        });
    }

    [Test]
    public void Constructor_UserScope_IsKept()
    {
        var key = new ExplorerPreferenceKey("feature.theme", "your theme", ExplorerPreferenceScope.User);

        Assert.That(key.Scope, Is.EqualTo(ExplorerPreferenceScope.User));
    }

    [Test]
    public void Constructor_UpperCaseName_Throws()
    {
        Assert.That(
            () => new ExplorerPreferenceKey("Feature.Thing", "the thing"),
            Throws.ArgumentException);
    }

    [Test]
    public void Constructor_NameWithASlash_Throws()
    {
        Assert.That(() => new ExplorerPreferenceKey("a/b", "the thing"), Throws.ArgumentException);
    }

    [Test]
    public void Constructor_EmptyName_Throws()
    {
        Assert.That(() => new ExplorerPreferenceKey(string.Empty, "the thing"), Throws.ArgumentException);
    }

    [Test]
    public void Constructor_EmptyDescription_Throws()
    {
        Assert.That(() => new ExplorerPreferenceKey("feature.thing", string.Empty), Throws.ArgumentException);
    }

    [Test]
    public void Constructor_NullDescription_Throws()
    {
        Assert.That(() => new ExplorerPreferenceKey("feature.thing", null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ToString_IsTheName()
    {
        Assert.That(new ExplorerPreferenceKey("feature.thing", "the thing").ToString(),
            Is.EqualTo("feature.thing"));
    }

    [Test]
    public void Keys_AreComparedByReference()
    {
        var left = new ExplorerPreferenceKey("feature.thing", "the thing");
        var right = new ExplorerPreferenceKey("feature.thing", "the thing");

        Assert.That(left, Is.Not.EqualTo(right));
    }

    [Test]
    public void DeclaredShellKeys_AreAllCanonicalAndDescribed()
    {
        Assert.Multiple(() =>
        {
            foreach (var key in ExplorerPreferenceKeys.All)
            {
                Assert.That(
                    Orleans.Lattice.Explorer.Core.Navigation.ExplorerRouteSlug.IsCanonical(key.Name),
                    Is.True,
                    $"'{key.Name}' must be canonical lower case");
                Assert.That(key.Description, Is.Not.Empty, $"'{key.Name}' must describe itself");
            }
        });
    }

    [Test]
    public void DeclaredShellKeys_AreAllScopedPerUserAndCluster()
    {
        // Every shell key names something that lives in a cluster, so none of
        // them may leak across an account or a cluster switch.
        Assert.That(
            ExplorerPreferenceKeys.All.Select(static k => k.Scope),
            Is.All.EqualTo(ExplorerPreferenceScope.UserAndCluster));
    }

    [Test]
    public void DeclaredShellKeys_HaveDistinctNames()
    {
        Assert.That(
            ExplorerPreferenceKeys.All.Select(static k => k.Name).Distinct(StringComparer.Ordinal).Count(),
            Is.EqualTo(ExplorerPreferenceKeys.All.Count));
    }
}
