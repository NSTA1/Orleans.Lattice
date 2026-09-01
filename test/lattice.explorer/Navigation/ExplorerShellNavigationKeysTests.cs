using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// The preference key the rail declares, over and above the route-shaped keys
/// the shell already had.
/// </summary>
[TestFixture]
public sealed class ExplorerShellNavigationKeysTests
{
    [Test]
    public void The_hide_preference_follows_the_operator_rather_than_the_cluster()
    {
        // It records how much of the product you want to see, which is the same
        // answer wherever you point the Explorer - unlike every key in
        // ExplorerPreferenceKeys, each of which names something inside a cluster.
        Assert.That(
            ExplorerShellNavigationKeys.HideInaccessibleAreas.Scope,
            Is.EqualTo(ExplorerPreferenceScope.User));
    }

    [Test]
    public void The_hide_preference_is_declared_in_the_shells_own_namespace_and_lower_case()
    {
        var name = ExplorerShellNavigationKeys.HideInaccessibleAreas.Name;

        Assert.Multiple(() =>
        {
            Assert.That(name, Is.EqualTo("shell.hide-inaccessible"));
            Assert.That(
                ExplorerRouteSlug.IsCanonical(name),
                Is.True,
                "a key name is held to the same lower-case rule as a route segment");
        });
    }

    [Test]
    public void The_hide_preference_describes_itself_as_a_noun_phrase()
    {
        // The description is used verbatim mid-sentence when a remembered value
        // no longer resolves, so it has to read as one.
        Assert.That(
            ExplorerShellNavigationKeys.HideInaccessibleAreas.Description,
            Is.EqualTo("whether areas you cannot open are hidden"));
    }

    [Test]
    public void The_key_is_one_shared_instance_so_registering_it_twice_is_a_no_op()
    {
        // Keys are compared by reference, so a second declaration with the same
        // name would throw rather than merge.
        var catalog = new ExplorerPreferenceCatalog();

        var first = catalog.Register(ExplorerShellNavigationKeys.HideInaccessibleAreas);
        var second = catalog.Register(ExplorerShellNavigationKeys.HideInaccessibleAreas);

        Assert.That(second, Is.SameAs(first));
    }
}
