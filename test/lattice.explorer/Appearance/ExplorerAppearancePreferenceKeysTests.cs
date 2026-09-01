using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.UI.Appearance;

namespace Orleans.Lattice.Explorer.Tests.Appearance;

/// <summary>
/// The appearance feature's contribution to the preference contract: three keys,
/// scoped to the person rather than to the cluster.
/// </summary>
[TestFixture]
public sealed class ExplorerAppearancePreferenceKeysTests
{
    [Test]
    public void All_lists_exactly_the_declared_keys()
    {
        Assert.That(
            ExplorerAppearancePreferenceKeys.All,
            Is.EquivalentTo(new[]
            {
                ExplorerAppearancePreferenceKeys.Theme,
                ExplorerAppearancePreferenceKeys.Contrast,
                ExplorerAppearancePreferenceKeys.Density,
            }));
    }

    [Test]
    public void Every_key_is_a_single_shared_instance()
    {
        // Keys are compared by reference, so a property returning a fresh
        // instance per read would make every registration a duplicate-name
        // failure and every read an unregistered-key failure.
        Assert.Multiple(() =>
        {
            Assert.That(
                ReferenceEquals(ExplorerAppearancePreferenceKeys.Theme, ExplorerAppearancePreferenceKeys.Theme),
                Is.True);
            Assert.That(
                ReferenceEquals(ExplorerAppearancePreferenceKeys.Contrast, ExplorerAppearancePreferenceKeys.Contrast),
                Is.True);
            Assert.That(
                ReferenceEquals(ExplorerAppearancePreferenceKeys.Density, ExplorerAppearancePreferenceKeys.Density),
                Is.True);
        });
    }

    [Test]
    public void Every_key_follows_the_operator_rather_than_the_cluster()
    {
        // A palette is a property of the person and the room they are sitting in.
        // Pointing the Explorer at another cluster must not throw somebody back
        // into a palette they cannot read.
        Assert.Multiple(() =>
        {
            foreach (var key in ExplorerAppearancePreferenceKeys.All)
            {
                Assert.That(key.Scope, Is.EqualTo(ExplorerPreferenceScope.User), key.Name);
            }
        });
    }

    [Test]
    public void Every_key_name_is_canonical_lower_case()
    {
        // The shell has one spelling convention across URLs and stored state, and
        // one hygiene assertion guards both.
        Assert.Multiple(() =>
        {
            foreach (var key in ExplorerAppearancePreferenceKeys.All)
            {
                Assert.That(ExplorerRouteSlug.IsCanonical(key.Name), Is.True, key.Name);
                Assert.That(key.Name, Does.StartWith("appearance."));
            }
        });
    }

    [Test]
    public void Every_key_describes_itself_from_the_operators_point_of_view()
    {
        // The description is used verbatim mid-sentence when the shell has to say
        // a remembered value could not be used, and it is what the reset-view page
        // lists, so it has to read as a noun phrase.
        Assert.Multiple(() =>
        {
            foreach (var key in ExplorerAppearancePreferenceKeys.All)
            {
                Assert.That(key.Description, Is.Not.Empty);
                Assert.That(key.Description, Does.StartWith("the "));
            }
        });
    }

    [Test]
    public void The_keys_do_not_collide_with_the_shells_own()
    {
        var catalog = new ExplorerPreferenceCatalog();

        Assert.That(
            () =>
            {
                foreach (var key in ExplorerAppearancePreferenceKeys.All)
                {
                    catalog.Register(key);
                }
            },
            Throws.Nothing);
    }
}
