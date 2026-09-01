using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// The shell's area visibility policy: one access decision in, one presentation
/// out.
/// </summary>
/// <remarks>
/// A pure function, so this fixture is where the epic's agreed policy is pinned
/// once and the rail's own tests can be about rendering rather than about
/// re-deriving the rule.
/// </remarks>
[TestFixture]
public sealed class ExplorerAreaVisibilityPolicyTests
{
    [Test]
    public void An_allowed_area_is_offered_in_the_rail()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                ExplorerAreaVisibilityPolicy.Decide(ExplorerPluginAccessState.Allowed, hideInaccessible: false),
                Is.EqualTo(ExplorerAreaEntryPresentation.Primary));
            Assert.That(
                ExplorerAreaVisibilityPolicy.Decide(ExplorerPluginAccessState.Allowed, hideInaccessible: true),
                Is.EqualTo(ExplorerAreaEntryPresentation.Primary),
                "the preference is about what you cannot use, not about what you can");
        });
    }

    [Test]
    public void An_authentication_required_area_stays_prominent_whatever_the_preference_says()
    {
        // An invitation outranks the preference: hiding it would hide the remedy
        // along with the refusal, and there is nothing to be granted - the caller
        // only has to sign in.
        Assert.Multiple(() =>
        {
            Assert.That(
                ExplorerAreaVisibilityPolicy.Decide(
                    ExplorerPluginAccessState.AuthenticationRequired,
                    hideInaccessible: false),
                Is.EqualTo(ExplorerAreaEntryPresentation.Primary));
            Assert.That(
                ExplorerAreaVisibilityPolicy.Decide(
                    ExplorerPluginAccessState.AuthenticationRequired,
                    hideInaccessible: true),
                Is.EqualTo(ExplorerAreaEntryPresentation.Primary));
        });
    }

    [Test]
    public void A_denied_area_is_demoted_by_default_and_hidden_only_on_request()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                ExplorerAreaVisibilityPolicy.Decide(ExplorerPluginAccessState.Denied, hideInaccessible: false),
                Is.EqualTo(ExplorerAreaEntryPresentation.Demoted),
                "a caller who cannot see that an area exists cannot ask to be granted it");
            Assert.That(
                ExplorerAreaVisibilityPolicy.Decide(ExplorerPluginAccessState.Denied, hideInaccessible: true),
                Is.EqualTo(ExplorerAreaEntryPresentation.Hidden));
        });
    }

    [Test]
    public void An_unavailable_area_is_hidden_because_there_is_nothing_to_be_granted()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                ExplorerAreaVisibilityPolicy.Decide(ExplorerPluginAccessState.Unavailable, hideInaccessible: false),
                Is.EqualTo(ExplorerAreaEntryPresentation.Hidden));
            Assert.That(
                ExplorerAreaVisibilityPolicy.Decide(ExplorerPluginAccessState.Unavailable, hideInaccessible: true),
                Is.EqualTo(ExplorerAreaEntryPresentation.Hidden));
        });
    }

    [Test]
    public void An_unrecognised_state_is_withheld_rather_than_offered()
    {
        // Fails closed. A cast integer is the only way to reach this, and the
        // answer must not be "show it anyway".
        Assert.That(
            ExplorerAreaVisibilityPolicy.Decide((ExplorerPluginAccessState)99, hideInaccessible: false),
            Is.EqualTo(ExplorerAreaEntryPresentation.Hidden));
    }

    [Test]
    public void The_default_presentation_is_hidden_so_an_unset_value_offers_nothing()
    {
        Assert.That(default(ExplorerAreaEntryPresentation), Is.EqualTo(ExplorerAreaEntryPresentation.Hidden));
    }

    [Test]
    public void Only_allowed_and_authentication_required_do_anything_when_activated()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerAreaVisibilityPolicy.IsActivable(ExplorerPluginAccessState.Allowed), Is.True);
            Assert.That(
                ExplorerAreaVisibilityPolicy.IsActivable(ExplorerPluginAccessState.AuthenticationRequired),
                Is.True,
                "activating it offers the sign-in that would open it");
            Assert.That(ExplorerAreaVisibilityPolicy.IsActivable(ExplorerPluginAccessState.Denied), Is.False);
            Assert.That(ExplorerAreaVisibilityPolicy.IsActivable(ExplorerPluginAccessState.Unavailable), Is.False);
            Assert.That(ExplorerAreaVisibilityPolicy.IsActivable((ExplorerPluginAccessState)99), Is.False);
        });
    }

    [Test]
    public void Only_unavailable_is_an_absence_the_capabilities_affordance_explains()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                ExplorerAreaVisibilityPolicy.IsUnavailableOnCluster(ExplorerPluginAccessState.Unavailable),
                Is.True);
            Assert.That(
                ExplorerAreaVisibilityPolicy.IsUnavailableOnCluster(ExplorerPluginAccessState.Denied),
                Is.False,
                "a refusal is about the caller, not about the cluster");
            Assert.That(
                ExplorerAreaVisibilityPolicy.IsUnavailableOnCluster(ExplorerPluginAccessState.Allowed),
                Is.False);
            Assert.That(
                ExplorerAreaVisibilityPolicy.IsUnavailableOnCluster(
                    ExplorerPluginAccessState.AuthenticationRequired),
                Is.False);
        });
    }
}
