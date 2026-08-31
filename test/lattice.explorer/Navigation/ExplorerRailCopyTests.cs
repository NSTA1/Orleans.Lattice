using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// The few strings the rail owns, and the boundary that keeps it from owning
/// any more than that.
/// </summary>
/// <remarks>
/// The point of this fixture is as much what it does <em>not</em> contain: no
/// denial sentence, no sign-in prompt and no remedy, because those come from
/// <see cref="ExplorerAccessCopy"/> and from the gate. A second copy layer for
/// refusals is the drift the epic's vocabulary work exists to remove.
/// </remarks>
[TestFixture]
public sealed class ExplorerRailCopyTests
{
    [Test]
    public void The_missing_areas_affordance_names_what_the_cluster_does_not_run()
    {
        var message = ExplorerRailCopy.MissingAreas(["Telemetry", "Backups"]);

        Assert.Multiple(() =>
        {
            Assert.That(
                message.Explanation,
                Does.Contain("Not installed on this cluster: Telemetry, Backups."),
                "a capability name is safe to reveal - the gate is advisory and the server enforces");
            Assert.That(message.Kind, Is.EqualTo(ExplorerStateKind.Unavailable));
            Assert.That(
                message.IsDenial,
                Is.False,
                "nothing is being withheld from the caller; the cluster simply does not run it");
        });
    }

    [Test]
    public void The_missing_areas_remedy_is_the_shared_one_rather_than_a_second_wording()
    {
        var message = ExplorerRailCopy.MissingAreas(["Telemetry"]);
        var shared = ExplorerAccessCopy.Unavailable("Telemetry");

        Assert.Multiple(() =>
        {
            Assert.That(message.Remedy, Is.EqualTo(shared.Remedy));
            Assert.That(message.RemedyLabel, Is.EqualTo(shared.RemedyLabel));
            Assert.That(message.RemedyLabel, Is.EqualTo(ExplorerVocabulary.RemedyLabel));
        });
    }

    [Test]
    public void The_missing_areas_affordance_still_answers_its_question_when_nothing_is_missing()
    {
        var message = ExplorerRailCopy.MissingAreas([]);

        Assert.Multiple(() =>
        {
            Assert.That(message.Explanation, Does.Contain("Every area this cluster has is listed."));
            Assert.That(message.Explanation, Does.Not.Contain("Not installed"));
            Assert.That(message.Remedy, Is.Null, "there is nothing to do about nothing being missing");
        });
    }

    [Test]
    public void The_missing_areas_affordance_rejects_a_null_set()
    {
        Assert.That(() => ExplorerRailCopy.MissingAreas(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void An_unhonoured_address_says_which_area_it_asked_for_and_where_it_landed()
    {
        Assert.That(
            ExplorerRailCopy.UnreachableAddressNotice("Backups"),
            Is.EqualTo(
                "This address asks for Backups, which your account cannot open, "
                + "so the Explore surface is shown instead."));
    }

    [Test]
    public void An_unhonoured_address_notice_rejects_a_null_area()
    {
        Assert.That(
            () => ExplorerRailCopy.UnreachableAddressNotice(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void The_rail_owns_only_the_strings_that_have_no_home_in_the_shared_vocabulary()
    {
        // Each of these names a grouping or a control that exists nowhere else
        // in the product, which is the whole test for whether the rail may own a
        // string at all.
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerRailCopy.DemotedGroupLabel, Is.Not.Empty);
            Assert.That(ExplorerRailCopy.MissingAreasTerm, Is.Not.Empty);
            Assert.That(ExplorerRailCopy.MissingAreasTriggerText, Is.Not.Empty);
            Assert.That(ExplorerRailCopy.HideInaccessibleLabel, Is.Not.Empty);
        });
    }
}
